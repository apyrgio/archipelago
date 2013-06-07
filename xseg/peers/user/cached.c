/*
 * Copyright 2013 GRNET S.A. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 *   1. Redistributions of source code must retain the above
 *      copyright notice, this list of conditions and the following
 *      disclaimer.
 *   2. Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following
 *      disclaimer in the documentation and/or other materials
 *      provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY GRNET S.A. ``AS IS'' AND ANY EXPRESS
 * OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL GRNET S.A OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * The views and conclusions contained in the software and
 * documentation are those of the authors and should not be
 * interpreted as representing official policies, either expressed
 * or implied, of GRNET S.A.
 */

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <pthread.h>
#include <xseg/xseg.h>
#include <peer.h>
#include <time.h>
#include <xtypes/xlock.h>
#include <xtypes/xq.h>
#include <xtypes/xhash.h>
#include <xtypes/xworkq.h>
#include <xtypes/xwaitq.h>
#include <xseg/protocol.h>
#include <xtypes/xcache.h>

#define MAX_ARG_LEN 12

/* bucket statuses
 *
 * Allocation status occupies 1st-2nd flag bit.
 * Data status occupied 2nd-4th flag bit.
 */

#define BUCKET_ALLOC_STATUSES 2
#define BUCKET_ALLOC_STATUS_FLAG_POS 0
#define BUCKET_ALLOC_STATUS_BITMASK 1
#define FREE 0
#define CLAIMED   1

#define BUCKET_DATA_STATUSES 5
#define BUCKET_DATA_STATUS_FLAG_POS 1
#define BUCKET_DATA_STATUS_BITMASK 3
#define INVALID   0
#define LOADING   1
#define VALID     2
#define DIRTY     3
#define WRITING   4

/*
 * Find position of flag, make it zero, get requested flag value, store it to
 * this position
 */
#define SET_FLAG(__ftype, __flag, __val)	\
	__flag = (__flag & ~((uint32_t)__ftype##_BITMASK << __ftype##_FLAG_POS)) | \
	((uint32_t)__val << __ftype##_FLAG_POS);

/* Apply bitmask to flags, shift result to the right to get correct value */
#define GET_FLAG(__ftype, __flag)			\
	(__flag & ((uint64_t)__ftype##_BITMASK << __ftype##_FLAG_POS)) >> \
	(uint32_t)__ftype##_FLAG_POS

/* write policies */
#define WRITETHROUGH 1
#define WRITEBACK    2

#define WRITE_POLICY(__wcp)				\
	(__wcp == WRITETHROUGH	? "writethrough" :	\
	__wcp == WRITEBACK	? "writeback"	 :	\
	"undefined")

/* cio states */
#define CIO_FAILED		1
#define CIO_ACCEPTED		2
#define CIO_READING		3
#define CIO_WRITING		4
#define CIO_SERVED		5

/* ce states */
#define CE_READY		1
#define CE_WRITING		2
#define CE_FLUSHING		3
#define CE_DELETING		4
#define CE_INVALIDATED		5
#define CE_FAILED		6

#define BUCKET_SIZE_QUANTUM 4096

struct cache_io {
	uint32_t state;
	xcache_handler h;
	uint32_t pending_reqs;
	struct work work;
};

struct cached {
	struct xcache *cache;
	uint64_t cache_size; /*Number of objects*/
	uint64_t max_req_size;
	uint32_t object_size; /*Bytes*/
	uint32_t bucket_size; /*In bytes*/
	uint32_t buckets_per_object;
	xport bportno;
	int write_policy;
	struct xworkq workq;
	struct xwaitq pending_waitq;
	unsigned char *bucket_data;
	struct xq bucket_idx;
	//scheduler
};

struct bucket {
	xqindex data;
	uint32_t flags;
};

struct ce {
	uint32_t status;		/* cache entry status */
	uint32_t *bucket_alloc_status_counters;
	uint32_t *bucket_data_status_counters;
	struct bucket *buckets;
	struct xlock lock;		/* cache entry lock */
	struct xworkq workq;		/* workq of the cache entry */
	struct xworkq deferred_workq;		/* async workq for TODO */
	struct peer_req pr;
};

struct req_completion{
	struct peer_req *pr;
	struct xseg_request *req;
};

/*
 * Helper functions
 */

#define MIN(__a__, __b__) ((__a__ < __b__) ? __a__ : __b__)

static inline struct cached * __get_cached(struct peerd *peer)
{
	return (struct cached *) peer->priv;
}

static inline struct cache_io * __get_cache_io(struct peer_req *pr)
{
	return (struct cache_io *) pr->priv;
}

static inline uint32_t __calculate_size(struct cached *cached,
		uint32_t start, uint32_t end)
{
	return (end - start + 1) * cached->bucket_size;
}

static inline uint32_t __calculate_offset(struct cached *cached,
		uint32_t start)
{
	return start * cached->bucket_size;
}

static inline uint64_t __quantize(uint64_t num, uint32_t quantum)
{
	quantum--;
	return num & (uint64_t)(~quantum);
}

static inline int __is_handler_valid(xcache_handler h)
{
	return h != NoEntry;
}

/* Bucket specific operations */
static inline unsigned char *__get_bucket_data(struct bucket *b)
{
	return (unsigned char *)b->data;
}

static inline int __get_bucket_alloc_status(struct bucket *b)
{
	return GET_FLAG(BUCKET_ALLOC_STATUS, b->flags);
}

static inline int __get_bucket_data_status(struct bucket *b)
{
	return GET_FLAG(BUCKET_DATA_STATUS, b->flags);
}

static inline void __set_bucket_alloc_status(struct ce *ce,
		struct bucket *b, int new_status)
{
	int old_status = __get_bucket_alloc_status(b);

	ce->bucket_alloc_status_counters[old_status]--;
	ce->bucket_alloc_status_counters[new_status]++;
	SET_FLAG(BUCKET_ALLOC_STATUS, b->flags, new_status);
}

static inline void __set_bucket_data_status(struct ce *ce,
		struct bucket *b, int new_status)
{
	int old_status = __get_bucket_data_status(b);

	ce->bucket_data_status_counters[old_status]--;
	ce->bucket_data_status_counters[new_status]++;
	SET_FLAG(BUCKET_DATA_STATUS, b->flags, new_status);
}

static inline void __set_bucket_data_status_range(struct ce *ce,
		uint32_t start_bucket, uint32_t end_bucket, int new_status)
{
	struct bucket *b;
	uint32_t i;

	for (i = start_bucket; i <= end_bucket; i++) {
		b = &ce->buckets[i];
		__set_bucket_data_status(ce, b, new_status);
	}
}

static inline int __is_bucket_readable(struct bucket *b)
{
	int bucket_status = __get_bucket_data_status(b);
	return (bucket_status == VALID ||
		bucket_status == DIRTY ||
		bucket_status == WRITING);
}

static inline int __is_bucket_claimed(struct bucket *b)
{
	return __get_bucket_alloc_status(b) == CLAIMED;
}

#if 0
static inline void __update_bucket_status_counters(struct ce *ce,
		uint32_t bucket, uint32_t new_status)
{
	uint32_t old_status = ce->bucket_status[bucket];

	ce->bucket_status_counters[old_status]--;
	ce->bucket_status_counters[new_status]++;
}
#endif

static uint32_t __get_bucket(struct cached *cache, uint64_t offset)
{
	return (offset / cache->bucket_size);
}

static uint32_t __get_last_per_status(struct ce *ce, uint32_t start,
			uint32_t limit, uint32_t status)
{
	struct peerd *peer = ce->pr.peer;
	struct cached *cached = __get_cached(peer);
	struct bucket *b;
	uint32_t end = start + 1;
	uint32_t upper_bound;

	upper_bound = start + (cached->max_req_size / cached->bucket_size) - 1;
	limit = upper_bound < limit ? upper_bound : limit;

	while (end <= limit) {
		b = &ce->buckets[end];
		if (__get_bucket_data_status(b) != status)
			break;
		end++;
	}

	return end - 1;
}

static uint32_t __get_last_invalid(struct ce *ce, uint32_t start, uint32_t limit)
{
	return __get_last_per_status(ce, start, limit, INVALID);
}

static uint32_t __get_last_dirty(struct ce *ce, uint32_t start, uint32_t limit)
{
	return __get_last_per_status(ce, start, limit, DIRTY);
}

static int claim_bucket(struct ce *ce, uint32_t bucket_no)
{
	struct peerd *peer = ce->pr.peer;
	struct cached *cached = __get_cached(peer);
	struct bucket *b = &ce->buckets[bucket_no];
	xqindex idx;

	idx = xq_pop_head(&cached->bucket_idx, 1);
	if (idx == Noneidx) {
		XSEGLOG2(&lc, E, "Could not claim bucket");
		return -1;
	}

	__set_bucket_alloc_status(ce, b, CLAIMED);
	b->data = cached->bucket_data + (idx * cached->bucket_size);

	return 0;
}

static int claim_bucket_range(struct ce *ce,
		uint32_t start_bucket, uint32_t end_bucket)
{
	struct bucket *b;
	uint32_t i;
	int status;
	int r = 0;

	for (i = start_bucket; i <= end_bucket; i++) {
		b = &ce->buckets[i];
		status = __get_bucket_alloc_status(b);

		if (status == CLAIMED)
			continue;

		r = claim_bucket(ce, i);
		if (r < 0) {
			/* FIXME: Enqueue work */
			XSEGLOG2(&lc, E, "Could not claim bucket %u for ce %p",
					i, ce);
			return r;
		}
	}

	return 0;
}

static void rw_bucket(struct bucket *b, int op, unsigned char *data,
		uint64_t offset, uint64_t size)
{
	unsigned char *to, *from;

	if (op == X_WRITE) {
		to = __get_bucket_data(b) + offset;
		from = data;
	} else if (op == X_READ) {
		to = data;
		from = __get_bucket_data(b) + offset;
	}
	memcpy(to, from, size);
}

static void rw_bucket_range(struct ce *ce, int op, unsigned char *data,
		uint64_t offset, uint64_t size)
{
	struct peerd *peer = ce->pr.peer;
	struct cached *cached = __get_cached(peer);
	struct bucket *b;
	uint32_t start_bucket, end_bucket;
	uint64_t bucket_offset, data_size;
	uint32_t i;

	start_bucket = __get_bucket(cached, offset);
	end_bucket = __get_bucket(cached, offset + size - 1);

	for (i = start_bucket; i <= end_bucket; i++) {
		b = &ce->buckets[i];
		/*
		 * 1. Calculate offset inside bucket
		 * 2. Do not write more than bucket size (data_size)
		 */
		bucket_offset = offset % cached->bucket_size;
		data_size = bucket_offset + size < cached->bucket_size ?
			size : cached->bucket_size - bucket_offset;

		rw_bucket(b, op, data, bucket_offset, data_size);

		size -= data_size;
		offset -= bucket_offset;
	}

	if (size > 0 || offset > 0)
		XSEGLOG2(&lc, E, "Read/write error");
}

static int cache_not_full(void *arg)
{
	struct cached *cached = (struct cached *)arg;
	return xcache_free_nodes(cached->cache) > 0;
}

__attribute__ ((unused))
static void __print_bc(uint32_t *bc) {
	XSEGLOG("Bucket statuses:\n"
			"Loading %u,\n"
			"Writing: %u,\n"
			"Dirty %u,\n"
			"Valid %u,\n"
			"Invalid %u",
			bc[LOADING], bc[WRITING], bc[DIRTY], bc[VALID], bc[INVALID]);
}

static int __is_entry_clean(struct cached *cached, struct ce *ce)
{
	uint32_t *bdc = ce->bucket_data_status_counters;
	struct cache_io *ce_cio = __get_cache_io(&ce->pr);

	if (cached->write_policy == WRITETHROUGH ||
			ce->status == CE_INVALIDATED ||
			bdc[DIRTY] == 0)
		return 1;

	if (ce->status == CE_FLUSHING || ce->status == CE_DELETING)
		XSEGLOG2(&lc, I, "ce %p has pending work to do (status: %lu, "
				"pending_reqs: %u)", ce, ce->status, ce_cio->pending_reqs);

	return 0;
}

/*
 * Do not respond to a request if the issuer is the ce itself. This happens on
 * implicit flushes i.e. flushing of dirty buckets before the final put
 */
static int __can_respond_request(struct peerd *peer, struct peer_req *pr)
{
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = pr->priv;
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);

	return pr != &ce->pr;
}

static void print_cached(struct cached *cached)
{
	if (!cached) {
		XSEGLOG2(&lc, W, "Struct cached is NULL\n");
		return;
	}

	XSEGLOG2(&lc, I, "Struct cached fields:\n"
			"                     cache        = %p\n"
			"                     cache_size   = %lu\n"
			"                     max_req_size = %lu\n"
			"                     object_size  = %lu\n"
			"                     bucket_size  = %lu\n"
			"                     bucks_per_obj= %lu\n"
			"                     Bportno      = %d\n"
			"                     write_policy = %s\n",
			cached->cache, cached->cache_size, cached->max_req_size,
			cached->object_size, cached->bucket_size,
			cached->buckets_per_object, cached->bportno,
			WRITE_POLICY(cached->write_policy));
}

int read_write_policy(char *write_policy)
{
	if (strcmp(write_policy, "writethrough") == 0)
		return WRITETHROUGH;
	if (strcmp(write_policy, "writeback") == 0)
		return WRITEBACK;
	return -1;
}

/*
 * Convert string to size in bytes.
 * If syntax is invalid, return 0. Values such as zero and non-integer
 * multiples of segment's page size should not be accepted.
 */
uint64_t str2num(char *str)
{
	char *unit;
	uint64_t num;

	num = strtoll(str, &unit, 10);
	if (strlen(unit) > 1) //Invalid syntax
		return 0;
	else if (strlen(unit) < 1) //Plain number in bytes
		return num;

	switch (*unit) {
		case 'g':
		case 'G':
			num *= 1024;
		case 'm':
		case 'M':
			num *= 1024;
		case 'k':
		case 'K':
			num *= 1024;
			break;
		default:
			num = 0;
	}
	return num;
}

/*
 * Signal a waitq.
 */
//WORK
static void signal_waitq(void *q, void *arg)
{
	struct xwaitq *waitq = (struct xwaitq *)arg;
	xwaitq_signal(waitq);
}

/*
 * Signal a workq.
 */
//WORK
static void signal_workq(void *q, void *arg)
{
	struct xworkq *workq = (struct xworkq *)arg;
	xworkq_signal(workq);
}

/*
 * serve_req is called only when all the requested buckets are readable.
 */
static int serve_req(struct peerd *peer, struct peer_req *pr)
{
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);
	struct xseg *xseg = peer->xseg;
	struct xseg_request *req = pr->req;
	unsigned char *req_data = (unsigned char *)xseg_get_data(xseg, req);

	XSEGLOG2(&lc, D, "Started\n");
	req->serviced = req->size;

	//assert req->serviced <= req->datalen
	//memcpy(req_data, ce->data + req->offset, req->serviced);
	rw_bucket_range(ce, X_READ, req_data, req->offset, req->size);
	XSEGLOG2(&lc, D, "Finished\n");

	return 0;
}

/*
 * Helper functions to fail/complete a cache io.
 * Instead of simply fail/complete the assocciated peer request, these functions
 * put the assocciated cache entry, if any.
 * FIXME: Why complete/fail the pr with cache entry lock held?
 */
static void cached_fail(struct peerd *peer, struct peer_req *pr)
{
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);

	XSEGLOG2(&lc, I, "Failing pr %p (h: %lu)", pr, cio->h);
	if (__can_respond_request(peer, pr))
		fail(peer, pr);

	if (__is_handler_valid(cio->h))
		xcache_put(cached->cache, cio->h);
}

static void cached_complete(struct peerd *peer, struct peer_req *pr)
{
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	xcache_handler h = cio->h;

	XSEGLOG2(&lc, I, "Completing pr %p (h: %lu)", pr, h);
	if (__can_respond_request(peer, pr))
		complete(peer, pr);

	if (__is_handler_valid(h))
		xcache_put(cached->cache, h);
}

static void cached_fake_complete(struct peerd *peer, struct peer_req *pr)
{
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct xseg_request *req = pr->req;
	char *req_data;

	XSEGLOG2(&lc, I, "Fake-completing pr %p (h: %lu)", pr, cio->h);
	if (req->op == X_READ) {
		req_data = xseg_get_data(peer->xseg, req);
		memset(req_data, 0, req->datalen);
	}
	if (__can_respond_request(peer, pr))
		complete(peer, pr);

	if (__is_handler_valid(cio->h))
		xcache_put(cached->cache, cio->h);
}

/*
 * rw_range handles the issuing of requests to the blocker. Usually called when
 * we need to read(write) data from(to) slower media.
 *
 * read/write an object range in buckets.
 *
 * Associate the request with the given pr.
 *
 */

/* FIXME: rw_range must use the new way of data */
static int rw_range(struct peerd *peer, struct peer_req *pr, uint32_t op,
		uint32_t start, uint32_t end)
{
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct xseg_request *req;
	struct xseg *xseg = peer->xseg;
	xport srcport = pr->portno;
	xport dstport = cached->bportno;
	xport p;
	xcache_handler h = cio->h;
	char *req_target, *req_data;
	int r;
	char *target;
	uint32_t targetlen;
	struct ce *ce;

	/* Get target name */
	target = xcache_get_name(cached->cache, h);
	targetlen = strlen(target);
	ce = xcache_get_entry(cached->cache, h);

	/* Allocate request */
	req = xseg_get_request(xseg, srcport, dstport, X_ALLOC);
	if (!req) {
		XSEGLOG2(&lc, W, "Cannot get request");
		return -1;
	}
	req->size = __calculate_size(cached, start, end);
	req->offset = __calculate_offset(cached, start);

	/* Get xseg operation */
	if (op == X_WRITE || op == X_READ) {
		req->op = op;
	} else {
		XSEGLOG2(&lc, W, "Invalid op (%lu)", op);
		goto put_xseg_request;
	}

	/* Prepare request */
	r = xseg_prep_request(xseg, req, targetlen, req->size);
	if (r < 0) {
		XSEGLOG2(&lc, W, "Cannot prepare request! (%lu, %llu)",
				targetlen, (unsigned long long)req->size);
		goto put_xseg_request;
	}

	req_target = xseg_get_target(xseg, req);
	strncpy(req_target, target, targetlen);

	if (req->op == X_WRITE) {
		 req_data = xseg_get_data(xseg, req);
		 //memcpy(req_data, ce->data + req->offset, req->size);
	}

	/* Set request data */
	r = xseg_set_req_data(xseg, req, pr);
	if (r < 0) {
		XSEGLOG2(&lc, W, "Cannot set request data");
		goto put_xseg_request;
	}

	/* Submit request */
	p = xseg_submit(xseg, req, srcport, X_ALLOC);
	if (p == NoPort) {
		XSEGLOG2(&lc, W, "Cannot submit request");
		goto out_unset_data;
	}

	r = xseg_signal(xseg, p);

	return 0;

out_unset_data:
	xseg_set_req_data(xseg, req, NULL);
put_xseg_request:
	if (xseg_put_request(xseg, req, srcport))
		XSEGLOG2(&lc, W, "Cannot put request");
	return -1;
}

//WORK
static void flush_dirty_buckets(struct cached *cached, struct peer_req *pr)
{
	struct peerd *peer = pr->peer;
	struct cache_io *cio = pr->priv;
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);
	struct bucket *b;
	uint32_t end = cached->buckets_per_object - 1;
	uint32_t i, first_dirty, last_dirty;

	/* write all dirty buckets */
	for (i = 0; i <= end && cio->pending_reqs < 13; i++) {
		b = &ce->buckets[i];
		if (__get_bucket_data_status(b) != DIRTY)
			continue;

		first_dirty = i;
		last_dirty = __get_last_dirty(ce, first_dirty, end);
		i = last_dirty;

		XSEGLOG2(&lc, D, "Flush range for %p (start: %lu, end: %lu )",
				ce, first_dirty, last_dirty);

		if (rw_range(peer, pr, X_WRITE, first_dirty, last_dirty) < 0){
			XSEGLOG2(&lc, E, "Flush of entry %p failed (h: %lu)", ce, cio->h);
			cio->state = CIO_FAILED;
		}
		__set_bucket_data_status_range(ce, first_dirty, last_dirty, WRITING);
		cio->pending_reqs++;
	}
}

/*
 * Insertion/eviction process with writeback policy:
 *
 * Insertion removes an LRU entry from cache.
 * After all pending operations on this entry have finished, on_evict is called.
 *
 * If cache entry is invalidated then we have no flushing to do.
 * Else we check if there are any dirty buckets. If there are, we issue an
 * explicit cache flush and mark the cache entry as evicted.
 *
 * The cache flush is serialized on the object work queue. New operations
 * derived from reinsertions pose no threat since in this case, the eviction is
 * being treated as a usual cache flush. Writeback can also take place safely
 * during the flush.
 *
 * When eviction has finished, it marks the cache entry as ready and signals the
 * deferred_workq for any jobs that waited during the flush.
 * If writeback has occurred during eviction, the last put will check again if
 * dirty buckets exist and if needed it will issue a new flush.
 */
//WORK
void flush_work(void *wq, void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct peerd *peer = pr->peer;
	struct cached *cached = peer->priv;
	struct cache_io *cio = pr->priv;
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);

	XSEGLOG2(&lc, I, "Flushing cache entry %p (h: %lu)", ce, cio->h);

	if (wq == &ce->deferred_workq) {
		XSEGLOG2(&lc, W, "ce %p and pr %p is in deferred workq", ce, pr);
	}

	if (__is_entry_clean(cached, ce)) {
		cached_complete(peer, pr);
		return;
	}

	/*
	 * FIXME: What if we are already on deferred workq? Then if we enqueue our
	 * job, it will be signalled immediately
	 */
	if (ce->status == CE_DELETING || ce->status == CE_FLUSHING) {
		XSEGLOG2(&lc, W, "Blocker cannot receive flush request "
				"(ce: %p, ce->status: %lu)", ce, ce->status);
		if (wq == &ce->deferred_workq) {
			XSEGLOG2(&lc, W, "Cannot enqueue to deferred workq of ce %p", ce);
			xworkq_enqueue(&ce->workq, flush_work, (void *)pr);
			xworkq_enqueue(&cached->workq, signal_workq, (void *)&ce->workq);
			return;
		}
		if (xworkq_enqueue(&ce->deferred_workq, flush_work, (void *)pr) < 0) {
			cio->state = CIO_FAILED;
			XSEGLOG2(&lc, E, "Error: cannot enqueue request");
			goto out;
		}
		return;
	}

	ce->status = CE_FLUSHING;
	flush_dirty_buckets(cached, pr);

out:
	/* FIXME: Handle failing of requests */
	if (cio->state == CIO_FAILED) {
		XSEGLOG2(&lc, E, "Flush failed");
	} else if (cio->pending_reqs) {
		XSEGLOG2(&lc, D, "Sent %lu flush request(s) to blocker",
				cio->pending_reqs);
	} else {
		XSEGLOG2(&lc, W, "BUG: Entry (ce :%p) was(?) dirty but flush was not "
				"necessary", ce);
		cached_complete(peer, pr);
	}
}

void *init_node(void *c, void *xh)
{
	struct peerd *peer = (struct peerd *)c;
	xcache_handler h = *(xcache_handler *)(xh);
	struct cached *cached = peer->priv;
	struct cache_io *ce_cio;
	struct ce *ce;
	uint32_t *bac;
	uint32_t *bdc;

	ce = malloc(sizeof(struct ce));
	if (!ce)
		goto ce_fail;

	memset(ce, 0, sizeof(struct ce)); /* Clear the struct from junk values yy*/
	xlock_release(&ce->lock);

	ce->buckets = calloc(cached->buckets_per_object, sizeof(struct bucket));
	bac = calloc(BUCKET_ALLOC_STATUSES, sizeof(uint32_t));
 	ce->bucket_alloc_status_counters = bac;
	bdc = calloc(BUCKET_DATA_STATUSES, sizeof(uint32_t));
 	ce->bucket_data_status_counters = bdc;

	ce->pr.priv = malloc(sizeof(struct cache_io));

	if (!ce->buckets || !bac || !bdc || !ce->pr.priv) {
		XSEGLOG2(&lc, E, "Node allocation failed");
		goto ce_fields_fail;
	}

	ce->pr.peer = peer;
	ce->pr.portno = peer->portno_start;

	ce_cio = (struct cache_io *)ce->pr.priv;
	ce_cio->h = h;

	xworkq_init(&ce->workq, &ce->lock, 0);
	xworkq_init(&ce->deferred_workq, &ce->lock, 0);
	//waitq_init(&ce->bucket_waitq, all_buckets_claimed, ce, 0)
	return ce;

ce_fields_fail:
	free(ce->buckets);
	free(bac);
	free(bdc);
	free(ce->pr.priv);
	free(ce);
ce_fail:
	return NULL;
}

/*
 * on_init is called when a new object is inserted in cache. It invalidates the
 * buckets of the previous object that was paired with this ce and resets its
 * stats.
 */
int on_init(void *c, void *e)
{
	uint32_t i;
	struct peerd *peer = (struct peerd *)c;
	struct cached *cached = peer->priv;
	struct ce *ce = (struct ce *)e;
	struct cache_io *ce_cio = ce->pr.priv;
	uint32_t *bac = ce->bucket_alloc_status_counters;
	uint32_t *bdc = ce->bucket_data_status_counters;

	XSEGLOG2(&lc, I, "Initializing cache entry %p (ce_cio: %p, h: %lu)",
			ce, ce_cio, ce_cio->h);

	ce->status = CE_READY;
	ce_cio->state = CIO_ACCEPTED;
	ce_cio->pending_reqs = 0;

	/*
	 * We don't use __set_bucket_*_status_range here, since previous bucket
	 * statuses will affect our counters
	 */
	for (i = 0; i < BUCKET_ALLOC_STATUSES; i++)
		bac[i] = 0;
	for (i = 0; i < BUCKET_DATA_STATUSES; i++)
		bdc[i] = 0;
	for (i = 0; i < cached->buckets_per_object; i++) {
		SET_FLAG(BUCKET_ALLOC_STATUS, ce->buckets[i].flags, INVALID);
		SET_FLAG(BUCKET_DATA_STATUS, ce->buckets[i].flags, FREE);
	}
	bac[FREE] = cached->buckets_per_object;
	bdc[INVALID] = cached->buckets_per_object;

	return 0;
}

void on_reinsert(void *c, void *e)
{
	struct ce *ce = (struct ce *)e;
	struct cache_io *ce_cio = ce->pr.priv;

	XSEGLOG2(&lc, I, "Re-inserted cache entry %p (h: %lu)", ce, ce_cio->h);
}

int on_evict(void *c, void *e)
{
	struct ce *ce = (struct ce *)e;
	struct cache_io *ce_cio = ce->pr.priv;

	XSEGLOG2(&lc, I, "Evicted cache entry %p (h: %lu)", ce, ce_cio->h);
	return 0;
}

int on_finalize(void *c, void *e)
{
	struct peerd *peer = (struct peerd *)c;
	struct cached *cached = peer->priv;
	struct ce *ce = (struct ce *)e;
	struct cache_io *ce_cio = ce->pr.priv;

	XSEGLOG2(&lc, I, "Finalizing cache entry %p (h: %lu)", ce, ce_cio->h);

	if (__is_entry_clean(cached, ce))
		return 0;

	xcache_get(cached->cache, ce_cio->h);
	if (xworkq_enqueue(&ce->workq, flush_work, (void *)&ce->pr) < 0)
		goto fail;
	if (xworkq_enqueue(&cached->workq, signal_workq, (void *)&ce->workq) < 0)
		goto fail;

	return 1;

fail:
	XSEGLOG2(&lc, E, "Cannot flush dirty entry %p (h: %lu)", ce, ce_cio->h);
	return 1;

}

/*
 * on free is called when the entry is guaranteed to be clean and cannot be
 * found through any hash table.
 * Since we are the last referrer, no lock is needed.
 */
void on_free(void *c, void *e)
{
	struct peerd *peer = (struct peerd *)c;
	struct cached *cached = peer->priv;
	struct ce *ce = (struct ce *)e;
	struct cache_io *ce_cio = ce->pr.priv;

	XSEGLOG2(&lc, I, "Freeing cache entry %p (ce_cio: %p, h: %lu)",
			ce, ce_cio, ce_cio->h);
	/*
	 * Doesn't matter if signal can't be enqueued, pending_waitq will be
	 * signalled eventually.
	 */
	xworkq_enqueue(&cached->workq, signal_waitq, &cached->pending_waitq);
}

struct xcache_ops c_ops = {
	.on_init = on_init,
	.on_evict = on_evict,
	.on_finalize = on_finalize,
	.on_reinsert = on_reinsert,
	.on_put = NULL,
	.on_free  = on_free,
	.on_node_init = init_node
};

static void handle_read(void *q, void *arg);
static void handle_write(void *q, void *arg);
static int forward_req(struct peerd *peer, struct peer_req *pr,
			struct xseg_request *req);

/*
 * handle_read reads all buckets within a given request's range.
 * If a bucket is:
 * VALID || DIRTY || WRITING: it's good to read.
 * INVALID: we have to issue a request (via blocker) to read it from slower
 *          media.
 * LOADING: We have to wait (on a waitq) for the slower media to answer our
 *          previous request.
 *
 * If unreadable buckets exist, it waits on the last unreadable bucket.
 *
 * If a cio is failed or ce is invalidated, it waits for all pending requests to
 * return and then fails the pr.
 * If ce is processing a delete request, then all operations on ce are on hold.
 */
//WORK
static void handle_read(void *q, void *arg)
{
	/*
	 * In this context we hold a reference to the cache entry and
	 * the assocciated lock
	 */

	struct peer_req *pr = (struct peer_req *)arg;
	struct peerd *peer = pr->peer;
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct xseg_request *req = pr->req;
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);
	struct bucket *b;
	uint32_t start_bucket, end_bucket;
	uint32_t i, first_invalid, last_invalid;
	uint32_t loading_buckets = 0;
	uint32_t pending_requests = 0;
	uint32_t status;

	XSEGLOG2(&lc, D, "Started for %p (h: %lu, pr: %p)", ce, cio->h, pr);

	/* Check ce and cio status to handle special cases */
	if (cio->state == CIO_FAILED || ce->status == CE_INVALIDATED)
		goto out;

	/* Get request bucket limits */
	start_bucket = __get_bucket(cached, req->offset);
	end_bucket = __get_bucket(cached, req->offset + req->size - 1);
	if (end_bucket > cached->buckets_per_object) {
		XSEGLOG2(&lc, W, "Request exceeds object's bucket range (%lu)",
				end_bucket);
		end_bucket = cached->buckets_per_object;
	}
	XSEGLOG2(&lc, D, "Start: %lu, end %lu for ce: %p", start_bucket, end_bucket);

	/* Issue read requests to blocker for invalid buckets */
	for (i = start_bucket; i <= end_bucket; i++) {
		b = &ce->buckets[i];

		if (__is_bucket_readable(b))
			continue;

		status = __get_bucket_data_status(b);
		if (status == INVALID) {
			XSEGLOG2(&lc, D, "Found invalid bucket %lu", i);
			first_invalid = i;
			last_invalid = __get_last_invalid(ce, first_invalid, end_bucket);
			i = last_invalid;

			if (rw_range(peer, pr, X_READ, first_invalid, last_invalid) < 0) {
				cio->state = CIO_FAILED;
				break;
			}

			__set_bucket_data_status_range(ce, first_invalid,
					last_invalid, LOADING);
			cio->pending_reqs++;
			cio->state =  CIO_READING;
			pending_requests++;
		} else {
			loading_buckets++;
		}
	}

	XSEGLOG2(&lc, D, "Loading buckets: %lu, pending requests: %lu (ce: %p)",
			loading_buckets, pending_requests, ce);

	/* FIXME: Handle this correctly */
	if (!pending_requests && loading_buckets) {
		XSEGLOG2(&lc, E, "Tough luck buddy...");
		cio->state = CIO_FAILED;
	}

out:
	/*
	 * Since we cannot safely de-associate the pending requests from the
	 * peer request, do not complete peer, until there are no pending_reqs
	 * requests.
	 */
	if (cio->pending_reqs) {
		if (!loading_buckets && !pending_requests)
			XSEGLOG2(&lc, W, "BUG: Pending reqs in clean request range");
		return;
	}

	if (cio->state == CIO_FAILED) {
		cached_fail(peer, pr);
	} else if (ce->status == CE_INVALIDATED) {
		cached_fake_complete(peer, pr);
	} else {
		if (serve_req(peer, pr)) {
			XSEGLOG2(&lc, E, "Serve of request failed");
			cached_fail(peer, pr);
		} else {
			cached_complete(peer, pr);
		}
	}
	XSEGLOG2(&lc, D, "Finished for %p (h: %lu, pr: %p)", ce, cio->h, pr);
}

/*
 * handle_write writes the buckets to a given request's range or writes to
 * permanent storage and updates the buckets later based on the write policy.
 *
 * In case of misaligned write on the first or the last bucket, we should ensure
 * that the first and the last buckets contain valid data (aka are readable). If
 * they are not, we must read them and wait for them to load before continuing.
 *
 * Now, depending on policy we:
 *
 * Writethrough:
 * 	forward the request to the permanent storage, and on return we update
 * 	the affected buckets.
 * Writeback:
 * 	immediately write to the bucket, marking them as dirty.
 *
 * FIXME: support max req size
 */
//WORK
static void handle_write(void *q, void *arg)
{
	/*
	 * In this context we hold a reference to the cache entry and
	 * the assocciated lock
	 */

	struct peer_req *pr = (struct peer_req *)arg;
	struct peerd *peer = pr->peer;
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);
	struct xseg_request *req = pr->req;
	struct bucket *b;

	unsigned char *req_data = (unsigned char *)xseg_get_data(peer->xseg, req);
	uint32_t start_bucket, end_bucket, last_read_bucket = -1;
	uint64_t first_bucket_offset = req->offset % cached->bucket_size;
	uint64_t last_bucket_offset = (req->offset + req->size) % cached->bucket_size;
	int start_bucket_status, end_bucket_status;

	XSEGLOG2(&lc, D, "Started for %p (h: %lu, pr: %p)", ce, cio->h, pr);
	//what about FUA?

	/* Check ce and cio status to handle special cases */
	if (cio->state == CIO_FAILED || ce->status == CE_INVALIDATED)
		goto out;

	start_bucket = __get_bucket(cached, req->offset);
	end_bucket = __get_bucket(cached, req->offset + req->size - 1);

	/*
	 * In case of a misaligned write, if the start, end buckets of the write
	 * are invalid, we have to read them before continuing with the write.
	 * FIXME 2: If loading?
	 */
	b = &ce->buckets[start_bucket];
	start_bucket_status = __get_bucket_data_status(b);
	if (start_bucket_status == INVALID && first_bucket_offset) {
		if (rw_range(peer, pr, X_READ, start_bucket, start_bucket) < 0) {
			cio->state = CIO_FAILED;
			goto out;
		}
		__set_bucket_data_status(ce, b, LOADING);
		cio->pending_reqs++;
		cio->state = CIO_WRITING;
		last_read_bucket = start_bucket;
	}

	b = &ce->buckets[end_bucket];
	end_bucket_status = __get_bucket_data_status(b);
	if (end_bucket_status == INVALID && last_bucket_offset) {
		if (rw_range(peer, pr, X_READ, end_bucket, end_bucket) < 0) {
			cio->state = CIO_FAILED;
			goto out;
		}
		__set_bucket_data_status(ce, b, LOADING);
		cio->pending_reqs++;
		cio->state = CIO_WRITING;
		last_read_bucket = end_bucket;
	}

	if (last_read_bucket != -1)
		return;

	/*
	 * We proceed here only if the start and end buckets of a misalligned write
	 * are valid
	 */

	if (cached->write_policy == WRITETHROUGH) {
		if (forward_req(peer, pr, pr->req) < 0) {
			XSEGLOG2(&lc, E, "Couldn't forward write request %p to blocker", req);
			cio->state = CIO_FAILED;
			goto out;
		}
		cio->pending_reqs++;
	} else if (cached->write_policy == WRITEBACK) {
		rw_bucket_range(ce, X_WRITE, req_data, req->offset, req->size);
		__set_bucket_data_status_range(ce, start_bucket, end_bucket, DIRTY);
		req->state |= XS_SERVED;
		req->serviced = req->size;
	} else {
		cio->state = CIO_FAILED;
		XSEGLOG2(&lc, E, "Invalid cache write policy");
	}

out:
	if (!cio->pending_reqs) {
		if (ce->status == CE_INVALIDATED)
			cached_fake_complete(peer, pr);
		else if (cio->state == CIO_FAILED)
			cached_fail(peer, pr);
		else
			cached_complete(peer, pr);
	}

	XSEGLOG2(&lc, D, "Finished for %p (h: %lu, pr: %p)", ce, cio->h, pr);
}

static void handle_readwrite_claim(void *q, void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct peerd *peer = pr->peer;
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = (struct ce *)xcache_get_entry(cached->cache, cio->h);
	struct xseg_request *req = pr->req;
	char *target = xseg_get_target(peer->xseg, req);
	uint32_t start_bucket, end_bucket;
	int r;

	XSEGLOG2(&lc, D, "Started for %p (h: %lu, pr: %p)", ce, cio->h, pr);

	/* TODO: Check here for error. We may not need to claim the buckets */

	/* Get request bucket limits */
	start_bucket = __get_bucket(cached, req->offset);
	end_bucket = __get_bucket(cached, req->offset + req->size - 1);

	XSEGLOG2(&lc, D, "Trying to claim buckets [%u, %u]",
			start_bucket, end_bucket);

	r = claim_bucket_range(ce, start_bucket, end_bucket);
	/* FIXME: No this is wrong */
	if (r < 0)
		goto fail;

	switch (req->op) {
		case X_WRITE:
			handle_write(q, arg);
			goto out;
		case X_READ:
			handle_read(q, arg);
			goto out;
		default:
			XSEGLOG2(&lc, E, "Invalid op %u", req->op);
	}

fail:
	XSEGLOG2(&lc, E, "Failing pr %p", pr);
	cached_fail(peer, pr);
out:
	XSEGLOG2(&lc, D, "Finished");
}

/*
 * handle_readwrite is called when we accept a read/write request.
 * Its purpose is to find a handler associated with the request's target (cache
 * hit) or insert a new one (cache miss). Then, the request will be enqueued as
 * a work according to the XSEG opeation type (read/write).
 *
 * Problematic scenarios:
 * a. (alloc) The cache can become full, which leaves no room for insertion. The
 *    request must then wait on a special waitq (pending_waitq) that is signaled
 *    when a cache node gets freed. The condition for this waitq is the number
 *    of free nodes, which are checked before and after the enqueue to make sure
 *    that the signal won't be lost.
 * b. (insert) Insertion may fail for undefined reasons (NoEntry). In this case,
 *    we won't retry and the request will be failed.
 */
//ASYNC WORK
static void handle_readwrite(void *q, void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct peerd *peer = pr->peer;
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce;
	struct xseg_request *req = pr->req;
	char name[XSEG_MAX_TARGETLEN + 1];
	char *target;
	int r = 0;
	xcache_handler h = NoEntry;
	xcache_handler nh;

	//TODO: assert req->size != 0 --> complete req
	//assert (req->offset % cached->bucket_size) == 0;
	XSEGLOG2(&lc, D, "Started");

	target = xseg_get_target(peer->xseg, req);
	strncpy(name, target, req->targetlen);
	name[req->targetlen] = 0;
	XSEGLOG2(&lc, D, "Target is %s (pr: %p)", name, pr);

	/*
	 * TODO: In case our target is in "rm_entries", you must allocate a cache
	 * node first, find your target entry in "rm_entries" and then free that
	 * node. If cache is full though, you won't be able to do so and will wait
	 * for no reason. Make this faster.
	 */
	h = xcache_lookup(cached->cache, name);
	if (!__is_handler_valid(h)) {
		XSEGLOG2(&lc, D, "Cache miss for %s", name);

		h = xcache_alloc_init(cached->cache, name);
		if (!__is_handler_valid(h)) {
			XSEGLOG2(&lc, I, "Could not allocate cache entry for %s (pr: %p)",
					name, pr);
			cio->work.job_fn = handle_readwrite;
			cio->work.job = (void *)pr;
			r = xwaitq_enqueue(&cached->pending_waitq, &cio->work);
			goto out;
		}

		nh = xcache_insert(cached->cache, h);
		if (!__is_handler_valid(nh)) {
			XSEGLOG2(&lc, E, "Could not insert cache entry");
			xcache_free_new(cached->cache, h);
			r = -1;
			goto out;
		} else if (nh != h) {
			/* if insert returns another cache entry than the one we
			 * allocated and requested to be inserted, then
			 * someone else beat us to the insertion of a cache
			 * entry assocciated with the same name. Use this cache
			 * entry instead and put the one we allocated.
			 */
			XSEGLOG2(&lc, D, "Partial cache hit:"
					"\tObject already in cache. "
					"Alloced handler: %lu, New handler: %lu", h, nh);
			xcache_free_new(cached->cache, h);
			h = nh;
		}
	} else {
		XSEGLOG2(&lc, D, "Cache hit");
	}

	ce = (struct ce *)xcache_get_entry(cached->cache, h);
	if (!ce) {
		XSEGLOG2(&lc, E, "Received cache entry handler %lu but no cache entry", h);
		r = -1;
		goto out;
	}

	cio->h = h;

	XSEGLOG2(&lc, I, "Target %s is in cache (h: %lu)", name, h);

	r = xworkq_enqueue(&ce->workq, handle_readwrite_claim, (void *)pr);
	if (r >= 0)
		xworkq_signal(&ce->workq);

out:
	if (r < 0) {
		XSEGLOG2(&lc, E, "Failing pr %p", pr);
		cached_fail(peer, pr);
	}

	XSEGLOG2(&lc, D, "Finished");
}

/*
 * complete_read is called when we receive a reply from a request issued by
 * rw_range. The process mentioned below applies only to buckets previously
 * marked as LOADING:
 *
 * If all requested buckets are serviced, we mark these buckets as VALID.
 * If not, we mark serviced buckets as VALID, non-serviced buckets as INVALID
 * and the cio is failed
 *
 * If there are no more pending requests for this cio, then and only then we can
 * complete/fail/fake_complete the pr.
 */
//WORK
static void complete_read(void *q, void *arg)
{
	/*
	 * In this context we hold a reference to the cache entry and
	 * the assocciated lock
	 */
	struct req_completion *rc = (struct req_completion *)arg;
	struct peer_req *pr = rc->pr;
	struct xseg_request *req = rc->req;
	struct peerd *peer = pr->peer;
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);
	struct bucket *b;
	unsigned char *req_data = (unsigned char *)xseg_get_data(peer->xseg, req);
	uint32_t start, start_unserviced, end_size, i;
	int success;

	XSEGLOG2(&lc, D, "Started");
	XSEGLOG2(&lc, D, "Target: %s (ce: %p). Serviced vs total: %lu/%lu",
			xseg_get_target(peer->xseg, req), ce, req->serviced, req->size);

	/*
	 * Synchronize pending_reqs of the cache_io here, since each cache_io
	 * refers to only one object, and therefore we can use the object lock
	 * to synchronize between receive contextes.
	 */
	cio->pending_reqs--;

	/* Check ce and cio status to handle special cases */
	if (cio->state == CIO_FAILED || ce->status == CE_INVALIDATED)
		goto out;

	/* Assertions for request size */
	if (!req->size) {
		XSEGLOG2(&lc, E, "BUG: zero sized read");
		cio->state = CIO_FAILED;
		goto out;
	} else if (req->size % cached->bucket_size) {
		XSEGLOG2(&lc, E, "BUG: Misalligned read");
		cio->state = CIO_FAILED;
		goto out;
	}

	/* Check if request has been correctly served */
	success = ((req->state & XS_SERVED) && req->serviced == req->size);
	if (!success)
		cio->state = CIO_FAILED;

	/* Get request bucket limits */
	start = __get_bucket(cached, req->offset);
	end_size = __get_bucket(cached, req->offset + req->size - 1);
	start_unserviced = __get_bucket(cached, req->offset + req->serviced);

	XSEGLOG2(&lc, D,"Stats: \n"
			"start            = %lu\n"
			"start_unserviced = %lu\n"
			"end_size         = %lu",
			start, start_unserviced, end_size);

	/* Check serviced buckets */
	for (i = start; i < start_unserviced; i++) {
		b = &ce->buckets[i];
		if (__get_bucket_data_status(b) != LOADING)
			continue;

		XSEGLOG2(&lc, D, "Bucket %lu loading and reception successful", i);
		b = &ce->buckets[i];

		rw_bucket(b, X_WRITE, req_data, 0, cached->bucket_size);

		req_data += cached->bucket_size;
		__set_bucket_data_status(ce, b, VALID);
	}

	/* Check non-serviced buckets */
	for (i = start_unserviced; i <= end_size; i++) {
		b = &ce->buckets[i];
		if (__get_bucket_data_status(b) != LOADING)
			continue;

		XSEGLOG2(&lc, D, "Bucket %lu loading but reception unsuccessful", i);
		__set_bucket_data_status(ce, b, INVALID);
	}

out:
	xseg_put_request(peer->xseg, rc->req, pr->portno);
	free(rc);

	/* Take actions only when there are no pending reqs */
	if (!cio->pending_reqs) {
		if (ce->status == CE_INVALIDATED)
			cached_fake_complete(peer, pr);
		else if (cio->state == CIO_READING)
			handle_read((void *)&ce->workq, (void *)pr);
		else if (cio->state == CIO_WRITING)
			handle_write((void *)&ce->workq, (void *)pr);
		else if (cio->state == CIO_FAILED)
			cached_fail(peer, pr);
	}

	XSEGLOG2(&lc, D, "Finished");
}

//WORK
static void complete_write_through(struct peerd *peer, struct peer_req *pr,
					struct xseg_request *req)
{
	/*
	 * In this context we hold a reference to the cache entry and
	 * the assocciated lock
	 */
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);
	uint32_t start, end_serviced;
	unsigned char *req_data = (unsigned char *)xseg_get_data(peer->xseg, req);
	int success;

	XSEGLOG2(&lc, D, "Started");
	XSEGLOG2(&lc, D, "Target: %s. Serviced vs total: %lu/%lu",
			xseg_get_target(peer->xseg, req), req->serviced, req->size);

	/* Assertions for request size */
	if (!req->size) {
		XSEGLOG2(&lc, E, "BUG: zero sized write");
		cio->state = CIO_FAILED;
		goto out;
	}

	/* Check if request has been correctly served */
	success = ((req->state & XS_SERVED) && req->serviced == req->size);
	if (!success)
		cio->state = CIO_FAILED;

	/* Fill serviced buckets */
	if (req->serviced) {
		start = __get_bucket(cached, req->offset);
		//memcpy(ce->data + req->offset, req_data, req->serviced);
		rw_bucket_range(ce, X_WRITE, req_data, req->offset, req->size);
		end_serviced = __get_bucket(cached, req->offset + req->serviced - 1);
		__set_bucket_data_status_range(ce, start, end_serviced, VALID);
	}

out:
	/*
	 * Here we do not put request, because we forwarded the original request.
	 */
	if (!cio->pending_reqs) {
		if (cio->state == CIO_FAILED)
			cached_fail(peer, pr);
		else
			cached_complete(peer, pr);
	} else {
		XSEGLOG2(&lc, W, "BUG: Pending requests remaining after write");
	}

	XSEGLOG2(&lc, D, "Finished");
	return;
}

//WORK
static void complete_write_back(struct peerd *peer, struct peer_req *pr,
		struct xseg_request *req)
{
	/*
	 * In this context we hold a reference to the cache entry and
	 * the assocciated lock
	 */
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);
	struct bucket *b;
	char *name = xcache_get_name(cached->cache, cio->h);
	uint32_t start, end, i;
	int success;

	XSEGLOG2(&lc, D, "Started. Target is %s (h: %lu)", name, cio->h);

	success = ((req->state & XS_SERVED) && req->serviced == req->size);
	if (!success) {
		XSEGLOG2(&lc, E, "Write failed");
		cio->state = CIO_FAILED;
		goto out;
	}

	start = __get_bucket(cached, req->offset);
	end = __get_bucket(cached, req->offset + req->size - 1);

	for (i = start; i <= end; i++) {
		b = &ce->buckets[i];
		if (__get_bucket_data_status(b) == WRITING)
			__set_bucket_data_status(ce, b, VALID);
	}

out:
	xseg_put_request(peer->xseg, req, pr->portno);

	if (cio->pending_reqs){
		XSEGLOG2(&lc, D, "%lu request(s) remaining for pr %p (ce: %p, h: %lu)",
				cio->pending_reqs, pr, ce, cio->h);
		return;
	}

	if (xworkq_enqueue(&cached->workq, signal_workq, &ce->deferred_workq) < 0)
		cio->state = CIO_FAILED;

	/* FIXME: Handle failings properly */
	if (cio->state == CIO_FAILED) {
		ce->status = CE_FAILED;
		cached_fail(peer, pr);
	} else {
		ce->status = CE_READY;
		cached_complete(peer, pr);
	}

	XSEGLOG2(&lc, D, "Finished");
	return;
}

//WORK
void complete_write(void *q, void *arg)
{
	/*
	 * In this context we hold a reference to the cache entry and
	 * the assocciated lock
	 */

	struct req_completion *rc = (struct req_completion *)arg;
	struct peer_req *pr = rc->pr;
	struct xseg_request *req = rc->req;
	struct peerd *peer = pr->peer;
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);

	XSEGLOG2(&lc, D, "Started");

	/*
	 * Synchronize pending_reqs of the cache_io here, since each cache_io
	 * refers to only one object, and therefore we can use the object lock
	 * to synchronize between receive contextes.
	 */
	cio->pending_reqs--;
	free(rc);

	/* Check ce and cio status to handle special cases */
	if (cio->state == CIO_FAILED || ce->status == CE_INVALIDATED)
		goto out;

	if (cached->write_policy == WRITETHROUGH)
		complete_write_through(peer, pr, req);
	else if (cached->write_policy == WRITEBACK)
		complete_write_back(peer, pr, req);

	return;

out:
	if (!cio->pending_reqs) {
		if (ce->status == CE_INVALIDATED)
			cached_fake_complete(peer, pr);
		else if (cio->state == CIO_FAILED)
			cached_fail(peer, pr);
	}
}

static int handle_receive_read(struct peerd *peer, struct peer_req *pr,
			struct xseg_request *req)
{
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);
	struct req_completion *rc;

	XSEGLOG2(&lc, D, "Started");

	rc = malloc(sizeof(struct req_completion));
	if (!rc) {
		perror("malloc");
		return -1;
	}

	rc->pr = pr;
	rc->req = req;
	if (xworkq_enqueue(&ce->workq, complete_read, (void *)rc) < 0){
		free(rc);
		XSEGLOG2(&lc, E, "Failed to enqueue work");
		return -1;
	}
	xworkq_signal(&ce->workq);

	XSEGLOG2(&lc, D, "Finished");
	return 0;
}

static int handle_receive_write(struct peerd *peer, struct peer_req *pr,
			struct xseg_request *req)
{
	XSEGLOG2(&lc, D, "Started");
	/*
	 * Should be rentrant
	 */
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	/*assert there is a handler for received cio*/
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);

	struct req_completion *rc;

	rc = malloc(sizeof(struct req_completion));
	if (!rc) {
		perror("malloc");
		return -1;
	}

	rc->pr = pr;
	rc->req = req;
	if (xworkq_enqueue(&ce->workq, complete_write, (void *)rc) < 0){
		free(rc);
		return -1;
		//TODO WHAT?
	}
	xworkq_signal(&ce->workq);
	XSEGLOG2(&lc, D, "Finished");
	return 0;
}

#if 0
//WORK
void complete_delete(void *q, void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct peerd *peer = pr->peer;
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);

	XSEGLOG2(&lc, D, "Started\n");
	if (UNLIKELY(ce->state != CE_DELETING))
		XSEGLOG2(&lc, W, "BUG: ce is not in deleting state");

	if (req->state != XS_SERVED) {
		ce->status = CE_READY;
		cached_fail(peer, pr);
	} else {
		/*
		 * xcache_remove
		 */
		ce->status = CE_INVALIDATED;
		cached_complete(peer, pr);
	}

	/*
	 * enqueue signal ce->deferred_workq
	 */
	XSEGLOG2(&lc, D, "Finished\n");
}


void deletion_work(void *wq, void *arg)
{
	struct ce *ce = (struct ce *)arg;
	struct peer_req *pr = &ce->pr;
	struct peerd *peer = pr->peer;
	struct cached *cached = peer->priv;
	struct cache_io *cio = pr->priv;

	XSEGLOG2(&lc, I, "Deleting cache entry %p (h: %lu)", ce, ce_cio->h);

	if (ce->state == CE_FLUSHING || ce->state == CE_DELETING) {
		/*
		 * enqueue deletion_work in ce->deferred_workq
		 */
		return;
	}

	if (ce->state == CE_INVALIDATED)
		cached_complete(peer, pr);

	ce->state = CE_DELETING;
	/*
	 * forward delete to blocker
	 */
}

/*
 * handle delete is used when cached accepts a delete request.
 * It does not delete the cache entry immediately, since the blocker may fail
 * the request afterwards. Instead, it forwards the delete to the blocker and
 * returns.
 * FIXME: If two deletes are received paralelly, the first one will wait but the
 * second one wii be completed immediately. Do we want this behavior?
 */
static int handle_delete(struct peerd *peer, struct peer_req *pr)
{
	struct xseg_request *req = pr->req;
	int r = 0;

	XSEGLOG2(&lc, D, "Started\n");

	/*
	 * look up on entries and rm_entries
	 * cio->h = h
	 * enqueue deletion_work in ce->workq
	 * signal ce->workq
	 */

	XSEGLOG2(&lc, D, "Finished\n");
	return r;
}

/*
 * handle_receive_delete must be called lockless.  First, it invalidates the
 * entry. The invalidation is done in cache to guarantee that no reinsert will
 * occur. Then, complete_delete work is issued.
 */
static int handle_receive_delete(struct peerd *peer, struct peer_req *pr,
			struct xseg_request *req)
{
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct xseg *xseg = peer->xseg;
	struct ce *ce;
	char name[XSEG_MAX_TARGETLEN + 1];
	char *target;
	int r;
	xcache_handler h;

	XSEGLOG2(&lc, D, "Started");

	if (xworkq_enqueue(&ce->workq, complete_delete, pr) < 0) {
		/* FIXME: BUG! */
		return -1;
	}
	xworkq_signal(&ce->workq);

	XSEGLOG2(&lc, D, "Finished\n");
	return 0;
}
#endif

/*
 * Special forward request function, that associates the request with the pr
 * before forwarding.
 */
static int forward_req(struct peerd *peer, struct peer_req *pr,
			struct xseg_request *req)
{
	struct cached *cached = __get_cached(peer);

	xport p;
	xseg_set_req_data(peer->xseg, req, (void *)pr);
	p = xseg_forward(peer->xseg, req, cached->bportno, pr->portno, X_ALLOC);
	if (p == NoPort){
		xseg_set_req_data(peer->xseg, req, NULL);
		return -1;
	}

	xseg_signal(peer->xseg, p);
	return 0;
}

/*
 * handle_derailed is called when a request has reached us even though its
 * destination port doesn't match with ours. We amend this by submitting the
 * request to its destination port.
 */
static int handle_derailed(struct peerd *peer, struct peer_req *pr,
			struct xseg_request *req)
{
	struct cached *cached = __get_cached(peer);
	xport p;

	XSEGLOG2(&lc, W, "Request has other port destination.\n"
			"\tBlocker port is %u while dst port is %u.",
			req->dst_portno, cached->bportno);

	p = xseg_submit(peer->xseg, req, cached->bportno, X_ALLOC);
	if (p == NoPort) {
		XSEGLOG2(&lc, W, "Cannot submit request");
		fail(peer, pr);
		return -1;
	}
	xseg_signal(peer->xseg, p);
	free_peer_req(peer, pr);
	return 0;
}

static int handle_accept(struct peerd *peer, struct peer_req *pr,
			struct xseg_request *req)
{
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	int r = 0;

	XSEGLOG2(&lc, D, "Started");

	/* Handle the scenario where a request has different target port than ours */
	if (req->dst_portno != cached->bportno){
		r = handle_derailed(peer, pr, req);
		goto out;
	}

	cio->state = CIO_ACCEPTED;
	switch (req->op){
		case X_READ:
		case X_WRITE:
			/* handle_readwrite is purposefully in job format */
			handle_readwrite(NULL, (void *)pr);
			break;
#if 0
		/* NOT YET IMPLEMENTED */
		case X_DELETE:
			r = handle_delete(peer, pr);
			break;
		/* NOT YET IMPLEMENTED */
		case X_SNAPSHOT:
			/* On snapshot, we may need to write dirty buckets */
			handle_snapshot(peer, pr);
			break;
#endif
		default:
			/* In all other cases, defer request to blocker */
			if (canDefer(peer)){
				defer_request(peer, pr);
			} else {
				XSEGLOG2(&lc, E, "Cannot defer request!");
				fail(peer, pr);
			}
	}

out:
	XSEGLOG2(&lc, D, "Finished");
	return r;
}

static int handle_receive(struct peerd *peer, struct peer_req *pr,
			struct xseg_request *req)
{
	int r = 0;
	xport p;

	XSEGLOG2(&lc, D, "Handle receive started");

	switch (req->op){
		case X_READ:
			r = handle_receive_read(peer, pr, req);
			break;
		case X_WRITE:
			r = handle_receive_write(peer, pr, req);
			break;
#if 0
		/* NOT YET IMPLEMENTED */
		case X_DELETE:
			r = handle_receive_delete(peer, pr, req);
			break;
#endif
		default:
			p = xseg_respond(peer->xseg, req, pr->portno, X_ALLOC);
			if (p == NoPort)
				r = xseg_put_request(peer->xseg, req, pr->portno);
			break;
	}

	XSEGLOG2(&lc, D, "Handle receive ended");
	return r;
}

int dispatch(struct peerd *peer, struct peer_req *pr, struct xseg_request *req,
		enum dispatch_reason reason)
{
	struct cached *cached = __get_cached(peer);

	switch (reason) {
		case dispatch_accept:
			handle_accept(peer, pr, req);
			break;
		case dispatch_receive:
			handle_receive(peer, pr, req);
			break;
		case dispatch_internal:
		default:
			XSEGLOG2(&lc, E, "Invalid dispatch reason (%d)", reason);
	}

	/*
	 * Before returning, perform pending jobs.
	 * This should probably be called before xseg_wait_signal.
	 */
	xworkq_signal(&cached->workq);
	return 0;
}

int custom_peer_init(struct peerd *peer, int argc, char *argv[])
{
	int i;
	char bucket_size[MAX_ARG_LEN + 1];
	char object_size[MAX_ARG_LEN + 1];
	char max_req_size[MAX_ARG_LEN + 1];
	char write_policy[MAX_ARG_LEN + 1];
	long bportno = -1;
	long cache_size = -1;
	int r;

	bucket_size[0] = 0;
	object_size[0] = 0;
	max_req_size[0] = 0;
	write_policy[0] = 0;

	/* Memory allocation of nessecary structs */
	struct cached *cached = malloc(sizeof(struct cached));
	if (!cached) {
		perror("malloc");
		goto fail;
	}
	cached->cache = malloc(sizeof(struct xcache));
	if (!cached->cache) {
		perror("malloc");
		goto cache_fail;
	}
	peer->priv = cached;

	for (i = 0; i < peer->nr_ops; i++) {
		struct cache_io *cio = malloc(sizeof(struct cache_io));
		if (!cio) {
			perror("malloc");
			goto cio_fail;
		}
		cio->h = NoEntry;
		cio->pending_reqs = 0;
		peer->peer_reqs[i].priv = cio;
	}

	/* Read arguments */
	BEGIN_READ_ARGS(argc, argv);
	READ_ARG_ULONG("-bp", bportno);
	READ_ARG_ULONG("-cs", cache_size);
	READ_ARG_STRING("-mrs", max_req_size, MAX_ARG_LEN);
	READ_ARG_STRING("-os", object_size, MAX_ARG_LEN);
	READ_ARG_STRING("-bs", bucket_size, MAX_ARG_LEN);
	READ_ARG_STRING("-wcp", write_policy, MAX_ARG_LEN);
	END_READ_ARGS();

	/*** Parse arguments for: ***/

	/* Bucket size */
	if (!bucket_size[0]) {
		cached->bucket_size = BUCKET_SIZE_QUANTUM; /*Default value*/
	} else {
		cached->bucket_size = str2num(bucket_size);
		if (!cached->bucket_size) {
			XSEGLOG2(&lc, E, "Invalid syntax: -bs %s\n", bucket_size);
			goto arg_fail;
		}
		if (cached->bucket_size % BUCKET_SIZE_QUANTUM) {
			XSEGLOG2(&lc, E, "Misaligned bucket size: %s\n", bucket_size);
			goto arg_fail;
		}
	}

	/* Object size */
	if (!object_size[0])
		strcpy(object_size, "4M"); /*Default value*/

	cached->object_size = str2num(object_size);
	if (!cached->object_size) {
		XSEGLOG2(&lc, E, "Invalid syntax: -os %s\n", object_size);
		goto arg_fail;
	}
	if (cached->object_size % cached->bucket_size) {
		XSEGLOG2(&lc, E, "Misaligned object size: %s\n", object_size);
		goto arg_fail;
	}

	/* Max request size */
	if (!max_req_size[0])
		strcpy(max_req_size, "512K"); /*Default value*/

	cached->max_req_size = str2num(max_req_size);
	if (!cached->max_req_size) {
		XSEGLOG2(&lc, E, "Invalid syntax: -mrs %s\n", max_req_size);
		goto arg_fail;
	}
	if (cached->max_req_size % BUCKET_SIZE_QUANTUM) {
		XSEGLOG2(&lc, E, "Misaligned maximum request size: %s\n",
				max_req_size);
		goto arg_fail;
	}

	/* Cache size */
	if (cache_size < 0)
		cache_size = peer->nr_ops;

	cached->cache_size = cache_size;

	/* Blocker port */
	if (bportno < 0){
		XSEGLOG2(&lc, E, "Blocker port must be provided");
		goto arg_fail;
	}
	cached->bportno = bportno;

	/* Write policy */
	if (!write_policy[0]) {
		strcpy(write_policy, "writethrough");
	}
	cached->write_policy = read_write_policy(write_policy);
	if (cached->write_policy < 0) {
		XSEGLOG2(&lc, E, "Invalid syntax: -wcp %s\n", write_policy);
		goto arg_fail;
	}

	/*** End of parsing ***/

	/* Initialize xcache and queues */
	cached->buckets_per_object = cached->object_size / cached->bucket_size;
	r = xcache_init(cached->cache, cached->cache_size,
			&c_ops, XCACHE_LRU_O1 | XCACHE_USE_RMTABLE, peer);
	if (r < 0) {
		XSEGLOG2(&lc, E, "Could initialize cache");
		goto arg_fail;
	}
	cached->cache_size = cached->cache->size; /* cache size may have changed
						     if not power of 2 */
	if (cached->cache_size < peer->nr_ops){
		XSEGLOG2(&lc, E, "Cache size should be at least nr_ops\n"
				 "\tEffective cache size %u < nr_ops: %u",
				 cached->cache_size, peer->nr_ops);
		goto arg_fail;
	}

	/* Initialize buckets */
	cached->bucket_data = malloc(cached->object_size * cached->cache_size);
	if (!cached->bucket_data) {
		XSEGLOG2(&lc, E, "Cannot allocate enough space for bucket data");
		goto cio_fail;
	}
	if (!xq_alloc_seq(&cached->bucket_idx,
				cached->object_size * cached->cache_size,
				cached->object_size * cached->cache_size)){
		XSEGLOG2(&lc, E, "Cannot create bucket index");
		return -1;
	}

	/* Initialize workqs/waitqs */
	xworkq_init(&cached->workq, NULL, 0);
	xwaitq_init(&cached->pending_waitq, cache_not_full, cached, 0);

	xseg_set_max_requests(peer->xseg, peer->portno_start, 10000);
	xseg_set_freequeue_size(peer->xseg, peer->portno_start, 10000, 0);
	print_cached(cached);
	return 0;

arg_fail:
	custom_peer_usage();
cio_fail:
	for (i = 0; i < peer->nr_ops && peer->peer_reqs[i].priv != NULL; i++)
		free(peer->peer_reqs[i].priv);
	free(cached->cache);
cache_fail:
	free(cached);
fail:
	return -1;
}

void custom_peer_finalize(struct peerd *peer)
{
	//write dirty objects
	//or cache_close(cached->cache);
	return;
}

void custom_peer_usage()
{
	fprintf(stderr, "Custom peer options: \n"
			"  ------------------------------------------------\n"
			"    -cs       | Number of ops | Number of objects to cache\n"
			"    -mrs      | 512KiB        | Max request size\n"
			"    -os       | 4MiB          | Object size\n"
			"    -bs       | 4KiB          | Bucket size\n"
			"    -bp       | None          | Blocker port\n"
			"    -wcp      | writethrough  | Write policy [writethrough|writeback]\n"
			"\n");
}
