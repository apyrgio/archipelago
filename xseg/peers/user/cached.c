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
#include <sys/syscall.h>
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
#include <cached.h>

/*
 * We can break from the loop only when:
 * a. We have been asked to terminate
 * b. All peer requests are freed (however, this is not a sufficient condition
 *    for the peer requests stored in each ce)
 * c. There are no buckets that are in dirty, loading or writing state
 */
#define CAN_LEAVE(__peer, __gbdc)				\
	(isTerminate() && all_peer_reqs_free(__peer) &&		\
	 !__gbdc[DIRTY] && !__gbdc[LOADING] && !__gbdc[WRITING])

/*
 * Helper functions
 */

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

static inline uint64_t __count_queue_size(struct xq *q)
{
	return (uint64_t)xq_count(q);
}

static int waiters_exist(struct xwaitq *wq)
{
	return __count_queue_size(wq->q) > 0;
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

/* Bucket specific operations */
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
	struct peerd *peer = ce->pr.peer;
	struct cached *cached = __get_cached(peer);
	int old_status = __get_bucket_alloc_status(b);
	uint64_t *gbac = cached->bucket_alloc_status_counters;

	if (new_status == old_status)
		return;

	__sync_add_and_fetch(&gbac[new_status], 1);
	__sync_sub_and_fetch(&gbac[old_status], 1);
	ce->bucket_alloc_status_counters[new_status]++;
	ce->bucket_alloc_status_counters[old_status]--;
	SET_FLAG(BUCKET_ALLOC_STATUS, b->flags, new_status);
}

static inline void __set_bucket_data_status(struct ce *ce,
		struct bucket *b, int new_status)
{
	struct peerd *peer = ce->pr.peer;
	struct cached *cached = __get_cached(peer);
	int old_status = __get_bucket_data_status(b);
	uint64_t *gbdc = cached->bucket_data_status_counters;

	if (new_status == old_status)
		return;

	__sync_add_and_fetch(&gbdc[new_status], 1);
	__sync_sub_and_fetch(&gbdc[old_status], 1);
	ce->bucket_data_status_counters[new_status]++;
	ce->bucket_data_status_counters[old_status]--;
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

/*
 * BUCKET CLAIMING:
 * During cached_init, we pre-allocate a fixed number of buckets. These buckets
 * can serve as the data for the cached objects. The bucket data is actually a
 * huge malloced space of "cached->total_size" bytes.
 *
 * To index this space, we utilize a thread-safe stack where bucket indexes can
 * be pushed and popped. The cache is thread-safe because it's locked, but in
 * the future it can be lock free.
 *
 * The process is the following:
 *
 * 1) When a new request arrives and its target object is inserted in cache,
 * handle_readwrite_claim() is called with the cache entry lock.
 * 2) handle_readwrite_claim() calls claim_bucket_range() to pop the necessary
 * data indexes from the stack.
 * 3) If the stack is empty, we cannot spin forever. Instead, a job is enqueued
 * in cached's bucket waitq that will be signalled when a new index has been
 * pushed in the stack. Subsequently, this job will call the
 * handle_readwrite_claim() function with the cache entry lock held to continue
 * its job.
 * 4) Finally, when all buckets have been claimed, the request can proceed to
 * the actual work (handle_read or handle_write) depending on its XSEG operation
 * type.
 */

static int __claim_bucket(struct ce *ce, struct bucket *b)
{
	struct peerd *peer = ce->pr.peer;
	struct cached *cached = __get_cached(peer);
	xqindex index;

	index = __xq_pop_head(&cached->bucket_indexes);
	if (index == Noneidx)
		return -1;

	b->data = cached->bucket_data + (index * cached->bucket_size);
	__set_bucket_alloc_status(ce, b, CLAIMED);

	return 0;
}

/*
 * claim_bucket_range() iterates all buckets and tries to claim space from the
 * shared memory pool.
 */
static int claim_bucket_range(struct ce *ce,
		uint32_t start_bucket, uint32_t end_bucket)
{
	struct peerd *peer = ce->pr.peer;
	struct cached *cached = __get_cached(peer);
	struct bucket *b;
	struct xq *bq = &cached->bucket_indexes;
	int alloc_status;
	int r = 0;
	uint32_t i;

	/* Acquire lock for bucket pool */
	xlock_acquire(&bq->lock, 4);
	for (i = start_bucket; i <= end_bucket; i++) {
		b = &ce->buckets[i];
		alloc_status = __get_bucket_alloc_status(b);

		if (alloc_status == CLAIMED)
			continue;

		r = __claim_bucket(ce, b);
		if (r < 0)
			goto out;
	}

out:
	xlock_release(&bq->lock);
	return r;
}

static int __free_bucket(struct ce *ce, struct bucket *b)
{
	struct peerd *peer = ce->pr.peer;
	struct cached *cached = __get_cached(peer);
	xqindex index;
	xserial serial;

	index = (b->data - cached->bucket_data) / cached->bucket_size;
	serial = __xq_append_head(&cached->bucket_indexes, index);
	if (UNLIKELY(serial == Noneidx))
		XSEGLOG("BUG: Could not free bucket index. Queue is full");

	__set_bucket_alloc_status(ce, b, FREE);
	__set_bucket_data_status(ce, b, INVALID);

	return 0;
}

/*
 * free_bucket_range() frees all claimed buckets within range and issues a
 * signal, if necessary.
 */
static uint32_t free_bucket_range(struct ce *ce,
		uint32_t start_bucket, uint32_t end_bucket,
		int safe_switch)
{
	struct peerd *peer = ce->pr.peer;
	struct cached *cached = __get_cached(peer);
	struct bucket *b;
	struct xq *bq = &cached->bucket_indexes;
	uint32_t *bac = ce->bucket_alloc_status_counters;
	uint32_t i;
	uint32_t freed = 0;
	int alloc_status, data_status;

	XSEGLOG2(&lc, D, "Started for ce %p [%u, %u]",
			ce, start_bucket, end_bucket);

	/* Acquire lock for bucket pool */
	xlock_acquire(&bq->lock, 4);
	for (i = start_bucket; i <= end_bucket && bac[CLAIMED]; i++) {
		b = &ce->buckets[i];
		alloc_status = __get_bucket_alloc_status(b);

		if (alloc_status == FREE)
			continue;

		data_status = __get_bucket_data_status(b);
		/* Safe to free buckets are only valid buckets */
		if (data_status != VALID) {
			if (safe_switch)
				XSEGLOG2(&lc, E, "BUG: Unsafe bucket (%u, %d) "
						"within range", i, data_status);
			continue;
		}

		/* TODO: Check if bucket has been returned to the pool */
		__free_bucket(ce, b);
		freed++;
	}
	xlock_release(&bq->lock);

	XSEGLOG2(&lc, D, "%u buckets have been freed from ce %p", freed, ce);

	return freed;
}

static void rw_bucket(struct bucket *b, int op, unsigned char *data,
		uint64_t offset, uint64_t size)
{
	unsigned char *to = NULL;
	unsigned char *from = NULL;

	if (op == X_WRITE) {
		to = b->data + offset;
		from = data;
	} else if (op == X_READ) {
		to = data;
		from = b->data + offset;
	}

	if (!to || !from) {
		XSEGLOG2(&lc, D, "Wrong memory address");
		return;
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

		if (op == X_READ)
			data += data_size;

		size -= data_size;
		offset += bucket_offset;
	}

	if (size > 0)
		XSEGLOG2(&lc, E, "Read/write error");
}

static inline uint64_t __count_free_buckets(struct cached *cached)
{
	return (uint64_t)xq_count(&cached->bucket_indexes);
}

static inline uint64_t __count_free_reqs(struct xseg_port *p, struct xseg *xseg)
{
	struct xq *q;
	uint64_t count = 0;

	q = XPTR_TAKE(p->free_queue, xseg->segment);
	count += (uint64_t)xq_count(q);
	count += p->max_alloc_reqs - p->alloc_reqs;

	return count;
}

static int bucket_pool_not_empty(void *arg)
{
	struct cached *cached = (struct cached *)arg;
	return __count_free_buckets(cached) > 0;
}

static int cache_not_full(void *arg)
{
	struct cached *cached = (struct cached *)arg;
	return xcache_free_nodes(cached->cache) > 0;
}

static int req_pool_not_empty(void *arg)
{
	struct peerd *peer = (struct peerd *)arg;
	struct xseg *xseg = peer->xseg;
	xport portno = peer->portno_start;
	struct xseg_port *port = xseg_get_port(xseg, portno);

	return __count_free_reqs(port, xseg) > 0;
}

/* Requests can go to blocker only when ce isn't on flushing or deleting state */
static int can_send_to_blocker(void *arg)
{
	struct ce *ce = (struct ce *)arg;

	return (ce->status != CE_FLUSHING && ce->status != CE_DELETING);
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
			bdc[INVALID] + bdc[VALID] == cached->buckets_per_object)
		return 1;

	if (ce->status == CE_FLUSHING || ce->status == CE_DELETING)
		XSEGLOG2(&lc, I, "ce %p has pending work to do (status: %lu, "
				"pending_reqs: %u)",
				ce, ce->status, ce_cio->pending_reqs);

	return 0;
}

__attribute__ ((unused))
static int __are_buckets_clean(struct ce *ce)
{
	struct peerd *peer = ce->pr.peer;
	struct cached *cached = __get_cached(peer);
	uint32_t *bac = ce->bucket_alloc_status_counters;
	uint32_t *bdc = ce->bucket_data_status_counters;

	if (bac[FREE] != cached->buckets_per_object ||
			bdc[INVALID] != cached->buckets_per_object) {
		XSEGLOG2(&lc, W, "Entry %p, has:\n"
				"\t%u free buckets\n"
				"\t%u invalid buckets\n",
				ce, bac[FREE], bdc[INVALID]);
		return -1;
	}

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
			"                     max_objects  = %lu\n"
			"                     total_size   = %lu\n"
			"                     max_req_size = %lu\n"
			"                     object_size  = %u\n"
			"                     bucket_size  = %u\n"
			"                     bucks_per_obj= %u\n"
			"                     Bportno      = %d\n"
			"                     write_policy = %s\n",
			cached->cache, cached->max_objects,
			cached->total_size, cached->max_req_size,
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

static void put_ce(void *q, void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct peerd *peer = pr->peer;
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	xcache_handler h = cio->h;

	XSEGLOG2(&lc, E, "Please don't");
	if (__is_handler_valid(h))
		xcache_put(cached->cache, h);
	else
		XSEGLOG2(&lc, W, "Invalid handler for cio %p", cio);
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

	XSEGLOG2(&lc, D, "Started");
	req->serviced = req->size;

	//assert req->serviced <= req->datalen
	//memcpy(req_data, ce->data + req->offset, req->serviced);
	rw_bucket_range(ce, X_READ, req_data, req->offset, req->size);
	XSEGLOG2(&lc, D, "Finished");

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

	XSEGLOG2(&lc, E, "Failing pr %p (h: %lu)", pr, cio->h);

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
	char *req_target;
	unsigned char *req_data;
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
		 req_data = (unsigned char *)xseg_get_data(xseg, req);
		 /* Read from buket into the request buffer */
		 rw_bucket_range(ce, X_READ, req_data, req->offset, req->size);
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
	struct xwaitq *bwaitq = &cached->bucket_waitq;
	uint32_t *bdc = ce->bucket_data_status_counters;
	uint32_t end = cached->buckets_per_object - 1;
	uint32_t i, r, first_dirty, last_dirty;
	int data_status, alloc_status;

	/* write all dirty buckets */
	for (i = 0; i <= end && bdc[DIRTY]; i++) {
		b = &ce->buckets[i];
		data_status = __get_bucket_data_status(b);
		alloc_status = __get_bucket_alloc_status(b);

		if (alloc_status == FREE) {
			continue;
		} else if (data_status != DIRTY) {
			continue;
		}

		first_dirty = i;
		last_dirty = __get_last_dirty(ce, first_dirty, end);
		i = last_dirty;

		XSEGLOG2(&lc, D, "Flush range for %p (start: %lu, end: %lu )",
				ce, first_dirty, last_dirty);

		if (rw_range(peer, pr, X_WRITE, first_dirty, last_dirty) < 0){
			XSEGLOG2(&lc, W, "Flush of entry %p failed (h: %lu).\n"
					"\tOut of requests.", ce, cio->h);
			/*
			 * Since there is a request sent, we can postpone
			 * sending the rest. We will be notified when this one
			 * returns.
			 */
			if (cio->pending_reqs > 0) {
				XSEGLOG2(&lc, I, "Requests have already been "
						"sent though");
				return;
			}
			cio->work.job_fn = put_ce;
			cio->work.job = (void *)pr;
			xwaitq_enqueue(&cached->req_waitq, &cio->work);
			return;
		}
		__set_bucket_data_status_range(ce, first_dirty, last_dirty, WRITING);
		cio->pending_reqs++;
	}

	/*
	 * If we have been flushed and there is no space left, free buckets
	 * aggressively
	 */
	if (!bucket_pool_not_empty(cached) && waiters_exist(bwaitq)) {
		r = free_bucket_range(ce, 0, end, 0);
		if (r > 0) {
			xworkq_enqueue(&cached->workq, signal_waitq, bwaitq);
		}
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
void flush_work(void *wq, void *arg);

static void reenter_flush_work(void *q, void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct peerd *peer = pr->peer;
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);
	int r;

	XSEGLOG2(&lc, D, "Started for %p (h: %lu, pr: %p)", ce, cio->h, pr);

	r = xworkq_enqueue(&ce->workq, flush_work, (void *)pr);

	/*
	 * Calling xworkq_signal here is harmless, since it will fail if we
	 * already have the lock
	 */
	if (r >= 0) {
		xworkq_signal(&ce->workq);
	} else {
		XSEGLOG2(&lc, E, "Failing pr %p", pr);
		cached_fail(peer, pr);
	}

	XSEGLOG2(&lc, D, "Finished for %p (h: %lu, pr: %p)", ce, cio->h, pr);
}

//WORK
void flush_work(void *wq, void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct peerd *peer = pr->peer;
	struct cached *cached = peer->priv;
	struct cache_io *cio = pr->priv;
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);

	XSEGLOG2(&lc, I, "Flushing cache entry %p (h: %lu)", ce, cio->h);

	if (__is_entry_clean(cached, ce)) {
		/*
		 * The request may have originated from a copy request which
		 * can't be completed yet.
		 */
		if (!(pr->req && pr->req->op == X_COPY))
			cached_complete(peer, pr);
		xwaitq_signal(&ce->pending_waitq);
		return;
	}

	if (ce->status == CE_DELETING || ce->status == CE_FLUSHING) {
		XSEGLOG2(&lc, W, "Blocker cannot receive flush request "
				"(ce: %p, ce->status: %lu)", ce, ce->status);
		cio->work.job_fn = reenter_flush_work;
		cio->work.job = (void *)pr;
		xwaitq_enqueue(&ce->pending_waitq, &cio->work);
	}

	ce->status = CE_FLUSHING;
	flush_dirty_buckets(cached, pr);

	/* FIXME: Handle failing of requests */
	if (cio->state == CIO_FAILED) {
		XSEGLOG2(&lc, E, "Flush failed");
	} else if (cio->pending_reqs) {
		XSEGLOG2(&lc, D, "Sent %lu flush request(s) to blocker",
				cio->pending_reqs);
	} else {
		ce->status = CE_READY;
	}

	XSEGLOG2(&lc, I, "Finished for %p (h: %lu)", ce, cio->h);
}

void force_flush_work(void *wq, void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct peerd *peer = pr->peer;
	struct cached *cached = peer->priv;
	struct cache_io *cio = pr->priv;
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);
	struct xwaitq *bwaitq = &cached->bucket_waitq;
	uint32_t *bac = ce->bucket_alloc_status_counters;
	uint32_t r;

	XSEGLOG2(&lc, I, "Force-flushing cache entry %p (h: %lu)", ce, cio->h);

	if (ce->status == CE_FLUSHING ||
			bac[CLAIMED] == 0) {
		cached_complete(peer, pr);
		return;
	}

	if (__is_entry_clean(cached, ce)) {
		r = free_bucket_range(ce, 0, cached->buckets_per_object - 1, 1);
		if (r > 0) {
			xworkq_enqueue(&cached->workq, signal_waitq, bwaitq);
		}
		cached_complete(peer, pr);
		return;
	}

	XSEGLOG2(&lc, D, "Cache entry %p (h: %lu) is not clean. Flushing...",
			ce, cio->h);
	flush_work(wq, arg);

	/*
	 * In case we had to flush our entry, we will probably be completed
	 * on complete_writeback. However, the flush may have been a red
	 * herring (e.g. due to LOADING buckets, so we check if we are in
	 * flush mode and if not, we complete our actions.
	 */
	if (ce->status == CE_READY) {
		cached_complete(peer, pr);
		return;
	}

	XSEGLOG2(&lc, I, "Finished for %p (h: %lu)", ce, cio->h);
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
	int i;

	ce = malloc(sizeof(struct ce));
	if (!ce)
		goto ce_fail;

	memset(ce, 0, sizeof(struct ce)); /* Clear the struct from junk values */
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
	for (i = 0; i < cached->buckets_per_object; i++) {
		SET_FLAG(BUCKET_ALLOC_STATUS, ce->buckets[i].flags, INVALID);
		SET_FLAG(BUCKET_DATA_STATUS, ce->buckets[i].flags, FREE);
	}
	bac[FREE] = cached->buckets_per_object;
	bdc[INVALID] = cached->buckets_per_object;

	ce->pr.peer = peer;
	ce->pr.portno = peer->portno_start;

	ce_cio = (struct cache_io *)ce->pr.priv;
	ce_cio->h = h;

	xworkq_init(&ce->workq, &ce->lock, 0);
	xwaitq_init(&ce->pending_waitq, can_send_to_blocker, ce, 0);
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
	struct ce *ce = (struct ce *)e;
	struct cache_io *ce_cio = ce->pr.priv;

	XSEGLOG2(&lc, I, "Initializing cache entry %p (ce_cio: %p, h: %lu)",
			ce, ce_cio, ce_cio->h);

	ce->status = CE_READY;
	ce_cio->state = CIO_ACCEPTED;
	ce_cio->pending_reqs = 0;

	/*
	 * When a new entry is initialized, it should be already clean.  If
	 * not, through a warning.
	 */
	__are_buckets_clean(ce);

	return 0;
}

void on_reinsert(void *c, void *e)
{
	struct ce *ce = (struct ce *)e;
	struct cache_io *ce_cio = ce->pr.priv;
	struct peerd *peer = ce->pr.peer;
	struct cached *cached = __get_cached(peer);

	XSEGLOG2(&lc, I, "Re-inserted cache entry %p (h: %lu)", ce, ce_cio->h);
	__sync_sub_and_fetch(&cached->stats.evicted, 1);
}

int on_evict(void *c, void *e)
{
	struct ce *ce = (struct ce *)e;
	struct cache_io *ce_cio = ce->pr.priv;
	struct peerd *peer = ce->pr.peer;
	struct cached *cached = __get_cached(peer);

	XSEGLOG2(&lc, I, "Evicted cache entry %p (h: %lu)", ce, ce_cio->h);
	__sync_add_and_fetch(&cached->stats.evicted, 1);
	return 0;
}

int on_finalize(void *c, void *e)
{
	struct peerd *peer = (struct peerd *)c;
	struct cached *cached = peer->priv;
	struct ce *ce = (struct ce *)e;
	struct peer_req *pr = &ce->pr;
	struct cache_io *ce_cio = pr->priv;

	XSEGLOG2(&lc, I, "Finalizing cache entry %p (h: %lu)", ce, ce_cio->h);

	if (__is_entry_clean(cached, ce))
		return 0;

	xcache_get(cached->cache, ce_cio->h);
	ce_cio->work.job_fn = reenter_flush_work;
	ce_cio->work.job = (void *)pr;
	/* Handle fails properly */
	xwaitq_enqueue(&ce->pending_waitq, &ce_cio->work);
	return 1;
}

void on_put(void *c, void *e)
{
	struct peerd *peer = (struct peerd *)c;
	struct cached *cached = peer->priv;
	struct ce *ce = (struct ce *)e;
	struct cache_io *ce_cio = ce->pr.priv;
	struct xwaitq *bwaitq = &cached->bucket_waitq;
	uint32_t r;

	XSEGLOG2(&lc, I, "Puting cache entry %p (ce_cio: %p, h: %lu)",
			ce, ce_cio, ce_cio->h);

	__sync_sub_and_fetch(&cached->stats.evicted, 1);

	if (waiters_exist(&ce->pending_waitq))
		XSEGLOG2(&lc, W, "BUG: There are waiters in pending waitq!");

	r = free_bucket_range(ce, 0, cached->buckets_per_object - 1, 1);

	/* This function is here simply to alert us if buckets are not clean */
	//__are_buckets_clean(ce);

	/* Enqueue a signal if there are any waiters in the bucket waitq */
	if (r && waiters_exist(bwaitq))
		xworkq_enqueue(&cached->workq, signal_waitq, bwaitq);
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
	struct xwaitq *pwaitq = &cached->pending_waitq;

	XSEGLOG2(&lc, I, "Freeing cache entry %p (ce_cio: %p, h: %lu)",
			ce, ce_cio, ce_cio->h);

	/*
	 * Doesn't matter if signal can't be enqueued, pending_waitq will be
	 * signalled eventually.
	 */
	if (waiters_exist(pwaitq))
		xworkq_enqueue(&cached->workq, signal_waitq, pwaitq);
}

struct xcache_ops c_ops = {
	.on_init = on_init,
	.on_evict = on_evict,
	.on_finalize = on_finalize,
	.on_reinsert = on_reinsert,
	.on_put = on_put,
	.on_free  = on_free,
	.on_node_init = init_node
};



static void handle_read(void *q, void *arg);
static void handle_write(void *q, void *arg);
static int forward_req(struct peerd *peer, struct peer_req *pr,
			struct xseg_request *req);

/*
 * ===== COPY REQUESTS 	======
 *
 * When a copy request is accepted, we must guarantee that the data that are
 * currently stored for this object will be copied too. However, if write
 * requests for an object come asynchronously with the copy request, we will not
 * try to include them to the data that will be copied. That's because we expect
 * that the users that issue the copy request will wait for the crucial data to
 * be written and then send the copy request.
 *
 * Thus, in writethrough mode we can safely forward the request. In writeback
 * mode though, we must do the following:
 *
 * * First, we check if the object exists in cache.
 *   * If it exists, we check if it has dirty buckets.
 *     * If it has dirty buckets, we enqueue ourselves to pending waitq, flush
 *       the buckets, wait for the flush to return and then check the object
 *       again.
 *     * Else, we check if the object is currently on flushing mode. If so we do
 *       the above, minus the flushing.
 * * In all the other cases and when the flushes stop, we simply forward the
 *   request to the blocker.
 */

static void copy_work(void *wq, void *arg);

static void reenter_copy_work(void *q, void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct peerd *peer = pr->peer;
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);
	int r;

	XSEGLOG2(&lc, D, "Started for %p (h: %lu, pr: %p)", ce, cio->h, pr);

	r = xworkq_enqueue(&ce->workq, copy_work, (void *)pr);

	/*
	 * Calling xworkq_signal here is harmless, since it will fail if we
	 * already have the lock
	 */
	if (r >= 0) {
		xworkq_signal(&ce->workq);
	} else {
		XSEGLOG2(&lc, E, "Failing pr %p", pr);
		cached_fail(peer, pr);
	}

	XSEGLOG2(&lc, D, "Finished for %p (h: %lu, pr: %p)", ce, cio->h, pr);
}

//WORK
static void copy_work(void *wq, void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct xseg_request *req = pr->req;
	struct peerd *peer = pr->peer;
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);

	XSEGLOG2(&lc, I, "Copying cache entry %p (h: %lu)", ce, cio->h);

	if (__is_entry_clean(cached, ce)) {
		if (forward_req(peer, pr, req) < 0) {
			XSEGLOG2(&lc, E, "Couldn't forward request %p "
				"to blocker", req);
			cached_fail(peer, pr);
		}
		XSEGLOG2(&lc, D, "Copy for cache entry %p (h: %lu) "
				"has been forwarded to blocker.", ce, cio->h);
		return;
	}

	XSEGLOG2(&lc, D, "Entry %p (h: %lu) must be flushed before copying",
			ce, cio->h);

	flush_work(NULL, pr);

	/*
	 * Now that the entry has been flushed, enqueue the copy request to the
	 * pending waitq.
	 */
	cio->work.job_fn = reenter_copy_work;
	cio->work.job = (void *)pr;
	xwaitq_enqueue(&ce->pending_waitq, &cio->work);

	XSEGLOG2(&lc, D, "Finished for %p (h: %lu, pr: %p)", ce, cio->h, pr);
}


static void handle_copy(struct peerd *peer, struct peer_req *pr)
{
	struct cached *cached = __get_cached(peer);
	struct xseg_request *req = pr->req;
	struct cache_io *cio = __get_cache_io(pr);
	xcache_handler h = NoEntry;
	char name[XSEG_MAX_TARGETLEN + 1];
	char *copy_target = xseg_get_target(peer->xseg, req);
	struct xseg_request_copy *xcopy = (struct xseg_request_copy *)
		xseg_get_data(peer->xseg, req);

	XSEGLOG2(&lc, D, "Started (pr: %p)", pr);
	XSEGLOG2(&lc, D, "original target: %s, copy target: %s",
			xcopy->target, copy_target);

	if (cached->write_policy == WRITETHROUGH)
		goto forward;

	strncpy(name, xcopy->target, xcopy->targetlen);
	name[xcopy->targetlen] = 0;

	h = xcache_lookup(cached->cache, name);
	if (__is_handler_valid(h)) {
		cio->h = h;
		/* We must check if it is dirty */
		reenter_copy_work(NULL, (void *)pr);
		return;
	}

forward:
       if (forward_req(peer, pr, pr->req) < 0) {
                XSEGLOG2(&lc, E, "Couldn't forward request %p "
                        "to blocker", req);
		cached_fail(peer, pr);
        }

	XSEGLOG2(&lc, D, "Finished (pr: %p)", pr);
}

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
	XSEGLOG2(&lc, D, "Start: %lu, end %lu for ce: %p",
			start_bucket, end_bucket, ce);

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

	XSEGLOG2(&lc, D, "Start: %lu, end %lu for ce: %p",
			start_bucket, end_bucket, ce);

	/*
	 * In case of a misaligned write, if the start, end buckets of the write
	 * are invalid, we have to read them before continuing with the write.
	 * FIXME 2: If loading?
	 */
	b = &ce->buckets[start_bucket];
	start_bucket_status = __get_bucket_data_status(b);
	if (start_bucket_status == INVALID && first_bucket_offset) {
		XSEGLOG2(&lc, W, "Misalligned write for %p (h: %lu, pr: %p)",
				ce, cio->h, pr);
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
		XSEGLOG2(&lc, W, "Misalligned write for %p (h: %lu, pr: %p)",
				ce, cio->h, pr);
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

static void handle_readwrite_claim(void *q, void *arg);

/*
 * reenter_handle_readwrite_claim() acts as a context-swicher for
 * handle_readwrite_claim(). Its main purpose is to enqueue
 * handle_readwrite_claim() as a ce job i.e. change from cached context to ce
 * context.
 */
static void reenter_handle_readwrite_claim(void *q, void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct peerd *peer = pr->peer;
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = xcache_get_entry(cached->cache, cio->h);
	int r;

	XSEGLOG2(&lc, D, "Started for %p (h: %lu, pr: %p)", ce, cio->h, pr);

	r = xworkq_enqueue(&ce->workq, handle_readwrite_claim, (void *)pr);
	if (r >= 0) {
		xworkq_signal(&ce->workq);
	} else {
		XSEGLOG2(&lc, E, "Failing pr %p", pr);
		cached_fail(peer, pr);
	}

	XSEGLOG2(&lc, D, "Finished for %p (h: %lu, pr: %p)", ce, cio->h, pr);
}

/*
 * handle_readwrite_claim() tries to claim the necessary object data buckets to
 * complete the request.  If it can't, it enqueues a job to call
 * handle_read_write_claim() when a bucket is available. You can read more in
 * the "Bucket claiming" section.
 */
static void handle_readwrite_claim(void *q, void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct peerd *peer = pr->peer;
	struct cached *cached = __get_cached(peer);
	struct cache_io *cio = __get_cache_io(pr);
	struct ce *ce = (struct ce *)xcache_get_entry(cached->cache, cio->h);
	struct xseg_request *req = pr->req;
	uint32_t start_bucket, end_bucket;
	int r;

	XSEGLOG2(&lc, D, "Started for %p (h: %lu, pr: %p)", ce, cio->h, pr);

	/* TODO: Check here for error. We may not need to claim the buckets */

	/* Get request bucket limits */
	start_bucket = __get_bucket(cached, req->offset);
	end_bucket = __get_bucket(cached, req->offset + req->size - 1);

	XSEGLOG2(&lc, D, "Trying to claim buckets [%u, %u]",
			start_bucket, end_bucket);

	/* Try to claim the necessary buckets */
	r = claim_bucket_range(ce, start_bucket, end_bucket);
	if (r < 0) {
		XSEGLOG2(&lc, I, "Bucket pool is empty.\n"
				"\tEnqueuing work for ce %p, pr %p", ce, pr);
		cio->work.job_fn = reenter_handle_readwrite_claim;
		cio->work.job = (void *)pr;
		r = xwaitq_enqueue(&cached->bucket_waitq, &cio->work);
		return;
	}

	/* If all buckets are claimed, proceed to the actual work */
	switch (req->op) {
		case X_WRITE:
			handle_write(q, arg);
			break;
		case X_READ:
			handle_read(q, arg);
			break;
		default:
			XSEGLOG2(&lc, E, "Invalid op %u", req->op);
			cached_fail(peer, pr);
	}

	XSEGLOG2(&lc, D, "Finished");
}

/*
 * handle_readwrite is called when we accept a read/write request.
 * Its purpose is to find a handler associated with the request's target (cache
 * hit) or insert a new one (cache miss). Then, a work will be enqueued for this
 * request to claim the necessary buckets.
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
	xcache_handler h = NoEntry;
	xcache_handler nh;
	char name[XSEG_MAX_TARGETLEN + 1];
	char *target;
	int r = 0;

	//TODO: assert req->size != 0 --> complete req
	//assert (req->offset % cached->bucket_size) == 0;
	XSEGLOG2(&lc, D, "Started");

	target = xseg_get_target(peer->xseg, req);
	strncpy(name, target, req->targetlen);
	name[req->targetlen] = 0;
	XSEGLOG2(&lc, D, "Target is %s (pr: %p)", name, pr);

	/*
	 * TODO: In case our target is in "rm_entries", you must allocate a
	 * cache node first, find your target entry in "rm_entries" and then
	 * free that node. If cache is full though, you won't be able to do so
	 * and will wait for no reason. Make this faster.
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

	reenter_handle_readwrite_claim(q, arg);
	XSEGLOG2(&lc, D, "Finished");
	return;
out:
	if (r < 0)
		cached_fail(peer, pr);
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
                /*
                 * FIXME: The following are wrong. We don't have to rely on
                 * the cio's state to check if we have originated from another
                 * request. We can just as well check the original req in
                 * pr->req.
                 */
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
	struct xwaitq *rwaitq = &cached->req_waitq;
	struct xwaitq *bwaitq = &cached->bucket_waitq;
	char *name = xcache_get_name(cached->cache, cio->h);
	uint32_t i, r, start = -1, end = -1;
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

	XSEGLOG2(&lc, D, "Start: %lu, end %lu for ce: %p",
			start, end, ce);

	for (i = start; i <= end; i++) {
		b = &ce->buckets[i];
		if (__get_bucket_data_status(b) == WRITING)
			__set_bucket_data_status(ce, b, VALID);
	}

out:
	xseg_put_request(peer->xseg, req, pr->portno);
	if (waiters_exist(rwaitq)) {
		xworkq_enqueue(&cached->workq, signal_waitq, rwaitq);
	}

	/*
	 * If we have been flushed and there is no space left, free buckets
	 * aggressively
	 */
	if (!bucket_pool_not_empty(cached) && waiters_exist(bwaitq)) {
		r = free_bucket_range(ce, start, end, 0);
		if (r > 0) {
			xworkq_enqueue(&cached->workq, signal_waitq, bwaitq);
		}
	}

	if (cio->pending_reqs){
		XSEGLOG2(&lc, D, "%lu request(s) remaining for pr %p (ce: %p, "
				"h: %lu)", cio->pending_reqs, pr, ce, cio->h);
		return;
	}

	/* FIXME: Handle failings properly */
	if (cio->state == CIO_FAILED) {
		ce->status = CE_FAILED;
		cached_fail(peer, pr);
	} else {
		ce->status = CE_READY;
		/*
		 * The request may have originated from a copy request which
		 * can't be completed yet.
		 */
		if (!(pr->req && pr->req->op == X_COPY))
			cached_complete(peer, pr);
	}

	xwaitq_signal(&ce->pending_waitq);

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

static int handle_receive_copy(struct peerd *peer, struct peer_req *pr,
			struct xseg_request *req)
{
	XSEGLOG2(&lc, D, "Started");

	if (req->state == XS_SERVED) {
		cached_complete(peer, pr);
		return 0;
	} else {
		cached_fail(peer, pr);
		return -1;
	}

	XSEGLOG2(&lc, D, "Finished");
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
			cached->bportno, req->dst_portno);

	p = xseg_submit(peer->xseg, req, pr->portno, X_ALLOC);
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

	XSEGLOG2(&lc, D, "Started for pr %p, req %p", pr, req);

	/*
	 * Handle the scenario where a request has different target port than
	 * ours and doesn't target us.
	 */
	if (req->dst_portno != cached->bportno &&
			(req->dst_portno < peer->portno_start ||
			 req->dst_portno > peer->portno_end)) {
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
                case X_COPY:
                        handle_copy(peer, pr);
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
		case X_PING:
			complete(peer, pr);
			break;
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
	XSEGLOG2(&lc, D, "Finished for pr %p, req %p", pr, req);
	return r;
}

static int handle_receive(struct peerd *peer, struct peer_req *pr,
			struct xseg_request *req)
{
	int r = 0;
	xport p;

	XSEGLOG2(&lc, D, "Started for pr %p, req %p", pr, req);

	switch (req->op){
		case X_READ:
			r = handle_receive_read(peer, pr, req);
			break;
		case X_WRITE:
			r = handle_receive_write(peer, pr, req);
			break;
		case X_COPY:
			r = handle_receive_copy(peer, pr, req);
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

	XSEGLOG2(&lc, D, "Finished for pr %p, req %p", pr, req);
	return r;
}

int dispatch(struct peerd *peer, struct peer_req *pr, struct xseg_request *req,
		enum dispatch_reason reason)
{
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

	return 0;
}

static void force_reclaim_buckets(struct cached *cached)
{
	struct ce *ce;
	uint32_t *bac;
	xcache_handler lru;


	if (!xlock_try_lock(&cached->stats.lock, 4))
		return;

	XSEGLOG2(&lc, D, "Started");

	if (cached->stats.evicted)
		goto out;

	lru = xcache_peek_and_get_lru(cached->cache);
	if (lru == NoEntry)
		goto out;

	ce = xcache_get_entry(cached->cache, lru);
	if (!ce)
		goto out;
	XSEGLOG2(&lc, D, "LRU is %lu", lru);

	bac = ce->bucket_alloc_status_counters;
	if (bac[CLAIMED] > 0) {
		if (ce->status != CE_FLUSHING && ce->status != CE_DELETING) {
			XSEGLOG2(&lc, I, "Force flush of LRU object");
			xworkq_enqueue(&ce->workq, force_flush_work,
					(void *)&ce->pr);
			xworkq_signal(&ce->workq);
			/*
			 * We don't want to put the entry to make sure that
			 * force_flush_work finds a valid ce
			 */
			goto out;
		}
	} else {
		XSEGLOG2(&lc, I, "Force eviction of LRU object");
		xcache_evict_lru(cached->cache);
	}
	xcache_put(cached->cache, lru);

out:
	xlock_release(&cached->stats.lock);

	XSEGLOG2(&lc, D, "Finished");
}

/*
 * generic_peerd_loop is a general-purpose port-checker loop that is
 * suitable both for multi-threaded and single-threaded peers.
 */
static int custom_cached_loop(void *arg)
{
#ifdef MT
	struct thread *t = (struct thread *) arg;
	struct peerd *peer = t->peer;
	char *id = t->arg;
#else
	struct peerd *peer = (struct peerd *) arg;
	char id[4] = {'P','e','e','r'};
#endif
	struct xseg *xseg = peer->xseg;
	xport portno_start = peer->portno_start;
	xport portno_end = peer->portno_end;
	pid_t pid = syscall(SYS_gettid);
	uint64_t threshold=1000/(1 + portno_end - portno_start);
	uint64_t loops;

	struct cached *cached = __get_cached(peer);
	uint64_t *gbdc = cached->bucket_data_status_counters;
	struct xwaitq *bwaitq = &cached->bucket_waitq;

	XSEGLOG2(&lc, I, "%s has tid %u.\n", id, pid);
	xseg_init_local_signal(xseg, peer->portno_start);
#ifdef GPERF
	ProfilerStart("/tmp/profile-cached1.prof");
#endif
	while (!CAN_LEAVE(peer, gbdc)) {
		//Heart of peerd_loop. This loop is common for everyone.
		for(loops = threshold; loops > 0; loops--) {
			if (loops == 1)
				xseg_prepare_wait(xseg, peer->portno_start);
#ifdef MT
			if (check_ports(peer, t))
#else
			if (check_ports(peer))
#endif
				loops = threshold;

			if (!bucket_pool_not_empty(cached) &&
					waiters_exist(bwaitq) &&
					!cached->stats.evicted)
				force_reclaim_buckets(cached);

			xworkq_signal(&cached->workq);
		}

		while (isTerminate() && gbdc[DIRTY]) {
			if (xcache_evict_lru(cached->cache) != NoEntry)
				break;
		}

		/* Add this check here in order not to sleep for no reason */
		if (CAN_LEAVE(peer, gbdc))
			break;

		XSEGLOG2(&lc, I, "%s goes to sleep\n", id);
		xseg_wait_signal(xseg, 10000000UL);
		xseg_cancel_wait(xseg, peer->portno_start);
		XSEGLOG2(&lc, I, "%s woke up\n", id);
	}
#ifdef GPERF
	ProfilerStop();
#endif
	return 0;
}

int custom_peer_init(struct peerd *peer, int argc, char *argv[])
{
	int i;
	char bucket_size[MAX_ARG_LEN + 1];
	char total_size[MAX_ARG_LEN + 1];
	char max_objects[MAX_ARG_LEN + 1];
	char object_size[MAX_ARG_LEN + 1];
	char max_req_size[MAX_ARG_LEN + 1];
	char write_policy[MAX_ARG_LEN + 1];
	uint64_t total_buckets;
	uint64_t *bac = NULL;
	uint64_t *bdc = NULL;
	long bportno = -1;
	int r;

	total_size[0] = 0;
	max_objects[0] = 0;
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
	memset(cached, 0, sizeof(struct cached));
	cached->cache = malloc(sizeof(struct xcache));
	if (!cached->cache) {
		perror("malloc");
		goto cache_fail;
	}
	peer->priv = cached;
	xlock_release(&cached->stats.lock);

	/* Allocate global bucket status counters */
	bac = calloc(BUCKET_ALLOC_STATUSES, sizeof(uint64_t));
 	cached->bucket_alloc_status_counters = bac;
	bdc = calloc(BUCKET_DATA_STATUSES, sizeof(uint64_t));
 	cached->bucket_data_status_counters = bdc;
	if (!bac || !bdc)
		goto cache_fail;

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
	READ_ARG_STRING("-ts", total_size, MAX_ARG_LEN);
	READ_ARG_STRING("-mo", max_objects, MAX_ARG_LEN);
	READ_ARG_STRING("-mrs", max_req_size, MAX_ARG_LEN);
	READ_ARG_STRING("-os", object_size, MAX_ARG_LEN);
	READ_ARG_STRING("-bs", bucket_size, MAX_ARG_LEN);
	READ_ARG_ULONG("-bp", bportno);
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

	/* Total size */
	if (!total_size[0]) {
		XSEGLOG2(&lc, E, "Total size must be provided");
		goto arg_fail;
	}

	cached->total_size = str2num(total_size);
	if (!cached->total_size) {
		XSEGLOG2(&lc, E, "Invalid syntax: -ts %s\n", total_size);
		goto arg_fail;
	}
	if (cached->total_size % BUCKET_SIZE_QUANTUM) {
		XSEGLOG2(&lc, E, "Misaligned total size: %s\n",
				total_size);
		goto arg_fail;
	}

	/* Max objects */
	if (!max_objects[0])
		cached->max_objects = peer->nr_ops;
	else
		cached->max_objects = str2num(max_objects);

	if (!cached->max_objects) {
		XSEGLOG2(&lc, E, "Invalid syntax: -mo %s\n", max_objects);
		goto arg_fail;
	}

	/* Blocker port */
	if (bportno < 0){
		XSEGLOG2(&lc, E, "Blocker port must be provided");
		goto arg_fail;
	}
	cached->bportno = bportno;
	peer->defer_portno = bportno;

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
	r = xcache_init(cached->cache, cached->max_objects,
			&c_ops, XCACHE_LRU_O1 | XCACHE_USE_RMTABLE, peer);
	if (r < 0) {
		XSEGLOG2(&lc, E, "Could initialize cache");
		goto arg_fail;
	}
	cached->max_objects = cached->cache->size; /* cache size may have changed
						     if not power of 2 */
	if (cached->max_objects < peer->nr_ops){
		XSEGLOG2(&lc, E, "Cache size should be at least nr_ops\n"
				 "\tEffective cache size %u < nr_ops: %u",
				 cached->max_objects, peer->nr_ops);
		goto arg_fail;
	}

	/* Initialize buckets */
	cached->bucket_data = malloc(cached->total_size);
	if (!cached->bucket_data) {
		XSEGLOG2(&lc, E, "Cannot allocate enough space for bucket data");
		goto cio_fail;
	}
	/* Touch the malloced space to be fast even for cold cache */
	memset(cached->bucket_data, 0, cached->total_size);

	total_buckets = cached->total_size / cached->bucket_size;

	/* Initialize bucket counters */
	bac[FREE] = total_buckets;
	bdc[INVALID] = total_buckets;

	if (!xq_alloc_seq(&cached->bucket_indexes, total_buckets, total_buckets)) {
		XSEGLOG2(&lc, E, "Cannot create bucket index");
		return -1;
	}

	/* Initialize workqs/waitqs */
	xworkq_init(&cached->workq, NULL, 0);
	xwaitq_init(&cached->pending_waitq, cache_not_full, cached, 0);
	xwaitq_init(&cached->bucket_waitq, bucket_pool_not_empty, cached, 0);
	xwaitq_init(&cached->req_waitq, req_pool_not_empty, peer, 0);

	xseg_set_max_requests(peer->xseg, peer->portno_start, 10000);
	xseg_set_freequeue_size(peer->xseg, peer->portno_start, 10000, 0);

	peer->peerd_loop = custom_cached_loop;

	print_cached(cached);
	return 0;

arg_fail:
	custom_peer_usage();
cio_fail:
	for (i = 0; i < peer->nr_ops && peer->peer_reqs[i].priv != NULL; i++)
		free(peer->peer_reqs[i].priv);
	free(cached->cache);
cache_fail:
	free(bac);
	free(bdc);
	free(cached);
fail:
	return -1;
}

void custom_peer_finalize(struct peerd *peer)
{
	struct cached *cached = __get_cached(peer);

	xcache_close(cached->cache);
	return;
}

void custom_peer_usage()
{
	fprintf(stderr, "Custom peer options: \n"
			"  ------------------------------------------------\n"
			"    -mo       | Number of ops | Max objects to cache\n"
			"    -ts       | None          | Total cache size\n"
			"    -os       | 4MiB          | Object size\n"
			"    -bs       | 4KiB          | Bucket size\n"
			"    -mrs      | 512KiB        | Max request size\n"
			"    -bp       | None          | Blocker port\n"
			"    -wcp      | writethrough  | Write policy [writethrough|writeback]\n"
			"\n");
}
