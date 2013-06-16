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

#define MAX_ARG_LEN 12

/*
 * Bucket status flags:
 *
 * Allocation status occupies 1st flag bit. It can either be FREE (unallocated)
 * or claimed.
 * Data status occupied 2nd-4th flag bit. It can be:
 *
 * 1) INVALID: Its content is junk
 * 2) LOADING: There is a pending read for this bucket
 * 3) VALID: Its content is in sync with the storage
 * 4) DIRTY: Its content is newer than the storage's
 * 5) WRITING: There is pending flush for this bucket
 */

#define BUCKET_ALLOC_STATUSES 2
#define BUCKET_ALLOC_STATUS_FLAG_POS 0
#define BUCKET_ALLOC_STATUS_BITMASK 1
#define FREE 0
#define CLAIMED   1

#define BUCKET_DATA_STATUSES 5
#define BUCKET_DATA_STATUS_FLAG_POS 1
#define BUCKET_DATA_STATUS_BITMASK 7	/* i.e. "111" in binary form */
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
	(__flag & ((uint32_t)__ftype##_BITMASK << __ftype##_FLAG_POS)) >> \
	(uint32_t)__ftype##_FLAG_POS

/* write policies */
#define WRITETHROUGH 1
#define WRITEBACK    2

#define WRITE_POLICY(__wcp)				\
	(__wcp == WRITETHROUGH	? "writethrough" :	\
	__wcp == WRITEBACK	? "writeback"	 :	\
	"undefined")

#define MIN(__a__, __b__) ((__a__ < __b__) ? __a__ : __b__)

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

struct cached_stats {
	struct xlock lock;
	volatile uint64_t evicted;
};

struct cached {
	struct xcache *cache;
	uint64_t total_size; /* Total cache size (bytes) */
	uint64_t max_objects; /* Max number of objects (plain) */
	uint64_t max_req_size; /* Max request size to blocker (bytes) */
	uint32_t object_size; /* Max object size (bytes) */
	uint32_t bucket_size; /* Bucket size (bytes) */
	uint32_t buckets_per_object; /* Max buckets per object (plain) */
	xport bportno;
	int write_policy;
	struct xworkq workq;
	struct xwaitq pending_waitq;
	struct xwaitq bucket_waitq;
	struct xwaitq req_waitq;
	unsigned char *bucket_data;
	struct xq bucket_indexes;
	struct cached_stats stats;
	//scheduler
};

struct bucket {
	unsigned char *data;
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

static int custom_cached_loop(void *arg);
