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

#include "xcache.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <xtypes/xlock.h>
#include <sys/time.h>


unsigned long sum_put = 0;
unsigned long sum_free = 0;
unsigned long sum_evict = 0;
unsigned long sum_reinsert = 0;
unsigned long sum = 0;
struct xlock lock;
uint32_t lru = 0;
unsigned long k, l, m;
pthread_barrier_t barr;

#define PT_BARRIER()									\
	rc = pthread_barrier_wait(&barr);					\
    if(rc != 0 && rc != PTHREAD_BARRIER_SERIAL_THREAD) {	\
		fprintf(stderr, "Could not wait on barrier\n");		\
		exit(-1);											\
	}

struct ce{
	unsigned long tid;
};

struct thread_arg{
	struct xcache *cache;
	unsigned long tid;
	unsigned long n;
	void *priv;
};

void *thread_test1(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n;
	unsigned long ki = 0, li = 0, mi = 0;
	xcache_handler h, nh;
	char name[XSEG_MAX_TARGETLEN];

	do {
		sprintf(name, "%lu", ki);
		h = xcache_lookup(cache, name);
		if (h != NoEntry){
			fprintf(stderr, "Cold cache returned cache entry\n");
			return NULL;
		}

		h = xcache_alloc_init(cache, name);
		if (h == NoEntry){
			fprintf(stderr, "Could not allocate cache entry\n");
			return NULL;
		}
		nh = xcache_insert(cache, h);
		if (nh == NoEntry){
			fprintf(stderr, "Could not insert cache entry\n");
			return NULL;
		} else if (nh != h) {
			fprintf(stderr, "Other cache entry found\n");
			xcache_free_new(cache, h);
		}
		xcache_put(cache, h);
	} while (ki++ < n);

	do {
		sprintf(name, "%lu", li);
		h = xcache_lookup(cache, name);
		if (h == NoEntry){
			fprintf(stderr, "Cache lookup failed\n");
			return NULL;
		}
		xcache_put(cache, h);
	} while (li++ < n);

	/* for (i = n; i < 2*n; i++) */
	do {
		sprintf(name, "%lu", mi);
		h = xcache_lookup(cache, name);
		if (h != NoEntry){
			fprintf(stderr, "Lookup returned non-inserted cache entry\n");
			return NULL;
		}

		h = xcache_alloc_init(cache, name);
		if (h == NoEntry){
			fprintf(stderr, "Could not allocate cache entry\n");
			return NULL;
		}
		nh = xcache_insert(cache, h);
		if (nh == NoEntry){
			fprintf(stderr, "Could not insert cache entry\n");
			return NULL;
		} else if (nh != h) {
			xcache_free_new(cache, h);
		}
		xcache_put(cache, h);
	} while (mi++ < 2*n);
	return NULL;
}

void *thread_test2(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	unsigned long n = targ->n;
	unsigned long i;
	xcache_handler h;
	char name[XSEG_MAX_TARGETLEN];
	int r;

	for (i = 0; i < n; i++) {
		sprintf(name, "%lu_%lu", targ->tid, i);
		h = xcache_alloc_init(targ->cache, name);
		if (h == NoEntry){
			fprintf(stderr, "Could not allocate cache entry\n");
			return NULL;
		}
		r = xcache_insert(targ->cache, h);
		if (r < 0){
			xcache_put(targ->cache, h);
			fprintf(stderr, "Could not insert cache entry\n");
			return NULL;
		}
		xcache_put(targ->cache, h);
	}
	return NULL;
}

int init(void *c, void *e)
{
	return 0;
}

void free_unsafe(void *c, void *e)
{
	sum_free++;
}

void free_safe(void *c, void *e)
{
	__sync_add_and_fetch(&sum_free, 1);
}

void reinsert_safe(void *c, void *e)
{
	__sync_add_and_fetch(&sum_reinsert, 1);
}

int evict_safe(void *c, void *e)
{
	__sync_add_and_fetch(&sum_evict, 1);
	return 0;
}

void put_safe(void *c, void *e)
{
	__sync_add_and_fetch(&sum_put, 1);
}

int put_unsafe(void *c, void *e)
{
	sum_put++;
	return 0;
}

int finalize_safe(void *c, void *e)
{
	__sync_add_and_fetch(&sum_put, 1);
	return 0;
}

int put_test(void *c, void *e)
{
	struct ce *ce = e;
	if (ce->tid){
		fprintf(stderr, "Invalid lru eviction\n");
		fprintf(stderr, "test3: failed\n");
	} else {
		fprintf(stderr, "test3: PASSED\n");
	}
	return 0;
}

void *node_init(void *c, void *xh)
{
	return malloc(sizeof(struct ce));
}

int test1(unsigned long n)
{
	struct xcache cache;
	struct xcache_ops c_ops = {
		.on_init = NULL,
		.on_evict = NULL,
		.on_free = free_safe,
		.on_node_init = NULL,
		.on_put = put_safe
	};
	xcache_handler h, nh;
	sum_put = 0;
	sum_free = 0;
	char name[XSEG_MAX_TARGETLEN + 1];
	unsigned long i;

	xcache_init(&cache, n, &c_ops, lru, NULL);
	n = cache.size;

	for (i = 0; i < n; i++) {
		sprintf(name, "%lu", i);
		h = xcache_lookup(&cache, name);
		if (h != NoEntry){
			fprintf(stderr, "Cache return cache entry\n");
			return -1;
		}

		h = xcache_alloc_init(&cache, name);
		if (h == NoEntry){
			fprintf(stderr, "Could not allocate cache entry\n");
			return -1;
		}
		nh = xcache_insert(&cache, h);
		if (nh == NoEntry){
			fprintf(stderr, "Could not insert cache entry\n");
			return -1;
		} else if (nh != h) {
			xcache_free_new(&cache, h);
		}
		xcache_put(&cache, h);
	}

	for (i = 0; i < n; i++) {
		sprintf(name, "%lu", i);
		h = xcache_lookup(&cache, name);
		if (h == NoEntry){
			fprintf(stderr, "Cache lookup failed for %s\n", name);
			return -1;
		}
		xcache_put(&cache, h);
	}

	for (i = n; i < 2*n; i++) {
		sprintf(name, "%lu", i);
		h = xcache_lookup(&cache, name);
		if (h != NoEntry){
			fprintf(stderr, "Cache return cache entry\n");
			return -1;
		}

		h = xcache_alloc_init(&cache, name);
		if (h == NoEntry){
			fprintf(stderr, "Could not allocate cache entry\n");
			return -1;
		}
		nh = xcache_insert(&cache, h);
		if (nh == NoEntry){
			fprintf(stderr, "Could not insert cache entry\n");
			return -1;
		} else if (nh != h) {
			xcache_free_new(&cache, h);
		}
		xcache_put(&cache, h);
	}
	xcache_close(&cache);
	if (sum_put != 2*n || sum_free != 2*n){
		fprintf(stderr, "Sum_free:%lu puts instead of %lu\n"
				"sum_put:%lu puts instead of %lu\n",
				sum_free, 2*n, sum_put, 2*n);
		return -1;
	}
	return 0;
}


void *thread_test2_part1(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n, i = 0;
	unsigned long tid = targ->tid;
	xcache_handler h, nh;
	char name[XSEG_MAX_TARGETLEN];

	do {
		sprintf(name, "tid:%lu_i:%lu", tid, i);
		h = xcache_lookup(cache, name);
		if (h != NoEntry){
			fprintf(stderr, "Cold cache returned cache entry\n");
			return NULL;
		}

		h = xcache_alloc_init(cache, name);
		if (h == NoEntry){
			fprintf(stderr, "Could not allocate cache entry\n");
			return NULL;
		}
		nh = xcache_insert(cache, h);
		if (nh == NoEntry){
			fprintf(stderr, "Could not insert cache entry\n");
			return NULL;
		} else if (nh != h) {
			fprintf(stderr, "Other cache entry found\n");
			xcache_free_new(cache, h);
		}
		xcache_put(cache, h);
	} while (++i < n);

	return (void *)i;
}

void *thread_test2_part2(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n, i = 0;
	unsigned long lookups = 0;
	unsigned long tid = targ->tid;
	xcache_handler h;
	char name[XSEG_MAX_TARGETLEN];

	do {
		sprintf(name, "tid:%lu_i:%lu", tid, i);
		h = xcache_lookup(cache, name);
		if (h != NoEntry){
			lookups++;
			xcache_put(cache, h);
		}
	} while (++i < n);

	return (void *)lookups;
}

void *thread_test2_part3(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n, i = 0;
	unsigned long invalidations = 0;
	unsigned long tid = targ->tid;
	char name[XSEG_MAX_TARGETLEN];
	int r;

	do {
		sprintf(name, "tid:%lu_i:%lu", tid, i);
		r =  xcache_invalidate(cache, name);
		if (r >= 0){
			invalidations++;
		}
	} while (++i < n);

	return (void *)invalidations;
}

int test2(unsigned long cache_size, unsigned long nr_threads)
{
	struct xcache cache;
	struct xcache_ops c_ops = {
		.on_init = init,
		.on_put = put_safe,
		.on_free = free_safe,
		.on_node_init = node_init
	};
	sum_free = 0;
	sum_put = 0;
	int r, i;
	unsigned long n;
	void *ret;
	unsigned long inserts = 0;
	unsigned long invalidations = 0;
	unsigned long lookups = 0;

	xcache_init(&cache, cache_size, &c_ops, lru, NULL);
	n = cache.size;

	struct thread_arg *targs = malloc(nr_threads * sizeof(struct thread_arg));
	pthread_t *threads = malloc(nr_threads * sizeof(pthread_t));

	for (i = 0; i < nr_threads; i++) {
		targs[i].tid = i+1;
		targs[i].n = n;
		targs[i].cache = &cache;
	}

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test2_part1, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		inserts += (unsigned long)ret;
	}

	if (inserts != nr_threads * n) {
		fprintf(stderr, "inserts: %lu, expected %lu\n",
				inserts, nr_threads * n);
		return -1;
	}

	if (sum_put != (nr_threads - 1) * n){
		fprintf(stderr, "After inserts: sum_put: %lu, expected %lu\n",
				sum_put, (nr_threads - 1) * n);
		return -1;
	}

	if (sum_free != (nr_threads -1) * n){
		fprintf(stderr, "After invalidate: sum_free: %lu, expected %lu\n",
				sum_free, (nr_threads - 1) * n);
		return -1;
	}

	sum_put = 0;

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test2_part2, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		lookups += (unsigned long)ret;
	}

	if (lookups != n) {
		fprintf(stderr, "lookups: %lu, expected %lu\n.",
				lookups, n);
		return -1;
	}

	if (sum_put != 0){
		fprintf(stderr, "After lookups: sum_put: %lu, expected 0.\n",
				sum_put);
		return -1;
	}

	sum_put = 0;
	sum_free = 0;

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test2_part3, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		invalidations += (unsigned long)ret;
	}

	if (invalidations != nr_threads * n) {
		fprintf(stderr, "invalidations: %lu, expected %lu\n",
				invalidations, nr_threads * n);
		return -1;
	}

	//assert sum_put == n;
	//assert sum_free == n;
	
	if (sum_put != n){
		fprintf(stderr, "After invalidate: sum_put: %lu, expected %lu\n",
				sum_put, n);
		return -1;
	}

	if (sum_free != n){
		fprintf(stderr, "After invalidate: sum_free: %lu, expected %lu\n",
				sum_free, n);
		return -1;
	}

	lookups = 0;

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test2_part2, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		lookups += (unsigned long)ret;
	}

	if (lookups) {
		fprintf(stderr, "lookups: %lu, expected 0\n",
				lookups);
		return -1;
	}

	/* reinsert data */
	inserts = 0;

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test2_part1, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		inserts += (unsigned long)ret;
	}

	if (inserts != nr_threads * n) {
		fprintf(stderr, "inserts: %lu, expected %lu\n",
				inserts, nr_threads * n);
		return -1;
	}

	/* insert even more data */
	for (i = 0; i < nr_threads; i++) {
		targs[i].tid = nr_threads+i+1;
		targs[i].n = n;
		targs[i].cache = &cache;
	}

	inserts = 0;

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test2_part1, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		inserts += (unsigned long)ret;
	}

	if (inserts != nr_threads * n) {
		fprintf(stderr, "inserts: %lu, expected %lu\n",
				inserts, nr_threads * n);
		return -1;
	}

	lookups = 0;

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test2_part2, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		lookups += (unsigned long)ret;
	}

	if (lookups != n) {
		fprintf(stderr, "lookups: %lu, expected %lu\n",
				lookups, n);
		return -1;
	}


	free(targs);
	free(threads);
	/* This should do nothing */
	xcache_close(&cache);

/*
	if (sum_put != 2*n || sum_free != 2*n){
		fprintf(stderr, "Sum_free:%lu puts instead of %lu\n"
				"sum_put:%lu puts instead of %lu\n",
				sum_free, 2*n, sum_put, 2*n);
		return -1;
	}
*/
	return 0;
}

void *thread_test3_part1(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n, i = 0;
	xcache_handler h, nh;
	char name[XSEG_MAX_TARGETLEN];

	do {
		sprintf(name, "%lu", i);
		h = xcache_lookup(cache, name);
		if (h != NoEntry){
			xcache_put(cache, h);
			continue;
		}

		h = xcache_alloc_init(cache, name);
		if (h == NoEntry){
			fprintf(stderr, "Could not allocate cache entry\n");
			return NULL;
		}
		nh = xcache_insert(cache, h);
		if (nh == NoEntry){
			fprintf(stderr, "Could not insert cache entry\n");
			return NULL;
		} else if (nh != h) {
			xcache_free_new(cache, h);
			h = nh;
		}
		xcache_put(cache, h);
	} while (++i < n);

	return (void *)i;
}

void *thread_test3_part2(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n, i = 0;
	unsigned long lookups = 0;
	xcache_handler h;
	char name[XSEG_MAX_TARGETLEN];

	do {
		sprintf(name, "%lu", i);
		h = xcache_lookup(cache, name);
		if (h != NoEntry){
			lookups++;
			xcache_put(cache, h);
		}
	} while (++i < n);

	return (void *)lookups;
}

void *thread_test3_part3(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n, i = 0;
	unsigned long invalidations = 0;
	char name[XSEG_MAX_TARGETLEN];
	int r;

	do {
		sprintf(name, "%lu", i);
		r =  xcache_invalidate(cache, name);
		if (r >= 0){
			invalidations++;
		}
	} while (++i < n);

	return (void *)invalidations;
}

void *thread_test3_part4(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n, i = 0;
	xcache_handler h, nh;
	char name[XSEG_MAX_TARGETLEN];

	do {
		sprintf(name, "%lu", n + i);
		h = xcache_lookup(cache, name);
		if (h != NoEntry){
			xcache_put(cache, h);
			continue;
		}

		h = xcache_alloc_init(cache, name);
		if (h == NoEntry){
			fprintf(stderr, "Could not allocate cache entry\n");
			return NULL;
		}
		nh = xcache_insert(cache, h);
		if (nh == NoEntry){
			fprintf(stderr, "Could not insert cache entry\n");
			return NULL;
		} else if (nh != h) {
			xcache_free_new(cache, h);
			h = nh;
		}
		xcache_put(cache, h);
	} while (++i < n);

	return (void *)i;
}

void *thread_test3_part5(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n, i = 0;
	unsigned long lookups = 0;
	xcache_handler h;
	char name[XSEG_MAX_TARGETLEN];

	do {
		sprintf(name, "%lu", n + i);
		h = xcache_lookup(cache, name);
		if (h != NoEntry){
			lookups++;
			xcache_put(cache, h);
		}
	} while (++i < n);

	return (void *)lookups;
}

int test3(unsigned long cache_size, unsigned long nr_threads)
{
	struct xcache cache;
	struct xcache_ops c_ops = {
		.on_init = init,
		.on_finalize = finalize_safe,
		.on_free = free_safe,
		.on_node_init = node_init
	};
	sum_free = 0;
	sum_put = 0;
	int r, i;
	unsigned long n;
	void *ret;
	unsigned long inserts = 0;
	unsigned long invalidations = 0;
	unsigned long lookups = 0;

	xcache_init(&cache, cache_size, &c_ops, lru, NULL);
	n = cache.size;

	struct thread_arg *targs = malloc(nr_threads * sizeof(struct thread_arg));
	pthread_t *threads = malloc(nr_threads * sizeof(pthread_t));

	for (i = 0; i < nr_threads; i++) {
		targs[i].tid = i+1;
		targs[i].n = n;
		targs[i].cache = &cache;
	}

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test3_part1, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		inserts += (unsigned long)ret;
	}

	if (inserts != nr_threads * n) {
		fprintf(stderr, "inserts: %lu, expected %lu\n",
				inserts, nr_threads * n);
		return -1;
	}

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test3_part2, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		lookups += (unsigned long)ret;
	}

	if (lookups != n * nr_threads) {
		fprintf(stderr, "lookups: %lu, expected %lu\n",
				lookups, n * nr_threads);
		return -1;
	}

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test3_part3, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		invalidations += (unsigned long)ret;
	}

	if (invalidations !=  n * nr_threads) {
		fprintf(stderr, "invalidations: %lu, expected %lu\n",
				invalidations, n * nr_threads);
		return -1;
	}

	lookups = 0;

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test2_part2, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		lookups += (unsigned long)ret;
	}

	if (lookups) {
		fprintf(stderr, "lookups: %lu, expected 0\n",
				lookups);
		return -1;
	}

	/* reinsert data */
	inserts = 0;

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test3_part1, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		inserts += (unsigned long)ret;
	}

	if (inserts != nr_threads * n) {
		fprintf(stderr, "inserts: %lu, expected %lu\n",
				inserts, nr_threads * n);
		return -1;
	}

	inserts = 0;

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test3_part4, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		inserts += (unsigned long)ret;
	}

	if (inserts != nr_threads * n) {
		fprintf(stderr, "inserts: %lu, expected %lu\n",
				inserts, nr_threads * n);
		return -1;
	}

	lookups = 0;

	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL, thread_test3_part5, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		lookups += (unsigned long)ret;
	}

	if (lookups != n * nr_threads) {
		fprintf(stderr, "lookups: %lu, expected %lu\n",
				lookups, n * nr_threads);
		return -1;
	}



	free(targs);
	free(threads);
	/* This should do nothing */
	xcache_close(&cache);

/*
	if (sum_put != 2*n || sum_free != 2*n){
		fprintf(stderr, "Sum_free:%lu puts instead of %lu\n"
				"sum_put:%lu puts instead of %lu\n",
				sum_free, 2*n, sum_put, 2*n);
		return -1;
	}
*/
	return 0;
}

void *thread_test4_part1(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	int rc;
	unsigned long n = targ->n, i = 0;
	unsigned long tid = targ->tid;
	xcache_handler h, nh;
	char name[XSEG_MAX_TARGETLEN];
	unsigned long *handlers = (unsigned long *)targ->priv;

	/* The entries inserted by each thread ammount to the cache's size */
	for (i = 0; i < n; i++) {
		sprintf(name, "tid:%lu_i:%lu_v1", tid, i);
		h = xcache_alloc_init(cache, name);
		if (h == NoEntry) {
			fprintf(stderr, "Could not allocate cache entry\n");
			return NULL;
		}
		nh = xcache_insert(cache, h);
		if (nh == NoEntry){
			fprintf(stderr, "Could not insert cache entry\n");
			return NULL;
		} else if (nh != h) {
			fprintf(stderr, "Other cache entry found\n");
			return NULL;
		}
		handlers[i] = nh;
	}

	PT_BARRIER()

	/* Alternate between inserting a new enrty and putting the old one */
	for (i = 0; i < n; i++) {
		xcache_put(cache, handlers[i]);

		sprintf(name, "tid:%lu_i:%lu_v2", tid, i);
		h = xcache_alloc_init(cache, name);
		if (h == NoEntry) {
			fprintf(stderr, "Could not allocate cache entry\n");
			return NULL;
		}
		nh = xcache_insert(cache, h);
		if (nh == NoEntry){
			fprintf(stderr, "Could not insert cache entry\n");
			return NULL;
		} else if (nh != h) {
			fprintf(stderr, "Other cache entry found\n");
			return NULL;
		}
		handlers[i] = nh;
	}
	return (void *)i;
}

void *thread_test4_part2a(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n, i = 0;
	xcache_handler nh;
	unsigned long *handlers = (unsigned long *)targ->priv;

	/* The entries inserted by each thread ammount to the cache's size */
	for (i = 0; i < n; i++) {
		nh = xcache_insert(cache, handlers[i]);
		if (nh == NoEntry){
			fprintf(stderr, "Could not insert cache entry\n");
			return NULL;
		} else if (nh != handlers[i]) {
			fprintf(stderr, "Other cache entry found\n");
			return NULL;
		}
	}
	return (void *)i;
}

void *thread_test4_part2b(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n, i = 0;
	unsigned long tid = targ->tid;
	xcache_handler h, nh;
	char name[XSEG_MAX_TARGETLEN];
	unsigned long *handlers = (unsigned long *)targ->priv;

	/* The entries inserted by each thread ammount to the cache's size */
	for (i = 0; i < n; i++) {
		sprintf(name, "tid:%lu_i:%lu_v3", tid, i);
		h = xcache_alloc_init(cache, name);
		if (h == NoEntry) {
			fprintf(stderr, "Could not allocate cache entry\n");
			return NULL;
		}
		nh = xcache_insert(cache, h);
		if (nh == NoEntry){
			fprintf(stderr, "Could not insert cache entry\n");
			return NULL;
		} else if (nh != h) {
			fprintf(stderr, "Other cache entry found\n");
			return NULL;
		}
		handlers[i] = nh;
	}
	return (void *)i;
}

void *thread_test4_part3a(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n, i = 0;
	unsigned long invalidations = 0;
	unsigned long tid = targ->tid;
	char name[XSEG_MAX_TARGETLEN];
	int r;

	do {
		sprintf(name, "tid:%lu_i:%lu_v2", tid, i);
		r =  xcache_invalidate(cache, name);
		if (r >= 0){
			invalidations++;
		}
	} while (++i < n);

	return (void *)invalidations;
}

void *thread_test4_part3b(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n, i = 0;
	unsigned long invalidations = 0;
	unsigned long tid = targ->tid;
	char name[XSEG_MAX_TARGETLEN];
	int r;

	do {
		sprintf(name, "tid:%lu_i:%lu_v3", tid, i);
		r =  xcache_invalidate(cache, name);
		if (r >= 0){
			invalidations++;
		}

	} while (++i < n);

	return (void *)invalidations;
}

void *thread_test4_part4(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = targ->n, i = 0;
	unsigned long tid = targ->tid;
	xcache_handler h, nh;
	char name[XSEG_MAX_TARGETLEN];
	unsigned long *handlers = (unsigned long *)targ->priv;

	/* The entries inserted by each thread ammount to the cache's size */
	for (i = 0; i < n; i++) {
		sprintf(name, "tid:%lu_i:%lu_v4", tid, i);
		h = xcache_alloc_init(cache, name);
		if (h == NoEntry) {
			fprintf(stderr, "Could not allocate cache entry\n");
			return NULL;
		}
		nh = xcache_insert(cache, h);
		if (nh == NoEntry){
			fprintf(stderr, "Could not insert cache entry\n");
			return NULL;
		} else if (nh != h) {
			fprintf(stderr, "Other cache entry found\n");
			return NULL;
		}
		handlers[i] = nh;
		xcache_evict_lru(cache);
	}
	return (void *)i;
}

void *thread_test4_part5(void *arg)
{
	struct thread_arg *targ = (struct thread_arg *)arg;
	struct xcache *cache = targ->cache;
	unsigned long n = cache->size, i = 0, j = 0;

	for (i = 0; i < 2 * n; i++)
		xcache_put(cache, i);
	for (j = 0; j < 3; j++) {
		for (i = 0; i < 2 * n; i++)
			xcache_get(cache, i);
		for (i = 0; i < 2 * n; i++)
			xcache_put(cache, i);
	}
	for (i = 0; i < 2 * n; i++)
		xcache_put(cache, i);

	return (void *)i;
}

int test4(unsigned long cache_size, unsigned long nr_threads)
{
	struct xcache cache;
	struct xcache_ops c_ops = {
		.on_init = init,
		.on_evict = evict_safe,
		.on_reinsert = reinsert_safe,
		.on_put = put_safe,
		.on_free = free_safe,
		.on_node_init = node_init
	};
	struct timeval start, end, tv;
	int r, i;
	unsigned long n, invalidations;
	void *ret;

	/* Initialisations */
	xcache_init(&cache, cache_size, &c_ops, lru | XCACHE_USE_RMTABLE, NULL);

	if (cache.size % nr_threads != 0 || nr_threads < 2) {
		fprintf(stderr, "Number of threads is not suitable for this test\n");
		return -1;
	}

	struct thread_arg *targs = malloc(nr_threads * sizeof(struct thread_arg));
	pthread_t *threads = malloc(nr_threads * sizeof(pthread_t));

	n = cache.size / nr_threads * 2;
	for (i = 0; i < nr_threads; i++) {
		targs[i].tid = i+1;
		targs[i].n = n;
		targs[i].cache = &cache;
		targs[i].priv = calloc(n, sizeof(unsigned long));
		if (!targs[i].priv)
			return -1;
	}

	pthread_barrier_init(&barr, NULL, nr_threads / 2);

	/*
	 * PART 1: Insert entries (v1) until cache is full. Then, each thread
	 * alternates between putting a v1 entry and inserting a new entry (v2).
	 * The ammount of v2 entries is as much as the cache's size.
	 * EXPECTATION: The v1 entries will all be put and the v2 ones will be
	 * settled in cache.
	 */
	sum_free = 0;
	sum_put = 0;

	gettimeofday(&start, NULL);
	/* We use only half threads for this */
	for (i = 0; i < nr_threads; i += 2) {
		r = pthread_create(&threads[i], NULL, thread_test4_part1, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i += 2) {
		pthread_join(threads[i], &ret);
	}
	gettimeofday(&end, NULL);

	if (sum_put != cache.size || sum_free != cache.size) {
		fprintf(stderr, "Test 4, part1: sum_put: %lu, expected %u\n",
				sum_put, cache.size);
		fprintf(stderr, "Test 4, part1: sum_free: %lu, expected %u\n",
				sum_free, cache.size);
		return -1;
	}

	timersub(&end, &start, &tv);
	fprintf(stderr, "Part1: PASSED in "
			"%ds %dusec\n\n", (int)tv.tv_sec, (int)tv.tv_usec);

	/*
	 * PART 2: Half threads re-insert the v2 entries while the other half
	 * insert new ones (v3).
	 * EXPECTATION: There will be no puts since the v2 and v3 entries are
	 * still referenced. They will all reside in cache but half of these
	 * entries will be in evicted state.
	 */
	sum_put = 0;
	sum_free = 0;
	sum_reinsert = 0;
	sum_evict = 0;

	gettimeofday(&start, NULL);
	for (i = 0; i < nr_threads; i++) {
		if (i % 2 == 0)
			r = pthread_create(&threads[i], NULL,
					thread_test4_part2a, &targs[i]);
		else
			r = pthread_create(&threads[i], NULL,
					thread_test4_part2b, &targs[i]);

		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
	}
	gettimeofday(&end, NULL);

	if (sum_put != 0 || sum_evict - sum_reinsert != cache.size) {
		fprintf(stderr, "Test 4, part2: sum_put: %lu, expected 0\n",
				sum_put);
		fprintf(stderr, "Test 4, part2: sum_evict - sum_reinsert = "
				"%lu - %lu != %u",
				sum_evict, sum_reinsert, cache.size);
		return -1;
	}

	timersub(&end, &start, &tv);
	fprintf(stderr, "Part2: PASSED in "
			"%ds %dusec\n\n", (int)tv.tv_sec, (int)tv.tv_usec);

	/*
	 * PART 3: Invalidate all entries.
	 * EXPECTATION: There will be 2 * cache.size invalidations/puts/frees
	 */
	sum_put = 0;
	sum_free = 0;
	sum_reinsert = 0;
	sum_evict = 0;
	invalidations = 0;

	gettimeofday(&start, NULL);
	for (i = 0; i < nr_threads; i++) {
		if (i % 2 == 0)
			r = pthread_create(&threads[i], NULL,
					thread_test4_part3a, &targs[i]);
		else
			r = pthread_create(&threads[i], NULL,
					thread_test4_part3b, &targs[i]);

		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
		invalidations += (unsigned long)ret;
	}
	gettimeofday(&end, NULL);

	if (invalidations != 2 * cache.size ||
			sum_put != 0 ||
			sum_free != 0) {
		fprintf(stderr, "Test 4, part3: invalidations: %lu, expected %u\n",
				invalidations, 2 * cache.size);
		fprintf(stderr, "Test 4, part3: sum_put: %lu, expected 0\n",
				sum_put);
		fprintf(stderr, "Test 4, part3: sum_free: %lu, expected 0\n",
				sum_free);
		return -1;
	}

	timersub(&end, &start, &tv);
	fprintf(stderr, "Part3: PASSED in "
			"%ds %dusec\n\n", (int)tv.tv_sec, (int)tv.tv_usec);

	fprintf(stderr, "The rest of the tests are reserved for future use.\n"
			"xcache is not ready to support them.\n");
	return 0;

	xcache_close(&cache);

	if (sum_put != cache.size || sum_free != cache.size) {
		fprintf(stderr, "Test 4, part3.5: sum_put: %lu, expected %u\n",
				sum_put, cache.size);
		fprintf(stderr, "Test 4, part3.5: sum_free: %lu, expected %u\n",
				sum_free, cache.size);
		return -1;
	}

	/*
	 * PART 4: Insert new entries (v4) and evict them.
	 * EXPECTATION: There will be cache.size evictions and no puts
	 */
	sum_put = 0;
	sum_free = 0;
	sum_reinsert = 0;
	sum_evict = 0;

	gettimeofday(&start, NULL);
	for (i = 0; i < nr_threads; i++) {
		r = pthread_create(&threads[i], NULL,
				thread_test4_part4, &targs[i]);
		if (r) {
			fprintf(stderr, "error pthread_create\n");
			return -1;
		}
	}

	for (i = 0; i < nr_threads; i++) {
		pthread_join(threads[i], &ret);
	}
	gettimeofday(&end, NULL);

	if (sum_evict != cache.size || sum_put != 0) {
		fprintf(stderr, "Test 4, part4: sum_evict: %lu, expected %u\n",
				sum_evict, cache.size);
		fprintf(stderr, "Test 4, part4: sum_put: %lu, expected 0\n",
				sum_put);
		return -1;
	}

	timersub(&end, &start, &tv);
	fprintf(stderr, "Part4: PASSED in "
			"%ds %dusec\n\n", (int)tv.tv_sec, (int)tv.tv_usec);

	return 0;
}

int test5(unsigned long n)
{
	struct xcache cache;
	struct xcache_ops c_ops = {
		.on_init = NULL,
		.on_evict = NULL,
		.on_free = free_safe,
		.on_node_init = NULL,
		.on_put = put_safe
	};
	xcache_handler h, nh;
	sum_put = 0;
	sum_free = 0;
	char name[XSEG_MAX_TARGETLEN + 1];
	unsigned long i;

	xcache_init(&cache, n, &c_ops, lru | XCACHE_USE_RMTABLE, NULL);
	n = cache.size;

	for (i = 0; i < n; i++) {
		sprintf(name, "%lu", i);
		h = xcache_lookup(&cache, name);
		if (h != NoEntry){
			fprintf(stderr, "Cache return cache entry\n");
			return -1;
		}

		h = xcache_alloc_init(&cache, name);
		if (h == NoEntry){
			fprintf(stderr, "Could not allocate cache entry\n");
			return -1;
		}
		nh = xcache_insert(&cache, h);
		if (nh == NoEntry){
			fprintf(stderr, "Could not insert cache entry\n");
			return -1;
		} else if (nh != h) {
			xcache_free_new(&cache, h);
		}
	}

	do {
		h = xcache_evict_lru(&cache);
	} while (h != NoEntry);

	xcache_close(&cache);
	if (sum_put != n || sum_free != n){
		fprintf(stderr, "Sum_free:%lu puts instead of %lu\n"
				"sum_put:%lu puts instead of %lu\n",
				sum_free, n, sum_put, n);
		return -1;
	}
	return 0;
}

void usage()
{
	fprintf(stdout,
		"Usage: ./xcache_test <cache_size> <lru> <nr_threads> <n>\n"
		"----------------------------------------------------------\n"
		"[test1]\tLookup in cold cache if any entry is there. There must be "
		"none.\n\tThen, insert <cache_size> entries in cache and check for "
		"errors.\n\tLookup these new entries and verify that they're "
		"in cache.\n\tFinally, close the cache.\n"
		"\n"
		"[test2]\tCreate <nr_threads> threads and assign the work of test1 "
		"to them.\n\tThese threads greedily try to compete for every "
		" xcache_* operation of test1,\n\twhile trying to insert exactly "
		"the same number of entries.\n\tThe sole synchronization "
		"between threads are two barriers and some atomic operations,\n\t"
		"so it is a good indication of how well xcache scales.\n"
		"\n"
		"[test3]\tCreate <nr_threads> threads and order each of them to do "
		"<n> insertions in cache.\n");
}

int main(int argc, const char *argv[])
{
	struct timeval start, end, tv;
	int r;

	if (argc < 5) {
		usage();
		return 1;
	}

	int cache_size = atoi(argv[1]);
	int lru_type = atoi(argv[2]);
	int t = atoi(argv[3]);
	int pref = atoi(argv[4]);

	switch (lru_type) {
		case XCACHE_LRU_ARRAY:
			printf("LRU is Array\n");
			break;
		case XCACHE_LRU_HEAP:
			printf("LRU is Heap\n");
			break;
		case XCACHE_LRU_O1:
			printf("LRU is O(1)\n");
			break;
		default:
			fprintf(stderr, "Wrong LRU type. Exiting...\n");
			return -1;
	}

	if (pref < 0 || pref > 1) {
		usage();
		fprintf(stderr, "Wrong preference\n");
		return -1;
	}

	lru = lru_type;
	if (pref == 1)
		goto rmtable;

	fprintf(stderr, "Running test1\n");
	gettimeofday(&start, NULL);
	r = test1(cache_size);
	if (r < 0){
		fprintf(stderr, "Test1: FAILED\n");
		return -1;
	}
	gettimeofday(&end, NULL);
	timersub(&end, &start, &tv);
	fprintf(stderr, "Test1: PASSED\n");
	fprintf(stderr, "Test time: %ds %dusec\n\n", (int)tv.tv_sec, (int)tv.tv_usec);

	fprintf(stderr, "running test2\n");
	gettimeofday(&start, NULL);
	r = test2(cache_size, t);
	gettimeofday(&end, NULL);
	timersub(&end, &start, &tv);
	if (r < 0){
		fprintf(stderr, "test2: failed\n");
		return -1;
	}
	fprintf(stderr, "test2: PASSED\n");
	fprintf(stderr, "Test time: %ds %dusec\n\n", (int)tv.tv_sec, (int)tv.tv_usec);

	XSEGLOG("Starting new test");
	fprintf(stderr, "running test3\n");
	gettimeofday(&start, NULL);
	r = test3(cache_size, t);
	if (r < 0){
		fprintf(stderr, "test3: failed\n");
		return -1;
	}
	gettimeofday(&end, NULL);
	timersub(&end, &start, &tv);
	fprintf(stderr, "test3: PASSED\n");
	fprintf(stderr, "Test time: %ds %dusec\n\n", (int)tv.tv_sec, (int)tv.tv_usec);

rmtable:
	XSEGLOG("Starting new test\n");
	fprintf(stderr, "Running Test 4\n");
	gettimeofday(&start, NULL);
	r = test4(cache_size, t);
	if (r < 0){
		fprintf(stderr, "Test 4: failed\n");
		return -1;
	}
	gettimeofday(&end, NULL);
	fprintf(stderr, "Test 4: PASSED\n");
	timersub(&end, &start, &tv);
	fprintf(stderr, "Test time: %ds %dusec\n\n", (int)tv.tv_sec, (int)tv.tv_usec);

	XSEGLOG("Starting new test\n");
	fprintf(stderr, "Running Test 5\n");
	gettimeofday(&start, NULL);
	r = test5(cache_size);
	if (r < 0){
		fprintf(stderr, "Test 5: failed\n");
		return -1;
	}
	gettimeofday(&end, NULL);
	fprintf(stderr, "Test 5: PASSED\n");
	timersub(&end, &start, &tv);
	fprintf(stderr, "Test time: %ds %dusec\n\n", (int)tv.tv_sec, (int)tv.tv_usec);

	return 0;
}
