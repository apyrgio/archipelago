/*
 * Copyright 2012 GRNET S.A. All rights reserved.
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
#include <stdlib.h>
#include <unistd.h>
#include <xseg/xseg.h>
#include <peer.h>
#include <xseg/protocol.h>
#include <glusterfs/api/glfs.h>
#include <pthread.h>
#include <openssl/sha.h>
#include <ctype.h>
#include <errno.h>
#include <hash.h>


#define LOCK_SUFFIX "_lock"
#define LOCK_SUFFIX_LEN 5
#define HASH_SUFFIX "_hash"
#define HASH_SUFFIX_LEN 5

#define MAX_POOL_NAME 64
#define MAX_OBJ_NAME (XSEG_MAX_TARGETLEN + LOCK_SUFFIX_LEN + 1)
#define RADOS_LOCK_NAME "RadosLock"
//#define RADOS_LOCK_COOKIE "Cookie"
#define RADOS_LOCK_COOKIE "foo"
#define RADOS_LOCK_TAG ""
#define RADOS_LOCK_DESC ""

#define MAX_GLFS_ARG_LEN 64

void custom_peer_usage()
{
	fprintf(stderr, "Custom peer options:\n"
		"--transport: transport type [tcp|rdma|unix] (default: tcp)\n"
		"--server: a server of the gluster pool (default: 127.0.0.1)\n"
		"--port: port of server's gluster daemon (defalut: 0(=24007))\n"
		"--volume: gluster volume to connect\n"
		"\n");
}

enum gluster_state {
	ACCEPTED = 0,
	PENDING = 1,
	READING = 2,
	WRITING = 3,
	STATING = 4,
	PREHASHING = 5,
	POSTHASHING = 6
};

struct glusterd {
	glfs_t *glfs;
	//char volume[MAX_volume_NAME + 1];
};

struct gluster_io{
	char obj_name[MAX_OBJ_NAME + 1];
	glfs_fd_t *fd;
	enum gluster_state state;
	uint64_t size;
	char *second_name, *buf;
	uint64_t read;
	uint64_t watch_handle;
	pthread_t tid;
	pthread_cond_t cond;
	pthread_mutex_t m;
};

static inline struct glusterd *__get_gluster(struct peerd *peer)
{
	return peer->priv;
}

static inline int __set_glfs(struct glusterd *gluster, char *volume)
{
	gluster->glfs = glfs_new(volume);

	if (!gluster->glfs)
		return -1;
	return 0;
}

static inline glfs_t *__get_glfs(struct glusterd *gluster)
{
	return gluster->glfs;
}

static void gluster_complete_aio(struct glfs_fd *fd, ssize_t ret, void *data)
{
	struct peer_req *pr = (struct peer_req*)data;
	struct peerd *peer = pr->peer;

	pr->retval = ret;
	dispatch(peer, pr, pr->req, dispatch_internal);
}

#if 0
void gluster_ack_cb(gluster_completion_t c, void *arg)
{
	struct peer_req *pr = (struct peer_req*) arg;
	struct peerd *peer = pr->peer;
	int ret = gluster_aio_get_return_value(c);
	pr->retval = ret;
	rados_aio_release(c);
	dispatch(peer, pr, pr->req, dispatch_internal);
}

void rados_commit_cb(rados_completion_t c, void *arg)
{
	struct peer_req *pr = (struct peer_req*) arg;
	struct peerd *peer = pr->peer;
	int ret = rados_aio_get_return_value(c);
	pr->retval = ret;
	rados_aio_release(c);
	dispatch(peer, pr, pr->req, dispatch_internal);
}
#endif

static int do_aio_generic(struct peerd *peer, struct peer_req *pr, uint32_t op,
		char *target, char *buf, uint64_t size, uint64_t offset)
{
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	glfs_fd_t *fd = gio->fd;
	int r;

	switch (op) {
		case X_READ:
			/*
			r = gluster_aio_create_completion(pr, gluster_ack_cb, NULL, &gluster_compl);
			if (r < 0)
				return -1;
			r = gluster_aio_read(gluster->ioctx, target, gluster_compl,
					buf, size, offset);
					*/
			r = glfs_pread_async(fd, buf, size, offset,
					0, gluster_complete_aio, pr);
			break;
		case X_WRITE:
			/*r = gluster_aio_create_completion(pr, NULL, gluster_commit_cb, &gluster_compl);
			if (r < 0)
				return -1;
			r = gluster_aio_write(gluster->ioctx, target, gluster_compl,
					buf, size, offset);
					*/
			r = glfs_pwrite_async(fd, buf, size, offset,
					0, gluster_complete_aio, pr);
			break;
#if 0
		case X_DELETE:
			r = gluster_aio_create_completion(pr, gluster_ack_cb, NULL, &gluster_compl);
			if (r < 0)
				return -1;
			r = gluster_aio_remove(gluster->ioctx, target, gluster_compl);
			break;
		case X_INFO:
			r = gluster_aio_create_completion(pr, gluster_ack_cb, NULL, &gluster_compl);
			if (r < 0)
				return -1;
			r = gluster_aio_stat(gluster->ioctx, target, gluster_compl, &gio->size, NULL); 
			break;
#endif
		default:
			return -1;
			break;
	}
	return r;
}

static int do_aio_read(struct peerd *peer, struct peer_req *pr)
{
	struct xseg_request *req = pr->req;
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	char *data = xseg_get_data(peer->xseg, pr->req);

	return do_aio_generic(peer, pr, X_READ, gio->obj_name,
			data + req->serviced,
			req->size - req->serviced,
			req->offset + req->serviced);
}

static int do_aio_write(struct peerd *peer, struct peer_req *pr)
{
	struct xseg_request *req = pr->req;
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	char *data = xseg_get_data(peer->xseg, pr->req);

	return do_aio_generic(peer, pr, X_WRITE, gio->obj_name,
			data + req->serviced,
			req->size - req->serviced,
			req->offset + req->serviced);
}

#if 0
static int resubmit_aio_write(struct peerd *peer, struct peer_req *pr, int ret)
{
	struct xseg_request *req = pr->req;
	struct gluster_io *gio = (struct gluster_io *)pr->priv;
	char *data = xseg_get_data(peer->xseg, pr->req);

	gio->serviced += ret;
	gio->offset += ret;




}
#endif

/* FIXME: Do we really need target or we can get away with pr as do_aio_write */
static glfs_fd_t *do_block_open(struct peer_req *pr, char *target, int mode)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	glfs_t *glfs = __get_glfs(gluster);
	glfs_fd_t *fd;

	fd = glfs_open(glfs, target, O_RDWR);
	if (fd || !(mode & O_CREAT)) {
		return fd;
	}

	fd = glfs_creat(glfs, target,
		O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);

	return fd;
}

#if 0
int handle_delete(struct peerd *peer, struct peer_req *pr)
{
	int r;
	//struct glusterd *gluster = (struct glusterd *) peer->priv;
	struct gluster_io *gio = (struct gluster_io *) pr->priv;

	XSEGLOG2(&lc, D, "Started for pr %p, req %p, target %s",
			pr, req, gio->obj_name);


	if (gio->state == ACCEPTED) {
		XSEGLOG2(&lc, I, "Deleting %s", gio->obj_name);
		gio->state = PENDING;
		r = do_aio_generic(peer, pr, X_DELETE, gio->obj_name, NULL, 0, 0);
		if (r < 0) {
			XSEGLOG2(&lc, E, "Deletion of %s failed", gio->obj_name);
			fail(peer, pr);
		}
	}
	else {
		if (pr->retval < 0){
			XSEGLOG2(&lc, E, "Deletion of %s failed", gio->obj_name);
			fail(peer, pr);
		}
		else {
			XSEGLOG2(&lc, I, "Deletion of %s completed", gio->obj_name);
			complete(peer, pr);
		}
	}
	return 0;
}

int handle_info(struct peerd *peer, struct peer_req *pr)
{
	int r;
	struct xseg_request *req = pr->req;
	//struct glusterd *gluster = (struct glusterd *) peer->priv;
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	char *req_data;
	struct xseg_reply_info *xinfo;
	char buf[XSEG_MAX_TARGETLEN + 1];
	char *target;

	XSEGLOG2(&lc, D, "Started for pr %p, req %p, target %s",
			pr, req, gio->obj_name);

	if (gio->state == ACCEPTED) {
		XSEGLOG2(&lc, I, "Getting info of %s", gio->obj_name);
		gio->state = PENDING;
		r = do_aio_generic(peer, pr, X_INFO, gio->obj_name, NULL, 0, 0);
		if (r < 0) {
			XSEGLOG2(&lc, E, "Getting info of %s failed", gio->obj_name);	
			fail(peer, pr);
		}
	}
	else {
		if (req->datalen < sizeof(struct xseg_reply_info)) {
			target = xseg_get_target(peer->xseg, req);
			strncpy(buf, target, req->targetlen);
			r = xseg_resize_request(peer->xseg, req, req->targetlen, sizeof(struct xseg_reply_info));
			if (r < 0) {
				XSEGLOG2(&lc, E, "Cannot resize request");
				fail(peer, pr);
				return 0;
			}
			target = xseg_get_target(peer->xseg, req);
			strncpy(target, buf, req->targetlen);
		}

		req_data = xseg_get_data(peer->xseg, req);
		xinfo = (struct xseg_reply_info *)req_data;
		if (pr->retval < 0){
			xinfo->size = 0;
			XSEGLOG2(&lc, E, "Getting info of %s failed", gio->obj_name);	
			fail(peer, pr);
		}
		else {
			xinfo->size = gio->size;
			pr->retval = sizeof(uint64_t);
			XSEGLOG2(&lc, I, "Getting info of %s completed", gio->obj_name);	
			complete(peer, pr);
		}
	}
	return 0;
}
#endif

void handle_ping(struct peerd *peer, struct peer_req *pr)
{
	XSEGLOG2(&lc, D, "Ping accepted. Acknowledging...");

	complete(peer, pr);
}

int handle_read(struct peerd *peer, struct peer_req *pr)
{
	struct gluster_io *gio = (struct gluster_io *) (pr->priv);
	struct xseg_request *req = pr->req;
	glfs_fd_t *fd;
	char *data = xseg_get_data(peer->xseg, pr->req);

	XSEGLOG2(&lc, D, "Started for pr %p, req %p, target %s",
			pr, req, gio->obj_name);

	if (req->datalen < req->size) {
		XSEGLOG2(&lc, E, "Request datalen is less than req size");
		return -1;
	}

	if (gio->state == ACCEPTED) {
		if (!req->size) {
			complete(peer, pr);
			return 0;
		}

		fd = do_block_open(pr, gio->obj_name, 0);
		/* Get correct status */
		if (!fd) {
			XSEGLOG2(&lc, I, "Object %s does not exist"
					"Serving zero data\n", gio->obj_name);
			/* object not found. return zeros instead */
			memset(data, 0, req->size);
			req->serviced = req->size;
			complete(peer, pr);
			return 0;
		}
		gio->fd = fd;

		gio->state = READING;
		XSEGLOG2(&lc, I, "Reading %s", gio->obj_name);
		if (do_aio_read(peer, pr) < 0) {
			XSEGLOG2(&lc, I, "Reading of %s failed on do_aio_read",
						gio->obj_name);
			fail(peer, pr);
		}
	}
	else if (gio->state == READING) {
		XSEGLOG2(&lc, I, "Reading of %s callback", gio->obj_name);
		if (pr->retval > 0)
			req->serviced += pr->retval;
		else if (pr->retval == 0) {
			XSEGLOG2(&lc, I, "Reading of %s reached end of file at "
				"%llu bytes. Zeroing out rest", gio->obj_name,
				(unsigned long long) req->serviced);
			/* reached end of object. zero out rest of data
			 * requested from this object
			 */
			memset(data + req->serviced, 0, req->size - req->serviced);
			req->serviced = req->size;
		}
		else {
			XSEGLOG2(&lc, E, "Reading of %s failed", gio->obj_name);
			/* pr->retval < 0 && pr->retval != -2 */
			fail(peer, pr);
			return 0;
		}
		if (req->serviced >= req->size) {
			XSEGLOG2(&lc, I, "Reading of %s completed", gio->obj_name);
			complete(peer, pr);
			return 0;
		}
		if (!req->size) {
			/* should not happen */
			fail(peer, pr);
			return 0;
		}
		/* resubmit */
		XSEGLOG2(&lc, I, "Resubmitting read of %s", gio->obj_name);
		if (do_aio_read(peer, pr) < 0) {
			XSEGLOG2(&lc, E, "Reading of %s failed on do_aio_read",
					gio->obj_name);
			fail(peer, pr);
		}
	}
	else {
		/* should not reach this */
		printf("read request reached this\n");
		fail(peer, pr);
	}
	return 0;
}

int handle_write(struct peerd *peer, struct peer_req *pr)
{
	struct gluster_io *gio = (struct gluster_io *) (pr->priv);
	struct xseg_request *req = pr->req;
	glfs_fd_t *fd;

	XSEGLOG2(&lc, D, "Started for pr %p, req %p, target %s",
			pr, req, gio->obj_name);

	if (pr->req->datalen < pr->req->size) {
		XSEGLOG2(&lc, D, "Request datalen is less than req size");
		return -1;
	}

	if (gio->state == ACCEPTED) {
		XSEGLOG2(&lc, D, "In accepted state: pr %p, req %p, target %s",
				pr, req, gio->obj_name);

		if (!req->size) {
			// for future use
			if (req->flags & XF_FLUSH) {
				complete(peer, pr);
				return 0;
			}
			else {
				complete(peer, pr);
				return 0;
			}
		}

		fd = do_block_open(pr, gio->obj_name, O_CREAT);
		if (!fd) {
			XSEGLOG2(&lc, E, "Cannot open/create %s",
					gio->obj_name);
			fail(peer, pr);
			return 0;
		}
		gio->fd = fd;

		//should we ensure req->op = X_READ ?
		gio->state = WRITING;
		XSEGLOG2(&lc, I, "Writing %s", gio->obj_name);
		if (do_aio_write(peer, pr) < 0) {
			XSEGLOG2(&lc, E, "Writing of %s failed on do_aio_write",
					gio->obj_name);
			fail(peer, pr);
		}
	} else if (gio->state == WRITING) {
		XSEGLOG2(&lc, I, "Writing of %s callback", gio->obj_name);
		if (pr->retval + req->serviced == req->size) {
			XSEGLOG2(&lc, I, "Writing of %s completed",
					gio->obj_name);
			req->serviced = req->size;
			complete(peer, pr);
			return 0;
		} else if (pr->retval >= 0) {
			XSEGLOG2(&lc, I, "Resubmiting write for %s",
					gio->obj_name);
			req->serviced += pr->retval;
			if (do_aio_write(peer, pr) < 0) {
				XSEGLOG2(&lc, E, "Writing of %s failed on"
						"do_aio_write", gio->obj_name);
				fail(peer, pr);
			}
		} else {
			XSEGLOG2(&lc, E, "Writing of %s failed", gio->obj_name);
			fail(peer, pr);
			return 0;
		}
	}
	else {
		/* should not reach this */
		printf("write request reached this\n");
		fail(peer, pr);
	}
	return 0;
}

#if 0
int handle_copy(struct peerd *peer, struct peer_req *pr)
{
	//struct glusterd *gluster = (struct glusterd *) peer->priv;
	struct xseg_request *req = pr->req;
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	int r;
	struct xseg_request_copy *xcopy = (struct xseg_request_copy *)xseg_get_data(peer->xseg, req);

	XSEGLOG2(&lc, D, "Started for pr %p, req %p, target %s",
			pr, req, gio->obj_name);

	if (gio->state == ACCEPTED){
		XSEGLOG2(&lc, I, "Copy of object %s to object %s started",
				gio->second_name, gio->obj_name);
		if (!req->size) {
			complete(peer, pr); //or fail?
			return 0;
		}

		gio->second_name = malloc(MAX_OBJ_NAME + 1);
		if (!gio->second_name){
			fail(peer, pr);
			return -1;
		}
		//NULL terminate or fail if targetlen > MAX_OBJ_NAME ?
		unsigned int end = (xcopy->targetlen > MAX_OBJ_NAME) ? MAX_OBJ_NAME : xcopy->targetlen;
		strncpy(gio->second_name, xcopy->target, end);
		gio->second_name[end] = 0;

		gio->buf = malloc(req->size);
		if (!gio->buf) {
			r = -1;
			goto out_src;
		}

		gio->state = READING;
		gio->read = 0;
		XSEGLOG2(&lc, I, "Reading %s", gio->second_name);
		if (do_aio_generic(peer, pr, X_READ, gio->second_name, gio->buf + gio->read,
			req->size - gio->read, req->offset + gio->read) < 0) {
			XSEGLOG2(&lc, I, "Reading of %s failed on do_aio_read", gio->obj_name);
			fail(peer, pr);
			r = -1;
			goto out_buf;
		}
	}
	else if (gio->state == READING){
		XSEGLOG2(&lc, I, "Reading of %s callback", gio->obj_name);
		if (pr->retval > 0)
			gio->read += pr->retval;
		else if (pr->retval == 0) {
			XSEGLOG2(&lc, I, "Reading of %s reached end of file at "
				"%llu bytes. Zeroing out rest",	gio->obj_name,
				(unsigned long long) req->serviced);
			memset(gio->buf + gio->read, 0, req->size - gio->read);
			gio->read = req->size ;
		}
		else {
			XSEGLOG2(&lc, E, "Reading of %s failed", gio->second_name);
			r = -1;
			goto out_buf;
		}

		if (gio->read >= req->size) {
			XSEGLOG2(&lc, I, "Reading of %s completed", gio->obj_name);
			//do_aio_write
			gio->state = WRITING;
			XSEGLOG2(&lc, I, "Writing %s", gio->obj_name);
			if (do_aio_generic(peer, pr, X_WRITE, gio->obj_name,
					gio->buf, req->size, req->offset) < 0) {
				XSEGLOG2(&lc, E, "Writing of %s failed on do_aio_write", gio->obj_name);
				r = -1;
				goto out_buf;
			}
			return 0;
		}

		XSEGLOG2(&lc, I, "Resubmitting read of %s", gio->obj_name);
		if (do_aio_generic(peer, pr, X_READ, gio->second_name, gio->buf + gio->read,
			req->size - gio->read, req->offset + gio->read) < 0) {
			XSEGLOG2(&lc, E, "Reading of %s failed on do_aio_read",
					gio->obj_name);
			r = -1;
			goto out_buf;
		}
	}
	else if (gio->state == WRITING){
		XSEGLOG2(&lc, I, "Writing of %s callback", gio->obj_name);
		if (pr->retval == 0) {
			XSEGLOG2(&lc, I, "Writing of %s completed", gio->obj_name);
			XSEGLOG2(&lc, I, "Copy of object %s to object %s completed", gio->second_name, gio->obj_name);
			req->serviced = req->size;
			r = 0;
			goto out_buf;
		}
		else {
			XSEGLOG2(&lc, E, "Writing of %s failed", gio->obj_name);
			XSEGLOG2(&lc, E, "Copy of object %s to object %s failed", gio->second_name, gio->obj_name);
			r = -1;
			goto out_buf;
		}
	}
	else {
		XSEGLOG2(&lc, E, "Unknown state");
	}
	return 0;


out_buf:
	free(gio->buf);
out_src:
	free(gio->second_name);

	gio->buf = NULL;
	gio->second_name = NULL;
	gio->read = 0;

	if (r < 0)
		fail(peer ,pr);
	else
		complete(peer, pr);
	return 0;
}

int handle_hash(struct peerd *peer, struct peer_req *pr)
{
	//struct glusterd *gluster = (struct glusterd *) peer->priv;
	struct xseg_request *req = pr->req;
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	uint64_t trailing_zeros = 0;
	unsigned char sha[SHA256_DIGEST_SIZE];
	struct xseg_reply_hash *xreply;
	int r;
	char hash_name[HEXLIFIED_SHA256_DIGEST_SIZE + 1];
	uint32_t pos;

	XSEGLOG2(&lc, D, "Started for pr %p, req %p, target %s",
			pr, req, gio->obj_name);

	if (gio->state == ACCEPTED){
		XSEGLOG2(&lc, I, "Starting hashing of object %s", gio->obj_name);
		if (!req->size) {
			fail(peer, pr); //or fail?
			return 0;
		}

		gio->second_name = malloc(MAX_OBJ_NAME+1);
		if (!gio->second_name){
			return -1;
		}
		gio->buf = malloc(req->size);
		if (!gio->buf) {
			r = -1;
			goto out_src;
		}

		gio->second_name[0] = 0;
		gio->state = PREHASHING;
		pos = 0;
		strncpy(hash_name, gio->obj_name, strlen(gio->obj_name));
		pos += strlen(gio->obj_name);
		strncpy(hash_name+pos, HASH_SUFFIX, HASH_SUFFIX_LEN);
		pos += HASH_SUFFIX_LEN;
		hash_name[pos] = 0;

		if (do_aio_generic(peer, pr, X_READ, hash_name, gio->second_name,
			HEXLIFIED_SHA256_DIGEST_SIZE, 0) < 0) {
			XSEGLOG2(&lc, I, "Reading of %s failed on do_aio_read", gio->obj_name);
			fail(peer, pr);
			r = -1;
			goto out_buf;
		}
	} else if (gio->state == PREHASHING) {
		if (gio->second_name[0] != 0) {
			XSEGLOG2(&lc, D, "Precalculated hash found");
			xreply = (struct xseg_reply_hash*)xseg_get_data(peer->xseg, req);
			r = xseg_resize_request(peer->xseg, pr->req, pr->req->targetlen,
					sizeof(struct xseg_reply_hash));
			strncpy(xreply->target, gio->second_name, HEXLIFIED_SHA256_DIGEST_SIZE);
			xreply->targetlen = HEXLIFIED_SHA256_DIGEST_SIZE;

			XSEGLOG2(&lc, I, "Calculated %s as hash of %s",
					gio->second_name, gio->obj_name);
			req->serviced = req->size;
			goto out_buf;

		}
		gio->state = READING;
		gio->read = 0;
		XSEGLOG2(&lc, I, "Reading %s", gio->obj_name);
		if (do_aio_generic(peer, pr, X_READ, gio->obj_name, gio->buf + gio->read,
			req->size - gio->read, req->offset + gio->read) < 0) {
			XSEGLOG2(&lc, I, "Reading of %s failed on do_aio_read", gio->obj_name);
			fail(peer, pr);
			r = -1;
			goto out_buf;
		}
	} else if (gio->state == READING){
		XSEGLOG2(&lc, I, "Reading of %s callback", gio->obj_name);
		if (pr->retval >= 0)
			gio->read += pr->retval;
		else {
			XSEGLOG2(&lc, E, "Reading of %s failed", gio->second_name);
			r = -1;
			goto out_buf;
		}

		if (!pr->retval || gio->read >= req->size) {
			XSEGLOG2(&lc, I, "Reading of %s completed", gio->obj_name);
			//rstrip here in case zeros were written in the end
			for (;trailing_zeros < gio->read; trailing_zeros++)
				if (gio->buf[gio->read-trailing_zeros -1])
					break;
			XSEGLOG2(&lc, D, "Read %llu, Trainling zeros %llu",
					gio->read, trailing_zeros);

			gio->read -= trailing_zeros;
			SHA256((unsigned char *) gio->buf, gio->read, sha);
			hexlify(sha, SHA256_DIGEST_SIZE, gio->second_name);
			gio->second_name[HEXLIFIED_SHA256_DIGEST_SIZE] = 0;

			xreply = (struct xseg_reply_hash*)xseg_get_data(peer->xseg, req);
			r = xseg_resize_request(peer->xseg, pr->req, pr->req->targetlen,
					sizeof(struct xseg_reply_hash));
			strncpy(xreply->target, gio->second_name, HEXLIFIED_SHA256_DIGEST_SIZE);
			xreply->targetlen = HEXLIFIED_SHA256_DIGEST_SIZE;

			XSEGLOG2(&lc, I, "Calculated %s as hash of %s",
					gio->second_name, gio->obj_name);


			//aio_stat
			gio->state = STATING;
			r = do_aio_generic(peer, pr, X_INFO, gio->second_name, NULL, 0, 0);
			if (r < 0){
				XSEGLOG2(&lc, E, "Stating %s failed", gio->second_name);
				r = -1;
				goto out_buf;
			}
			return 0;
		}
		XSEGLOG2(&lc, I, "Resubmitting read of %s", gio->obj_name);
		if (do_aio_generic(peer, pr, X_READ, gio->obj_name, gio->buf + gio->read,
			req->size - gio->read, req->offset + gio->read) < 0) {
			XSEGLOG2(&lc, E, "Reading of %s failed on do_aio_read",
					gio->obj_name);
			r = -1;
			goto out_buf;
		}
		return 0;
	} else if (gio->state == STATING){
		if (pr->retval < 0){
			//write
			XSEGLOG2(&lc, I, "Stating %s failed. Writing.",
							gio->second_name);
			gio->state = WRITING;
			if (do_aio_generic(peer, pr, X_WRITE, gio->second_name,
						gio->buf, gio->read, 0) < 0) {
				XSEGLOG2(&lc, E, "Writing of %s failed on do_aio_write", gio->second_name);
				r = -1;
				goto out_buf;
			}
			return 0;
		}
		else {
			XSEGLOG2(&lc, I, "Stating %s completed Successfully."
					"No need to write.", gio->second_name);
			XSEGLOG2(&lc, I, "Hash of object %s to object %s completed",
					gio->obj_name, gio->second_name);
			req->serviced = req->size;
			r = 0;
			goto out_buf;
		}

	}
	else if (gio->state == WRITING){
		XSEGLOG2(&lc, I, "Writing of %s callback", gio->obj_name);
		if (pr->retval == 0) {
			XSEGLOG2(&lc, I, "Writing of %s completed", gio->second_name);
			XSEGLOG2(&lc, I, "Hash of object %s to object %s completed",
					gio->obj_name, gio->second_name);

			pos = 0;
			strncpy(hash_name, gio->obj_name, strlen(gio->obj_name));
			pos += strlen(gio->obj_name);
			strncpy(hash_name+pos, HASH_SUFFIX, HASH_SUFFIX_LEN);
			pos += HASH_SUFFIX_LEN;
			hash_name[pos] = 0;

			gio->state = POSTHASHING;
			if (do_aio_generic(peer, pr, X_WRITE, hash_name, gio->second_name,
						HEXLIFIED_SHA256_DIGEST_SIZE, 0) < 0) {
				XSEGLOG2(&lc, E, "Writing of %s failed on do_aio_write", hash_name);
				r = -1;
				goto out_buf;
			}
			return 0;
		}
		else {
			XSEGLOG2(&lc, E, "Writing of %s failed", gio->obj_name);
			XSEGLOG2(&lc, E, "Hash of object %s failed",
								gio->obj_name);
			r = -1;
			goto out_buf;
		}
	} else if (gio->state == POSTHASHING) {
		XSEGLOG2(&lc, I, "Writing of prehashed value callback");
		if (pr->retval == 0) {
			XSEGLOG2(&lc, I, "Writing of prehashed value completed");
			XSEGLOG2(&lc, I, "Hash of object %s to object %s completed",
					gio->obj_name, gio->second_name);

		}
		else {
			XSEGLOG2(&lc, E, "Writing of prehash failed");
		}
		req->serviced = req->size;
		r = 0;
		goto out_buf;

	}
	else {
		XSEGLOG2(&lc, E, "Unknown state");
	}
	return 0;


out_buf:
	free(gio->buf);
out_src:
	free(gio->second_name);

	gio->buf = NULL;
	gio->second_name = NULL;
	gio->read = 0;

	if (r < 0)
		fail(peer ,pr);
	else
		complete(peer, pr);
	return 0;
}

int spawnthread(struct peerd *peer, struct peer_req *pr,
			void *(*func)(void *arg))
{
	//struct glusterd *gluster = (struct glusterd *) peer->priv;
	struct gluster_io *gio = (struct gluster_io *) (pr->priv);

	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	return (pthread_create(&gio->tid, &attr, func, (void *) pr));
}

void watch_cb(uint8_t opcode, uint64_t ver, void *arg)
{
	//assert pr valid
	struct peer_req *pr = (struct peer_req *)arg;
	//struct glusterd *gluster = (struct glusterd *) pr->peer->priv;
	struct gluster_io *gio = (struct gluster_io *) (pr->priv);

	if (pr->req->op == X_ACQUIRE){
		XSEGLOG2(&lc, I, "watch cb signaling gio of %s", gio->obj_name);
		pthread_cond_signal(&gio->cond);
	}
	else
		XSEGLOG2(&lc, E, "Invalid req op in watch_cb");
}

void * lock_op(void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct glusterd *gluster = (struct glusterd *) pr->peer->priv;
	struct gluster_io *gio = (struct gluster_io *) (pr->priv);
	uint32_t len = strlen(gio->obj_name);
	strncpy(gio->obj_name + len, LOCK_SUFFIX, LOCK_SUFFIX_LEN);
	gio->obj_name[len + LOCK_SUFFIX_LEN] = 0;

	XSEGLOG2(&lc, I, "Starting lock op for %s", gio->obj_name);
	if (!(pr->req->flags & XF_NOSYNC)){
		if (gluster_watch(gluster->ioctx, gio->obj_name, 0,
				&gio->watch_handle, watch_cb, pr) < 0){
			XSEGLOG2(&lc, E, "gluster watch failed for %s",
					gio->obj_name);
			fail(pr->peer, pr);
			return NULL;
		}
	}

	/* passing flag 1 means renew lock */
	while(gluster_lock_exclusive(gluster->ioctx, gio->obj_name, gluster_LOCK_NAME,
		gluster_LOCK_COOKIE, gluster_LOCK_DESC, NULL, LIBgluster_LOCK_FLAG_RENEW) < 0){
		if (pr->req->flags & XF_NOSYNC){
			XSEGLOG2(&lc, E, "gluster lock failed for %s",
					gio->obj_name);
			fail(pr->peer, pr);
			return NULL;
		}
		else{
			XSEGLOG2(&lc, D, "gluster lock for %s sleeping",
					gio->obj_name);
			pthread_mutex_lock(&gio->m);
			pthread_cond_wait(&gio->cond, &gio->m);
			pthread_mutex_unlock(&gio->m);
			XSEGLOG2(&lc, D, "gluster lock for %s woke up",
					gio->obj_name);
		}
	}
	if (!(pr->req->flags & XF_NOSYNC)){
		if (gluster_unwatch(gluster->ioctx, gio->obj_name,
					gio->watch_handle) < 0){
			XSEGLOG2(&lc, E, "gluster unwatch failed");
		}
	}
	XSEGLOG2(&lc, I, "Successfull lock op for %s", gio->obj_name);
	complete(pr->peer, pr);
	return NULL;
}

int break_lock(struct glusterd *gluster, struct gluster_io *gio)
{
	int r, exclusive;
	char *tag = NULL, *clients = NULL, *cookies = NULL, *addrs = NULL;
	size_t tag_len = 1024, clients_len = 1024, cookies_len = 1024;
	size_t addrs_len = 1024;
	ssize_t nr_lockers;

	for (;;) {
		tag = malloc(sizeof(char) * tag_len);
		clients = malloc(sizeof(char) * clients_len);
		cookies = malloc(sizeof(char) * cookies_len);
		addrs = malloc(sizeof(char) * addrs_len);
		if (!tag || !clients || !cookies || !addrs) {
			XSEGLOG2(&lc, E, "Out of memmory");
			r = -1;
			break;
		}

		nr_lockers = gluster_list_lockers(gluster->ioctx, gio->obj_name,
				gluster_LOCK_NAME, &exclusive, tag, &tag_len,
				clients, &clients_len, cookies, &cookies_len,
				addrs, &addrs_len);
		if (nr_lockers < 0 && nr_lockers != -ERANGE) {
			XSEGLOG2(&lc, E, "Could not list lockers for %s", gio->obj_name);
			r = -1;
			break;
		} else if (nr_lockers == -ERANGE) {
			free(tag);
			free(clients);
			free(cookies);
			free(addrs);
		} else {
			if (nr_lockers != 1) {
				XSEGLOG2(&lc, E, "Number of lockers for %s != 1 !(%d)",
						gio->obj_name, nr_lockers);
				r = -1;
				break;
			} else if (!exclusive) {
				XSEGLOG2(&lc, E, "Lock for %s is not exclusive",
						gio->obj_name);
				r = -1;
				break;
			} else if (strcmp(gluster_LOCK_TAG, tag)) {
				XSEGLOG2(&lc, E, "List lockers returned wrong tag "
						"(\"%s\" vs \"%s\")", tag, gluster_LOCK_TAG);
				r = -1;
				break;
			}
			r = gluster_break_lock(gluster->ioctx, gio->obj_name,
				gluster_LOCK_NAME, clients, gluster_LOCK_COOKIE);
			break;
		}
	}

	free(tag);
	free(clients);
	free(cookies);
	free(addrs);

	return r;
}

void * unlock_op(void *arg)
{
	struct peer_req *pr = (struct peer_req *)arg;
	struct glusterd *gluster = (struct glusterd *) pr->peer->priv;
	struct gluster_io *gio = (struct gluster_io *) (pr->priv);
	uint32_t len = strlen(gio->obj_name);
	strncpy(gio->obj_name + len, LOCK_SUFFIX, LOCK_SUFFIX_LEN);
	gio->obj_name[len + LOCK_SUFFIX_LEN] = 0;
	int r;

	XSEGLOG2(&lc, I, "Starting unlock op for %s", gio->obj_name);
	if (pr->req->flags & XF_FORCE) {
		r = break_lock(gluster, gio);
	}
	else {
		r = gluster_unlock(gluster->ioctx, gio->obj_name, gluster_LOCK_NAME,
			gluster_LOCK_COOKIE);
	}
	/* ENOENT means that the lock did not existed.
	 * This still counts as a successfull unlock operation
	 */
	//if (r < 0 && r != -ENOENT){
	if (r < 0){
		XSEGLOG2(&lc, E, "gluster unlock failed for %s (r: %d)", gio->obj_name, r);
		fail(pr->peer, pr);
	}
	else {
		if (gluster_notify(gluster->ioctx, gio->obj_name, 
					0, NULL, 0) < 0) {
			XSEGLOG2(&lc, E, "gluster notify failed");
		}
		XSEGLOG2(&lc, I, "Successfull unlock op for %s", gio->obj_name);
		complete(pr->peer, pr);
	}
	return NULL;
}

int handle_acquire(struct peerd *peer, struct peer_req *pr)
{
	int r = spawnthread(peer, pr, lock_op);
	if (r < 0)
		fail(pr->peer, pr);
	return 0;
}


int handle_release(struct peerd *peer, struct peer_req *pr)
{
	int r = spawnthread(peer, pr, unlock_op);
	if (r < 0)
		fail(pr->peer, pr);
	return 0;
}
#endif

int custom_peer_init(struct peerd *peer, int argc, char *argv[])
{
	struct glusterd *gluster = malloc(sizeof(struct glusterd));
	struct gluster_io *gio;
	glfs_t *glfs = NULL;
	char transport[MAX_GLFS_ARG_LEN + 1];
	char server[MAX_GLFS_ARG_LEN + 1];
	char volume[MAX_GLFS_ARG_LEN + 1];
	int port = 0;
	int ret = 0;
	int i = 0;
	int j = 0;

	if (!gluster) {
		perror("malloc");
		return -1;
	}

	transport[0] = 0;
	server[0] = 0;
	volume[0] = 0;

	BEGIN_READ_ARGS(argc, argv);
	READ_ARG_STRING("--transport", transport, MAX_GLFS_ARG_LEN);
	READ_ARG_STRING("--server", server, MAX_GLFS_ARG_LEN);
	READ_ARG_ULONG("--port", port);
	READ_ARG_STRING("--volume", volume, MAX_GLFS_ARG_LEN);
	END_READ_ARGS();


	if (!volume[0]){
		XSEGLOG2(&lc, E , "Volume must be provided");
		usage(argv[0]);
		goto err_arg;
	}

	/* Use defaults if user has not provided his/her own */
	if (!transport[0])
		strncpy(transport, "tcp", 4);

	if (!server[0])
		strncpy(server, "127.0.0.1", 10);

	ret = __set_glfs(gluster, volume);
	if (ret < 0) {
		XSEGLOG2(&lc, E, "Error at glfs_new");
		goto err_glfs;
	}
	glfs = __get_glfs(gluster);

	ret = glfs_set_volfile_server(glfs, transport, server, port);
	if (ret < 0) {
		XSEGLOG2(&lc, E, "Error at glfs_set_volfile_server");
		goto err_glfs;
	}

	ret = glfs_init(glfs);
	if (ret) {
		XSEGLOG2(&lc, E, "Error at glfs_init\n");
		goto err_glfs;
	}
#if 0
	fd = glfs_creat(glfs, filename,
		O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
	if (!fd) {
		XSEGLOG2(&lc, E, "Error at glfs_creat\n");
		goto out;
	}

        if (glfs_close(fd) != 0) {
		XSEGLOG2(&lc, E, "Error at glfs_close");
		goto out;
	}

	fd = glfs_open(glfs, filename, O_RDWR);
	if (!fd) {
		XSEGLOG2(&lc, E, "Error at glfs_open");
		goto out;
	}
#endif

	peer->priv = (void *)gluster;
	for (i = 0; i < peer->nr_ops; i++) {
		gio = malloc(sizeof(struct gluster_io));

		if (!gio)
			goto err_gio;

		gio->fd = NULL;
		gio->buf = 0;
		gio->read = 0;
		gio->size = 0;
		gio->second_name = 0;
		gio->watch_handle = 0;
		pthread_cond_init(&gio->cond, NULL);
		pthread_mutex_init(&gio->m, NULL);
		peer->peer_reqs[i].priv = (void *) gio;
	}
	return 0;

err_arg:
	free(gluster);
err_gio:
	for (j = 0; j < i; j++)
		free(peer->peer_reqs[j].priv);
err_glfs:
	glfs_fini(glfs);
	return -1;
}

// nothing to do here for now
int custom_arg_parse(int argc, const char *argv[])
{
	return 0;
}

void custom_peer_finalize(struct peerd *peer)
{
	return;
}

int dispatch(struct peerd *peer, struct peer_req *pr, struct xseg_request *req,
		enum dispatch_reason reason)
{
	struct gluster_io *gio = (struct gluster_io *) (pr->priv);
	char *target = xseg_get_target(peer->xseg, pr->req);
	unsigned int end = (pr->req->targetlen > MAX_OBJ_NAME) ?
		MAX_OBJ_NAME : pr->req->targetlen;

	strncpy(gio->obj_name, target, end);
	gio->obj_name[end] = 0;
	req->serviced = 0;

	if (reason == dispatch_accept)
		gio->state = ACCEPTED;

	switch (pr->req->op){
		case X_READ:
			handle_read(peer, pr);
			break;
		case X_WRITE:
			handle_write(peer, pr);
			break;
#if 0
		case X_DELETE:
			if (canDefer(peer))
				defer_request(peer, pr);
			else
				handle_delete(peer, pr);
			break;
		case X_INFO:
			if (canDefer(peer))
				defer_request(peer, pr);
			else
				handle_info(peer, pr);
			break;
		case X_COPY:
			if (canDefer(peer))
				defer_request(peer, pr);
			else
				handle_copy(peer, pr);
			break;
		case X_ACQUIRE:
			handle_acquire(peer, pr);
			break;
		case X_RELEASE:
			handle_release(peer, pr);
			break;
		case X_HASH:
			handle_hash(peer, pr);
			break;
#endif
		case X_PING:
			handle_ping(peer, pr);
			break;
		default:
			fail(peer, pr);
	}
	return 0;
}
