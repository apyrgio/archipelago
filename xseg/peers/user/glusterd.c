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

#define REQ_UNDEFINED -1
#define REQ_FAILED -1
#define REQ_SUBMITTED 0
#define REQ_COMPLETED 1

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

static void create_hash_name(struct peer_req *pr, char *hash_name)
{
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	int pos = 0;

	strncpy(hash_name, gio->obj_name, strlen(gio->obj_name));
	pos += strlen(gio->obj_name);
	strncpy(hash_name + pos, HASH_SUFFIX, HASH_SUFFIX_LEN);
	pos += HASH_SUFFIX_LEN;
	hash_name[pos] = 0;
}

static int allocate_gio_secname(struct peer_req *pr)
{
	struct gluster_io *gio = (struct gluster_io *) pr->priv;

	gio->second_name = malloc(MAX_OBJ_NAME + 1);
	if (!gio->second_name)
		return -1;

	return 0;
}

static int allocate_gio_buf(struct peer_req *pr)
{
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	struct xseg_request *req = pr->req;

	gio->buf = malloc(req->size);
	if (!gio->buf)
		return -1;

	return 0;
}

static int prepare_copy(struct peer_req *pr)
{
	struct peerd *peer = pr->peer;
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	char *data = xseg_get_data(peer->xseg, pr->req);
	struct xseg_request_copy *xcopy = (struct xseg_request_copy *)data;
	unsigned int end = (xcopy->targetlen > MAX_OBJ_NAME) ?
		MAX_OBJ_NAME : xcopy->targetlen;
	int r;

	r = allocate_gio_secname(pr);
	if (r < 0)
		return r;

	/* FIXME: terminate or fail if targetlen > MAX_OBJ_NAME ? */
	strncpy(gio->second_name, xcopy->target, end);
	gio->second_name[end] = 0;
	gio->read = 0;

	r = allocate_gio_buf(pr);
	return r;
}

static int prepare_hash(struct peer_req *pr, char *hash_name)
{
	int r;

	r = allocate_gio_secname(pr);
	if (r < 0)
		return r;

	create_hash_name(pr, hash_name);

	r = allocate_gio_buf(pr);
	return r;
}

static int do_aio_generic(struct peer_req *pr, uint32_t op,
		char *buf, uint64_t size, uint64_t offset)
{
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	glfs_fd_t *fd = gio->fd;
	int r;

	switch (op) {
	case X_READ:
		r = glfs_pread_async(fd, buf, size, offset, 0,
				gluster_complete_aio, pr);
		break;
	case X_WRITE:
		r = glfs_pwrite_async(fd, buf, size, offset, 0,
				gluster_complete_aio, pr);
		break;
	default:
		return -1;
		break;
	}
	return r;
}

static int begin_aio_read(struct peer_req *pr, char *buf,
		uint64_t size, uint64_t offset)
{
	int r = 0;

	r = do_aio_generic(pr, X_READ, buf, size, offset);
	/* TODO: check errors */
	return r;
}

static int complete_aio_read(struct peer_req *pr, char *buf,
		uint64_t size, uint64_t offset, uint64_t *serviced)
{
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	int r = 0;

	if (pr->retval < 0) {
		/* TODO: check errors */
		return REQ_FAILED;
	} else if (pr->retval == 0) {
		XSEGLOG2(&lc, I, "Reading of %s reached end of file at "
			"%lu bytes. Zeroing out rest",
			gio->obj_name, *serviced);
		memset(buf + *serviced, 0, size - *serviced);
		*serviced = size;
		return REQ_COMPLETED;
	}

	*serviced += pr->retval;

	if (*serviced == size) {
		return REQ_COMPLETED;
	} else {
		r = do_aio_generic(pr, X_READ, buf + *serviced,
				size - *serviced, offset + *serviced);
		/* TODO: check errors */
		return REQ_SUBMITTED;
	}

	return r;
}

static int begin_aio_write(struct peer_req *pr, char *buf,
		uint64_t size, uint64_t offset)
{
	int r = 0;

	r = do_aio_generic(pr, X_WRITE, buf, size, offset);
	/* TODO: check errors */
	return r;
}

static int complete_aio_write(struct peer_req *pr, char *buf,
		uint64_t size, uint64_t offset, uint64_t *serviced)
{
	int r = 0;

	if (pr->retval < 0){
		/* TODO: check errors */
		return REQ_FAILED;
	}

	*serviced += pr->retval;

	if (*serviced == size) {
		return REQ_COMPLETED;
	} else {
		r = do_aio_generic(pr, X_WRITE, buf + *serviced,
				size - *serviced, offset + *serviced);
		/* TODO: check errors */
		return REQ_SUBMITTED;
	}

	return r;
}

/* FIXME: Do we really need target or we can get away with pr as do_aio_write */
static glfs_fd_t *do_block_open(struct peer_req *pr, char *target, int mode)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	glfs_t *glfs = __get_glfs(gluster);
	glfs_fd_t *fd;

	fd = glfs_open(glfs, target, O_RDWR);
	if (!fd || !(mode & O_CREAT)) {
		return fd;
	}

	fd = glfs_creat(glfs, target,
		O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);

	return fd;
}

static int do_block_close(struct peer_req *pr)
{
	struct gluster_io *gio = (struct gluster_io *)(pr->priv);
	glfs_fd_t *fd = gio->fd;
	int r;

	r = glfs_close(fd);
	/* TODO: Error check */
	return r;
}

static glfs_fd_t *do_block_creat(struct peer_req *pr, char *target, int mode)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	glfs_t *glfs = __get_glfs(gluster);
	glfs_fd_t *fd = NULL;

	fd = glfs_creat(glfs, target,
		O_WRONLY | O_CREAT | mode, S_IRUSR | S_IWUSR);
	/* TODO: Error check */
	return fd;
}

int do_block_lock(struct peer_req *pr, char *target, int mode)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	glfs_t *glfs = __get_glfs(gluster);
	glfs_fd_t *fd = NULL;
	int r;

	do {
		fd = glfs_creat(glfs, target,
			O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);

		if (!fd && mode == XF_NOSYNC)
			return -1;

		sleep(1);
	} while (fd);

	r = glfs_close(fd);

	return r;
}

int do_block_unlock(struct peer_req *pr, char *target, int mode)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	glfs_t *glfs = __get_glfs(gluster);

	/* Mode is currently only for breaking locks */
	return glfs_unlink(glfs, target);
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
	char *target = gio->obj_name;
	int ret = REQ_UNDEFINED;

	XSEGLOG2(&lc, D, "Started for pr %p, req %p, target %s",
			pr, req, target);

	if (gio->state == ACCEPTED) {
		if (req->datalen < req->size) {
			XSEGLOG2(&lc, E, "Request datalen is less than "
					"request size");
			ret = REQ_FAILED;
			goto out;
		}

		if (!req->size) {
			ret = REQ_COMPLETED;
			goto out;
		}

		fd = do_block_open(pr, target, 0);
		if (!fd) {
			XSEGLOG2(&lc, I, "Object %s does not exist. "
					"Serving zero data\n", target);
			/* object not found. return zeros instead */
			memset(data, 0, req->size);
			req->serviced = req->size;
			ret = REQ_COMPLETED;
			goto out;
		}
		gio->fd = fd;

		gio->state = READING;

		XSEGLOG2(&lc, I, "Reading %s", target);
		ret = begin_aio_read(pr, data, req->size, req->offset);
	} else if (gio->state == READING) {
		XSEGLOG2(&lc, I, "Reading of %s callback", target);
		ret = complete_aio_read(pr, data, req->size,
				req->offset, &req->serviced);
	}

out:
	switch (ret) {
	case REQ_FAILED:
		XSEGLOG2(&lc, I, "Reading of %s failed", target);
		do_block_close(pr);
		fail(peer, pr);
		break;
	case REQ_SUBMITTED:
		XSEGLOG2(&lc, I, "Reading of %s submitted", target);
		break;
	case REQ_COMPLETED:
		XSEGLOG2(&lc, I, "Reading of %s completed", target);
		do_block_close(pr);
		complete(peer, pr);
		break;
	default:
		XSEGLOG2(&lc, E, "Unknown request state. Failing.");
		do_block_close(pr);
		fail(peer, pr);
		break;
	}
	return 0;
}

int handle_write(struct peerd *peer, struct peer_req *pr)
{
	struct gluster_io *gio = (struct gluster_io *) (pr->priv);
	struct xseg_request *req = pr->req;
	glfs_fd_t *fd;
	char *data = xseg_get_data(peer->xseg, pr->req);
	char *target = gio->obj_name;
	int ret = REQ_UNDEFINED;

	XSEGLOG2(&lc, D, "Started for pr %p, req %p, target %s",
			pr, req, target);

	if (gio->state == ACCEPTED) {
		if (req->datalen < req->size) {
			XSEGLOG2(&lc, E, "Request datalen is less than "
					"request size");
			ret = REQ_FAILED;
			goto out;
		}

		if (!req->size) {
			/* TODO: Flush data if req->flags & XF_FLUSH */
			ret = REQ_COMPLETED;
			goto out;
		}

		fd = do_block_open(pr, target, O_CREAT);
		if (!fd) {
			XSEGLOG2(&lc, E, "Cannot open/create %s", target);
			ret = REQ_FAILED;
			goto out;
		}
		gio->fd = fd;

		gio->state = WRITING;
		XSEGLOG2(&lc, I, "Writing %s", target);
		ret = begin_aio_write(pr, data, req->size, req->offset);
	} else if (gio->state == WRITING) {
		XSEGLOG2(&lc, I, "Writing of %s callback", target);
		ret = complete_aio_write(pr, data, req->size,
				req->offset, &req->serviced);
	}

out:
	switch (ret) {
	case REQ_FAILED:
		XSEGLOG2(&lc, I, "Writing of %s failed", target);
		do_block_close(pr);
		fail(peer, pr);
		break;
	case REQ_SUBMITTED:
		XSEGLOG2(&lc, I, "Writing of %s submitted", target);
		break;
	case REQ_COMPLETED:
		XSEGLOG2(&lc, I, "Writing of %s completed", target);
		do_block_close(pr);
		complete(peer, pr);
		break;
	default:
		XSEGLOG2(&lc, E, "Unknown request state. Failing.");
		do_block_close(pr);
		fail(peer, pr);
		break;
	}
	return 0;
}

int handle_copy(struct peerd *peer, struct peer_req *pr)
{
	struct gluster_io *gio = (struct gluster_io *) (pr->priv);
	struct xseg_request *req = pr->req;
	glfs_fd_t *fd;
	char *target = gio->obj_name;
	int r;
	int ret = REQ_UNDEFINED;

	XSEGLOG2(&lc, D, "Started for pr %p, req %p, target %s",
			pr, req, target);

	if (gio->state == ACCEPTED) {
		XSEGLOG2(&lc, I, "Copy of object %s to object %s started",
				gio->second_name, gio->obj_name);
		if (!req->size) {
			ret = REQ_COMPLETED;
			goto out;
		}

		/* Create second name and buf */
		r = prepare_copy(pr);
		/* TODO: Check errors */
		target = gio->second_name;

		/* FIXME: Will we fail here? */
		fd = do_block_open(pr, target, 0);
		if (!fd) {
			XSEGLOG2(&lc, I, "Object %s does not exist. "
					"Serving zero data\n", target);
			/* object not found. return zeros instead */
			memset(gio->buf, 0, req->size);
			goto write;
		}
		gio->fd = fd;

		gio->state = READING;
		gio->read = 0;

		XSEGLOG2(&lc, I, "Reading %s", target);
		ret = begin_aio_read(pr, gio->buf, req->size, req->offset);
	}
	else if (gio->state == READING){
		target = gio->second_name;
		XSEGLOG2(&lc, I, "Reading of %s callback", target);
		ret = complete_aio_read(pr, gio->buf, req->size,
				req->offset, &gio->read);

		if (ret == REQ_FAILED || ret == REQ_SUBMITTED)
			goto out;

		do_block_close(pr);
write:
		//do_aio_write
		target = gio->obj_name;

		fd = do_block_open(pr, target, O_CREAT);
		if (!fd) {
			XSEGLOG2(&lc, E, "Cannot open/create %s", target);
			ret = REQ_FAILED;
			goto out;
		}
		gio->fd = fd;

		gio->state = WRITING;

		XSEGLOG2(&lc, I, "Writing %s", target);
		ret = begin_aio_write(pr, gio->buf, req->size, req->offset);
	} else if (gio->state == WRITING) {
		XSEGLOG2(&lc, I, "Writing of %s callback", target);
		ret = complete_aio_write(pr, gio->buf, req->size,
				req->offset, &req->serviced);
	}

out:
	if (ret != REQ_SUBMITTED) {
		free(gio->buf);
		free(gio->second_name);
		gio->buf = NULL;
		gio->second_name = NULL;
		gio->read = 0;
	}

	switch (ret) {
	case REQ_FAILED:
		XSEGLOG2(&lc, I, "Copying of %s failed", target);
		do_block_close(pr);
		fail(peer, pr);
		break;
	case REQ_SUBMITTED:
		break;
	case REQ_COMPLETED:
		XSEGLOG2(&lc, I, "Copying of %s completed", target);
		do_block_close(pr);
		complete(peer, pr);
		break;
	default:
		XSEGLOG2(&lc, E, "Unknown request state. Failing.");
		do_block_close(pr);
		fail(peer, pr);
		break;
	}
	return 0;
}

int handle_hash(struct peerd *peer, struct peer_req *pr)
{
	struct xseg_request *req = pr->req;
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	struct xseg_reply_hash *xreply;
	glfs_fd_t *fd;
	uint64_t trailing_zeros = 0;
	unsigned char sha[SHA256_DIGEST_SIZE];
	char hash_name[HEXLIFIED_SHA256_DIGEST_SIZE + 1];
	uint32_t pos;
	char *target = gio->obj_name;
	int r;
	int ret = REQ_UNDEFINED;

	XSEGLOG2(&lc, D, "Started for pr %p, req %p, target %s",
			pr, req, target);

	if (gio->state == ACCEPTED){
		XSEGLOG2(&lc, I, "Starting hashing of object %s", target);

		if (!req->size) {
			ret = REQ_COMPLETED; /* or fail? */
			goto out;
		}

		/* Create hash_name, gio second_name and gio->buf */
		r = prepare_hash(pr, hash_name);
		target = hash_name;
		gio->state = PREHASHING;

		/* Get correct status */
		fd = do_block_open(pr, target, 0);
		if (!fd) {
			XSEGLOG2(&lc, I, "Hash %s does not exist.", target);
			goto read;
		}
		gio->fd = fd;

		XSEGLOG2(&lc, I, "Reading %s", target);
		/* Read contents of hash_name in the gio->second_name buffer */
		ret = begin_aio_read(pr, gio->second_name,
				HEXLIFIED_SHA256_DIGEST_SIZE, req->offset);
	} else if (gio->state == PREHASHING) {
		target = hash_name;
		XSEGLOG2(&lc, I, "Reading of %s callback", target);
		ret = complete_aio_read(pr, gio->second_name,
				HEXLIFIED_SHA256_DIGEST_SIZE,
				req->offset, &gio->read);

		if (ret == REQ_FAILED || ret == REQ_SUBMITTED)
			goto out;

		/* Construct answer */
		XSEGLOG2(&lc, D, "Precalculated hash found");
		xreply = (struct xseg_reply_hash*)xseg_get_data(peer->xseg, req);
		r = xseg_resize_request(peer->xseg, pr->req, pr->req->targetlen,
				sizeof(struct xseg_reply_hash));
		strncpy(xreply->target, gio->second_name, HEXLIFIED_SHA256_DIGEST_SIZE);
		xreply->targetlen = HEXLIFIED_SHA256_DIGEST_SIZE;

		XSEGLOG2(&lc, I, "Calculated %s as hash of %s",
				gio->second_name, gio->obj_name);
		req->serviced = req->size;
		goto out;
		/* Leave */

read:
		/*
		 * We reach this point only if there was no precalculated hash
		 */
		gio->state = READING;
		gio->read = 0;
		target = gio->obj_name;

		XSEGLOG2(&lc, I, "Reading %s", target);
		ret = begin_aio_read(pr, gio->buf, req->size, req->offset);
	} else if (gio->state == READING) {
		target = gio->obj_name;
		XSEGLOG2(&lc, I, "Reading of %s callback", target);
		ret = complete_aio_read(pr, gio->buf, req->size,
				req->offset, &gio->read);

		if (ret == REQ_FAILED || ret == REQ_SUBMITTED)
			goto out;

		/* Strip here trailing zeroes */
		for (; trailing_zeros < gio->read; trailing_zeros++) {
			if (gio->buf[gio->read-trailing_zeros -1])
				break;
		}
		XSEGLOG2(&lc, D, "Read %llu, Trailing zeros %llu",
				gio->read, trailing_zeros);

		gio->read -= trailing_zeros;
		SHA256((unsigned char *) gio->buf, gio->read, sha);
		hexlify(sha, SHA256_DIGEST_SIZE, gio->second_name);
		gio->second_name[HEXLIFIED_SHA256_DIGEST_SIZE] = 0;

		/* Construct reply */
		xreply = (struct xseg_reply_hash*)xseg_get_data(peer->xseg, req);
		r = xseg_resize_request(peer->xseg, pr->req, pr->req->targetlen,
				sizeof(struct xseg_reply_hash));
		strncpy(xreply->target, gio->second_name, HEXLIFIED_SHA256_DIGEST_SIZE);
		xreply->targetlen = HEXLIFIED_SHA256_DIGEST_SIZE;

		XSEGLOG2(&lc, I, "Calculated %s as hash of %s",
				gio->second_name, gio->obj_name);
		/*
		 * We can't leave since we need to write the
		 * content-addressable object to the backend.
		 */
		do_block_close(pr);

		fd = do_block_creat(pr, gio->second_name, O_EXCL);
		if (!fd) {
			XSEGLOG2(&lc, I, "Hash of object %s to object %s completed",
					gio->obj_name, gio->second_name);
			req->serviced = req->size;
			ret = REQ_COMPLETED;
			/* TODO: Check errors */
			goto write;
		}
		gio->fd = fd;
		target = gio->second_name;

		gio->state = WRITING;
		XSEGLOG2(&lc, I, "Writing %s", target);
		ret = begin_aio_write(pr, gio->buf, req->size, req->offset);

	} else if (gio->state == WRITING) {
		target = gio->second_name;
		XSEGLOG2(&lc, I, "Writing of %s callback", target);
		ret = complete_aio_write(pr, gio->buf, req->size, req->offset,
				&req->serviced);

		if (ret == REQ_FAILED || ret == REQ_SUBMITTED)
			goto out;

		XSEGLOG2(&lc, I, "Writing of %s completed", gio->second_name);

		/*
		 * We can't leave since we need to write the precalculated hash
		 * value to the backend.
		 */
		do_block_close(pr);
write:
		pos = 0;
		strncpy(hash_name, gio->obj_name, strlen(gio->obj_name));
		pos += strlen(gio->obj_name);
		strncpy(hash_name+pos, HASH_SUFFIX, HASH_SUFFIX_LEN);
		pos += HASH_SUFFIX_LEN;
		hash_name[pos] = 0;

		gio->state = POSTHASHING;
		fd = do_block_creat(pr, hash_name, O_EXCL);
		if (!fd) {
			XSEGLOG2(&lc, I, "Writing of prehashed value completed");
			XSEGLOG2(&lc, I, "Hash of object %s to object %s completed",
					gio->obj_name, gio->second_name);
			req->serviced = req->size;
			ret = REQ_COMPLETED;
			/* TODO: Check errors */
			goto out;
		}
		gio->fd = fd;
		target = hash_name;

		XSEGLOG2(&lc, I, "Writing prehashed value");
		ret = begin_aio_write(pr, gio->second_name,
				HEXLIFIED_SHA256_DIGEST_SIZE, 0);
	} else if (gio->state == POSTHASHING) {
		XSEGLOG2(&lc, I, "Writing of prehashed value callback");
		ret = complete_aio_write(pr, gio->second_name,
				HEXLIFIED_SHA256_DIGEST_SIZE, 0, &gio->read);

		if (ret == REQ_FAILED || ret == REQ_SUBMITTED)
			goto out;

		XSEGLOG2(&lc, I, "Writing of prehashed value completed");
		req->serviced = req->size;
	}

out:
	target = gio->obj_name;
	if (ret != REQ_SUBMITTED) {
		free(gio->buf);
		free(gio->second_name);
		gio->buf = NULL;
		gio->second_name = NULL;
		gio->read = 0;
	}

	switch (ret) {
	case REQ_FAILED:
		XSEGLOG2(&lc, I, "Hashing of %s failed", target);
		do_block_close(pr);
		fail(peer, pr);
		break;
	case REQ_SUBMITTED:
		break;
	case REQ_COMPLETED:
		XSEGLOG2(&lc, I, "Hashing of %s completed", target);
		do_block_close(pr);
		complete(peer, pr);
		break;
	default:
		XSEGLOG2(&lc, E, "Unknown request state. Failing.");
		do_block_close(pr);
		fail(peer, pr);
		break;
	}
	return 0;
}

int handle_acquire(struct peerd *peer, struct peer_req *pr)
{
	struct gluster_io *gio = (struct gluster_io *)(pr->priv);
	struct xseg_request *req = pr->req;
	uint32_t len = strlen(gio->obj_name);
	int ret;

	strncpy(gio->obj_name + len, LOCK_SUFFIX, LOCK_SUFFIX_LEN);
	gio->obj_name[len + LOCK_SUFFIX_LEN] = 0;

	XSEGLOG2(&lc, I, "Starting lock op for %s", gio->obj_name);

	/* TODO: Check error codes and retry if needed */
	ret = do_block_lock(pr, gio->obj_name, req->flags & XF_NOSYNC);
	if (ret < 0) {
		XSEGLOG2(&lc, E, "Lock op failed for %s", gio->obj_name);
		fail(peer, pr);
	} else {
		XSEGLOG2(&lc, E, "Lock op succeededfor %s", gio->obj_name);
		complete(peer, pr);
	}
	return 0;
}


int handle_release(struct peerd *peer, struct peer_req *pr)
{
	struct gluster_io *gio = (struct gluster_io *)(pr->priv);
	struct xseg_request *req = pr->req;
	uint32_t len = strlen(gio->obj_name);
	int ret;

	strncpy(gio->obj_name + len, LOCK_SUFFIX, LOCK_SUFFIX_LEN);
	gio->obj_name[len + LOCK_SUFFIX_LEN] = 0;

	XSEGLOG2(&lc, I, "Starting unlock op for %s", gio->obj_name);

	/* TODO: Check error codes and retry if needed */
	ret = do_block_unlock(pr, gio->obj_name, req->flags & XF_FORCE);
	if (ret < 0) {
		XSEGLOG2(&lc, E, "Unlock op failed for %s", gio->obj_name);
		fail(peer, pr);
	} else {
		XSEGLOG2(&lc, E, "Unlock op succeededfor %s", gio->obj_name);
		complete(peer, pr);
	}
	return 0;
}

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
#endif
		case X_ACQUIRE:
			handle_acquire(peer, pr);
			break;
		case X_RELEASE:
			handle_release(peer, pr);
			break;
#if 0
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
