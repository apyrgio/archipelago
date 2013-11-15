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
#include <ctype.h>
#include <errno.h>
#include <hash.h>
#include <sys/types.h>
#include <unistd.h>


#define MAX_UNIQUESTR_LEN 128
#define LOCK_SUFFIX "_lock"
#define LOCK_SUFFIX_LEN 5
#define HASH_SUFFIX "_hash"
#define HASH_SUFFIX_LEN 5

#define MAX_OBJ_NAME (XSEG_MAX_TARGETLEN + LOCK_SUFFIX_LEN + 1)
#define MAX_GLFS_ARG_LEN 64

void custom_peer_usage()
{
	 fprintf(stderr, "Custom peer options:\n"
                "  Option        | Default    | \n"
                "  --------------------------------------------\n"
                "    --transport | tcp        | Transport type [tcp|rdma|unix]\n"
                "    --server    | 127.0.0.1  | A server that is part of the\n"
		"                |            | volume\n"
                "    --port      | 0(=24007)  | Port of server's gluster daemon\n"
                "    --volume    | None       | Gluster volume to connect to\n"
                "    --uniquestr | pid        | Unique string for this instance\n"
                "    --async     | 0          | If 1, glusted will use the async\n"
                "                |            | equivalent functions\n"
                "\n"
               );
}

#define REQ_UNDEFINED -2
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
	char uniquestr[MAX_UNIQUESTR_LEN + 1];
	int uniquestr_len;
	int async;
};

struct gluster_io{
	char obj_name[MAX_OBJ_NAME + 1];
	glfs_fd_t *fd;
	enum gluster_state state;
	uint64_t size;
	char *second_name, *buf;
	uint64_t read;
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

int handle_read(struct peerd *peer, struct peer_req *pr);
int handle_write(struct peerd *peer, struct peer_req *pr);


static void gluster_complete_read(struct glfs_fd *fd, ssize_t ret, void *data)
{
	struct peer_req *pr = (struct peer_req*)data;
	struct peerd *peer = pr->peer;

	pr->retval = ret;
	dispatch(peer, pr, pr->req, dispatch_internal);
}

static void gluster_complete_write(struct glfs_fd *fd, ssize_t ret, void *data)
{
	struct peer_req *pr = (struct peer_req*)data;
	struct peerd *peer = pr->peer;

	pr->retval = ret;
	dispatch(peer, pr, pr->req, dispatch_internal);
}

static void submit_hook(struct peer_req *pr)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);

	if (!gluster->async)
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

static int write_lock_owner(struct peer_req *pr, glfs_fd_t *fd)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	char *me = gluster->uniquestr;
	int len = strlen(me);
	int serviced = 0;
	int r = 0;

	while (len > serviced) {
		r = glfs_pwrite(fd, me + serviced, len - serviced,
				0 + serviced, 0);
		if (r > 0)
			serviced += r;
		else if (errno != EINTR)
			return -1;
	}

	return 0;
}

static int read_lock_owner(struct peer_req *pr, glfs_fd_t *fd, char *owner)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	char *buf = gluster->uniquestr;
	int len = strlen(buf);
	int serviced = 0;
	int r = 0;

	while (len > serviced) {
		r = glfs_pread(fd, owner + serviced, len - serviced,
				0 + serviced, 0);
		if (r > 0)
			serviced += r;
		else if (r == 0)
			break;
		else if (errno != EINTR)
			return -1;
	}

	return serviced;
}

static int validate_lock_owner(struct peer_req *pr, glfs_fd_t *fd)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	char lock_owner[MAX_UNIQUESTR_LEN + 1];
	char *me = gluster->uniquestr;
	int len = strlen(me);
	int r = 0;

	r = read_lock_owner(pr, fd, lock_owner);
	if (r < 0) {
		XSEGLOG2(&lc, E, "Failed to read lock owner of fd %p", fd);
		return -1;
	}

	if (r != len || strncmp(me, lock_owner, r) != 0) {
		XSEGLOG2(&lc, D, "Lock owner is %s while we are %s",
				lock_owner, me);
		return -1;
	}

	return 0;
}

static int do_aio_generic(struct peer_req *pr, uint32_t op,
		char *buf, uint64_t size, uint64_t offset)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	glfs_fd_t *fd = gio->fd;
	int r;

	switch (op) {
	case X_READ:
		if (gluster->async) {
			r = glfs_pread_async(fd, buf, size, offset, 0,
					gluster_complete_read, pr);
		} else {
			r = glfs_pread(fd, buf, size, offset, 0);
			if (r >= 0)
				pr->retval = r;
		}
		break;
	case X_WRITE:
		if (gluster->async) {
			r = glfs_pwrite_async(fd, buf, size, offset, 0,
					gluster_complete_write, pr);
		} else {
			r = glfs_pwrite(fd, buf, size, offset, 0);
			if (r >= 0)
				pr->retval = r;
		}
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
	if (r >= 0)
		return REQ_SUBMITTED;
	else
		return REQ_FAILED;
}

static int begin_aio_write(struct peer_req *pr, char *buf,
		uint64_t size, uint64_t offset)
{
	int r = 0;

	r = do_aio_generic(pr, X_WRITE, buf, size, offset);
	if (r >= 0)
		return REQ_SUBMITTED;
	else
		return REQ_FAILED;
}

static int complete_aio_read(struct peer_req *pr, char *buf,
		uint64_t size, uint64_t offset, uint64_t *serviced)
{
	int r = 0;

	/* Leave on fail or if there are no other data */
	if (pr->retval < 0) {
		/* TODO: check errors */
		return REQ_FAILED;
	} else if (pr->retval == 0) {
		XSEGLOG2(&lc, I, "Zeroing rest of data");
		memset(buf + *serviced, 0, size - *serviced);
		*serviced = size;
		return REQ_COMPLETED;
	}

	/*
	 * Else, check if all data have been served and resubmit if necessary
	 */
	*serviced += pr->retval;
	if (*serviced == size) {
		return REQ_COMPLETED;
	} else {
		r = do_aio_generic(pr, X_READ, buf + *serviced,
				size - *serviced, offset + *serviced);
		if (r >= 0)
			return REQ_SUBMITTED;
		else
			return REQ_FAILED;
	}
}

static int complete_aio_write(struct peer_req *pr, char *buf,
		uint64_t size, uint64_t offset, uint64_t *serviced)
{
	int r = 0;

	/* Leave on fail or if there are no other data */
	if (pr->retval < 0){
		/* TODO: check errors */
		return REQ_FAILED;
	}

	/*
	 * Else, check if all data have been served and resubmit if necessary
	 */
	*serviced += pr->retval;
	if (*serviced == size) {
		return REQ_COMPLETED;
	} else {
		r = do_aio_generic(pr, X_WRITE, buf + *serviced,
				size - *serviced, offset + *serviced);
		if (r >= 0)
			return REQ_SUBMITTED;
		else
			return REQ_FAILED;
	}

	return r;
}

static glfs_fd_t *do_block_create(struct peer_req *pr, char *target, int mode)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	glfs_t *glfs = __get_glfs(gluster);
	glfs_fd_t *fd = NULL;

	/*
	 * Create the requested file (in O_EXCL mode if requested)
	 * If errno is EINTR, retry. If it is other that EEXIST or EINTR,
	 * leave.
	 */
	XSEGLOG2(&lc, D, "Creating target %s", target);
	do {
		errno = 0;
		fd = glfs_creat(glfs, target,
			O_RDWR | O_CREAT | O_TRUNC | mode,
			S_IRUSR | S_IWUSR);

		if (fd)
			return fd;
		if (errno != EEXIST && errno != EINTR) {
			XSEGLOG2(&lc, E, "Unexpected error (errno %d) while "
					"creating %s", errno, target);
			return fd;
		}
	} while (errno == EINTR);

	return fd;
}

static glfs_fd_t *do_block_open(struct peer_req *pr, char *target, int mode)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	glfs_t *glfs = __get_glfs(gluster);
	glfs_fd_t *fd;

	/*
	 * Open the requested file.
	 * If errno is EINTR, retry. If it is other that ENOENT or EINTR,
	 * leave.
	 */
	XSEGLOG2(&lc, D, "Opening target %s", target);
	do {
		errno = 0;
		fd = glfs_open(glfs, target, O_RDWR);

		if (fd)
			return fd;
		if (errno != ENOENT && errno != EINTR) {
			XSEGLOG2(&lc, E, "Unexpected error (errno %d) while "
					"opening %s:", errno, target);
			return fd;
		}
	} while (errno == EINTR);

	if (!fd && !(mode & O_CREAT))
		return NULL;

	/*
	 * Create the requested file only if user has demanded so.
	 * If errno is EINTR, retry. Else, leave.
	 */
	fd = do_block_create(pr, target, 0);
	return fd;
}

static int do_block_close(struct peer_req *pr)
{
	struct gluster_io *gio = (struct gluster_io *)(pr->priv);
	glfs_fd_t *fd = gio->fd;
	int r;

	XSEGLOG2(&lc, D, "Closing fd %p", fd);
	if (!fd)
		return 0;

	r = glfs_close(fd);
	if (r < 0)
		XSEGLOG2(&lc, E, "Unexpected error (errno %d) while "
				"closing fd %p:", errno, fd);

	return r;
}

/*
 * do_block_lock() does the following:
 * a. Checks if there is a lock-file for this object.
 *    i. Checks if the lock-file has been placed by us.
 *    ii. If we own the lock-file or if we are in XF_NOSYNC mode, it returns.
 *    iii. Else, it proceeds to (b)
 * b. Attempts to create its own lockfile (obj_name + lock suffix). If it
 *    succeeds or is in XF_NOSYNC mode, it returns.
 * c. If it has not locked the target object and is NOT in XF_NOSYNC mode, it
 *    sleeps for 1 second and it reattempts.
 */
int do_block_lock(struct peer_req *pr, char *target, int mode)
{
	glfs_fd_t *fd = NULL;
	int r;

	XSEGLOG2(&lc, D, "Locking target %s", target);
	do {
		fd = do_block_open(pr, target, 0);
		if (fd) {
			r = validate_lock_owner(pr, fd);
			if (r > 0 || mode == XF_NOSYNC)
				break;
		}

		fd = do_block_create(pr, target, O_EXCL);
		if (fd) {
			r = write_lock_owner(pr, fd);
			break;
		} else if (mode == XF_NOSYNC) {
			r = -1;
			break;
		}

		sleep(1);
	} while (1);

	glfs_close(fd);

	return r;
}

/*
 * do_block_unlock() does the following:
 * a. Checks if there is a lock-file for this object. If not, it can safely
 *    return.
 * b. Checks if the lock-file has been placed by us.
 *    ii. If we own the lock-file or if we are in XF_FORCE mode, we unlink it.
 * c. If the unlinking was successful or if it failed due to ENOENT, the
 *    unlock operation is considered successful.
 */
int do_block_unlock(struct peer_req *pr, char *target, int mode)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	glfs_t *glfs = __get_glfs(gluster);
	glfs_fd_t *fd = NULL;
	int r = 0;

	XSEGLOG2(&lc, D, "Unlocking target %s", target);
	fd = do_block_open(pr, target, 0);
	if (!fd) {
		XSEGLOG2(&lc, D, "Target %s was not locked", target);
		return -1;
	}

	r = validate_lock_owner(pr, fd);
	if (r < 0 && !(mode & XF_FORCE))
		return -1;

	r = glfs_unlink(glfs, target);
	if (r >= 0 || errno == ENOENT)
		return 0;
	else
		return -1;
}

int do_block_stat(struct peer_req *pr, char *target, struct stat *buf)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	glfs_t *glfs = __get_glfs(gluster);

	XSEGLOG2(&lc, D, "Stating target %s", target);
	return glfs_stat(glfs, target, buf);
}

int do_block_delete(struct peer_req *pr, char *target)
{
	struct peerd *peer = pr->peer;
	struct glusterd *gluster = __get_gluster(peer);
	glfs_t *glfs = __get_glfs(gluster);

	XSEGLOG2(&lc, D, "Deleting target %s", target);
	return glfs_unlink(glfs, target);
}

int handle_delete(struct peerd *peer, struct peer_req *pr)
{
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	struct xseg_request *req = pr->req;
	int r;

	XSEGLOG2(&lc, D, "Started for pr %p, req %p, target %s",
			pr, req, gio->obj_name);

	r = do_block_delete(pr, gio->obj_name);
	if (r < 0) {
		XSEGLOG2(&lc, E, "Deletion of %s failed", gio->obj_name);
		fail(peer, pr);
	} else {
		XSEGLOG2(&lc, I, "Deletion of %s completed", gio->obj_name);
		complete(peer, pr);
	}
	return 0;
}

int handle_info(struct peerd *peer, struct peer_req *pr)
{
	struct gluster_io *gio = (struct gluster_io *) pr->priv;
	struct xseg_request *req = pr->req;
	struct xseg_reply_info *xinfo;
	struct stat stat;
	char *req_data;
	char buf[XSEG_MAX_TARGETLEN + 1];
	char *target = xseg_get_target(peer->xseg, req);
	int r;

	XSEGLOG2(&lc, D, "Started for pr %p, req %p, target %s",
			pr, req, gio->obj_name);

	if (req->datalen < sizeof(struct xseg_reply_info)) {
		/* FIXME: Is this normal? */
		strncpy(buf, target, req->targetlen);
		r = xseg_resize_request(peer->xseg, req, req->targetlen,
				sizeof(struct xseg_reply_info));
		if (r < 0) {
			XSEGLOG2(&lc, E, "Cannot resize request");
			fail(peer, pr);
			return -1;
		}
		target = xseg_get_target(peer->xseg, req);
		strncpy(target, buf, req->targetlen);
	}

	r = do_block_stat(pr, gio->obj_name, &stat);
	if (r < 0) {
		XSEGLOG2(&lc, E, "Stat failed for %s", gio->obj_name);
		fail(peer, pr);
		return -1;
	}

	req_data = xseg_get_data(peer->xseg, pr->req);
	xinfo = (struct xseg_reply_info *)req_data;
	xinfo->size = (uint64_t)stat.st_size;

	XSEGLOG2(&lc, I, "Getting info of %s completed", gio->obj_name);
	complete(peer, pr);
	return 0;
}

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

		XSEGLOG2(&lc, I, "Reading %s", target);

		gio->state = READING;
		ret = begin_aio_read(pr, data, req->size, req->offset);
	} else if (gio->state == READING) {
		XSEGLOG2(&lc, I, "Reading of %s callback", target);
		ret = complete_aio_read(pr, data, req->size,
				req->offset, &req->serviced);
	}

out:
	switch (ret) {
	case REQ_FAILED:
		XSEGLOG2(&lc, E, "Reading of %s failed", target);
		do_block_close(pr);
		fail(peer, pr);
		break;
	case REQ_SUBMITTED:
		XSEGLOG2(&lc, I, "Reading of %s submitted", target);
		submit_hook(pr);
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

		XSEGLOG2(&lc, I, "Writing %s", target);

		gio->state = WRITING;
		ret = begin_aio_write(pr, data, req->size, req->offset);
	} else if (gio->state == WRITING) {
		XSEGLOG2(&lc, I, "Writing of %s callback", target);
		ret = complete_aio_write(pr, data, req->size,
				req->offset, &req->serviced);
	}

out:
	switch (ret) {
	case REQ_FAILED:
		XSEGLOG2(&lc, E, "Writing of %s failed", target);
		do_block_close(pr);
		fail(peer, pr);
		break;
	case REQ_SUBMITTED:
		XSEGLOG2(&lc, I, "Writing of %s submitted", target);
		submit_hook(pr);
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
		/* Create second name and buf */
		r = prepare_copy(pr);
		if (r < 0) {
			ret = REQ_FAILED;
			goto out;
		}
		target = gio->second_name;

		XSEGLOG2(&lc, I, "Copy of object %s to object %s started",
				gio->second_name, gio->obj_name);

		if (!req->size) {
			ret = REQ_COMPLETED;
			goto out;
		}

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

		XSEGLOG2(&lc, I, "Reading %s", target);

		gio->state = READING;
		gio->read = 0;
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
		target = gio->obj_name;
		XSEGLOG2(&lc, I, "Target is %s", target);
		fd = do_block_open(pr, target, O_CREAT);
		if (!fd) {
			XSEGLOG2(&lc, E, "Cannot open/create %s", target);
			ret = REQ_FAILED;
			goto out;
		}
		gio->fd = fd;

		XSEGLOG2(&lc, I, "Writing %s", target);

		gio->state = WRITING;
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
		XSEGLOG2(&lc, E, "Copying of %s failed", target);
		do_block_close(pr);
		fail(peer, pr);
		break;
	case REQ_SUBMITTED:
		submit_hook(pr);
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
		if (r < 0) {
			ret = REQ_FAILED;
			goto out;
		}
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

		fd = do_block_open(pr, target, 0);
		if (!fd) {
			XSEGLOG2(&lc, I, "Original object %s does not "
					"exist.\nServing zeroes.", target);
			memset(gio->buf, 0, req->size);
			gio->read = req->size;
			goto hash;
		}
		gio->fd = fd;

		XSEGLOG2(&lc, I, "Reading %s", target);
		ret = begin_aio_read(pr, gio->buf, req->size, req->offset);
	} else if (gio->state == READING) {
		target = gio->obj_name;
		XSEGLOG2(&lc, I, "Reading of %s callback", target);
		ret = complete_aio_read(pr, gio->buf, req->size,
				req->offset, &gio->read);

		if (ret == REQ_FAILED || ret == REQ_SUBMITTED)
			goto out;

		do_block_close(pr);

hash:
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

		gio->read = 0;
		fd = do_block_create(pr, gio->second_name, O_EXCL);
		if (!fd) {
			XSEGLOG2(&lc, I, "Hash of object %s to object %s completed",
					gio->obj_name, gio->second_name);
			req->serviced = req->size;
			goto write;
		}
		gio->fd = fd;
		target = gio->second_name;

		XSEGLOG2(&lc, I, "Writing %s", target);

		gio->state = WRITING;
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
		fd = do_block_create(pr, hash_name, O_EXCL);
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
		XSEGLOG2(&lc, E, "Hashing of %s failed", target);
		do_block_close(pr);
		fail(peer, pr);
		break;
	case REQ_SUBMITTED:
		submit_hook(pr);
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
		XSEGLOG2(&lc, I, "Lock op succeeded for %s", gio->obj_name);
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
		XSEGLOG2(&lc, I, "Unlock op succeeded for %s", gio->obj_name);
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
	char uniquestr[MAX_UNIQUESTR_LEN + 1];
	int tid;
	int port = 0;
	int ret = 0;
	int i = 0;
	int j = 0;

	if (!gluster) {
		perror("malloc");
		return -1;
	}
	gluster->async = 0;

	uniquestr[0] = 0;
	transport[0] = 0;
	server[0] = 0;
	volume[0] = 0;

	BEGIN_READ_ARGS(argc, argv);
	READ_ARG_STRING("--transport", transport, MAX_GLFS_ARG_LEN);
	READ_ARG_STRING("--server", server, MAX_GLFS_ARG_LEN);
	READ_ARG_ULONG("--port", port);
	READ_ARG_STRING("--volume", volume, MAX_GLFS_ARG_LEN);
	READ_ARG_STRING("--uniquestr", gluster->uniquestr, MAX_UNIQUESTR_LEN);
	READ_ARG_BOOL("--async", gluster->async);
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

	if (!uniquestr[0]) {
		tid = getpid();
		snprintf(gluster->uniquestr, MAX_UNIQUESTR_LEN, "%d", tid);
		XSEGLOG2(&lc, W, "Warning, uniquestr not provided.\n"
				"\tUsing instead \"%s\" but uniqueness is not "
				"guaranteed.", gluster->uniquestr);
	} else {
		strncpy(gluster->uniquestr, uniquestr, MAX_UNIQUESTR_LEN);
	}

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
	struct glusterd *gluster = __get_gluster(peer);
	glfs_t *glfs = __get_glfs(gluster);

	glfs_fini(glfs);
	XSEGLOG2(&lc, I, "Glusterd has closed successfully");
}

int dispatch(struct peerd *peer, struct peer_req *pr, struct xseg_request *req,
		enum dispatch_reason reason)
{
	struct gluster_io *gio = (struct gluster_io *) (pr->priv);
	char *target = xseg_get_target(peer->xseg, pr->req);
	unsigned int end = (pr->req->targetlen > MAX_OBJ_NAME) ?
		MAX_OBJ_NAME : pr->req->targetlen;

	if (reason == dispatch_accept) {
		strncpy(gio->obj_name, target, end);
		gio->obj_name[end] = 0;
		gio->state = ACCEPTED;
		gio->fd = NULL;
		gio->read = 0;
	}

	switch (pr->req->op){
		case X_READ:
			handle_read(peer, pr);
			break;
		case X_WRITE:
			handle_write(peer, pr);
			break;
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
		case X_PING:
			handle_ping(peer, pr);
			break;
		default:
			fail(peer, pr);
	}
	return 0;
}
