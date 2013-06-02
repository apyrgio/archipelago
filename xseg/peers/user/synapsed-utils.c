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
#include <sys/syscall.h>
#include <sys/types.h>
#include <xseg/xseg.h>
#include <sys/util.h>
#include <signal.h>
#include <limits.h>
#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <synapsed.h>
#include <fcntl.h>
#include <poll.h>
#include <peer.h>

/***********************\
 * Auxiliary functions *
\***********************/

static void *get_in_addr(struct sockaddr *sa)
{
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*)sa)->sin_addr);
	}

	return &(((struct sockaddr_in6*)sa)->sin6_addr);
}


/**************************\
 * Sockfd cache functions *
\**************************/

void print_sockfd(struct cached_sockfd *cfd)
{
	XSEGLOG2(&lc, I, "fd: %d, addr: %ld, port: %d, status: %d",
			cfd->fd, cfd->s_addr, cfd->port, cfd->status);
}

int lookup_sockfds(struct cached_sockfd *cfd, struct sockaddr_in *sin)
{
	int i;

	for (i = 0; i < MAX_SOCKETS; i++) {
		if (cfd[i].s_addr == sin->sin_addr.s_addr &&
				cfd[i].port == sin->sin_port)
			return cfd[i].fd;
	}

	return SOCKFD_EEXIST;
}

int insert_sockfds(struct cached_sockfd *cfd,
		struct sockaddr_in *sin, int fd, int status)
{
	int i;

	for (i = 0; i < MAX_SOCKETS; i++) {
		if (cfd[i].s_addr != 0)
			continue;
		cfd[i].s_addr = sin->sin_addr.s_addr;
		cfd[i].port = sin->sin_port;
		cfd[i].fd = fd;
		cfd[i].status = status;
		print_sockfd(&cfd[i]);
		return 0;
	}
	return SOCKFD_ENOSPC;
}

int update_sockfds(struct cached_sockfd *cfd, int fd, int new_port)
{
	int i;

	for (i = 0; i < MAX_SOCKETS; i++) {
		if (cfd[i].fd == fd) {
			cfd[i].port = new_port;
			cfd[i].status = SOCKFD_VERIFIED;
			print_sockfd(&cfd[i]);
			return 0;
		}
	}
	return SOCKFD_EEXIST;
}

int stat_sockfds(struct cached_sockfd *cfd, int fd)
{
	int i;

	for (i = 0; i < MAX_SOCKETS; i++) {
		if (cfd[i].fd == fd)
			return cfd[i].status;
	}
	return SOCKFD_EEXIST;
}

/********************\
 * Pollfd functions *
\********************/

void pollfds_init(struct pollfd *fds)
{
	for (int i = 0; i < MAX_SOCKETS; i++)
		fds[i].fd = -1;
}

int pollfds_add(struct pollfd *fds, int fd, short flags)
{
	for (int i = 0; i < MAX_SOCKETS; i++) {
		if (fds[i].fd < 0) {
			fds[i].fd = fd;
			fds[i].events = flags;
			return 0;
		}
	}

	return -1;
}

int pollfds_remove(struct pollfd *fds, int fd)
{
	for (int i = 0; i < MAX_SOCKETS; i++) {
		if (fds[i].fd == fd) {
			fds[i].fd = -1;
			fds[i].events = 0;
			return 0;
		}
	}

	return -1;
}



/***************************\
 * Custom signal functions *
\***************************/

static void synapsed_signal_handler(int signum)
{
}

/* TODO: Explain what this function does */
int synapsed_init_local_signal(struct peerd *peer, sigset_t *oldset)
{
	struct sigaction oldact;
	int r;

#ifdef MT
	r = pthread_sigmask(0, NULL, oldset);
#else
	r = sigprocmask(0, NULL, oldset);
#endif
	if (r < 0)
		return -1;

	xseg_init_local_signal(peer->xseg, peer->portno_start);

	/* Override current signal handler for SIGIO */
	r = sigaction(SIGIO, NULL, &oldact);
	if (r < 0)
		return -1;
	oldact.sa_handler = synapsed_signal_handler;
	r = sigaction(SIGIO, &oldact, NULL);
	if (r < 0)
		return -1;
	return 0;
}

/********************************\
 * Data serialization functions *
\********************************/

void pack_request(struct synapsed_header *sh, struct peer_req *pr,
		struct xseg_request *req, uint32_t sh_flags)
{
	struct original_request *orig_req = &sh->orig_req;

	orig_req->pr = pr;
	orig_req->req = req;
	orig_req->sh_flags = sh_flags;

	sh->xseg_dst_portno = req->dst_portno;
	sh->op = req->op;
	sh->state = req->state;
	sh->flags = req->flags;
	sh->serviced = req->serviced;
	sh->datalen = req->datalen;
	sh->targetlen = req->targetlen;
	sh->size = req->size;
	sh->serviced = req->serviced;

	XSEGLOG2(&lc, D, "sh: %p, req: %p, op: %lu, sh_flags: %u",
			sh, orig_req->req, sh->op, orig_req->sh_flags);
}

void unpack_request(struct synapsed_header *sh, struct xseg_request *req)
{
	req->dst_portno = sh->xseg_dst_portno;
	req->op = sh->op;
	req->state = sh->state;
	req->flags = sh->flags;
	req->serviced = sh->serviced;
	req->datalen = sh->datalen;
	req->targetlen = sh->targetlen;
	req->size = sh->size;
	req->serviced = sh->serviced;

	XSEGLOG2(&lc, D, "sh: %p, req: %p, op: %lu, sh_flags: %u",
			sh, sh->orig_req.req, req->op, sh->orig_req.sh_flags);
}

int send_data(int fd, struct synapsed_header *sh, char *data, char *target)
{
	struct iovec iov[3];
	int iovcnt = 2;
	int r;

	iov[0].iov_base = sh;
	iov[0].iov_len = sizeof(struct synapsed_header);
	iov[1].iov_base = target;
	iov[1].iov_len = sh->targetlen;
	if (sh->op == X_WRITE && sh->orig_req.sh_flags & SH_REQUEST) {
		iov[2].iov_base = data;
		iov[2].iov_len = sh->datalen;
		iovcnt = 3;
	}

	r = writev(fd, iov, iovcnt);
	return r;
}

int recv_synapsed_header(int fd, struct synapsed_header *sh)
{
	read(fd, sh, sizeof(struct synapsed_header));

	XSEGLOG2(&lc, D, "sh: %p, req: %p, op: %lu, sh_flags: %u",
			sh, sh->orig_req.req, sh->op, sh->orig_req.sh_flags);
	return 0;
}

int recv_data(int fd, struct synapsed_header *sh, char *data, char *target)
{
	struct iovec iov[2];
	int iovcnt = 1;
	int r;

	iov[0].iov_base = target;
	iov[0].iov_len = sh->targetlen;
	if (sh->op == X_READ && sh->orig_req.sh_flags & SH_REPLY) {
		iov[1].iov_base = data;
		iov[1].iov_len = sh->datalen;
		iovcnt = 2;
	}

	r = readv(fd, iov, iovcnt);
	return r;
}

/********************************\
 * Connection-related functions *
\********************************/

/* TODO: Explain this function */
int accept_remote(struct synapsed *syn)
{
	struct sockaddr their_addr; // connector's address information
	struct sockaddr_in *sin = (struct sockaddr_in *)&their_addr;
	struct cached_sockfd *cfd = syn->cfd;
	socklen_t sin_size = sizeof(their_addr);
	char s[INET6_ADDRSTRLEN];
	int sockfd = syn->sockfd;
	int new_fd;

	new_fd = accept(sockfd, &their_addr, &sin_size);
	if (new_fd == -1) {
		XSEGLOG2(&lc, E, "Error during accept()");
	} else {
		if (lookup_sockfds(cfd, sin) < 0) {
			XSEGLOG2(&lc, I, "Cache miss for %lu:%u",
					sin->sin_addr.s_addr, sin->sin_port);
			if (insert_sockfds(cfd, sin, new_fd, SOCKFD_PENDING) < 0) {
				XSEGLOG2(&lc, E, "Cache is full");
				return -1;
			}
			if (pollfds_add(syn->pfds, new_fd,
						POLLIN | POLLERR | POLLHUP | POLLNVAL) < 0) {
				XSEGLOG2(&lc, E, "Pollfd is full");
				return -1;
			}
		} else {
			/* TODO: What here? */
			XSEGLOG2(&lc, W, "Connection has already been accepted");
		}

		inet_ntop(their_addr.sa_family, get_in_addr(&their_addr), s, sizeof(s));
		XSEGLOG2(&lc, I, "Got connection from %s", s);
	}
	return 0;
}

/* TODO: Explain this function */
int connect_to_remote(struct synapsed *syn, struct sockaddr_in *raddr_in)
{
	struct addrinfo hints, *reminfo, *p;
	char raddr[INET_ADDRSTRLEN];
	char rport[MAX_PORT_LEN + 1];
	int optval = 1;
	int sockfd = -1;
	int sockflags;
	int r;

	sockfd = lookup_sockfds(syn->cfd, raddr_in);
	if (sockfd >= 0) {
		XSEGLOG2(&lc, I, "Connection has already been established");
		return sockfd;
	}

	inet_ntop(AF_INET, &raddr_in->sin_addr, raddr, INET_ADDRSTRLEN);
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;

	/* Get info for the remote... */
	snprintf(rport, MAX_PORT_LEN, "%d", raddr_in->sin_port);
	rport[MAX_PORT_LEN] = 0;
	r = getaddrinfo(raddr, rport, &hints, &reminfo);
	if (r != 0) {
		XSEGLOG2(&lc, E, "getaddrinfo: %s\n", gai_strerror(r));
		return -1;
	}

	/* ...iterate all possible results */
	for (p = reminfo; p != NULL; p = p->ai_next) {
		/* ...create a socket */
		sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
		if (sockfd < 0)
			continue;

		/* Mark it as re-usable */
		r = setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int));
		if (r == -1) {
			XSEGLOG2(&lc, E, "Error while setting socket to SO_REUSEADDR");
			goto socket_fail;
		}

		/* Connect to it */
		if (connect(sockfd, p->ai_addr, p->ai_addrlen) != -1)
			break;

		close(sockfd);
		XSEGLOG2(&lc, W, "Created socket but cannot connect to it");
		continue;
	}

	if (p == NULL || sockfd < 0)  {
		XSEGLOG2(&lc, E, "Cannot connect to remote");
		return -1;
	}

	freeaddrinfo(reminfo);

	XSEGLOG2(&lc, I, "Connection has been established succesfully");

	/* Make socket NON-BLOCKING */
	if ((sockflags = fcntl(sockfd, F_GETFL, 0)) < 0 ||
		fcntl(sockfd, F_SETFL, sockflags | O_NONBLOCK) < 0) {
		XSEGLOG2(&lc, E, "Error while setting socket to O_NONBLOCK");
		goto socket_fail;
	}

	if (pollfds_add(syn->pfds, sockfd,
				POLLIN | POLLERR | POLLHUP | POLLNVAL) < 0) {
		XSEGLOG2(&lc, E, "Could not insert to pollfds");
		return -1;
	}
	if (insert_sockfds(syn->cfd, raddr_in, sockfd, SOCKFD_VERIFIED) < 0) {
		XSEGLOG2(&lc, E, "Cache is full");
		return -1;
	}

	XSEGLOG2(&lc, I, "Sending update packet");
	if (send(sockfd, &syn->hp, sizeof(syn->hp), 0) == -1)
		XSEGLOG2(&lc, E, "Error during send()");

	return sockfd;
socket_fail:
	close(sockfd);
	return -1;
}

/* TODO: Explain this function */
int update_remote(struct synapsed *syn, int fd)
{
	int r, rport;

	r = stat_sockfds(syn->cfd, fd);
	if (r == SOCKFD_EEXIST) {
		goto eexist;
	} else if (r == SOCKFD_PENDING) {
		if (recv(fd, &rport, sizeof(rport), 0) == -1) {
			XSEGLOG2(&lc, E, "Error during recv()");
			return -1;
		}
		r = update_sockfds(syn->cfd, fd, rport);
		if (r < 0)
			goto eexist;
		XSEGLOG2(&lc, I, "Remote has been updated with correct port (%d)", rport);
		return 0;
	}
	return 1;

eexist:
	XSEGLOG2(&lc, E, "fd (%d) of accepted "
			"connection is not in cache table", fd);
	return -1;
}
