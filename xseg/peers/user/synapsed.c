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

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <pthread.h>
#include <xseg/xseg.h>
#include <peer.h>
#include <time.h>
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

/* Helper functions */
static struct synapsed *__get_synapsed(struct peerd *peer)
{
	return (struct synapsed *)peer->priv;
}

void custom_peer_usage()
{
	fprintf(stderr, "Custom peer options: \n"
			"  --------------------------------------------\n"
			"    -hp       | 1134 | Host port to bind\n"
			"    -ra       | None | Remote address\n"
			"    -rp       | 1134 | Remote port to connect\n"
			"    -txp      | None | Target xseg port (on remote)\n"
			"\n"
			"Additional information:\n"
			"  --------------------------------------------\n"
			"\n");
}

int custom_peer_init(struct peerd *peer, int argc, char *argv[])
{
	struct synapsed *syn;
	struct addrinfo hints, *hostinfo, *p;
	char host_port[MAX_PORT_LEN + 1];
	char ra[MAX_ADDR_LEN + 1];
	unsigned long rp = -1;
	unsigned long txp = -1;
	int sockfd;
	int sockflags;
	int optval = 1;
	int r;

	ra[0] = 0;

	/**************************\
	 * Struct initializations *
	\**************************/

	syn = malloc(sizeof(struct synapsed));
	if (!syn) {
		XSEGLOG2(&lc, E, "Malloc fail");
		goto fail;
	}
	memset(syn, 0, sizeof(struct synapsed));
	syn->hp = -1;

	syn->cfd = malloc(MAX_SOCKETS * sizeof(struct cached_sockfd));
	if (!syn->cfd) {
		XSEGLOG2(&lc, E, "Malloc fail");
		goto fail;
	}
	memset(syn->cfd, 0, MAX_SOCKETS * sizeof(struct cached_sockfd));

	syn->pfds = malloc(MAX_SOCKETS * sizeof(struct pollfd));
	if (!syn->pfds) {
		XSEGLOG2(&lc, E, "Malloc fail");
		goto fail;
	}
	memset(syn->pfds, 0, MAX_SOCKETS * sizeof(struct pollfd));

	/**********************\
	 * Synapsed arguments *
	\**********************/

	BEGIN_READ_ARGS(argc, argv);
	READ_ARG_ULONG("-hp", syn->hp);
	READ_ARG_STRING("-ra", ra, MAX_ADDR_LEN);
	READ_ARG_ULONG("-rp", rp);
	READ_ARG_ULONG("-txp", txp);
	END_READ_ARGS();

	/*****************************\
	 * Check synapsed parameters *
	\*****************************/

	/*
	 * The host port (our port) can be a user's choice or can be set to the
	 * default port
	 */
	if (syn->hp == -1)
		syn->hp = DEFAULT_PORT;

	/* The remote address is mandatory */
	if (ra[0] == 0) {
		custom_peer_usage();
		XSEGLOG2(&lc, E, "Remote address must be provided");
		goto fail;
	}
	r = inet_pton(AF_INET, ra, &syn->raddr_in.sin_addr);
	if (r == 0) {
		XSEGLOG2(&lc, E, "-ra %s: Remote address is invalid", ra);
		goto fail;
	}

	/* The remote port can be set either by user or to default */
	if (rp == -1)
		rp = DEFAULT_PORT;
	syn->raddr_in.sin_port = rp;

	/* The target xseg port is mandatory */
	if (txp == -1) {
		custom_peer_usage();
		XSEGLOG2(&lc, E, "Target xseg port must be provided");
		goto fail;
	}

	/*********************************\
	 * Create a TCP listening socket *
	\*********************************/

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	/* Get info for the host... */
	snprintf(host_port, MAX_PORT_LEN, "%d", syn->hp);
	host_port[MAX_PORT_LEN] = 0;
	r = getaddrinfo(NULL, host_port, &hints, &hostinfo);
	if (r != 0) {
		XSEGLOG2(&lc, E, "getaddrinfo: %s\n", gai_strerror(r));
		goto fail;
	}

	/* ...iterate all possible results */
	for (p = hostinfo; p != NULL; p = p->ai_next) {
		/* ...create a socket */
		sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
		if (sockfd < 0)
			continue;

		/* Make socket NON-BLOCKING */
		if ((sockflags = fcntl(sockfd, F_GETFL, 0)) < 0 ||
			fcntl(sockfd, F_SETFL, sockflags | O_NONBLOCK) < 0) {
			XSEGLOG2(&lc, E, "Error while setting socket to O_NONBLOCK");
			goto socket_fail;
		}

		/* Mark it as re-usable */
		r = setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int));
		if (r == -1) {
			XSEGLOG2(&lc, E, "Error while setting socket to SO_REUSEADDR");
			goto socket_fail;
		}

		/* Bind it */
		if (bind(sockfd, p->ai_addr, p->ai_addrlen) < 0) {
			close(sockfd);
			XSEGLOG2(&lc, W, "Created socket but cannot bind it");
			continue;
		}

		break;
	}

	if (p == NULL || sockfd < 0)  {
		XSEGLOG2(&lc, E, "Cannot create listening socket");
		goto fail;
	}

	freeaddrinfo(hostinfo);

	/* and finally listen to it */
	if (listen(sockfd, BACKLOG) < 0) {
		XSEGLOG2(&lc, E, "Cannot listen to socket");
		goto socket_fail;
	}

	/*********************************\
	 * Miscellaneous initializations *
	\*********************************/

	syn->sockfd = sockfd;
	init_pollfds(syn->pfds);
	if (addto_pollfds(syn->pfds, syn->sockfd,
				POLLIN | POLLERR | POLLHUP | POLLNVAL) < 0)
		return -1;

	syn->peer = peer;
	peer->peerd_loop = synapsed_peerd_loop;
	peer->priv = (void *)syn;

	return 0;
socket_fail:
	close(sockfd);
fail:
	free(syn->cfd);
	free(syn->pfds);
	free(syn);
	return -1;
}

void custom_peer_finalize(struct peerd *peer)
{
}

/*************************\
 * XSEG request handlers *
\*************************/

/*
 * handle_accept() first creates a connection with the remote server.
 * Then, it creates the header for the request
 */
static int handle_accept(struct peerd *peer, struct peer_req *pr,
			struct xseg_request *req)
{
	struct synapsed *syn = __get_synapsed(peer);
	struct synapsed_header sh;

	/* The remote address is hardcoded in the synapsed struct for now */
	if (connect_to_remote(syn, &syn->raddr_in) < 0)
		return -1;

	create_synapsed_header(&sh, req);

	fail(peer, pr);
#if 0
	switch (req->op) {
		case X_READ:
		case X_WRITE:
		default:
	}
#endif
	return 0;
}

static int handle_receive(struct peerd *peer, struct peer_req *pr,
			struct xseg_request *req)
{
	return 0;
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
		default:
			fail(peer, pr);
	}
	return 0;
}

/*******************\
 * Socket handlers *
\*******************/

/*
 * handle_recv() first checks if the remote has sent an update packet (e.g. its
 * port)
 */
static void handle_recv(struct synapsed *syn, int fd)
{
	int r;

	r = update_remote(syn, fd);
	if (r < 0)
		return;

#if 0
	snprintf(bufs, MAX_MESSAGE_LEN, "server says: %s at your face", bufr);
	if (send(fd, bufs, MAX_MESSAGE_LEN, 0) == -1) {
		XSEGLOG2(&lc, E, "Error during send()");
		return;
	}
#endif
}

static void handle_send(struct synapsed *syn, int fd)
{
	XSEGLOG2(&lc, I, "Ready to send on fd %d", fd);
}

static int handle_accept_conn(struct synapsed *syn)
{
	accept_remote(syn);

	return 0;
}

/*
 * handle_event() is the poll() equivalent of dispatch(). It associates each
 * revent with the appropriate function
 */
static void handle_event(struct synapsed *syn, struct pollfd *pfd)
{
	/* Our listening socket must only accept connections */
	if (pfd->fd == syn->sockfd) {
		if (pfd->revents & POLLIN) {
			if (handle_accept_conn(syn) < 0)
				terminated = 1;
		} else {
			XSEGLOG2(&lc, W, "Received events %d for listening socket",
					pfd->revents);
		}
		return;
	}

	/* For any other socket, one or more of the following events may occur */
	if (pfd->revents & POLLERR)
		XSEGLOG2(&lc, E, "An error has occured for fd %d", pfd->fd);
	if (pfd->revents & POLLHUP)
		XSEGLOG2(&lc, W, "A hangup has occured for fd %d", pfd->fd);
	if (pfd->revents & POLLNVAL)
		XSEGLOG2(&lc, W, "Socket fd %d is not open", pfd->fd);
	if (pfd->revents & POLLIN)
		handle_recv(syn, pfd->fd);
	if (pfd->revents & POLLOUT)
		handle_send(syn, pfd->fd);
}

/*
 * synapsed_poll() is a wrapper for two modes of polling, depending on whether
 * the user has passed a valid sigset_t:
 *
 * 1. If the sigset_t is invalid (NULL), then we do a simple, non-blocking (0s
 *    timeout) poll(), which should return immediately.
 * 2. If the sigset_t is valid, we do a ppoll() (see man pages) for 10s. The
 *    handed sigset_t should probably unblock the SIGIO signal so that we can
 *    wake up if a SIGIO has been sent during or right before we enter poll().
 *
 * Finally, for every poll()ing mode, the return value is interpreted the same.
 * See the man pages for more info on the poll() return values.
 */
void synapsed_poll(struct synapsed *syn, sigset_t *oldset, char *id)
{
	struct pollfd *pfds = syn->pfds;
	struct timespec ts = {10, 0};
	int i, ret;

	if (oldset == NULL) {
		ret = poll(pfds, MAX_SOCKETS, 0);
	} else {
		XSEGLOG2(&lc, D, "%s sleeps on poll()", id);
		ret = ppoll(pfds, MAX_SOCKETS, &ts, oldset);
		XSEGLOG2(&lc, D, "%s stopped poll()ing", id);
	}

	if (ret > 0) {
		XSEGLOG2(&lc, D, "There are %d new events", ret);
		for (i = 0; i < MAX_SOCKETS; i++) {
			if (pfds[i].revents != 0)
				handle_event(syn, &pfds[i]);
		}
	} else if (ret < 0 && errno != EINTR) {
		XSEGLOG2(&lc, E, "Error during polling: %d", errno);
		terminated = 1;
	}
}

/*
 * This function substitutes the default generic_peerd_loop of peer.c.
 * It's plugged to struct peerd at custom peer's initialisation
 */
int synapsed_peerd_loop(void *arg)
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
	struct synapsed *syn = __get_synapsed(peer);
	xport portno_start = peer->portno_start;
	xport portno_end = peer->portno_end;
	pid_t pid = syscall(SYS_gettid);
	sigset_t oldset;
	uint64_t threshold=1000/(1 + portno_end - portno_start);
	uint64_t loops;
	int r;

	XSEGLOG2(&lc, I, "%s has tid %u.\n",id, pid);

	r = synapsed_init_local_signal(peer, &oldset);
	if (r < 0) {
		XSEGLOG2(&lc, E, "Failed to initialize local signal");
		return -1;
	}

	/*
	 * The current implementation is not very fast. Besides the fact that poll()
	 * is slow compared to libev/libevent, the rescheduling is not as fast as
	 * with sigtimedwait().
	 * TODO: See if the above can be solved by libevent or by "Realtime signals"
	 * (see C10k)
	 */
	for (;!(isTerminate() && all_peer_reqs_free(peer));) {
		for (loops = threshold; loops > 0; loops--) {
			/* Poll very briefly for new events*/
			synapsed_poll(syn, NULL, id);
			if (loops == 1)
				xseg_prepare_wait(xseg, peer->portno_start);
#ifdef MT
			if (check_ports(peer, t))
#else
			if (check_ports(peer))
#endif
				loops = threshold;
		}

		/* Sleep while poll()ing for new events */
		synapsed_poll(syn, &oldset, id);
		xseg_cancel_wait(xseg, peer->portno_start);
	}
	return 0;
}

