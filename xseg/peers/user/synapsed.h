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

#include <poll.h>

#define DEFAULT_PORT 1134
#define MAX_SOCKETS 3
#define MAX_MESSAGE_LEN 100
#define BACKLOG 10	 // how many pending connections queue will hold
#define MAX_PORT_LEN 5
#define MAX_ADDR_LEN 20

#define SOCKFD_PENDING 0
#define SOCKFD_VERIFIED 1

#define SOCKFD_EEXIST -1
#define SOCKFD_ESTALE -2
#define SOCKFD_ENOSPC -3


struct synapsed {
	struct peerd *peer;
	int sockfd;
	int hp;
	struct cached_sockfd *cfd;
	struct pollfd *pfds;
	struct sockaddr_in raddr_in;
	int txp;
};

struct cached_sockfd {
	unsigned short port;
	unsigned long s_addr;
	int fd;
	int status;
};

#pragma pack(push, 1)
struct synapsed_header {
	struct xseg_request *req;
	uint32_t op;
	uint32_t state;
	uint32_t flags;
	uint32_t targetlen;
	xport xseg_dst_portno;
	uint64_t offset;
	uint64_t size;
	uint64_t datalen;
	uint64_t serviced;
};
#pragma pack(pop)

int synapsed_peerd_loop(void *arg);

int lookup_sockfds(struct cached_sockfd *cfd, struct sockaddr_in *sin);
int insert_sockfds(struct cached_sockfd *cfd,
		struct sockaddr_in *sin, int fd, int status);
int update_sockfds(struct cached_sockfd *cfd, int fd, int new_port);
int stat_sockfds(struct cached_sockfd *cfd, int fd);
void init_pollfds(struct pollfd *pfds);
int addto_pollfds(struct pollfd *pfds, int fd, short flags);

int synapsed_init_local_signal(struct peerd *peer, sigset_t *oldset);

void create_synapsed_header(struct synapsed_header *sh, struct xseg_request *req);

int accept_remote(struct synapsed *syn);
int connect_to_remote(struct synapsed *syn, struct sockaddr_in *raddr_in);
int update_remote(struct synapsed *syn, int fd);
