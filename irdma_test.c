/*
 * Copyright (c) 2005 Topspin Communications.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#define _GNU_SOURCE
#include <config.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <netdb.h>
#include <malloc.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <time.h>
#include <inttypes.h>
#include "pingpong.h"
#include <ccan/minmax.h>

static int debug = 0;
#define DEBUG_LOG if (debug) printf

static unsigned int current_port;
#define RPING_MSG_FMT           "irdma-test-ping-%d: "
#define HUGE_MEM_FILE_START_BUF_NAME	"/mnt/huge/start_buf-port%d"
#define HUGE_MEM_FILE_RDMA_BUF_NAME	"/mnt/huge/rdma_buf-port%d"
#define HUGE_MEM_FILE_LEN		100

#define TEMP_BUF_LEN			4096
char tmp_buf[TEMP_BUF_LEN];

#define FPGA_PACKET_LEN			1024
#define NORMAL_PRINT_LEN		100

static char huge_mem_file_start_buf[HUGE_MEM_FILE_LEN];
static char huge_mem_file_rdma_buf[HUGE_MEM_FILE_LEN];

#define HUGE_PAGE_SIZE (1024*1024*1024)
#define RPING_SQ_DEPTH 128 //????
#define MAX_NUM_WQE 32

typedef uint64_t phys_addr_t;
#define BAD_IOVA ((phys_addr_t)-1)
#define PFN_MASK_SIZE	8

enum {
	PINGPONG_RECV_WRID = 1,
	PINGPONG_SEND_WRID = 2,
	PINGPONG_RDMA_WRITE_WRID = 3,
	PINGPONG_RDMA_READ_WRID = 4,
};

enum test_state {
	IDLE = 1,
	CONNECT_REQUEST,
	ADDR_RESOLVED,
	ROUTE_RESOLVED,
	CONNECTED,
	RDMA_READ_ADV,
	RDMA_READ_COMPLETE,
	RDMA_WRITE_ADV,
	RDMA_WRITE_COMPLETE,
	DISCONNECTED,
	TEST_STATE_ERROR
};

static int use_huge_page = 1;
static int do_send_recv = 0;
static int physical_addr = 0;
static int page_size;
static int validate_buf;
struct timeval time_start, time_end;

struct rdma_buf_info {
	__be64 buf;
	__be64 phys_buf;
	__be32 rkey;
	__be32 size;
};

struct irdma_context {
	struct ibv_context	*context;
	struct ibv_comp_channel *channel;
	struct ibv_pd		*pd;
	union {
		struct ibv_cq		*cq;
		struct ibv_cq_ex	*cq_ex;
	} cq_s;
	struct ibv_qp		*qp;
	struct ibv_qp_ex	*qpx;

	int fd_huge_start_buf;
	int fd_huge_rdma_buf;
	
	char			*start_buf;
	char			*rdma_buf;
	phys_addr_t		phy_start_buf;
	phys_addr_t		phy_rdma_buf;
	struct ibv_mr		*start_mr;
	struct ibv_mr		*rdma_mr;	
	
	int			 size;
	int			 num_wqe;
	int			 send_flags;
	int			 rx_depth;
	int			 pending;
	struct ibv_port_attr     portinfo;
	struct ibv_send_wr rdma_sq_wr[MAX_NUM_WQE];	/* rdma work request record */
	struct ibv_sge rdma_sgl[MAX_NUM_WQE];	/* rdma single SGE */
	struct rdma_buf_info self_rdma_buf_info;
	struct rdma_buf_info remote_rdma_buf_info;

	enum test_state state;		/* used for cond/signalling */

	int sockfd;
	int connfd;
};

static struct ibv_cq *irdma_cq(struct irdma_context *ctx)
{
	return ctx->cq_s.cq;
}

struct pingpong_dest {
	int lid;
	int qpn;
	int psn;
	union ibv_gid gid;
};

static void write_data_into_file(char *data, int size, const char *file)
{
#define FILE_NAME_LEN 50
	int fd;
	char file_name[FILE_NAME_LEN];

	snprintf(file_name, FILE_NAME_LEN, "%s-port%d", file, current_port);

	fd = open(file_name, O_RDWR | O_CREAT, 0666);
	if (fd < 0) {
		printf("open %s failed\n", file_name);
		return;
	}

	printf("start write file %s\n", file_name);
	if (write(fd, data, size) != size){
		printf("write %s failed\n", file_name);
	}

	close(fd);
}

static void calculate_and_print_speed(struct irdma_context *ctx)
{
	static float total_usec = 0;
	static long long total_size = 0;
	static int tested_times = 0;
	float usec = (time_end.tv_sec - time_start.tv_sec) * 1000000 +
		(time_end.tv_usec - time_start.tv_usec);
	long long bytes = (long long) ctx->size;

	if(usec == 0){
		perror("wrong time");
		return;
	}
	total_usec += usec;
	total_size += bytes;
	tested_times++;
	
	printf("%lld bytes in %.2f seconds (%.2f usec) = %.2f Mbit/sec; total_usec = %.2f usec, average time =  %.2f usec, average speed = %.2f Mbit/sec\n",
		   bytes, usec / 1000000., usec,  bytes * 8. / usec, total_usec, total_usec/tested_times, total_size * 8. /total_usec);
	//printf("finished in %.2f seconds = %.2f usec\n",
	//	   usec / 1000000., usec);
}

static void calculate_and_print_speed_with_iters(struct irdma_context *ctx, int iters)
{
	float usec = (time_end.tv_sec - time_start.tv_sec) * 1000000 +
		(time_end.tv_usec - time_start.tv_usec);
	long long bytes = (long long) ctx->size * iters * 2;

	if(usec == 0){
		perror("wrong time");
		return;
	}
	
	printf("%lld bytes in %.2f seconds = %.2f Mbit/sec\n",
		   bytes, usec / 1000000., bytes * 8. / usec);
	printf("%d iters  in %.2f seconds = %.2f usec/iter\n",
		   iters, usec / 1000000., usec / iters);
}

static void start_record_time(void)
{
	if (gettimeofday(&time_start, NULL)) {
		perror("gettimeofday");
	}
	return;
}

static void stop_record_time(void)
{
	if (gettimeofday(&time_end, NULL)) {
		perror("gettimeofday");
	}
	return;
}

static void fill_start_buf(char* start_buf, int size)
{
#define TMP_BUF_LEN 50
	int cc, i;
	static int number = 0;
	static int start = 65;
	unsigned char c;
	char tmp[TMP_BUF_LEN];

	/* Put some ascii text in the buffer. */
	cc = snprintf(start_buf, size, RPING_MSG_FMT, number++);
	for (i = cc, c = start; i < size; i++) {
		start_buf[i] = c;
		c++;
		if (c > 122)
			c = 65;
	}
	start++;
	if (start > 122)
		start = 65;
	start_buf[size - 1] = 0;

	/* set start of every page to page%d */
	for (i = 0; i < size; i+=FPGA_PACKET_LEN) {
			cc = snprintf(tmp, TMP_BUF_LEN, "port%d,packet%d:", current_port, i/FPGA_PACKET_LEN);
			memcpy(&start_buf[i], tmp, cc);
			start_buf[i+FPGA_PACKET_LEN-1] = 0;
			start_buf[i+FPGA_PACKET_LEN-2] = '\n';
			start_buf[i+FPGA_PACKET_LEN-3] = '\r';
	}

	return;
}

/*
 * Get physical address of any mapped virtual address in the current process.
 */
static phys_addr_t mem_virt2phy(void *virtaddr)
{
	int fd, retval;
	uint64_t page, physaddr;
	unsigned long virt_pfn;
	off_t offset;
	off_t offset_return = 0;

	fd = open("/proc/self/pagemap", O_RDONLY);
	if (fd < 0) {
		fprintf(stderr, "%s(): cannot open /proc/self/pagemap: %s\n",
			__func__, strerror(errno));
		return BAD_IOVA;
	}

	virt_pfn = (unsigned long)virtaddr / page_size;
	offset = sizeof(uint64_t) * virt_pfn;

	if ((offset_return = lseek(fd, offset, SEEK_SET)) == (off_t) -1) {
		fprintf(stderr, "%s(): seek error in /proc/self/pagemap: %s\n",
				__func__, strerror(errno));
		close(fd);
		return BAD_IOVA;
	}

	retval = read(fd, &page, PFN_MASK_SIZE);
	close(fd);
	if (retval < 0) {
		fprintf(stderr, "%s(): cannot read /proc/self/pagemap: %s\n",
				__func__, strerror(errno));
		return BAD_IOVA;
	} else if (retval != PFN_MASK_SIZE) {
		fprintf(stderr, "%s(): read %d bytes from /proc/self/pagemap "
				"but expected %d:\n",
				__func__, retval, PFN_MASK_SIZE);
		return BAD_IOVA;
	}

	/*
	 * the pfn (page frame number) are bits 0-54 (see
	 * pagemap.txt in linux Documentation)
	 */
	if ((page & 0x7fffffffffffffULL) == 0){
		fprintf(stderr, "%s(): wrong page = %lx, offset_return = 0x%lx, retval = %d\n",
				__func__, page, offset_return, retval);
		return BAD_IOVA;
	}

	physaddr = ((page & 0x7fffffffffffffULL) * page_size)
		+ ((unsigned long)virtaddr % page_size);

	DEBUG_LOG("virt addr = 0x%lx, phy addr = 0x%lx, page = %lx, offset = 0x%lx, offset_return = 0x%lx\n", 
		(uint64_t)virtaddr, physaddr, page, offset, offset_return);
	return physaddr;
}

static int irdma_connect_ctx(struct irdma_context *ctx, int port, int my_psn,
			  enum ibv_mtu mtu, int sl,
			  struct pingpong_dest *dest, int sgid_idx)
{
	int attr_mask = IBV_QP_STATE        |
			  IBV_QP_AV                 |
			  IBV_QP_PATH_MTU           |
			  IBV_QP_DEST_QPN           |
			  IBV_QP_RQ_PSN             |
			  IBV_QP_MAX_DEST_RD_ATOMIC |
			  IBV_QP_MIN_RNR_TIMER;

	struct ibv_qp_attr attr = {
		.qp_state		= IBV_QPS_RTR,
		.path_mtu		= mtu,
		.dest_qp_num		= dest->qpn,
		.rq_psn			= dest->psn,
		.max_dest_rd_atomic	= 1,
		.min_rnr_timer		= 12,
		.ah_attr		= {
			.is_global	= 0,
			.dlid		= dest->lid,
			.sl		= sl,
			.src_path_bits	= 0,
			.port_num	= port
		}
	};

	if (dest->gid.global.interface_id) {
		attr.ah_attr.is_global = 1;
		attr.ah_attr.grh.hop_limit = 1;
		attr.ah_attr.grh.dgid = dest->gid;
		attr.ah_attr.grh.sgid_index = sgid_idx;
	}

	if (ibv_modify_qp(ctx->qp, &attr, attr_mask)) {
		fprintf(stderr, "Failed to modify QP to RTR\n");
		return 1;
	}

	attr.qp_state	    = IBV_QPS_RTS;
	attr.timeout	    = 14;
	attr.retry_cnt	    = 7;
	attr.rnr_retry	    = 7;
	attr.sq_psn	    = my_psn;
	attr.max_rd_atomic  = 1;
	if (ibv_modify_qp(ctx->qp, &attr,
			  IBV_QP_STATE              |
			  IBV_QP_TIMEOUT            |
			  IBV_QP_RETRY_CNT          |
			  IBV_QP_RNR_RETRY          |
			  IBV_QP_SQ_PSN             |
			  IBV_QP_MAX_QP_RD_ATOMIC)) {
		fprintf(stderr, "Failed to modify QP to RTS\n");
		return 1;
	}

	return 0;
}

static struct pingpong_dest *irdma_client_exch_dest(struct irdma_context *ctx, const char *servername, int port,
						 const struct pingpong_dest *my_dest)
{
	struct addrinfo *res, *t;
	struct addrinfo hints = {
		.ai_family   = AF_UNSPEC,
		.ai_socktype = SOCK_STREAM
	};
	char *service;
	char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
	int n;
	struct pingpong_dest *rem_dest = NULL;
	char gid[33];

	ctx->sockfd = -1;

	if (asprintf(&service, "%d", port) < 0)
		return NULL;

	n = getaddrinfo(servername, service, &hints, &res);

	if (n < 0) {
		fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
		free(service);
		return NULL;
	}

	for (t = res; t; t = t->ai_next) {
		ctx->sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (ctx->sockfd >= 0) {
			if (!connect(ctx->sockfd, t->ai_addr, t->ai_addrlen))
				break;
			close(ctx->sockfd);
			ctx->sockfd = -1;
		}
	}

	freeaddrinfo(res);
	free(service);

	if (ctx->sockfd < 0) {
		fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
		return NULL;
	}

	gid_to_wire_gid(&my_dest->gid, gid);
	sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn,
							my_dest->psn, gid);
	if (write(ctx->sockfd, msg, sizeof msg) != sizeof msg) {
		fprintf(stderr, "Couldn't send local address\n");
		goto out;
	}

	if (read(ctx->sockfd, msg, sizeof msg) != sizeof msg ||
	    write(ctx->sockfd, "done", sizeof "done") != sizeof "done") {
		perror("client read/write");
		fprintf(stderr, "Couldn't read/write remote address\n");
		goto out;
	}

	rem_dest = malloc(sizeof *rem_dest);
	if (!rem_dest)
		goto out;

	sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn,
						&rem_dest->psn, gid);
	wire_gid_to_gid(gid, &rem_dest->gid);

	if (read(ctx->sockfd, (char*)&ctx->remote_rdma_buf_info, sizeof(ctx->remote_rdma_buf_info)) != sizeof(ctx->remote_rdma_buf_info) ||
	    write(ctx->sockfd, "done", sizeof "done") != sizeof "done") {
		perror("client read/write");
		fprintf(stderr, "Couldn't read/write remote_rdma_buf address\n");
		goto out;
	}

out:
	//close(ctx->sockfd);
	return rem_dest;
}

static struct pingpong_dest *irdma_server_exch_dest(struct irdma_context *ctx,
						 int ib_port, enum ibv_mtu mtu,
						 int port, int sl,
						 const struct pingpong_dest *my_dest,
						 int sgid_idx)
{
	struct addrinfo *res, *t;
	struct addrinfo hints = {
		.ai_flags    = AI_PASSIVE,
		.ai_family   = AF_UNSPEC,
		.ai_socktype = SOCK_STREAM
	};
	char *service;
	char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
	int n;
	int sockfd = -1;
	struct pingpong_dest *rem_dest = NULL;
	char gid[33];

	if (asprintf(&service, "%d", port) < 0)
		return NULL;

	n = getaddrinfo(NULL, service, &hints, &res);

	if (n < 0) {
		fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
		free(service);
		return NULL;
	}

	for (t = res; t; t = t->ai_next) {
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd >= 0) {
			n = 1;

			setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

			if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
				break;
			close(sockfd);
			sockfd = -1;
		}
	}

	freeaddrinfo(res);
	free(service);

	if (sockfd < 0) {
		fprintf(stderr, "Couldn't listen to port %d\n", port);
		return NULL;
	}

	listen(sockfd, 1);
	ctx->connfd = accept(sockfd, NULL, NULL);
	close(sockfd);
	if (ctx->connfd < 0) {
		fprintf(stderr, "accept() failed\n");
		return NULL;
	}

	n = read(ctx->connfd, msg, sizeof msg);
	if (n != sizeof msg) {
		perror("server read");
		fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int) sizeof msg);
		goto out;
	}

	rem_dest = malloc(sizeof *rem_dest);
	if (!rem_dest)
		goto out;

	sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn,
							&rem_dest->psn, gid);
	wire_gid_to_gid(gid, &rem_dest->gid);

	if (irdma_connect_ctx(ctx, ib_port, my_dest->psn, mtu, sl, rem_dest,
								sgid_idx)) {
		fprintf(stderr, "Couldn't connect to remote QP\n");
		free(rem_dest);
		rem_dest = NULL;
		goto out;
	}


	gid_to_wire_gid(&my_dest->gid, gid);
	sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn,
							my_dest->psn, gid);
	if (write(ctx->connfd, msg, sizeof msg) != sizeof msg ||
	    read(ctx->connfd, msg, sizeof msg) != sizeof "done") {
		fprintf(stderr, "Couldn't send/recv local address\n");
		free(rem_dest);
		rem_dest = NULL;
		goto out;
	}

	if (write(ctx->connfd, (char*)&ctx->self_rdma_buf_info, sizeof(ctx->self_rdma_buf_info)) != sizeof(ctx->self_rdma_buf_info) ||
	    read(ctx->connfd, msg, sizeof msg) != sizeof "done") {
		fprintf(stderr, "Couldn't send/recv self_rdma_buf address\n");
		free(rem_dest);
		rem_dest = NULL;
		goto out;
	}

out:
	//close(ctx->connfd);
	return rem_dest;
}

static void irdma_wait_quit(struct irdma_context *ctx)
{
	char msg[100];
	if (read(ctx->connfd, msg, sizeof msg) != sizeof "quit") {
		fprintf(stderr, "Failed to get quit message\n");
	}
}

static void irdma_inform_quit(struct irdma_context *ctx)
{
	if (write(ctx->sockfd, "quit", sizeof "quit") != sizeof "quit") {
		fprintf(stderr, "Failed to send quit message\n");
	}
}

static void irdma_setup_wr_rdma_buf(struct irdma_context *ctx)
{
	int i;
	uint32_t remote_rkey;		/* remote guys RKEY */
	uint64_t remote_addr;		/* remote guys TO */
	int buffer_offset = ctx->size / ctx->num_wqe;

	remote_rkey = be32toh(ctx->remote_rdma_buf_info.rkey);
	remote_addr = be64toh(physical_addr ? ctx->remote_rdma_buf_info.phys_buf : ctx->remote_rdma_buf_info.buf);

	for (i = 0; i < ctx->num_wqe; i++) {
		ctx->rdma_sgl[i].addr = (physical_addr ? (uint64_t)ctx->phy_rdma_buf : (uint64_t)ctx->rdma_buf) + i * buffer_offset;
		ctx->rdma_sgl[i].lkey = ctx->rdma_mr->lkey;
		ctx->rdma_sq_wr[i].send_flags = (i == (ctx->num_wqe - 1)) ? IBV_SEND_SIGNALED : 0;
		ctx->rdma_sq_wr[i].sg_list = &ctx->rdma_sgl[i];
		ctx->rdma_sq_wr[i].num_sge = 1;

		/* Issue RDMA Read. */
		ctx->rdma_sq_wr[i].opcode = IBV_WR_RDMA_READ;
		ctx->rdma_sq_wr[i].wr.rdma.rkey = remote_rkey;
		ctx->rdma_sq_wr[i].wr.rdma.remote_addr = remote_addr + i * buffer_offset;
		ctx->rdma_sq_wr[i].sg_list->length = buffer_offset;
		ctx->rdma_sq_wr[i].wr_id = PINGPONG_RDMA_READ_WRID;

		if (i < (ctx->num_wqe - 1)){
			ctx->rdma_sq_wr[i].next = &ctx->rdma_sq_wr[i+1];
		}
	}
}

static void irdma_setup_wr_start_buf(struct irdma_context *ctx)
{
	int i;
	uint32_t remote_rkey;		/* remote guys RKEY */
	uint64_t remote_addr;		/* remote guys TO */
	uint32_t remote_len;		/* remote guys LEN */
	int buffer_offset = ctx->size / ctx->num_wqe;

	remote_rkey = be32toh(ctx->remote_rdma_buf_info.rkey);
	remote_addr = be64toh(physical_addr ? ctx->remote_rdma_buf_info.phys_buf : ctx->remote_rdma_buf_info.buf);
	remote_len  = be32toh(ctx->remote_rdma_buf_info.size);
	DEBUG_LOG("Received rkey %x addr %" PRIx64 " len %d from peer, buffer_offset = 0x%x\n",
		remote_rkey, remote_addr, remote_len, buffer_offset);

	for (i = 0; i < ctx->num_wqe; i++) {
		ctx->rdma_sgl[i].addr = (physical_addr ? (uint64_t)ctx->phy_start_buf : (uint64_t)ctx->start_buf) + i * buffer_offset;
		ctx->rdma_sgl[i].lkey = ctx->start_mr->lkey;
		ctx->rdma_sq_wr[i].send_flags = (i == (ctx->num_wqe - 1)) ? IBV_SEND_SIGNALED : 0;
		ctx->rdma_sq_wr[i].sg_list = &ctx->rdma_sgl[i];
		ctx->rdma_sq_wr[i].num_sge = 1;

		/* RDMA Write echo data */
		ctx->rdma_sq_wr[i].opcode = IBV_WR_RDMA_WRITE;
		ctx->rdma_sq_wr[i].wr.rdma.rkey = remote_rkey;
		ctx->rdma_sq_wr[i].wr.rdma.remote_addr = remote_addr + i * buffer_offset;
		ctx->rdma_sq_wr[i].sg_list->length = buffer_offset;
		ctx->rdma_sq_wr[i].wr_id = PINGPONG_RDMA_WRITE_WRID;

		if (i < (ctx->num_wqe - 1)){
			ctx->rdma_sq_wr[i].next = &ctx->rdma_sq_wr[i+1];
		}

		DEBUG_LOG("rdma write from lkey %x laddr %" PRIx64 " len %d\n",
			ctx->rdma_sq_wr[i].sg_list->lkey,
			ctx->rdma_sq_wr[i].sg_list->addr,
			ctx->rdma_sq_wr[i].sg_list->length);
	}
}


static void irdma_format_buf_info(struct irdma_context *ctx, char *buf, phys_addr_t phys_buf, struct ibv_mr *mr)
{
	struct rdma_buf_info *info = &ctx->self_rdma_buf_info;

	info->buf = htobe64((uint64_t) buf);
	info->phys_buf = htobe64((uint64_t) phys_buf);
	info->rkey = htobe32(mr->rkey);
	info->size = htobe32(ctx->size);

	DEBUG_LOG("RDMA addr %" PRIx64" physical addr %" PRIx64" rkey %x len %d\n",
		  be64toh(info->buf), be64toh(info->phys_buf), be32toh(info->rkey), be32toh(info->size));
}

static void irdma_print_buf_info(struct irdma_context *ctx)
{
	struct rdma_buf_info *self_info = &ctx->self_rdma_buf_info;
	struct rdma_buf_info *remote_info = &ctx->remote_rdma_buf_info;

	DEBUG_LOG("self RDMA addr %" PRIx64" physical addr %" PRIx64" rkey %x len %d\n",
		  be64toh(self_info->buf), be64toh(self_info->phys_buf), be32toh(self_info->rkey), be32toh(self_info->size));
	DEBUG_LOG("remote RDMA addr %" PRIx64" physical addr %" PRIx64"  rkey %x len %d\n",
		  be64toh(remote_info->buf), be64toh(remote_info->phys_buf), be32toh(remote_info->rkey), be32toh(remote_info->size));
}

static void irdma_dealloc_hugepage_buffer(struct irdma_context *ctx)
{
	if (ctx->rdma_buf)
		munmap(ctx->rdma_buf, HUGE_PAGE_SIZE);
	
	if (ctx->start_buf)
		munmap(ctx->start_buf, HUGE_PAGE_SIZE);
	
	if (ctx->fd_huge_rdma_buf) {
		close(ctx->fd_huge_rdma_buf);
		unlink(huge_mem_file_rdma_buf);
	}
	
	if (ctx->fd_huge_start_buf) {
		close(ctx->fd_huge_start_buf);
		unlink(huge_mem_file_start_buf);
	}

	ctx->rdma_buf = (char*)0;
	ctx->start_buf = (char*)0;
	ctx->fd_huge_rdma_buf = 0;
	ctx->fd_huge_start_buf = 0;
	return;
}

static int irdma_alloc_hugepage_buffer(struct irdma_context *ctx)
{
	snprintf(huge_mem_file_start_buf, HUGE_MEM_FILE_LEN, HUGE_MEM_FILE_START_BUF_NAME, current_port);
	snprintf(huge_mem_file_rdma_buf, HUGE_MEM_FILE_LEN, HUGE_MEM_FILE_RDMA_BUF_NAME, current_port);

	ctx->fd_huge_start_buf = open(huge_mem_file_start_buf, O_CREAT | O_RDWR, 0777);
    if(ctx->fd_huge_start_buf < 0){
		fprintf(stderr, "Open fd_huge_start_buf error.\n");
        return -1;
    }

	ctx->fd_huge_rdma_buf = open(huge_mem_file_rdma_buf, O_CREAT | O_RDWR, 0777);
    if(ctx->fd_huge_rdma_buf < 0){
		fprintf(stderr, "Open fd_huge_rdma_buf error.\n");
        goto close_huge_file_start_buf;
    }

	ctx->start_buf = mmap(0, HUGE_PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ctx->fd_huge_start_buf, 0);
    if(ctx->start_buf == MAP_FAILED){
        fprintf(stderr, "mmap fd_huge_start_buf error.\n");
        goto close_huge_file_rdma_buf;
    }

	ctx->rdma_buf = mmap(0, HUGE_PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ctx->fd_huge_rdma_buf, 0);
    if(ctx->rdma_buf == MAP_FAILED){
        fprintf(stderr, "mmap fd_huge_rdma_buf error.\n");
        goto unmap_start_buffer;
    }
	return 0;

unmap_start_buffer:
	munmap(ctx->start_buf, HUGE_PAGE_SIZE);

close_huge_file_rdma_buf:
	close(ctx->fd_huge_rdma_buf);
	unlink(huge_mem_file_rdma_buf);

close_huge_file_start_buf:
	close(ctx->fd_huge_start_buf);
	unlink(huge_mem_file_start_buf);

	return -1;
}

static void irdma_dealloc_common_buffer(struct irdma_context *ctx)
{
	if (ctx->rdma_buf)
		munmap(ctx->rdma_buf, ctx->size);

	if (ctx->start_buf)
		munmap(ctx->start_buf, ctx->size);

	ctx->rdma_buf = (char*)0;
	ctx->start_buf = (char*)0;
	return;
}

static int irdma_alloc_common_buffer(struct irdma_context *ctx)
{
	ctx->start_buf = mmap(0, ctx->size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE | MAP_ANONYMOUS, -1, 0);
    if(ctx->start_buf == MAP_FAILED){
        fprintf(stderr, "mmap start_buf error.\n");
        return -1;
    }

	ctx->rdma_buf = mmap(0, ctx->size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE | MAP_ANONYMOUS, -1, 0);
    if(ctx->rdma_buf == MAP_FAILED){
        fprintf(stderr, "mmap rdma_buf error.\n");
        goto unmap_start_buffer;
    }
	return 0;

unmap_start_buffer:
	munmap(ctx->start_buf, HUGE_PAGE_SIZE);

	return -1;
}

static int irdma_alloc_buffer(struct irdma_context *ctx)
{
	if (use_huge_page)
		return irdma_alloc_hugepage_buffer(ctx);
	else
		return irdma_alloc_common_buffer(ctx);
}

static void irdma_dealloc_buffer(struct irdma_context *ctx)
{
	if (use_huge_page)
		irdma_dealloc_hugepage_buffer(ctx);
	else
		irdma_dealloc_common_buffer(ctx);

	return;
}

static struct irdma_context *irdma_init_ctx(struct ibv_device *ib_dev, int size,
					    int rx_depth, int port, int use_event, char *dest_ip, int num_wqe)
{
	int ret;
	struct in_addr ipv4_addr;
	struct irdma_context *ctx;
	int access_flags = IBV_ACCESS_LOCAL_WRITE |
				 IBV_ACCESS_REMOTE_READ |
				 IBV_ACCESS_REMOTE_WRITE |
				 IBV_ACCESS_HUGETLB;

	ctx = calloc(1, sizeof *ctx);
	if (!ctx)
		return NULL;

	ctx->size       = size;
	ctx->send_flags = IBV_SEND_SIGNALED;
	ctx->rx_depth   = rx_depth;
	ctx->num_wqe    = num_wqe;

    if(irdma_alloc_buffer(ctx) == -1){
        fprintf(stderr, "alloc buffer error.\n");
        goto clean_ctx;
    }

	ctx->phy_start_buf = mem_virt2phy((void*)ctx->start_buf);
	ctx->phy_rdma_buf = mem_virt2phy((void*)ctx->rdma_buf);

	memset(ctx->rdma_buf, 0x7b, size);	

	ctx->context = ibv_open_device(ib_dev);
	if (!ctx->context) {
		fprintf(stderr, "Couldn't get context for %s\n",
			ibv_get_device_name(ib_dev));
		goto demalloc_buffer;
	}

	if (use_event) {
		ctx->channel = ibv_create_comp_channel(ctx->context);
		if (!ctx->channel) {
			fprintf(stderr, "Couldn't create completion channel\n");
			goto clean_device;
		}
		DEBUG_LOG("created channel %p\n", ctx->channel);
	} else
		ctx->channel = NULL;

	ctx->pd = ibv_alloc_pd(ctx->context);
	if (!ctx->pd) {
		fprintf(stderr, "Couldn't allocate PD\n");
		goto clean_comp_channel;
	}

	ctx->start_mr = ibv_reg_mr(ctx->pd, ctx->start_buf, size, access_flags);

	if (!ctx->start_mr) {
		fprintf(stderr, "Couldn't register start MR\n");
		goto clean_pd;
	}

	ctx->rdma_mr = ibv_reg_mr(ctx->pd, ctx->rdma_buf, size, access_flags);

	if (!ctx->rdma_mr) {
		fprintf(stderr, "Couldn't register rdma MR\n");
		goto clean_start_mr;
	}

	irdma_format_buf_info(ctx, ctx->rdma_buf, ctx->phy_rdma_buf, ctx->rdma_mr);

	ctx->cq_s.cq = ibv_create_cq(ctx->context, rx_depth + 1, NULL,
					     ctx->channel, 0);

	if (!irdma_cq(ctx)) {
		fprintf(stderr, "Couldn't create CQ\n");
		goto clean_rdma_mr;
	}

	ret = ibv_req_notify_cq(irdma_cq(ctx), 0);
	if (ret) {
		fprintf(stderr, "ibv_create_cq(notify) failed, ret = %d, errno = %d\n", ret, errno);
		goto clean_cq;
	}

	{
		struct ibv_qp_init_attr init_attr = {
			.send_cq = irdma_cq(ctx),
			.recv_cq = irdma_cq(ctx),
			.cap     = {
				.max_send_wr  = RPING_SQ_DEPTH,
				.max_recv_wr  = rx_depth,
				.max_send_sge = 1,
				.max_recv_sge = 1
			},
			.qp_type = IBV_QPT_RC
		};

		if (dest_ip) {	//added for dest IP
			inet_pton(AF_INET, dest_ip, (void *)&ipv4_addr);
			init_attr.cap.max_recv_sge = ipv4_addr.s_addr;
			printf("dest ip = 0x%x\r\n", init_attr.cap.max_recv_sge);
		};

		ctx->qp = ibv_create_qp(ctx->pd, &init_attr);

		if (!ctx->qp)  {
			fprintf(stderr, "Couldn't create QP\n");
			goto clean_cq;
		}
		printf("qp number = %d\r\n", ctx->qp->qp_num);

		//ibv_query_qp(ctx->qp, &attr, IBV_QP_CAP, &init_attr);
	}

	{
		struct ibv_qp_attr attr = {
			.qp_state        = IBV_QPS_INIT,
			.pkey_index      = 0,
			.port_num        = port,
			.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE
		};

		if (ibv_modify_qp(ctx->qp, &attr,
				  IBV_QP_STATE              |
				  IBV_QP_PKEY_INDEX         |
				  IBV_QP_PORT               |
				  IBV_QP_ACCESS_FLAGS)) {
			fprintf(stderr, "Failed to modify QP to INIT\n");
			goto clean_qp;
		}
	}

	return ctx;

clean_qp:
	ibv_destroy_qp(ctx->qp);

clean_cq:
	ibv_destroy_cq(irdma_cq(ctx));

clean_rdma_mr:
	ibv_dereg_mr(ctx->rdma_mr);

clean_start_mr:
	ibv_dereg_mr(ctx->start_mr);

clean_pd:
	ibv_dealloc_pd(ctx->pd);

clean_comp_channel:
	if (ctx->channel)
		ibv_destroy_comp_channel(ctx->channel);

clean_device:
	ibv_close_device(ctx->context);

demalloc_buffer:
	irdma_dealloc_buffer(ctx);

clean_ctx:
	free(ctx);

	return NULL;
}

static int irdma_close_ctx(struct irdma_context *ctx)
{
	if (ibv_destroy_qp(ctx->qp)) {
		fprintf(stderr, "Couldn't destroy QP\n");
		return 1;
	}

	if (ibv_destroy_cq(irdma_cq(ctx))) {
		fprintf(stderr, "Couldn't destroy CQ\n");
		return 1;
	}

	if (ibv_dereg_mr(ctx->start_mr)) {
		fprintf(stderr, "Couldn't deregister start MR\n");
		return 1;
	}

	if (ibv_dereg_mr(ctx->rdma_mr)) {
		fprintf(stderr, "Couldn't deregister rdma MR\n");
		return 1;
	}

	if (ibv_dealloc_pd(ctx->pd)) {
		fprintf(stderr, "Couldn't deallocate PD\n");
		return 1;
	}

	if (ctx->channel) {
		if (ibv_destroy_comp_channel(ctx->channel)) {
			fprintf(stderr, "Couldn't destroy completion channel\n");
			return 1;
		}
	}

	if (ibv_close_device(ctx->context)) {
		fprintf(stderr, "Couldn't release context\n");
		return 1;
	}

	irdma_dealloc_buffer(ctx);

	free(ctx);

	return 0;
}

static int irdma_post_recv(struct irdma_context *ctx, int n)
{
#if 0
	struct ibv_sge list = {
		.addr	= (uintptr_t) ctx->start_buf,
		.length = ctx->size,
		.lkey	= ctx->start_mr->lkey
	};
	struct ibv_recv_wr wr = {
		.wr_id	    = PINGPONG_RECV_WRID,
		.sg_list    = &list,
		.num_sge    = 1,
	};
	struct ibv_recv_wr *bad_wr;
	int i;

	for (i = 0; i < n; ++i)
		if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
			break;

	return i;
#endif
	DEBUG_LOG("post_recv is not supported\n");
	return n;
}

static int irdma_post_send(struct irdma_context *ctx)
{
	struct ibv_sge list = {
		.addr	= (uintptr_t) ctx->start_buf,
		.length = ctx->size,
		.lkey	= ctx->start_mr->lkey
	};
	struct ibv_send_wr wr = {
		.wr_id	    = PINGPONG_SEND_WRID,
		.sg_list    = &list,
		.num_sge    = 1,
		.opcode     = IBV_WR_SEND,
		.send_flags = ctx->send_flags,
	};
	struct ibv_send_wr *bad_wr;

	return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

static inline int parse_single_wc(struct irdma_context *ctx, int *scnt,
				  int *rcnt, int *routs, int iters,
				  uint64_t wr_id, enum ibv_wc_status status)
{
	if (status != IBV_WC_SUCCESS) {
		fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
			ibv_wc_status_str(status),
			status, (int)wr_id);
		return 1;
	}

	switch ((int)wr_id) {
	case PINGPONG_SEND_WRID:
		++(*scnt);
		break;

	case PINGPONG_RECV_WRID:
		if (--(*routs) <= 1) {
			*routs += irdma_post_recv(ctx, ctx->rx_depth - *routs);
			if (*routs < ctx->rx_depth) {
				fprintf(stderr,
					"Couldn't post receive (%d)\n",
					*routs);
				return 1;
			}
		}
		++(*rcnt);

		break;

	default:
		fprintf(stderr, "Completion for unknown wr_id %d\n",
			(int)wr_id);
		return 1;
	}

	ctx->pending &= ~(int)wr_id;
	if (*scnt < iters && !ctx->pending) {
		if (irdma_post_send(ctx)) {
			fprintf(stderr, "Couldn't post send\n");
			return 1;
		}
		ctx->pending = PINGPONG_RECV_WRID |
			PINGPONG_SEND_WRID;
	}

	return 0;
}

static int get_event_from_cq(struct irdma_context *ctx)
{
  struct ibv_wc wc;
  int ret = -1;
  int flushed = 0;
  int poll_times = 0;

  while (1) {
  	  poll_times++;
      ret = ibv_poll_cq(irdma_cq(ctx), 1, &wc);
	  if (ret == 0)
	  	continue;
	  ret = 0;
	  DEBUG_LOG("poll_times = %d\n", (int)poll_times);
	  //usleep(1000000);

	  if (wc.status) {
		  if (wc.status == IBV_WC_WR_FLUSH_ERR) {
			  flushed = 1;
			  fprintf(stderr, "flushed = %d\n", flushed);
			  continue;

		  }
		  fprintf(stderr,
			  "cq completion failed status %d\n",
			  wc.status);
		  ret = -1;
		  goto error;
	  }
	  DEBUG_LOG("wc.opcode = %d\n", (int)wc.opcode);

	  switch (wc.opcode) {
	  case IBV_WC_SEND:
		  DEBUG_LOG("send completion\n");
		  break;

	  case IBV_WC_RDMA_WRITE:
		  DEBUG_LOG("rdma write completion\n");
		  ctx->state = RDMA_WRITE_COMPLETE;
		  break;

	  case IBV_WC_RDMA_READ:
		  DEBUG_LOG("rdma read completion\n");
		  ctx->state = RDMA_READ_COMPLETE;
		  break;

	  case IBV_WC_RECV:
		  DEBUG_LOG("recv completion\n");
		  break;

	  default:
		  DEBUG_LOG("unknown!!!!! completion\n");
		  ret = -1;
		  goto error;
	  }
	  break;
  }
  if (ret) {
	  fprintf(stderr, "poll error %d\n", ret);
	  goto error;
  }
  return flushed;

error:
  ctx->state = TEST_STATE_ERROR;
  return ret;
}


static void write_then_read_remote_rdma_buf(struct irdma_context *ctx)
{
	struct ibv_send_wr *bad_wr;
	int ret;
	int i;

	irdma_setup_wr_start_buf(ctx);
	start_record_time();
  
	ret = ibv_post_send(ctx->qp, &ctx->rdma_sq_wr[0], &bad_wr);
	if (ret) {
		fprintf(stderr, "post send error %d\n", ret);
		return;
	}

	for (i = 0; i < ctx->num_wqe; i++) {
		if (get_event_from_cq(ctx) == 0) {
			if (ctx->state != RDMA_WRITE_COMPLETE) {
				fprintf(stderr, "Failed to get event of RDMA_WRITE_COMPLETE, ctx->state = %d\n", (int)ctx->state);
				return;
			}
			else
			{
				DEBUG_LOG("RDMA_WRITE_COMPLETE\n");
			}
		}
		else
			return;
	}

	stop_record_time();
	printf("RDMA Write Speed: ");
	calculate_and_print_speed(ctx);

	irdma_setup_wr_rdma_buf(ctx);
	start_record_time();
	
	ret = ibv_post_send(ctx->qp, &ctx->rdma_sq_wr[0], &bad_wr);
	if (ret) {
		fprintf(stderr, "post send error %d\n", ret);
		return;
	}
	DEBUG_LOG("server posted rdma read req \n");

	for (i = 0; i < ctx->num_wqe; i++) {
		if (get_event_from_cq(ctx) == 0) {
			if (ctx->state != RDMA_READ_COMPLETE) {
				fprintf(stderr, "Failed to get event of RDMA_READ_COMPLETE, ctx->state = %d\n", (int)ctx->state);
				return;
			}
			else
			{
				DEBUG_LOG("RDMA_READ_COMPLETE\n");
			}
		}
		else
			return;
	}

	stop_record_time();
	printf("RDMA Read Speed: ");
	calculate_and_print_speed(ctx);	
}

static void debug_printf_start_buffer(struct irdma_context *ctx)
{
	int i;	
	int buffer_offset = ctx->size / ctx->num_wqe;

	if(debug == 0)
		return;

	for (i = 0; i < ctx->num_wqe; i++) {
		memcpy(tmp_buf, ctx->start_buf + i * buffer_offset, NORMAL_PRINT_LEN-1);
		tmp_buf[NORMAL_PRINT_LEN-1] = 0;
		printf("start_buf segment %d: %s\n", i, tmp_buf);
	}
}

static void debug_printf_rdma_buffer(struct irdma_context *ctx)
{
	int i;
	int buffer_offset = ctx->size / ctx->num_wqe;

	if(debug == 0)
		return;

	for (i = 0; i < ctx->num_wqe; i++) {
		memcpy(tmp_buf, ctx->rdma_buf + i * buffer_offset, NORMAL_PRINT_LEN-1);
		tmp_buf[NORMAL_PRINT_LEN-1] = 0;
		printf("rdma_buf segment %d: %s\n", i, tmp_buf);
	}
}

static void print_buffer_diff(char *start_buf, char *rdma_buf, int size)
{
	int i, j;
	int print_offset;
	int print_size;
	
	for (i = 0; i < size; i++) {
		if (start_buf[i] == rdma_buf[i])
			continue;
		else
			break;
	}

	if(i == size)
		return;

	print_offset = i/FPGA_PACKET_LEN*FPGA_PACKET_LEN;
	if (print_offset < 0)
		print_offset = 0;

	print_size = TEMP_BUF_LEN;
	if ((size - i) < print_size-1)
		print_size = size - i;

	memcpy(tmp_buf, start_buf + print_offset, print_size);
	tmp_buf[print_size-1] = 0;
	printf("start_buf data, print_offset = %d, i = %d:\n", print_offset, i);
	for(j = 0; j < TEMP_BUF_LEN/FPGA_PACKET_LEN; j++)
		printf("%s\n", tmp_buf+j*FPGA_PACKET_LEN);

	memcpy(tmp_buf, rdma_buf + print_offset, print_size);
	tmp_buf[print_size-1] = 0;
	printf("rdma_buf data, print_offset = %d, i = %d:\n", print_offset, i);
	for(j = 0; j < TEMP_BUF_LEN/FPGA_PACKET_LEN; j++)
		printf("%s\n", tmp_buf+j*FPGA_PACKET_LEN);
}

static void debug_printf_buffer(struct irdma_context *ctx)
{
	debug_printf_start_buffer(ctx);
	debug_printf_rdma_buffer(ctx);
}

static void usage(const char *argv0)
{
	printf("Usage:\n");
	printf("  %s            start a server and wait for connection\n", argv0);
	printf("  %s <host>     connect to server at <host>\n", argv0);
	printf("\n");
	printf("Options:\n");
	printf("  -p, --port=<port>      listen on/connect to port <port> (default 18515)\n");
	printf("  -d, --ib-dev=<dev>     use IB device <dev> (default first device found)\n");
	printf("  -i, --ib-port=<port>   use port <port> of IB device (default 1)\n");
	printf("  -s, --size=<size>      size of message to exchange (default 4096)\n");
	printf("  -m, --mtu=<size>       path MTU (default 1024)\n");
	printf("  -b, --dest_ip            set ipv4 dest IP to hardware\n");
	printf("  -r, --rx-depth=<dep>   number of receives to post at a time (default 500)\n");
	printf("  -n, --iters=<iters>    number of exchanges (default 1000)\n");
	printf("  -w, --num_wqe=<num_wqe>    Max number of new wqe in one QP before one time doorbell (default 32)\n");
	printf("  -l, --sl=<sl>          service level value\n");
	printf("  -e, --events           sleep on CQ events (default poll)\n");
	printf("  -g, --gid-idx=<gid index> local port gid index\n");
	printf("  -o, --odp		    use on demand paging\n");
	printf("  -O, --iodp		    use implicit on demand paging\n");
	printf("  -P, --prefetch	    prefetch an ODP MR\n");
	printf("  -t, --ts	            get CQE with timestamp\n");
	printf("  -c, --chk	            validate received buffer\n");
	printf("  -j, --dm	            use device memory\n");
	printf("  -N, --new_send            use new post send WR API\n");
	printf("  -a, --physical            set physical addr of buffer to hardware\n");
}

int main(int argc, char *argv[])
{
	struct ibv_device      **dev_list;
	struct ibv_device	*ib_dev;
	struct irdma_context *ctx;
	struct pingpong_dest     my_dest;
	struct pingpong_dest    *rem_dest;
	char                    *ib_devname = NULL;
	char                    *servername = NULL;
	char                    *dest_ip = NULL;
	unsigned int             port = 18515;
	int                      ib_port = 1;
	unsigned int             size = 4096;
	enum ibv_mtu		 mtu = IBV_MTU_1024;
	unsigned int             rx_depth = 127; //500;
	int                      use_event = 0;
	int                      routs;
	int                      rcnt, scnt;
	int                      num_cq_events = 0;
	int                      num_wqe = 1;
	unsigned int             iters = 1000;
	int                      sl = 0;	
	int			 gidx = -1;
	char			 gid[33];

	setvbuf(stdout, NULL, _IOLBF, 0);
	srand48(getpid() * time(NULL));

	while (1) {
		int c;

		static struct option long_options[] = {
			{ .name = "port",     .has_arg = 1, .val = 'p' },
			{ .name = "ib-dev",   .has_arg = 1, .val = 'd' },
			{ .name = "ib-port",  .has_arg = 1, .val = 'i' },
			{ .name = "size",     .has_arg = 1, .val = 's' },
			{ .name = "mtu",      .has_arg = 1, .val = 'm' },
			{ .name = "dest_ip",  .has_arg = 1, .val = 'b' },
			{ .name = "rx-depth", .has_arg = 1, .val = 'r' },
			{ .name = "iters",    .has_arg = 1, .val = 'n' },
			{ .name = "num_wqe",  .has_arg = 1, .val = 'w' },
			{ .name = "sl",       .has_arg = 1, .val = 'l' },
			{ .name = "events",   .has_arg = 0, .val = 'e' },
			{ .name = "gid-idx",  .has_arg = 1, .val = 'g' },
			{ .name = "odp",      .has_arg = 0, .val = 'o' },
			{ .name = "iodp",     .has_arg = 0, .val = 'O' },
			{ .name = "prefetch", .has_arg = 0, .val = 'P' },
			{ .name = "ts",       .has_arg = 0, .val = 't' },
			{ .name = "chk",      .has_arg = 0, .val = 'c' },
			{ .name = "dm",       .has_arg = 0, .val = 'j' },
			{ .name = "new_send", .has_arg = 0, .val = 'N' },
			{ .name = "physical_addr", .has_arg = 0, .val = 'a' },
			{}
		};

		c = getopt_long(argc, argv, "p:d:i:s:m:b:r:n:w:l:eg:oOPtcjNa",
				long_options, NULL);

		if (c == -1)
			break;

		switch (c) {
		case 'p':
			port = strtoul(optarg, NULL, 0);
			if (port > 65535) {
				usage(argv[0]);
				return 1;
			}
			break;

		case 'd':
			ib_devname = strdupa(optarg);
			break;

		case 'i':
			ib_port = strtol(optarg, NULL, 0);
			if (ib_port < 1) {
				usage(argv[0]);
				return 1;
			}
			break;

		case 's':
			size = strtoul(optarg, NULL, 0);
			DEBUG_LOG("size = 0x%x\n", size);
			break;

		case 'm':
			mtu = pp_mtu_to_enum(strtol(optarg, NULL, 0));
			if (mtu == 0) {
				usage(argv[0]);
				return 1;
			}
			break;

		case 'r':
			rx_depth = strtoul(optarg, NULL, 0);
			break;

		case 'n':
			iters = strtoul(optarg, NULL, 0);
			break;

		case 'w':
			num_wqe = strtoul(optarg, NULL, 0);
			if (num_wqe > MAX_NUM_WQE) {
				printf("num_wqe (%d) should be less than %d", num_wqe, MAX_NUM_WQE);
				return 1;
			}
			break;

		case 'l':
			sl = strtol(optarg, NULL, 0);
			break;

		case 'e':
			++use_event;
			break;

		case 'g':
			gidx = strtol(optarg, NULL, 0);
			break;
		
		case 'c':
			validate_buf = 1;
			break;

		case 'a':
			physical_addr = 1;
			break;

		case 'b':
			dest_ip = strdupa(optarg);
			break;

		default:
			usage(argv[0]);
			return 1;
		}
	}

	if (optind == argc - 1)
		servername = strdupa(argv[optind]);
	else if (optind < argc) {
		usage(argv[0]);
		return 1;
	}

	current_port = port;

	printf("servername = %s, dest_ip = %s, num_wqe = %d\n", servername, dest_ip, num_wqe);

	if (size > HUGE_PAGE_SIZE)
	{
		size = HUGE_PAGE_SIZE;
		printf("The biggest supported size is %d\n", HUGE_PAGE_SIZE);
	}

	//page_size = sysconf(_SC_PAGESIZE);
	page_size = getpagesize();

	dev_list = ibv_get_device_list(NULL);
	if (!dev_list) {
		perror("Failed to get IB devices list");
		return 1;
	}

	if (!ib_devname) {
		ib_dev = *dev_list;
		if (!ib_dev) {
			fprintf(stderr, "No IB devices found\n");
			return 1;
		}
	} else {
		int i;
		for (i = 0; dev_list[i]; ++i)
			if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
				break;
		ib_dev = dev_list[i];
		if (!ib_dev) {
			fprintf(stderr, "IB device %s not found\n", ib_devname);
			return 1;
		}
	}

	ctx = irdma_init_ctx(ib_dev, size, rx_depth, ib_port, use_event, dest_ip, num_wqe);
	if (!ctx)
		return 1;

	routs = irdma_post_recv(ctx, ctx->rx_depth);
	if (routs < ctx->rx_depth) {
		fprintf(stderr, "Couldn't post receive (%d)\n", routs);
		return 1;
	}

	if (use_event)
		if (ibv_req_notify_cq(irdma_cq(ctx), 0)) {
			fprintf(stderr, "Couldn't request CQ notification\n");
			return 1;
		}

	if (pp_get_port_info(ctx->context, ib_port, &ctx->portinfo)) {
		fprintf(stderr, "Couldn't get port info\n");
		return 1;
	}

	my_dest.lid = ctx->portinfo.lid;
	if (ctx->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET &&
							!my_dest.lid) {
		fprintf(stderr, "Couldn't get local LID\n");
		return 1;
	}

	if (gidx >= 0) {
		if (ibv_query_gid(ctx->context, ib_port, gidx, &my_dest.gid)) {
			fprintf(stderr, "can't read sgid of index %d\n", gidx);
			return 1;
		}
	} else
		memset(&my_dest.gid, 0, sizeof my_dest.gid);

	my_dest.qpn = ctx->qp->qp_num;
	my_dest.psn = lrand48() & 0xffffff;
	inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
	printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
	       my_dest.lid, my_dest.qpn, my_dest.psn, gid);	

	if (servername)
		rem_dest = irdma_client_exch_dest(ctx, servername, port, &my_dest);
	else {
		debug_printf_rdma_buffer(ctx);
		rem_dest = irdma_server_exch_dest(ctx, ib_port, mtu, port, sl,
								&my_dest, gidx);
	}

	if (!rem_dest)
		return 1;

	inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
	printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
	       rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

	if (servername)
		if (irdma_connect_ctx(ctx, ib_port, my_dest.psn, mtu, sl, rem_dest,
					gidx))
			return 1;

	irdma_print_buf_info(ctx);

	if (servername) {
		for(scnt = 0; scnt < iters; scnt++) {
			fill_start_buf(ctx->start_buf, size);
		
			DEBUG_LOG("\nBefore write and read:\n");
			debug_printf_buffer(ctx);
			
			printf("Start test number %d\n", scnt);
			write_then_read_remote_rdma_buf(ctx);
			
			DEBUG_LOG("After write and read:\n");
			debug_printf_buffer(ctx);
			if (validate_buf) {
				if (memcmp(ctx->start_buf, ctx->rdma_buf, ctx->size)) {
					fprintf(stderr, "data mismatch!\n");
					print_buffer_diff(ctx->start_buf, ctx->rdma_buf, ctx->size);
					write_data_into_file(ctx->start_buf, ctx->size, "client_start_buf_data");
					write_data_into_file(ctx->rdma_buf, ctx->size, "client_rdma_buf_data");
					goto close;
				}
				else
				{
					printf("data match!!!\n");
				}
			}
		}
	}

	if (do_send_recv) {
		debug_printf_buffer(ctx);

		ctx->pending = PINGPONG_RECV_WRID;

		if (servername) {
			if (validate_buf)
				for (int i = 0; i < size; i += page_size)
					ctx->start_buf[i] = i / page_size % sizeof(char);

			if (irdma_post_send(ctx)) {
				fprintf(stderr, "Couldn't post send\n");
				return 1;
			}
			ctx->pending |= PINGPONG_SEND_WRID;
		}

		start_record_time();

		rcnt = scnt = 0;
		while (rcnt < iters || scnt < iters) {
			int ret;

			if (use_event) {
				struct ibv_cq *ev_cq;
				void          *ev_ctx;

				if (ibv_get_cq_event(ctx->channel, &ev_cq, &ev_ctx)) {
					fprintf(stderr, "Failed to get cq_event\n");
					return 1;
				}

				++num_cq_events;

				if (ev_cq != irdma_cq(ctx)) {
					fprintf(stderr, "CQ event for unknown CQ %p\n", ev_cq);
					return 1;
				}

				if (ibv_req_notify_cq(irdma_cq(ctx), 0)) {
					fprintf(stderr, "Couldn't request CQ notification\n");
					return 1;
				}
			}

			{
				int ne, i;
				struct ibv_wc wc[2];

				do {
					ne = ibv_poll_cq(irdma_cq(ctx), 2, wc);
					if (wc->status) {
						if (wc->status == IBV_WC_WR_FLUSH_ERR) {
							DEBUG_LOG("Flushed, ne = %d\n", ne);
							continue;
						}
					}
					if (ne < 0) {
						fprintf(stderr, "poll CQ failed %d\n", ne);
						return 1;
					}
				} while (!use_event && ne < 1);

				for (i = 0; i < ne; ++i) {
					ret = parse_single_wc(ctx, &scnt, &rcnt, &routs,
							      iters,
							      wc[i].wr_id,
							      wc[i].status);
					if (ret) {
						fprintf(stderr, "parse WC failed %d\n", ne);
						return 1;
					}
				}
			}
		}

		stop_record_time();
		calculate_and_print_speed_with_iters(ctx, iters);

		{
			if ((!servername) && (validate_buf)) {
				for (int i = 0; i < size; i += page_size)
					if (ctx->start_buf[i] != i / page_size % sizeof(char))
						printf("invalid data in page %d\n",
						       i / page_size);
			}
		}

		ibv_ack_cq_events(irdma_cq(ctx), num_cq_events);
	}

close:

	if (servername) {
		irdma_inform_quit(ctx);
		close(ctx->sockfd);
	}
	else
	{
		irdma_wait_quit(ctx);
		DEBUG_LOG("After write and read:\n");
		debug_printf_rdma_buffer(ctx);
		write_data_into_file(ctx->rdma_buf, ctx->size, "server_rdma_buf_data");
		close(ctx->connfd);
	}	
	
	if (irdma_close_ctx(ctx))
		return 1;

	ibv_free_device_list(dev_list);
	free(rem_dest);

	return 0;
}
