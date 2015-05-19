/*
 * yubo@yubo.org
 * 2015-04-20
 */
#define _XOPEN_SOURCE 600

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>
#include <stdbool.h>
#include <limits.h>
#include <time.h>

#include <stdio.h>
#include <string.h>
#include "rrd_archive.h"
#include "rrd_tool.h"

#define _DEBUG  7
#include "debug.h"


/* 支持本实现创建的文件 read, list, append
 * Version 7 AT&T UNIX, old-style tar archive
 */
static intmax_t from_header(char const *where, size_t digs) {
	uintmax_t value;
	char const *lim = where + digs;

	where += !*where;

	for (;;) {
		if (where == lim) {
			return -1;
		}
		if (!isspace((unsigned char) *where))
			break;
		where++;
	}

	value = 0;
	if (ISODIGIT (*where)) {
		bool overflow = false;

		for (;;) {
			value += *where++ - '0';
			if (where == lim || ! ISODIGIT (*where))
				break;
			overflow |= value != (value << LG_8 >> LG_8);
			value <<= LG_8;
		}

		if (overflow) {
			dlog("overflow\n");
			return -1;
		}
	}
	return value;
}

static ssize_t read_block(int fd, block_t *b, off_t *offset){
	ssize_t ret;
	if((ret =  pread(fd, b->buffer, BLOCKSIZE, *offset)) > 0){
		*offset += ret;
	}
	return ret;
}

static ssize_t write_block(int fd, block_t *b, off_t *offset){
	ssize_t ret;
	if((ret =  pwrite(fd, b->buffer, BLOCKSIZE, *offset)) > 0){
		*offset += ret;
	}
	return ret;
}


enum read_header tar_checksum (block_t *header) {
	size_t i;
	int unsigned_sum = 0;		/* the POSIX one :-) */
	int recorded_sum;
	int parsed_sum;
	char *p;

	p = header->buffer;
	for (i = sizeof *header; i-- != 0;) {
		unsigned_sum += (unsigned char) (*p++);
	}

	if (unsigned_sum == 0)
		return HEADER_ZERO_BLOCK;

	for (i = sizeof header->header.checksum; i-- != 0;) {
		unsigned_sum -= (unsigned char) header->header.checksum[i];
	}
	unsigned_sum += ' ' * sizeof header->header.checksum;

	parsed_sum = from_header(header->header.checksum,
			sizeof header->header.checksum);
	if (parsed_sum < 0){
		printf("parsed_sum:%d\n", parsed_sum);
		return HEADER_FAILURE;
	}

	recorded_sum = parsed_sum;

	if (unsigned_sum != recorded_sum){
		return HEADER_FAILURE;
	}

	return HEADER_SUCCESS;
}

int tar_filesize(block_t *header){
	int  ret;
	if((ret = tar_checksum(header)) == HEADER_SUCCESS){
		return  from_header(header->header.size, sizeof header->header.size);
	}
	dlog("checksum %d\n", ret);
	return -1;

}

// ugly hack
static off_t seek_to_last(int fd){
	block_t b;
	off_t offset, end, cur;

	offset = lseek(fd, 0, SEEK_END);
	if(offset % BLOCKSIZE)
		return -1;

	if(offset < 2*BLOCKSIZE)
		return -1;

	offset = offset - BLOCKSIZE;
	end = max(offset - (20 * BLOCKSIZE), 0);
	cur = offset;
	while(end < offset){
		read_block(fd, &b, &cur);
		if(tar_checksum(&b) != HEADER_ZERO_BLOCK)
			goto found;
		offset -= BLOCKSIZE;
		cur = offset;
	}

	// check if cur at the begin of the file
	if(cur)
		return -1;

found:
	return cur;
}

static off_t add_tail_archive(int fd){
	block_t b;
	off_t offset;

	offset = lseek(fd, 0, SEEK_END);

	if(offset != -1){
		memset(&b.buffer, 0, sizeof(block_t));
		write_block(fd, &b, &offset);
		write_block(fd, &b, &offset);
	}
	return offset;
}

/* 
 * if archive file not existed, 
 * creat new archivefile and add_tail_archive
 */
int open_archive(char *filename){
	int fd;

	// just open
	if((fd =  open(filename, O_RDWR, 0644)) != -1){
		goto out;
	}

	if((fd =  open(filename, O_RDWR|O_CREAT|O_EXCL, 0644)) != -1){
		add_tail_archive(fd);
		goto out;
	}
	return -1;

out:
#ifdef HAVE_POSIX_FADVISE
	/* In general we need no read-ahead when dealing with rrd_files.
	   When we stop reading, it is highly unlikely that we start up again.
	   In this manner we actually save time and diskaccess (and buffer cache).
	   Thanks to Dave Plonka for the Idea of using POSIX_FADV_RANDOM here. */
	posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);
#endif
	return fd;
}

int close_archive(arop_t *arop){
	int ret;
	if(arop->fd >= 0){
		ret = close(arop->fd);
		arop->fd = -1;
		return ret;
	}
	return -1;
}

static void set_head(block_t *header, struct stat *stat, const char *filename){
	int i, checksum = 0;
	char *p;

	memset(header, 0, sizeof(block_t));
	snprintf(header->header.name, 100, "%s", filename);
	snprintf(header->header.mode, 8, "%07o", stat->st_mode&0xFFF);
	snprintf(header->header.uid, 8, "%07o", stat->st_uid);
	snprintf(header->header.gid, 8, "%07o", stat->st_gid);
	snprintf(header->header.size, 12, "%011lo", stat->st_size);
	snprintf(header->header.mtime, 12, "%011lo", stat->st_mtime);
	memset(header->header.checksum, ' ', 8);
	header->header.linkflag[0] = '0';

	p = header->buffer;
	for (i = sizeof *header; i-- != 0;) {
		checksum += (unsigned char)(*p++);
	}
	snprintf(header->header.checksum, 7, "%06o", checksum);
}

//reset archive, should only reset last archivefile
off_t reset_archive(int fd, off_t data_start, ssize_t len){
	block_t b;
	off_t offset, lastblock_start;

	offset = data_start - BLOCKSIZE;
	if(offset < 0 || offset % BLOCKSIZE){
		return -1;
	}

	lastblock_start = BLOCK_START(offset + len);
	memset(&b, 0, sizeof(block_t));
	while(lastblock_start >= offset){
		write_block(fd, &b, &offset);
	}
	return 0;
}


// return data offset
off_t append_archive_buff(arop_t *arop, const char *rrdname, int64_t rrdsize){
	block_t b;
	struct stat st;
	off_t offset, start, lastblock_start, len;
	int fd = arop->fd;
	int i;

	if((start = seek_to_last(fd)) == -1){
		dlog("seek_to_last error\n");
		return -1;
	}

	offset = start;
	lastblock_start = BLOCK_START(start + rrdsize);
	//[offset, lastblock_start+BLOCKSIZE)[b][b][b]
	len = lastblock_start - start + BLOCKSIZE;
	//add head block and 2 zero block at end
	len += 3*BLOCKSIZE;

	if (posix_fallocate(fd, offset, len)){
		dlog("posix_fallocate error\n");
		return -1;
	}

	st.st_mode = 0644;
	st.st_uid = getuid();
	st.st_gid = getgid();
	st.st_size = rrdsize;
	st.st_mtime = time(NULL);
	set_head(&b, &st, rrdname);
	write_block(fd, &b, &offset);

	offset = lastblock_start;
	memset(&b, 0, sizeof(block_t));
	// empty the last block and 2 zero block at end
	for(i = 0; i < 3; i++){
		write_block(fd, &b, &offset);
	}
	return start+BLOCKSIZE;
}


// todo find from front?
// return data start offset or -1
off_t  append_archive(arop_t *arop, const char *filename, const char *key,
		time_t *tsp,  off_t *offsetp, ssize_t *sizep){
	int fd, i; 
	block_t b;
	struct stat st;
	ssize_t ret;
	off_t offset, start;

	fd =  open(filename, O_RDONLY);
	if(fd < 0 ){
		return -1;
	}

	fstat(fd, &st);
	set_head(&b, &st, key);
	if((start = seek_to_last(arop->fd)) == -1){
		return -1;
	}
	offset = start;

	write_block(arop->fd, &b, &offset);
	memset(&b, 0, BLOCKSIZE);
	while((ret = read(fd, b.buffer, BLOCKSIZE))>0){
		write_block(arop->fd, &b, &offset);
		memset(&b, 0, sizeof(block_t));
	}
	// empty 2 zero block at end
	for(i = 0; i < 2; i++){
		write_block(arop->fd, &b, &offset);
	}
	*tsp = st.st_mtime;
	*offsetp = start+BLOCKSIZE;
	*sizep = st.st_size;
	return start+BLOCKSIZE;
}


void dump_head(block_t  *header){
	printf("name %s\n"
			"mode %s\n"
			"uid %s\n"
			"gid %s\n"
			"size %s\n"		
			"mtime %s\n"
			"checksum %s\n"
			"linkflag %s\n"
			"linkname %s\n", 
			header->header.name, 
			header->header.mode, 
			header->header.uid, 
			header->header.gid, 
			header->header.size, 
			header->header.mtime,
			header->header.checksum, 
			header->header.linkflag, 
			header->header.linkname);
}
