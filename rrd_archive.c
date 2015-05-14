/*
 * yubo@yubo.org
 * 2015-04-20
 */
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
			return -1;
		}
	}
	return value;
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
	if(tar_checksum(header) == HEADER_SUCCESS){
		return  from_header(header->header.size, sizeof header->header.size);
	}
	return -1;

}

// ugly hack
static off_t seek_to_last(int fd){
	return lseek(fd, -2*BLOCKSIZE, SEEK_END);
}

static off_t add_tail_archive(int fd){
	block_t b;
	off_t offset;
	offset = lseek(fd, 0, SEEK_END);
	if(offset != -1){
		memset(&b.buffer, 0, sizeof(block_t));
		write_block(fd, &b);
		write_block(fd, &b);
		lseek(fd, offset, SEEK_SET);
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
	return close(arop->fd);
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

int read_block(int fd, block_t *b){
	return read(fd, b->buffer, BLOCKSIZE);
}

int write_block(int fd, block_t *b){
	return write(fd, b->buffer, BLOCKSIZE);
}

// return data offset
off_t append_archive_buff(arop_t *arop, const char *rrdname, int64_t rrdsize){
	block_t b;
	struct stat st;
	off_t offset, start, lastblock_start, len;
	int fd = arop->fd;
	int i;

	if((offset = seek_to_last(fd)) == -1){
		return -1;
	}
	start = lseek(fd, 0, SEEK_CUR);

	if(start % BLOCKSIZE)
		return -1;

	lastblock_start = BLOCK_START(start + rrdsize);
	//[start, lastblock)[b][b][b]
	len = lastblock_start - start + BLOCKSIZE;
	//add head block and 2 zero block at end
	len += 3*BLOCKSIZE;

	if (posix_fallocate(fd, start, len)){
		return -1;
	}

	offset = lseek(fd, start, SEEK_SET);
	st.st_mode = 0644;
	st.st_uid = getuid();
	st.st_gid = getgid();
	st.st_size = rrdsize;
	st.st_mtime = time(NULL);
	set_head(&b, &st, rrdname);
	write_block(fd, &b);

	offset = lseek(fd, lastblock_start, SEEK_SET);
	memset(&b, 0, sizeof(block_t));
	// empty the last block and 2 zero block at end
	for(i = 0; i < 3; i++){
		write_block(fd, &b);
	}
	return start+BLOCKSIZE;
}


// todo find from front?
int append_archive(arop_t *arop, const char *filename){
	int fd; 
	block_t b;
	struct stat st;
	ssize_t ret;

	fd =  open(filename, O_RDONLY);
	if(fd < 0 )
		return -1;

	fstat(fd, &st);
	set_head(&b, &st, filename);
	write_block(arop->fd, &b);

	memset(&b, 0, sizeof(block_t));
	while((ret = read(fd, b.buffer, BLOCKSIZE))>0){
		write_block(arop->fd, &b);
		memset(&b, 0, sizeof(block_t));
	}

	return 0;
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
