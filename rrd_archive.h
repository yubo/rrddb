/*
 * yubo@yubo.org
 * 2015-05-13
 */
#ifndef __RRD_ARCHIVE_H
#define __RRD_ARCHIVE_H
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "cgo_rrddb.h"

#define BLOCKSIZE 512
#define ISDIGIT(c) ((unsigned) (c) - '0' <= 9)
#define ISODIGIT(c) ((unsigned) (c) - '0' <= 7)

#define LG_8 3
#define LG_64 6
#define LG_256 8

#define BLOCK_START(addr) ((addr)&(~(BLOCKSIZE-1)))


struct header_old_tar {
	char name[100];		//char *
	char mode[8];		//oct
	char uid[8];		//oct
	char gid[8];		//oct
	char size[12];		//oct
	char mtime[12];		//oct
	char checksum[8];	//???
	char linkflag[1];	//???
	char linkname[100];	//???
	char pad[255];
};

enum read_header
{
  HEADER_STILL_UNREAD,		/* for when read_header has not been called */
  HEADER_SUCCESS,		/* header successfully read and checksummed */
  HEADER_SUCCESS_EXTENDED,	/* likewise, but we got an extended header */
  HEADER_ZERO_BLOCK,		/* zero block where header expected */
  HEADER_END_OF_FILE,		/* true end of file while header expected */
  HEADER_FAILURE		/* ill-formed header, or bad checksum */
};


typedef union block {
	char buffer[BLOCKSIZE];
	struct header_old_tar header;
}block_t;


int read_block(int fd, block_t *b);
int write_block(int fd, block_t *b);
int open_archive(char *filename);
int close_archive(arop_t *arop);
off_t append_archive_buff(arop_t *arop, const char *rrdname, int64_t rrdsize);
int append_archive(arop_t *arop, const char *filename);
#endif
