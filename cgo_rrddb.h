/*
 * yubo@yubo.org
 * 2015-05-13
 */
#ifndef _CGO_RRDDB_H
#define _CGO_RRDDB_H

#include "db.h"
#include "rrd.h"

typedef struct{
	time_t ts;
	off_t offset;
	ssize_t size;
} db_entry_t;

typedef struct{
	DB *db;
	HASHINFO ih;
	BTREEINFO ib;
	void *info;
	char fname[128];
} dbop_t;

typedef struct{
	int  fd;
	char fname[128];
}arop_t;

typedef struct rrddb_t{
	arop_t arop;
	dbop_t dbop;
} rrddb_t;

#define MAXKEYLEN 1024


struct rrd_info_t;
void * rrddb_open(char *arname, char *dbname, char *dtype, char *dinf, 
		int dlock);
int rrddb_close(void *rrddb);
int rrddb_append_file(void *r, const char *filename, const char *key);
int db_get(void *db, const char *name, time_t *ts, 
		off_t *offset, ssize_t *size, unsigned int flags);
int db_put(void *db, const char *name, time_t ts, 
		off_t offset, ssize_t size, unsigned int flags);
int db_delete(void *db, const char *name, unsigned int flags);

extern const char *rrdCreate(const char *filename, unsigned long step, 
		time_t start, int argc, const char **argv, void *rd, int overwrite);
extern const char *rrdUpdate(const char *filename, const char *template, 
		int argc, const char **argv, void *rd, off_t r_offset, ssize_t r_size);
extern const char *rrdInfo(struct rrd_info_t **ret, char *filename, void *rd,
		off_t r_offset, ssize_t r_size);
extern const char *rrdFetch(int *ret, char *filename, const char *cf, 
		time_t *start, time_t *end, unsigned long *step, unsigned long *ds_cnt, 
		char ***ds_namv, double **data, void *rd, off_t r_offset, ssize_t r_size);
extern char *arrayGetCString(char **values, int i);
#endif
