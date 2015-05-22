/*
 * yubo@yubo.org
 * 2015-05-13
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "rrd.h"
#include "rrd_error.h"
#include "rrd_archive.h"
#include "cgo_rrddb.h"

#define MAX_BUFSIZ (8 * 1024)
static int dbtype(const char *s) {
	if (!strcmp(s, "btree"))
		return (DB_BTREE);
	if (!strcmp(s, "hash"))
		return (DB_HASH);
	return -1;
	/* NOTREACHED */
}

static void * setinfo(dbop_t *dbop, int type, char *s){
	char *eq, *index();

	if ((eq = index(s, '=')) == NULL)
		return NULL;
	*eq++ = '\0';
	if (!isdigit(*eq))
		return NULL;

	switch (type) {
		case DB_BTREE:
			if (!strcmp("flags", s)) {
				dbop->ib.flags = atoi(eq);
				return &dbop->ib;
			}
			if (!strcmp("cachesize", s)) {
				dbop->ib.cachesize = atoi(eq);
				return &dbop->ib;
			}
			if (!strcmp("maxkeypage", s)) {
				dbop->ib.maxkeypage = atoi(eq);
				return &dbop->ib;
			}
			if (!strcmp("minkeypage", s)) {
				dbop->ib.minkeypage = atoi(eq);
				return &dbop->ib;
			}
			if (!strcmp("lorder", s)) {
				dbop->ib.lorder = atoi(eq);
				return &dbop->ib;
			}
			if (!strcmp("psize", s)) {
				dbop->ib.psize = atoi(eq);
				return &dbop->ib;
			}
			break;
		case DB_HASH:
			if (!strcmp("bsize", s)) {
				dbop->ih.bsize = atoi(eq);
				return &dbop->ih;
			}
			if (!strcmp("ffactor", s)) {
				dbop->ih.ffactor = atoi(eq);
				return &dbop->ih;
			}
			if (!strcmp("nelem", s)) {
				dbop->ih.nelem = atoi(eq);
				return &dbop->ih;
			}
			if (!strcmp("cachesize", s)) {
				dbop->ih.cachesize = atoi(eq);
				return &dbop->ih;
			}
			if (!strcmp("lorder", s)) {
				dbop->ih.lorder = atoi(eq);
				return &dbop->ih;
			}
			break;
	}
	return NULL;
	/* NOTREACHED */
}

void * rrddb_open(char *arname, char *dbname, char *dtype, char *dinf, 
		int dlock){
	rrddb_t *rd = NULL;
	char *infoarg, *p = NULL;
	int type;
	int oflags = O_CREAT | O_RDWR;

	rd = calloc(sizeof(*rd), 1);
	if(rd == NULL)
		return NULL;

	type = dbtype(dtype);
	if(type < 0){
		goto err_alloced;
	}
	if(dinf == NULL){
		infoarg = NULL;
	}else{
		infoarg = strdup(dinf);
		if(infoarg == NULL)
			goto err_alloced;
		for (p = strtok(infoarg, ",\t "); p != NULL; 
				p = strtok(0, ",\t ")){
			if (*p != '\0'){
				rd->dbop.info = setinfo(&rd->dbop, type, p);
			}
		}
		free(infoarg);
	}

	if(dlock)
		oflags |= DB_LOCK;

	if ((rd->dbop.db = dbopen(dbname, oflags, S_IRUSR | S_IWUSR, 
					type, rd->dbop.info)) == NULL){
		goto err_alloced;
	}

	if((rd->arop.fd = open_archive(arname)) == -1){
		goto err_dbopened;
	}

	strncpy(rd->arop.fname, arname, sizeof(rd->arop.fname));
	strncpy(rd->dbop.fname, dbname, sizeof(rd->dbop.fname));

	return (void *)rd;

err_dbopened:
	rd->dbop.db->close(rd->dbop.db);
err_alloced:
	free(rd);
	return NULL;
}

int rrddb_close_archive(void *d) {
	rrddb_t *rd = (rrddb_t *)d;
	if (close_archive(&rd->arop))
		return -RRD_ERR_CLOSE2;
	return 0;
}

int rrddb_sync_db(void *d) {
	int ret;
	rrddb_t *rd = (rrddb_t *)d;
	if(rd->dbop.db){
		if(rd->dbop.db->sync(rd->dbop.db, 0)){
			return -RRD_ERR_DB_SYNC;
		}
		return 0;
	}else{
		return -RRD_ERR_DB_SYNC1;
	}
}

int rrddb_close_db(void *d) {
	int ret;
	rrddb_t *rd = (rrddb_t *)d;
	if(rd->dbop.db){
		if(rd->dbop.db->close(rd->dbop.db)){
			rd->dbop.db = NULL; // ??
			return -RRD_ERR_CLOSE3;
		}
		rd->dbop.db = NULL;
		return 0;
	}else{
		return -RRD_ERR_CLOSE3;
	}
}

int rrddb_append_file(void *r, const char *filename, const char *key){
	int ret;
	time_t ts;
	off_t offset;
	ssize_t size;
	rrddb_t *rd = (rrddb_t *)r;

	ret = append_archive(&rd->arop, filename, key, &ts, &offset, &size);
	if(ret == -1){
		return -RRD_ERR_APPEND;
	}
	if ((ret = db_put(r, key, ts, offset, size, R_NOOVERWRITE))){
		// todo remove file from archive, empty the file_offset  header
		reset_archive(rd->arop.fd, offset, size);
		return ret;
	}
	return 0;
}

int db_get(void *r, const char *name, time_t *ts, off_t *offset, 
		ssize_t *size, unsigned int flags){
	DBT key, data;
	int ret;
	DB *db = ((rrddb_t *)r)->dbop.db;

	key.data = (char *)name;
	key.size = strlen(name)+1;

	ret = db->get(db, &key, &data, flags);
	if(ret){
		return ret;
	}else{
		*ts = ((db_entry_t *)(data.data))->ts;
		*offset = ((db_entry_t *)(data.data))->offset;
		*size = ((db_entry_t *)(data.data))->size;
		return 0;
	}
}

int db_put(void *r, const char *name, time_t ts, off_t offset, 
		ssize_t size, unsigned int flags){
	DBT key, dat;
	int len, ret;
	db_entry_t e;
	DB *db;

	db = ((rrddb_t *)r)->dbop.db;
	if (!(len = strlen(name)))
		return -RRD_ERR_DB_KEY;
	if (len > MAXKEYLEN)
		return -RRD_ERR_DB_KEY1;
	key.data = (char *)name;
	key.size = len+1;
	e.ts = ts;
	e.offset = offset;
	e.size = size;
	dat.data = &e;
	dat.size = sizeof(e);

	ret = db->put(db, &key, &dat, flags);
	if(ret == 1){
		return -RRD_ERR_DB_PUT;
	}else if(ret){
		return -RRD_ERR_DB_PUT1;
	}
	return 0;
}

int db_delete(void *d, const char *name, unsigned int flags) {
	DBT key;
	DB *db;

	db = ((rrddb_t *)d)->dbop.db;

	key.data = (char *)name;
	key.size = strlen(name)+1;
	if(db->del(db, &key, flags)){
		return -RRD_ERR_DB_DEL;
	}
	return 0;
}


const char *rrdCreate(const char *filename, unsigned long step, 
		time_t start, int argc, const char **argv, void *rd, int overwrite) {
	int ret;
	rrddb_t *r = (rrddb_t *)rd;
	ret = rrd_create_r(filename, step, start, argc, argv, r, overwrite);
	return rrd_strerror(ret);
}

const char *rrdUpdate(const char *filename, const char *template, 
		int argc, const char **argv, void *rd, off_t r_offset, ssize_t r_size) {
	int ret;
	ret = rrd_update_r(filename, template, argc, argv, (rrddb_t *)rd, r_offset, r_size);
	return rrd_strerror(ret);
}


const char *rrdInfo(rrd_info_t **info, char *filename, 
		void *rd, off_t r_offset, ssize_t r_size) {
	int ret = 0;
	*info = rrd_info_r(filename, &ret, (rrddb_t *)rd, r_offset, r_size);
	return rrd_strerror(ret);
}

const char *rrdFetch(int *ret, char *filename, const char *cf, time_t *start, 
		time_t *end, unsigned long *step, unsigned long *ds_cnt, 
		char ***ds_namv, double **data, void *rd, off_t r_offset, ssize_t r_size) {
	*ret = rrd_fetch_r(filename, cf, start, end, step, ds_cnt, ds_namv, data, (rrddb_t *)rd, r_offset, r_size);
	return rrd_strerror(*ret);
}

char *arrayGetCString(char **values, int i) {
	return values[i];
}
