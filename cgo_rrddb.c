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

int rrddb_close(void *d) {
	int ret = 0;

	rrddb_t *rd = (rrddb_t *)d;
	ret |= rd->dbop.db->close(rd->dbop.db);
	ret |= close_archive(&rd->arop);
	if(ret){
		return -1;
	}else{
		return 0;
	}
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
		return -1;
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
	int len;
	db_entry_t e;
	DB *db;

	db = ((rrddb_t *)r)->dbop.db;
	if (!(len = strlen(name)))
		return -1;
	if (len > MAXKEYLEN)
		return -1;
	key.data = (char *)name;
	key.size = len+1;
	e.ts = ts;
	e.offset = offset;
	e.size = size;
	dat.data = &e;
	dat.size = sizeof(e);

	return db->put(db, &key, &dat, flags);
}

int db_delete(void *d, const char *name, unsigned int flags) {
	DBT key;
	DB *db;

	db = ((rrddb_t *)d)->dbop.db;

	key.data = (char *)name;
	key.size = strlen(name)+1;
	return db->del(db, &key, flags);
}


const char *rrdCreate(const char *filename, unsigned long step, 
		time_t start, int argc, const char **argv, void *rd, int overwrite) {
	int ret;
	rrddb_t *r = (rrddb_t *)rd;
	ret = rrd_create_r(filename, step, start, argc, argv, r, overwrite);
	return rrd_strerror(ret);
}

const char *rrdUpdate(const char *filename, const char *template, 
		int argc, const char **argv) {
	int ret;
	ret = rrd_update_r(filename, template, argc, argv);
	return rrd_strerror(ret);
}


const char *rrdInfo(rrd_info_t **info, char *filename) {
	int ret = 0;
	*info = rrd_info_r(filename, &ret);
	return rrd_strerror(ret);
}

const char *rrdFetch(int *ret, char *filename, const char *cf, time_t *start, 
		time_t *end, unsigned long *step, unsigned long *ds_cnt, 
		char ***ds_namv, double **data) {
	*ret = rrd_fetch_r(filename, cf, start, end, step, ds_cnt, ds_namv, data);
	return rrd_strerror(*ret);
}

char *arrayGetCString(char **values, int i) {
	return values[i];
}
