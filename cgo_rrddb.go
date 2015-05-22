package rrddb

/*
#include <stdlib.h>
#include "rrd.h"
#include "cgo_rrddb.h"
#cgo CFLAGS: -std=c99 -D__DBINTERFACE_PRIVATE -DRRD_LITE -D_BSD_SOURCE -DHAVE_CONFIG_H -D_POSIX_SOURCE -DNUMVERS=1.4009
#cgo LDFLAGS: -lm
*/
import "C"

import (
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

func Open(arname, dbname string) (*Rrddb, error) {
	var rd *Rrddb
	var err error

	if rd, err = open(arname, dbname, "hash", "", 0); err != nil {
		return nil, err
	}

	return rd, nil
}

func open(arname, dbname, dbtype, dbinfo string, dblock int) (*Rrddb, error) {
	var null unsafe.Pointer
	d := &Rrddb{
		Db:     true,
		Ar:     true,
		arname: arname,
		dbname: dbname,
		dbtype: dbtype,
		dbinfo: dbinfo,
		dblock: dblock,
	}

	_arname := C.CString(arname)
	defer C.free(unsafe.Pointer(_arname))
	_dbname := C.CString(dbname)
	defer C.free(unsafe.Pointer(_dbname))
	_dbtype := C.CString(dbtype)
	defer C.free(unsafe.Pointer(_dbtype))
	_dbinfo := C.CString(dbinfo)
	defer C.free(unsafe.Pointer(_dbinfo))

	d.p = unsafe.Pointer(C.rrddb_open(_arname, _dbname, _dbtype, _dbinfo, C.int(dblock)))

	if d.p == null {
		return nil, fmt.Errorf("open() error")
	}

	return d, nil
}

func (r *Rrddb) sync_db() error {
	if r.Db {
		r.Db = false
		return int2Error(C.rrddb_sync_db(r.p))
	} else {
		return fmt.Errorf("Idx file has been closed")
	}
}

func (r *Rrddb) Sync_db() error {
	r.Lock()
	defer r.Unlock()
	return r.sync_db()
}

func (r *Rrddb) close_db() error {
	if r.Db {
		r.Db = false
		return int2Error(C.rrddb_close_db(r.p))
	} else {
		return fmt.Errorf("Idx file has been closed")
	}
}

func (r *Rrddb) Close_db() error {
	r.Lock()
	defer r.Unlock()
	return r.close_db()
}

func (r *Rrddb) close_archive() error {
	if r.Ar {
		r.Ar = false
		return int2Error(C.rrddb_close_archive(r.p))
	} else {
		return fmt.Errorf("Archive file has been closed")
	}
}

func (r *Rrddb) Close_archive() error {
	r.Lock()
	defer r.Unlock()
	return r.close_archive()
}

func (r *Rrddb) clean() error {
	var null unsafe.Pointer

	if r.p != null {
		C.free(r.p)
		r.p = null
		return nil
	} else {
		return fmt.Errorf("Rrddb has been cleaned")
	}
}

func (r *Rrddb) Clean() error {
	r.Lock()
	defer r.Unlock()
	return r.clean()
}

func (r *Rrddb) Close() error {
	var null unsafe.Pointer

	r.Lock()
	defer r.Unlock()
	if r.p != null {
		err1 := r.close_db()
		err2 := r.close_archive()
		err3 := r.clean()
		if err1 != nil || err2 != nil || err3 != nil {
			return fmt.Errorf("%s %s %s", err1, err2, err3)
		} else {
			return nil
		}
	} else {
		return fmt.Errorf("Rrddb has been closed")
	}
}

func (r *Rrddb) Append_file(filename, key string) error {
	_filename := C.CString(filename)
	defer C.free(unsafe.Pointer(_filename))
	_key := C.CString(key)
	defer C.free(unsafe.Pointer(_key))
	r.Lock()
	defer r.Unlock()

	return int2Error(C.rrddb_append_file(r.p, _filename, _key))
}

func (r *Rrddb) Get(key string) (int64, int64, int64, error) {
	var ts C.time_t
	var offset C.off_t
	var size C.ssize_t
	_key := C.CString(key)
	defer C.free(unsafe.Pointer(_key))
	r.RLock()
	defer r.RUnlock()

	if err := int2Error(C.db_get(r.p, _key, &ts, &offset, &size, 0)); err != nil {
		return 0, 0, 0, err
	} else {
		return int64(ts), int64(offset), int64(size), nil
	}
}

func (r *Rrddb) Put(key string, ts, offset, size int64) error {
	_key := C.CString(key)
	defer C.free(unsafe.Pointer(_key))
	r.Lock()
	defer r.Unlock()

	return int2Error(C.db_put(r.p, _key, C.time_t(ts), C.off_t(offset), C.ssize_t(size), R_NOOVERWRITE))
}

func (r *Rrddb) Update(key string, ts, offset, size int64) error {
	_key := C.CString(key)
	defer C.free(unsafe.Pointer(_key))
	r.Lock()
	defer r.Unlock()
	d.Lock()
	defer d.Unlock()

	return int2Error(C.db_put(r.p, _key, C.time_t(ts), C.off_t(offset), C.ssize_t(size), 0))
}

func (r *Rrddb) Delete(key string) error {
	_key := C.CString(key)
	defer C.free(unsafe.Pointer(_key))
	r.Lock()
	defer r.Unlock()

	return int2Error(C.db_delete(r.p, _key, 0))
}

// NewCreator returns new Creator object. You need to call Create to really
// create database file.
//	filename - name of database file
//	start    - don't accept any data timed before or at time specified
//	step     - base interval in seconds with which data will be fed into RRD
func (r *Rrddb) NewCreator(filename string, start time.Time, step uint) *Creator {
	return &Creator{
		filename: filename,
		start:    start,
		step:     step,
		rd:       r,
	}
}

func (c *Creator) DS(name, compute string, args ...interface{}) {
	c.args = append(c.args, "DS:"+name+":"+compute+":"+join(args))
}

func (c *Creator) RRA(cf string, args ...interface{}) {
	c.args = append(c.args, "RRA:"+cf+":"+join(args))
}

// Create creates new database file. If overwrite is true it overwrites
// database file if exists. If overwrite is false it returns error if file
// exists
func (c *Creator) Create(overwrite int) error {
	c.rd.Lock()
	defer c.rd.Unlock()
	return c.create(overwrite)
}

// Use cstring and unsafe.Pointer to avoid alocations for C calls

func (r *Rrddb) NewUpdater(filename string, offset, size int64) *Updater {
	return &Updater{
		filename: newCstring(filename),
		offset:   offset,
		size:     size,
		rd:       r,
	}
}

func (u *Updater) SetTemplate(dsName ...string) {
	u.template = newCstring(strings.Join(dsName, ":"))
}

// Cache chaches data for later save using Update(). Use it to avoid
// open/read/write/close for every update.
func (u *Updater) Cache(args ...interface{}) {
	u.args = append(u.args, newCstring(join(args)).p())
}

// Update saves data in RRDB.
// Without args Update saves all subsequent updates buffered by Cache method.
// If you specify args it saves them immediately.
func (u *Updater) Update(args ...interface{}) error {
	if len(args) != 0 {
		a := make([]unsafe.Pointer, 1)
		a[0] = newCstring(join(args)).p()
		return u.update(a)
	} else if len(u.args) != 0 {
		err := u.update(u.args)
		u.args = nil
		return err
	}
	return nil
}
func (c *Creator) create(overwrite int) error {
	filename := C.CString(c.filename)
	defer freeCString(filename)
	args := makeArgs(c.args)
	defer freeArgs(args)

	e := C.rrdCreate(
		filename,
		C.ulong(c.step),
		C.time_t(c.start.Unix()),
		C.int(len(args)),
		&args[0],
		c.rd.p,
		C.int(overwrite),
	)
	return makeError(e)
}

func (u *Updater) update(args []unsafe.Pointer) error {
	e := C.rrdUpdate(
		(*C.char)(u.filename.p()),
		(*C.char)(u.template.p()),
		C.int(len(args)),
		(**C.char)(unsafe.Pointer(&args[0])),
		u.rd.p,
		C.off_t(u.offset),
		C.ssize_t(u.size),
	)
	return makeError(e)
}

// Info returns information about RRD file.
func (r *Rrddb) Info(filename string, offset, size int64) (map[string]interface{}, error) {
	var i *C.rrd_info_t
	fn := C.CString(filename)
	defer freeCString(fn)

	err := makeError(C.rrdInfo(&i, fn, r.p, C.off_t(offset), C.ssize_t(size)))
	if err != nil {
		return nil, err
	}
	return parseRRDInfo(i), nil
}

func (r *FetchResult) ValueAt(dsIndex, rowIndex int) float64 {
	return r.values[len(r.DsNames)*rowIndex+dsIndex]
}

// Fetch retrieves data from RRD file.
func (r *Rrddb) Fetch(filename string, offset, size int64, cf string,
	start, end time.Time, step time.Duration) (FetchResult, error) {

	fn := C.CString(filename)
	defer freeCString(fn)
	cCf := C.CString(cf)
	defer freeCString(cCf)
	cStart := C.time_t(start.Unix())
	cEnd := C.time_t(end.Unix())
	cStep := C.ulong(step.Seconds())
	var (
		ret      C.int
		cDsCnt   C.ulong
		cDsNames **C.char
		cData    *C.double
	)
	err := makeError(C.rrdFetch(&ret, fn, cCf, &cStart, &cEnd, &cStep,
		&cDsCnt, &cDsNames, &cData, r.p, C.off_t(offset), C.ssize_t(size)))
	if err != nil {
		return FetchResult{filename, cf, start, end, step, nil, 0, nil}, err
	}

	start = time.Unix(int64(cStart), 0)
	end = time.Unix(int64(cEnd), 0)
	step = time.Duration(cStep) * time.Second
	dsCnt := int(cDsCnt)

	dsNames := make([]string, dsCnt)
	for i := 0; i < dsCnt; i++ {
		dsName := C.arrayGetCString(cDsNames, C.int(i))
		dsNames[i] = C.GoString(dsName)
		C.free(unsafe.Pointer(dsName))
	}
	C.free(unsafe.Pointer(cDsNames))

	rowCnt := (int(cEnd)-int(cStart))/int(cStep) + 1
	valuesLen := dsCnt * rowCnt
	var values []float64
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&values)))
	sliceHeader.Cap = valuesLen
	sliceHeader.Len = valuesLen
	sliceHeader.Data = uintptr(unsafe.Pointer(cData))
	return FetchResult{filename, cf, start, end, step, dsNames, rowCnt, values}, nil
}

// FreeValues free values memory allocated by C.
func (r *FetchResult) FreeValues() {
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&r.values)))
	C.free(unsafe.Pointer(sliceHeader.Data))
}

// Values returns copy of internal array of values.
func (r *FetchResult) Values() []float64 {
	return append([]float64{}, r.values...)
}
