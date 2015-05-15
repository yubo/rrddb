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

func open(arname, dbname, dbtype, dbinfo string, dblock int) (*Rrddb, error) {
	var null unsafe.Pointer
	d := &Rrddb{
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

func (d *Rrddb) close() error {
	ret := C.rrddb_close(d.p)
	if ret == 0 {
		return nil
	}
	return fmt.Errorf("db_close error")
}

func (r *Rrddb) Get(entry *Db_entry) error {
	_key := C.CString(entry.Key)
	defer C.free(unsafe.Pointer(_key))

	ret := C.db_get(r.p, _key, &entry.Ts, &entry.Offset, &entry.Size, 0)
	if ret == 0 {
		return nil
	}
	return fmt.Errorf("db_get error")
}

func (r *Rrddb) Put(entry *Db_entry) error {
	_key := C.CString(entry.Key)
	defer C.free(unsafe.Pointer(_key))

	ret := C.db_put(r.p, _key, entry.Ts, entry.Offset, entry.Size, R_NOOVERWRITE)
	if ret == 0 {
		return nil
	}
	return fmt.Errorf("db_put error")
}

func (r *Rrddb) Update(entry *Db_entry) error {
	_key := C.CString(entry.Key)
	defer C.free(unsafe.Pointer(_key))

	ret := C.db_put(r.p, _key, entry.Ts, entry.Offset, entry.Size, 0)
	if ret == 0 {
		return nil
	}
	return fmt.Errorf("db_put error")
}

func (r *Rrddb) Delete(entry *Db_entry) error {
	_key := C.CString(entry.Key)
	defer C.free(unsafe.Pointer(_key))

	ret := C.db_delete(r.p, _key, 0)
	if ret == 0 {
		return nil
	}
	return fmt.Errorf("db_delete error")
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
// exists (you can use os.IsExist function to check this case).
func (c *Creator) Create(overwrite int) error {
	return c.create(overwrite)
}

// Use cstring and unsafe.Pointer to avoid alocations for C calls

func (r *Rrddb) NewUpdater(filename string) *Updater {
	return &Updater{filename: newCstring(filename), rd: r}
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
	)
	return makeError(e)
}

// Info returns information about RRD file.
func (r *Rrddb) Info(filename string) (map[string]interface{}, error) {
	fn := C.CString(filename)
	defer freeCString(fn)
	var i *C.rrd_info_t
	err := makeError(C.rrdInfo(&i, fn, r.p))
	if err != nil {
		return nil, err
	}
	return parseRRDInfo(i), nil
}

func (r *FetchResult) ValueAt(dsIndex, rowIndex int) float64 {
	return r.values[len(r.DsNames)*rowIndex+dsIndex]
}

// Fetch retrieves data from RRD file.
func (r *Rrddb) Fetch(filename, cf string, start, end time.Time, step time.Duration) (FetchResult, error) {
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
	err := makeError(C.rrdFetch(&ret, fn, cCf, &cStart, &cEnd, &cStep, &cDsCnt, &cDsNames, &cData, r.p))
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
