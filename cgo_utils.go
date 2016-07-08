/*
 * yubo@yubo.org
 * 2015-05-13
 */
package rrddb

/*
#include <stdlib.h>
#include "cgo_rrddb.h"
#cgo CFLAGS: -std=c99 -D__DBINTERFACE_PRIVATE -DRRD_LITE -D_BSD_SOURCE -DHAVE_CONFIG_H -D_POSIX_SOURCE -DNUMVERS=1.4009
#cgo LDFLAGS: -lm
*/
import "C"

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const (
	maxUint         = ^uint(0)
	maxInt          = int(maxUint >> 1)
	minInt          = -maxInt - 1
	RET_ERROR       = -1 /* Return values. */
	RET_SUCCESS     = 0
	RET_SPECIAL     = 1
	MAX_PAGE_NUMBER = 0xffffffff /* >= # of pages in a file */
	MAX_PAGE_OFFSET = 65535      /* >= # of bytes in a page */
	MAX_REC_NUMBER  = 0xffffffff /* >= # of records in a tree */
	R_CURSOR        = 1          /* del, put, seq */
	__R_UNUSED      = 2          /* UNUSED */
	R_FIRST         = 3          /* seq */
	R_IAFTER        = 4          /* put (RECNO) */
	R_IBEFORE       = 5          /* put (RECNO) */
	R_LAST          = 6          /* seq (BTREE, RECNO) */
	R_NEXT          = 7          /* seq */
	R_NOOVERWRITE   = 8          /* put */
	R_PREV          = 9          /* seq (BTREE, RECNO) */
	R_SETCURSOR     = 10         /* put (RECNO) */
	R_RECNOSYNC     = 11         /* sync (RECNO) */
	DB_BTREE        = 0
	DB_HASH         = 1
	DB_RECNO        = 2
	DB_LOCK         = 0x20000000 /* Do locking. */
	DB_SHMEM        = 0x40000000 /* Use shared memory. */
	DB_TXN          = 0x80000000 /* Do transactions. */
	BTREEMAGIC      = 0x053162
	BTREEVERSION    = 3
	HASHMAGIC       = 0x061561
	HASHVERSION     = 2
	R_FIXEDLEN      = 0x01 /* fixed-length records */
	R_NOKEY         = 0x02 /* key not required */
	R_SNAPSHOT      = 0x04 /* snapshot the input */
)

var mutex sync.Mutex
var (
	oStart           = C.CString("-s")
	oEnd             = C.CString("-e")
	oTitle           = C.CString("-t")
	oVlabel          = C.CString("-v")
	oWidth           = C.CString("-w")
	oHeight          = C.CString("-h")
	oUpperLimit      = C.CString("-u")
	oLowerLimit      = C.CString("-l")
	oRigid           = C.CString("-r")
	oAltAutoscale    = C.CString("-A")
	oAltAutoscaleMin = C.CString("-J")
	oAltAutoscaleMax = C.CString("-M")
	oNoGridFit       = C.CString("-N")

	oLogarithmic   = C.CString("-o")
	oUnitsExponent = C.CString("-X")
	oUnitsLength   = C.CString("-L")

	oRightAxis      = C.CString("--right-axis")
	oRightAxisLabel = C.CString("--right-axis-label")

	oDaemon = C.CString("--daemon")

	oNoLegend = C.CString("-g")

	oLazy = C.CString("-z")

	oColor = C.CString("-c")

	oSlopeMode   = C.CString("-E")
	oImageFormat = C.CString("-a")
	oInterlaced  = C.CString("-i")

	oBase      = C.CString("-b")
	oWatermark = C.CString("-W")

	oStep    = C.CString("--step")
	oMaxRows = C.CString("-m")
)

type Error string

type Rrddb struct {
	sync.RWMutex
	Db     bool
	Ar     bool
	arname string
	dbname string
	dbtype string
	dbinfo string
	dblock int
	p      unsafe.Pointer
}

type Creator struct {
	filename string
	start    time.Time
	step     uint
	args     []string
	rd       *Rrddb
}
type Updater struct {
	filename cstring
	template cstring
	offset   int64
	size     int64
	args     []string
	rd       *Rrddb
}
type FetchResult struct {
	Filename string
	Cf       string
	Start    time.Time
	End      time.Time
	Step     time.Duration
	DsNames  []string
	RowCnt   int
	values   []float64
}

/*
type Archive struct {
	Filename string
	ar       unsafe.Pointer
}
*/
type cstring []byte

/* function */
func (e Error) Error() string {
	return string(e)
}

func newCstring(s string) cstring {
	cs := make(cstring, len(s)+1)
	copy(cs, s)
	return cs
}

func (cs cstring) p() unsafe.Pointer {
	if len(cs) == 0 {
		return nil
	}
	return unsafe.Pointer(&cs[0])
}

func (cs cstring) String() string {
	return string(cs[:len(cs)-1])
}

func (d *Rrddb) String() string {
	return fmt.Sprintf("db[0x%08x] arname[%s] dbname[%s] type[%s] info[%s] lock[%d]",
		d.p, d.arname, d.dbname, d.dbtype, d.dbinfo, d.dblock)
}

func join(args []interface{}) string {
	sa := make([]string, len(args))
	for i, a := range args {
		var s string
		switch v := a.(type) {
		case time.Time:
			s = i64toa(v.Unix())
		default:
			s = fmt.Sprint(v)
		}
		sa[i] = s
	}
	return strings.Join(sa, ":")
}

func makeArgs(args []string) []*C.char {
	ret := make([]*C.char, len(args))
	for i, s := range args {
		ret[i] = C.CString(s)
	}
	return ret
}

func freeCString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

func freeArgs(cArgs []*C.char) {
	for _, s := range cArgs {
		freeCString(s)
	}
}

func makeError(e *C.char) error {
	var null *C.char
	if e == null {
		return nil
	}
	return Error(C.GoString(e))
}

func int2Error(e C.int) error {
	return makeError(C.rrd_strerror(e))
}

func ftoa(f float64) string {
	return strconv.FormatFloat(f, 'e', 10, 64)
}

func ftoc(f float64) *C.char {
	return C.CString(ftoa(f))
}

func i64toa(i int64) string {
	return strconv.FormatInt(i, 10)
}

func i64toc(i int64) *C.char {
	return C.CString(i64toa(i))
}

func u64toa(u uint64) string {
	return strconv.FormatUint(u, 10)
}

func u64toc(u uint64) *C.char {
	return C.CString(u64toa(u))
}
func itoa(i int) string {
	return i64toa(int64(i))
}

func itoc(i int) *C.char {
	return i64toc(int64(i))
}

func utoa(u uint) string {
	return u64toa(uint64(u))
}

func utoc(u uint) *C.char {
	return u64toc(uint64(u))
}

func parseInfoKey(ik string) (kname, kkey string, kid int) {
	kid = -1
	o := strings.IndexRune(ik, '[')
	if o == -1 {
		kname = ik
		return
	}
	c := strings.IndexRune(ik[o+1:], ']')
	if c == -1 {
		kname = ik
		return
	}
	c += o + 1
	kname = ik[:o] + ik[c+1:]
	kkey = ik[o+1 : c]
	if strings.HasPrefix(kname, "ds.") {
		return
	} else if id, err := strconv.Atoi(kkey); err == nil && id >= 0 {
		kid = id
	}
	return
}

func updateInfoValue(i *C.struct_rrd_info_t, v interface{}) interface{} {
	switch i._type {
	case C.RD_I_VAL:
		return float64(*(*C.rrd_value_t)(unsafe.Pointer(&i.value[0])))
	case C.RD_I_CNT:
		return uint(*(*C.ulong)(unsafe.Pointer(&i.value[0])))
	case C.RD_I_STR:
		return C.GoString(*(**C.char)(unsafe.Pointer(&i.value[0])))
	case C.RD_I_INT:
		return int(*(*C.int)(unsafe.Pointer(&i.value[0])))
	case C.RD_I_BLO:
		blob := *(*C.rrd_blob_t)(unsafe.Pointer(&i.value[0]))
		b := C.GoBytes(unsafe.Pointer(blob.ptr), C.int(blob.size))
		if v == nil {
			return b
		}
		return append(v.([]byte), b...)
	}

	return nil
}

func parseRRDInfo(i *C.rrd_info_t) map[string]interface{} {
	defer C.rrd_info_free(i)

	r := make(map[string]interface{})
	for w := (*C.struct_rrd_info_t)(i); w != nil; w = w.next {
		kname, kkey, kid := parseInfoKey(C.GoString(w.key))
		v, ok := r[kname]
		switch {
		case kid != -1:
			var a []interface{}
			if ok {
				a = v.([]interface{})
			}
			if len(a) < kid+1 {
				oldA := a
				a = make([]interface{}, kid+1)
				copy(a, oldA)
			}
			a[kid] = updateInfoValue(w, a[kid])
			v = a
		case kkey != "":
			var m map[string]interface{}
			if ok {
				m = v.(map[string]interface{})
			} else {
				m = make(map[string]interface{})
			}
			old, _ := m[kkey]
			m[kkey] = updateInfoValue(w, old)
			v = m
		default:
			v = updateInfoValue(w, v)
		}
		r[kname] = v
	}
	return r
}
