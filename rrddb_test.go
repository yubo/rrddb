package rrddb

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

const (
	step      = 1
	heartbeat = 2 * step
	arname    = "/tmp/rrd.tar"
	dbname    = "/tmp/rrd.db"
	b_size    = 100000
	work_size = 10
)

type db_t struct {
	offset int64
	size   int64
}

var rd *Rrddb
var err error
var now int64
var db map[string]db_t
var wg sync.WaitGroup
var lock sync.RWMutex

func init() {
	now = time.Now().Unix()
	runtime.GOMAXPROCS(runtime.NumCPU())
	db = make(map[string]db_t)
}

func add_rrd(t *testing.T, key string) {
	c := rd.NewCreator(key, time.Now(), step)
	c.RRA("AVERAGE", 0.5, 1, 100)
	c.RRA("AVERAGE", 0.5, 5, 100)
	c.DS("g", "GAUGE", heartbeat, 0, 60)
	if err = c.Create(0); err != nil {
		t.Fatal(err)
	}
}

func test_update(t *testing.T, key string) {
	// Update
	u := rd.NewUpdater(key, 0, 0)
	for i := 0; i < 10; i++ {
		//time.Sleep(time.Second * step)
		err = u.Update(now+int64(i*step), 1.5*float64(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Update with cache
	for i := 10; i < 20; i++ {
		//time.Sleep(time.Second * step)
		u.Cache(now+int64(i*step), 2*float64(i))
	}
	if err = u.Update(); err != nil {
		t.Fatal(err)
	}
}

func test_info(t *testing.T, key string) map[string]interface{} {
	// Info
	if inf, err := rd.Info(key, 0, 0); err != nil {
		t.Fatal(err)
	} else {
		for k, v := range inf {
			fmt.Printf("%s (%T): %v\n", k, v, v)
		}
		return inf
	}
	return nil
}

func test_fetch(t *testing.T, key string, inf map[string]interface{}) {
	var fetchRes FetchResult

	end := time.Unix(int64(inf["last_update"].(uint)), 0)
	start := end.Add(-20 * step * time.Second)
	fmt.Printf("Fetch Params:\n")
	fmt.Printf("Start: %s\n", start)
	fmt.Printf("End: %s\n", end)
	fmt.Printf("Step: %s\n", step*time.Second)
	if fetchRes, err = rd.Fetch(key, 0, 0, "AVERAGE", start, end, step*time.Second); err != nil {
		t.Fatal(err)
	}
	defer fetchRes.FreeValues()
	fmt.Printf("FetchResult:\n")
	fmt.Printf("Start: %s\n", fetchRes.Start)
	fmt.Printf("End: %s\n", fetchRes.End)
	fmt.Printf("Step: %s\n", fetchRes.Step)
	for _, dsName := range fetchRes.DsNames {
		fmt.Printf("\t%s", dsName)
	}
	fmt.Printf("\n")

	row := 0
	for ti := fetchRes.Start.Add(fetchRes.Step); ti.Before(end) || ti.Equal(end); ti = ti.Add(fetchRes.Step) {
		fmt.Printf("%s / %d", ti, ti.Unix())
		for i := 0; i < len(fetchRes.DsNames); i++ {
			v := fetchRes.ValueAt(i, row)
			fmt.Printf("\t%e", v)
		}
		fmt.Printf("\n")
		row++
	}
}

func test_db(t *testing.T) {
	//db
	if err := rd.Put("test1", now, 12345, 3222); err != nil {
		t.Fatal(err)
	}

	if err := rd.Put("test1", now, 12345, 3222); err == nil {
		t.Fatal("put can overwrite?")
	} else {
		fmt.Println("Put ", err)
	}

	if ts, offset, size, err := rd.Get("test1"); err != nil {
		t.Fatal(err)
	} else {
		fmt.Println(ts, offset, size)
	}
}

func test_offset(t *testing.T) {
	for i := 20; i < 30; i++ {
		key := fmt.Sprintf("rrd%08d", i)
		add_rrd(t, key)
		if ts, offset, size, err := rd.Get(key); err != nil {
			fmt.Println(key, "not found or get error", err)
		} else {
			fmt.Println(ts, offset, size)
			u := rd.NewUpdater(key, offset, size)
			if err = u.Update(now+int64(i*step), 1.5*float64(i)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func testAll(t *testing.T) {

	os.Remove(arname)
	os.Remove(dbname)
	if rd, err = Open(arname, dbname); err != nil {
		t.Fatal(err)
	}
	defer func() {
		rd.Close()
		rd = nil
	}()

	add_rrd(t, "test")
	test_update(t, "test")
	inf := test_info(t, "test")
	test_fetch(t, "test", inf)

	test_db(t)

}

func add(b *testing.B, key string) {
	lock.Lock()
	defer lock.Unlock()
	c := rd.NewCreator(key, time.Now(), step)
	c.RRA("AVERAGE", 0.5, 1, 100)
	c.RRA("AVERAGE", 0.5, 5, 100)
	c.DS("g", "GAUGE", heartbeat, 0, 60)
	if err = c.Create(0); err != nil {
		b.Fatal(err)
	}
}

func update(b *testing.B, key string) {
	u := rd.NewUpdater(key, db[key].offset, db[key].size)
	if err := u.Update(now+step, float64(1.5)); err != nil {
		b.Fatal(err)
	}
}

func fetch(b *testing.B, key string, start, end time.Time) {
	if fetchRes, err := rd.Fetch(key, db[key].offset, db[key].size, "AVERAGE", start, end, step*time.Second); err != nil {
		b.Fatal(err)
	} else {
		fetchRes.FreeValues()
	}
}

func BenchmarkAdd(b *testing.B) {
	b.StopTimer()
	os.Remove(arname)
	os.Remove(dbname)
	if rd != nil {
		rd.Close()
	}
	if rd, err = Open(arname, dbname); err != nil {
		b.Fatal(err)
	}
	b.StartTimer()
	b.N = b_size
	n := b.N / work_size
	for j := 0; j < work_size; j++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("add-%d-%d", j, i)
				add(b, fmt.Sprintf(key))
			}
		}(j)
	}
	wg.Wait()
}

func BenchmarkUpdate(b *testing.B) {
	b.StopTimer()
	if rd == nil {
		b.Fatalf("rd is nil")
	}
	b.N = b_size
	n := b.N / work_size
	for j := 0; j < work_size; j++ {
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("add-%d-%d", j, i)
			if _, offset, size, err := rd.Get(key); err != nil {
				b.Fatal(err)
			} else {
				db[key] = db_t{offset: offset, size: size}
			}
		}
	}
	b.StartTimer()

	for j := 0; j < work_size; j++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("add-%d-%d", j, i)
				update(b, key)
			}
		}(j)
	}
	wg.Wait()
}

func BenchmarkFetch(b *testing.B) {
	if rd == nil {
		b.Fatalf("rd is nil")
	}
	b.N = b_size
	n := b.N / work_size
	start := time.Unix(now-step, 0)
	end := start.Add(20 * step * time.Second)
	for j := 0; j < work_size; j++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("add-%d-%d", j, i)
				fetch(b, key, start, end)
			}
		}(j)
	}
	wg.Wait()
}
