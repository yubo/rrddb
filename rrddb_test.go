package rrddb

import (
	"fmt"
	"os"
	"testing"
	"time"
)

const (
	step      = 1
	heartbeat = 2 * step
	arname    = "/tmp/rrd.tar"
	dbname    = "/tmp/rrd.db"
	rrdname   = "test"
)

func TestAll(t *testing.T) {
	var rd *Rrddb
	var err error

	defer os.Remove(arname)
	defer os.Remove(dbname)

	if rd, err = Open(arname, dbname); err != nil {
		t.Fatal(err)
	}

	// Create
	c := rd.NewCreator(rrdname, time.Now(), step)
	c.RRA("AVERAGE", 0.5, 1, 100)
	c.RRA("AVERAGE", 0.5, 5, 100)
	c.DS("cnt", "COUNTER", heartbeat, 0, 100)
	c.DS("g", "GAUGE", heartbeat, 0, 60)
	if err = c.Create(0); err != nil {
		t.Fatal(err)
	}

	// Update
	u := rd.NewUpdater(rrdname)
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second * step)
		err := u.Update(time.Now(), i, 1.5*float64(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Update with cache
	for i := 10; i < 20; i++ {
		time.Sleep(time.Second * step)
		u.Cache(time.Now(), i, 2*float64(i))
	}
	if err = u.Update(); err != nil {
		t.Fatal(err)
	}

	// Info
	var inf map[string]interface{}
	if inf, err = rd.Info(rrdname); err != nil {
		t.Fatal(err)
	}
	for k, v := range inf {
		fmt.Printf("%s (%T): %v\n", k, v, v)
	}

	// Fetch
	var fetchRes FetchResult
	end := time.Unix(int64(inf["last_update"].(uint)), 0)
	start := end.Add(-20 * step * time.Second)
	fmt.Printf("Fetch Params:\n")
	fmt.Printf("Start: %s\n", start)
	fmt.Printf("End: %s\n", end)
	fmt.Printf("Step: %s\n", step*time.Second)
	if fetchRes, err = rd.Fetch(rrdname, "AVERAGE", start, end, step*time.Second); err != nil {
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

	if err = rd.Close(); err != nil {
		t.Fatal(err)
	}

}
