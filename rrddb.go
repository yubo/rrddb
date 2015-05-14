/*
 * yubo@yubo.org
 * 2015-05-13
 */
package rrddb

func Open(arname, dbname string) (*Rrddb, error) {
	var rd *Rrddb
	var err error

	if rd, err = open(arname, dbname, "hash", "", 0); err != nil {
		return nil, err
	}

	return rd, nil
}

func (rd *Rrddb) Close() error {
	return rd.close()
}

/*
func (r *Rrddb) Rebuild() {
	//todo
}

func (r *Rrddb) Get_entry(key string) (*Db_entry, error) {
	e := &Db_entry{Key: key}

	if err := r.Db.Get(e); err != nil {
		return nil, err
	} else {
		return e, nil
	}
}

func (r *Rrddb) Put_entry(e *Db_entry) error {
	return r.Db.Put(e)
}

func (r *Rrddb) Update_entry(e *Db_entry) error {
	return r.Db.Update(e)
}

func (r *Rrddb) Rrd_create() {
}

func (r *Rrddb) Rrd_update() {
}

func (r *Rrddb) Rrd_fetch() {
}

func (r *Rrddb) Rrd_info() {
}
*/
