package rbolt

import (
	"bytes"

	"github.com/boltdb/bolt"
)

type Op int

func (op Op) String() string {
	switch op {
	case OpCreateBucket:
		return "CreateBucket"
	case OpCreateBucketIfNotExists:
		return "CreateBucketIfNotExists"
	case OpDelete:
		return "Delete"
	case OpDeleteBucket:
		return "DeleteBucket"
	case OpPut:
		return "Put"
	case OpBucketCursor:
		return "BucketCursor"
	case OpCursorDelete:
		return "CursorDelete"
	case OpCursorFirst:
		return "CursorFirst"
	case OpCursorLast:
		return "CursorLast"
	case OpCursorNext:
		return "CursorNext"
	case OpCursorPrev:
		return "CursorPrev"
	case OpCursorSeek:
		return "CursorSeek"
	default:
		return ""
	}
}

const (
	OpCreateBucket Op = iota + 1
	OpCreateBucketIfNotExists
	OpDelete
	OpDeleteBucket
	OpPut
	OpBucketCursor
	OpCursorDelete
	OpCursorFirst
	OpCursorLast
	OpCursorNext
	OpCursorPrev
	OpCursorSeek
)

type Journal struct {
	LSN  int
	TxID int
	Ws   []W
	Type JournalType

	cursors map[string][]*bolt.Cursor
}

type JournalType int

const (
	JournalTypeUpdate JournalType = iota + 1
	JournalTypeCommit
	JournalTypeRollback
)

func (j *Journal) Play(tx *bolt.Tx) error {
	j.cursors = make(map[string][]*bolt.Cursor)
	for _, w := range j.Ws {
		if err := j.playW(tx, w); err != nil {
			return err
		}
	}
	return nil
}

func pkey(p [][]byte) string {
	return string(bytes.Join(p, []byte("::")))
}

func (j *Journal) playW(tx *bolt.Tx, w W) error {
	tailKey := w.Path[len(w.Path)-1]
	switch w.Op {
	case OpCreateBucket:
		return j.opCreateBucket(tx, w, false)
	case OpCreateBucketIfNotExists:
		return j.opCreateBucket(tx, w, true)
	case OpDelete:
		b := j.bucketOrTx(tx, w.Path)
		// b can't be nil
		return b.Delete(tailKey)
	case OpDeleteBucket:
		b := j.bucketOrTx(tx, w.Path)
		if b == nil {
			return tx.DeleteBucket(tailKey)
		}
		return b.DeleteBucket(tailKey)
	case OpPut:
		b := j.bucketOrTx(tx, w.Path)
		// b can't be nil
		return b.Put(tailKey, w.Value)
	case OpBucketCursor:
		// we add an empty key since bucketOrTx() works for keys, and we have only a bucket path here.
		b := j.bucketOrTx(tx, append(w.Path, []byte{}))
		// b can't be nil, we don't process tx's cursors
		k := pkey(w.Path)
		j.cursors[k] = append(j.cursors[k], b.Cursor())
	case OpCursorDelete:
		c := j.cursor(tx, w.Path, w.CursorID)
		if err := c.Delete(); err != nil {
			return err
		}
	case OpCursorFirst:
		c := j.cursor(tx, w.Path, w.CursorID)
		c.First()
	case OpCursorLast:
		c := j.cursor(tx, w.Path, w.CursorID)
		c.Last()
	case OpCursorNext:
		c := j.cursor(tx, w.Path, w.CursorID)
		c.Next()
	case OpCursorPrev:
		c := j.cursor(tx, w.Path, w.CursorID)
		c.Prev()
	case OpCursorSeek:
		c := j.cursor(tx, w.Path, w.CursorID)
		c.Seek(w.Value)
	}
	return nil
}

func (j *Journal) bucketOrTx(tx *bolt.Tx, p [][]byte) *bolt.Bucket {
	if len(p) == 1 {
		return nil
	}
	b := tx.Bucket(p[0])
	for _, k := range p[1 : len(p)-1] {
		b = b.Bucket(k)
	}
	return b
}

func (j *Journal) opCreateBucket(tx *bolt.Tx, w W, withExists bool) error {
	b := j.bucketOrTx(tx, w.Path)
	if b == nil {
		if withExists {
			_, err := tx.CreateBucketIfNotExists(w.Path[0])
			return err
		}
		_, err := tx.CreateBucket(w.Path[0])
		return err
	}
	if withExists {
		_, err := b.CreateBucketIfNotExists(w.Path[len(w.Path)-1])
		return err
	}
	_, err := b.CreateBucket(w.Path[len(w.Path)-1])
	return err
}

func (j *Journal) cursor(tx *bolt.Tx, p [][]byte, id int) *bolt.Cursor {
	k := pkey(p)
	return j.cursors[k][id]
}

type W struct {
	Op       Op
	Path     [][]byte
	CursorID int
	Value    []byte
}

func RTx(tx *bolt.Tx) *Tx {
	return &Tx{Tx: tx, writable: tx.Writable(), j: &Journal{TxID: tx.ID(), Type: JournalTypeUpdate}}
}

type Tx struct {
	*bolt.Tx

	writable bool
	j        *Journal
}

func (tx *Tx) Journal() *Journal {
	return tx.j
}

func (tx *Tx) log(op Op, path [][]byte, v []byte, cursorID int) {
	tx.j.Ws = append(tx.j.Ws, W{Op: op, Path: path, Value: v, CursorID: cursorID})
}

func (tx *Tx) Bucket(name []byte) *Bucket {
	b := tx.Tx.Bucket(name)
	if b == nil {
		return nil
	}
	if !tx.writable {
		return &Bucket{b: b, tx: tx}
	}
	n := cpb(name)
	return &Bucket{b: b, tx: tx, path: [][]byte{n}}
}

func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	b, err := tx.Tx.CreateBucket(name)
	if err != nil {
		return nil, err
	}
	if !tx.writable {
		return &Bucket{b: b, tx: tx}, nil
	}
	n := cpb(name)
	tx.log(OpCreateBucket, [][]byte{n}, nil, 0)
	return &Bucket{b: b, tx: tx, path: [][]byte{n}}, nil
}

func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	b, err := tx.Tx.CreateBucketIfNotExists(name)
	if err != nil {
		return nil, err
	}
	if !tx.writable {
		return &Bucket{b: b, tx: tx}, nil
	}
	n := cpb(name)
	tx.log(OpCreateBucketIfNotExists, [][]byte{n}, nil, 0)
	return &Bucket{b: b, tx: tx, path: [][]byte{n}}, nil
}

/*
We don't need to record Tx's Cursor sessions, as Cursor.Delete() can't delete buckets.
func (tx *Tx) Cursor() *Cursor
*/

func (tx *Tx) DeleteBucket(name []byte) error {
	err := tx.Tx.DeleteBucket(name)
	if err != nil {
		return err
	}
	if !tx.writable {
		return nil
	}
	n := cpb(name)
	tx.log(OpDeleteBucket, [][]byte{n}, nil, 0)
	return nil
}

func (tx *Tx) ForEach(fn func([]byte, *Bucket) error) error {
	if !tx.writable {
		return tx.Tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return fn(name, &Bucket{b: b, tx: tx})
		})
	}
	return tx.Tx.ForEach(func(name []byte, b *bolt.Bucket) error {
		n := cpb(name)
		return fn(name, &Bucket{b: b, tx: tx, path: [][]byte{n}})
	})
}

type Bucket struct {
	b       *bolt.Bucket
	path    [][]byte
	tx      *Tx
	cursors []*bolt.Cursor
}

func (b *Bucket) Bucket(name []byte) *Bucket {
	sb := b.b.Bucket(name)
	if sb == nil {
		return nil
	}
	if !b.tx.writable {
		return &Bucket{b: sb, tx: b.tx}
	}
	return &Bucket{b: sb, tx: b.tx, path: append(b.path, cpb(name))}
}

func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	sb, err := b.b.CreateBucket(key)
	if err != nil {
		return nil, err
	}
	if !b.tx.writable {
		return &Bucket{b: sb, tx: b.tx}, nil
	}
	p := append(b.path, cpb(key))
	b.tx.log(OpCreateBucket, p, nil, 0)
	return &Bucket{b: sb, tx: b.tx, path: p}, nil
}

func (b *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	sb, err := b.b.CreateBucketIfNotExists(key)
	if err != nil {
		return nil, err
	}
	if !b.tx.writable {
		return &Bucket{b: sb, tx: b.tx}, nil
	}
	p := append(b.path, cpb(key))
	b.tx.log(OpCreateBucketIfNotExists, p, nil, 0)
	return &Bucket{b: sb, tx: b.tx, path: p}, nil
}

func (b *Bucket) Cursor() *Cursor {
	c := b.b.Cursor()
	b.cursors = append(b.cursors, c)
	cid := len(b.cursors) - 1
	b.tx.log(OpBucketCursor, b.path[:], nil, cid)
	return &Cursor{c: c, tx: b.tx, b: b, id: cid}
}

func (b *Bucket) Delete(key []byte) error {
	err := b.b.Delete(key)
	if err != nil {
		return err
	}
	if !b.tx.writable {
		return nil
	}
	p := append(b.path, cpb(key))
	b.tx.log(OpDelete, p, nil, 0)
	return nil
}

func (b *Bucket) DeleteBucket(key []byte) error {
	err := b.b.DeleteBucket(key)
	if err != nil {
		return err
	}
	if !b.tx.writable {
		return nil
	}
	p := append(b.path, cpb(key))
	b.tx.log(OpDeleteBucket, p, nil, 0)
	return nil
}

func (b *Bucket) Put(key []byte, value []byte) error {
	err := b.b.Put(key, value)
	if err != nil {
		return err
	}
	if !b.tx.writable {
		return nil
	}
	b.tx.log(OpPut, append(b.path, cpb(key)), cpb(value), 0)
	return nil
}

/*
Can't embed *bolt.Bucket, because Bucket() method name clashes with the field name,
so let's write the methods manually.
*/

func (b *Bucket) ForEach(fn func(k, v []byte) error) error { return b.b.ForEach(fn) }
func (b *Bucket) Get(key []byte) []byte                    { return b.b.Get(key) }
func (b *Bucket) NextSequence() (uint64, error)            { return b.b.NextSequence() }
func (b *Bucket) Root() uint64                             { return uint64(b.b.Root()) }
func (b *Bucket) Sequence() uint64                         { return b.b.Sequence() }
func (b *Bucket) SetSequence(v uint64) error               { return b.b.SetSequence(v) }
func (b *Bucket) Stats() bolt.BucketStats                  { return b.b.Stats() }
func (b *Bucket) Tx() *Tx                                  { return b.tx }
func (b *Bucket) Writable() bool                           { return b.b.Writable() }

type Cursor struct {
	id int
	c  *bolt.Cursor
	tx *Tx
	b  *Bucket
}

func (c *Cursor) Bucket() *Bucket {
	return c.b
}
func (c *Cursor) Delete() error {
	err := c.c.Delete()
	if c.tx.writable {
		c.tx.log(OpCursorDelete, c.b.path, nil, c.id)
	}
	return err
}
func (c *Cursor) First() (key []byte, value []byte) {
	k, v := c.c.First()
	if c.tx.writable {
		c.tx.log(OpCursorFirst, c.b.path, nil, c.id)
	}
	return k, v
}
func (c *Cursor) Last() (key []byte, value []byte) {
	k, v := c.c.Last()
	if c.tx.writable {
		c.tx.log(OpCursorLast, c.b.path, nil, c.id)
	}
	return k, v
}
func (c *Cursor) Next() (key []byte, value []byte) {
	k, v := c.c.Next()
	if c.tx.writable {
		c.tx.log(OpCursorNext, c.b.path, nil, c.id)
	}
	return k, v
}
func (c *Cursor) Prev() (key []byte, value []byte) {
	k, v := c.c.Prev()
	if c.tx.writable {
		c.tx.log(OpCursorPrev, c.b.path, nil, c.id)
	}
	return k, v
}
func (c *Cursor) Seek(seek []byte) (key []byte, value []byte) {
	k, v := c.c.Seek(seek)
	if c.tx.writable {
		c.tx.log(OpCursorSeek, c.b.path, cpb(seek), c.id)
	}
	return k, v
}

func cpb(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
