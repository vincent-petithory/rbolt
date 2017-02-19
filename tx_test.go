package rbolt_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"crypto/rand"

	"github.com/boltdb/bolt"
	"github.com/vincent-petithory/rbolt"
)

// tempfile returns a temporary file path.
func tempfile() string {
	f, _ := ioutil.TempFile("", "bolt-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

// DB represents a wrapper around a Bolt DB to handle temporary file
// creation and automatic cleanup on close.
type DB struct {
	*bolt.DB
}

// Close closes the database and deletes the underlying file.
func (tdb *DB) Close() error {
	// Close database and remove file.
	p := tdb.DB.Path()
	defer os.Remove(p)
	return tdb.DB.Close()
}

// New returns a new instance of DB.
func NewDB(tb testing.TB) *DB {
	db, err := bolt.Open(tempfile(), 0666, nil)
	if err != nil {
		tb.Fatal(err)
	}
	return &DB{DB: db}
}

func runSyncTest(tb testing.TB, initFunc func(*bolt.Tx) error, es string, testFunc func(*rbolt.Tx) error) {
	dbs := NewDB(tb)
	defer dbs.Close()
	dbt := NewDB(tb)
	defer dbt.Close()

	ackC := make(chan rbolt.Ack, 1) // buffer 1, as Recv() doesn't wait when sending the ack
	transport := rbolt.NewChanTransport()
	go transport.Recv(dbt.DB, ackC)

	if err := dbs.Update(initFunc); err != nil {
		tb.Error(err)
		return
	}
	if err := dbt.Update(initFunc); err != nil {
		tb.Error(err)
		return
	}

	s, ok, err := testDBEquals(dbs.DB, dbt.DB)
	if err != nil {
		tb.Error(err)
		return
	}
	if !ok {
		tb.Error("dbs differ at startup, whoops")
	}

	if err := dbs.Update(func(txo *bolt.Tx) error {
		tx := rbolt.RTx(txo, transport)
		txo.OnCommit(func() {
			if err := tx.Flush(); err != nil {
				tb.Error(err)
			}
		})
		return testFunc(tx)
	}); err != nil {
		tb.Error(err)
		return
	}

	// Wait for the Tx to sync
	ack := <-ackC
	if ack.Err != nil {
		tb.Error(ack.Err)
		return
	}

	s, ok, err = testDBEquals(dbs.DB, dbt.DB)
	if err != nil {
		tb.Error(err)
		return
	}
	if !ok {
		tb.Error("dbs differ")
	}
	es = strings.TrimSpace(es)
	s = strings.TrimSpace(s)
	if s != es {
		tb.Errorf("expected:\n%s\ngot:\n%s", es, s)
	}
}

type Pair struct {
	Ks [][]byte
	V  []byte
}

func (p *Pair) IsBucket() bool {
	return p.V == nil
}

func writePairs(pairs []Pair, w io.Writer) {
	for _, p := range pairs {
		for i, k := range p.Ks {
			fmt.Fprint(w, string(k))
			if i < len(p.Ks)-1 {
				fmt.Fprint(w, "/")
			}
		}
		if !p.IsBucket() {
			fmt.Fprintf(w, " => %s\n", string(p.V))
		} else {
			fmt.Fprintln(w, "/")
		}
	}
}

func cpb(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func getPairs(tx *bolt.Tx) ([]Pair, error) {
	var pairs []Pair
	err := tx.ForEach(func(k []byte, b *bolt.Bucket) error {
		pairs = append(pairs, Pair{Ks: [][]byte{cpb(k)}, V: nil})
		var err error
		pairs, err = getBucketKVs(b, pairs)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return pairs, nil
}

func getBucketKVs(b *bolt.Bucket, pairs []Pair) ([]Pair, error) {
	bks := pairs[len(pairs)-1].Ks
	err := b.ForEach(func(k []byte, v []byte) error {
		if v == nil {
			pairs = append(pairs, Pair{Ks: append(bks, cpb(k)), V: nil})
			var err error
			pairs, err = getBucketKVs(b.Bucket(k), pairs)
			if err != nil {
				return err
			}
			return nil
		}
		pairs = append(pairs, Pair{Ks: append(bks, cpb(k)), V: cpb(v)})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return pairs, nil
}

func testDBEquals(db1 *bolt.DB, db2 *bolt.DB) (string, bool, error) {
	var (
		buf1   bytes.Buffer
		buf2   bytes.Buffer
		pairs1 []Pair
		pairs2 []Pair
		err    error
	)
	err = db1.View(func(tx *bolt.Tx) error {
		p, err := getPairs(tx)
		if err != nil {
			return err
		}
		pairs1 = p
		return nil
	})
	if err != nil {
		return "", false, err
	}
	err = db2.View(func(tx *bolt.Tx) error {
		p, err := getPairs(tx)
		if err != nil {
			return err
		}
		pairs2 = p
		return nil
	})
	if err != nil {
		return "", false, err
	}

	writePairs(pairs1, &buf1)
	writePairs(pairs2, &buf2)

	return buf2.String(), buf1.String() == buf2.String(), nil
}

func TestAll(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte("init1"))
			if err != nil {
				return err
			}
			if err := b.Put([]byte("k1"), []byte("v1")); err != nil {
				return err
			}
			_, err = b.CreateBucket([]byte("subinit1"))
			if err != nil {
				return err
			}
			_, err = tx.CreateBucket([]byte("init2"))
			if err != nil {
				return err
			}
			return nil
		},
		`meow/
meow/k1 => v1
meow2/
meow2/k2 => v2
meow2/submeow2/
meow2/submeow2/sk2 => sv2
`,
		func(tx *rbolt.Tx) error {
			b, err := tx.CreateBucket([]byte("meow"))
			if err != nil {
				return err
			}
			if err := b.Put([]byte("k1"), []byte("v1")); err != nil {
				return err
			}
			b2, err := tx.CreateBucketIfNotExists([]byte("meow2"))
			if err != nil {
				return err
			}
			if err := b2.Put([]byte("k2"), []byte("v2")); err != nil {
				return err
			}
			sb2, err := b2.CreateBucket([]byte("submeow2"))
			if err != nil {
				return err
			}
			if err := sb2.Put([]byte("sk2"), []byte("sv2")); err != nil {
				return err
			}

			if err := tx.DeleteBucket([]byte("init2")); err != nil {
				return err
			}
			bi1 := tx.Bucket([]byte("init1"))
			if err := bi1.Delete([]byte("k1")); err != nil {
				return err
			}
			if err := bi1.DeleteBucket([]byte("subinit1")); err != nil {
				return err
			}
			if err := tx.DeleteBucket([]byte("init1")); err != nil {
				return err
			}
			return nil
		},
	)
}

func TestTxNoSyncIfErrs(t *testing.T) {
	dbs := NewDB(t)
	defer dbs.Close()
	dbt := NewDB(t)
	defer dbt.Close()

	ackC := make(chan rbolt.Ack)
	transport := rbolt.NewChanTransport()
	go transport.Recv(dbt.DB, ackC)

	initFunc := func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("bucket"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("v"))
	}

	if err := dbs.Update(initFunc); err != nil {
		t.Error(err)
		return
	}
	if err := dbt.Update(initFunc); err != nil {
		t.Error(err)
		return
	}

	_, ok, err := testDBEquals(dbs.DB, dbt.DB)
	if err != nil {
		t.Error(err)
		return
	}
	if !ok {
		t.Error("dbs differ at startup, whoops")
	}
	var errForce = errors.New("force")
	if err := dbs.Update(func(tx *bolt.Tx) error {
		rtx := rbolt.RTx(tx, transport)
		tx.OnCommit(func() {
			if err := rtx.Flush(); err != nil {
				t.Error(err)
			}
		})
		b := tx.Bucket([]byte("bucket"))
		if err := b.Delete([]byte("k")); err != nil {
			return err
		}
		return errForce
	}); err != nil && err != errForce {
		t.Errorf("unexpected error %v", err)
		return
	}

	select {
	case <-ackC:
		t.Error("received an ack")
	case <-time.After(time.Millisecond * 100):
	}

	if err := dbt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("bucket"))
		if v := b.Get([]byte("k")); v == nil {
			t.Error("unexpected db sync")
		}
		return nil
	}); err != nil {
		t.Error(err)
		return
	}
}

func TestLongRun(t *testing.T) {
	dbs := NewDB(t)
	defer dbs.Close()
	dbt := NewDB(t)
	defer dbt.Close()

	// we'll keep track of keys we created
	type Key struct {
		bpath [][]byte
		k     []byte
	}
	var keys []Key
	bucketForKey := func(tx *rbolt.Tx, kIdx uint) (*rbolt.Bucket, []byte, error) {
		ek := keys[kIdx%uint(len(keys))]
		if len(ek.bpath) == 0 {
			return nil, nil, nil
		}
		b := tx.Bucket(ek.bpath[0])
		if len(ek.bpath) == 1 {
			return b, nil, nil
		}
		for _, k := range ek.bpath[1:] {
			b = b.Bucket(k)
		}
		return b, ek.k, nil
	}
	const maxDepth = 12

	type Op struct {
		Depth uint
		Key   []byte
		Value []byte
		KIdx  uint
		Typ   uint
	}
	doOp := func(tx *rbolt.Tx, op Op) error {
		op.Depth = op.Depth % maxDepth
		if op.Depth == 0 {
			op.Depth = 1
		}
		if len(op.Key) == 0 {
			return nil
		}
		op.Typ = op.Typ % 100

		switch {
		case op.Typ < 10: // pick a random key, bucket.Delete() it
			if len(keys) == 0 {
				return nil
			}
			b, k, err := bucketForKey(tx, op.KIdx)
			if err != nil {
				return err
			}
			if err := b.Delete(k); err != nil {
				return err
			}
		case op.Typ < 20: // pick a random key, iterate with a Cursor() and Cursor.Delete() it
			if len(keys) == 0 {
				return nil
			}
			b, dk, err := bucketForKey(tx, op.KIdx)
			if err != nil {
				return err
			}
			c := b.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if bytes.Equal(k, dk) {
					return nil
				}
			}
		default: // use the random key, bucket.Put() it, creating buckets along the way
			var (
				b       *rbolt.Bucket
				notRoot bool
				ps      [][]byte
			)
			for i := 0; i < int(op.Depth); i++ {
				p := make([]byte, 16)
				_, err := rand.Read(p)
				if err != nil {
					return err
				}
				if notRoot {
					b, err = b.CreateBucketIfNotExists(p)
				} else {
					b, err = tx.CreateBucketIfNotExists(p)
					notRoot = true
				}
				if err != nil {
					return err
				}
				ps = append(ps, p)
			}
			if err := b.Put(op.Key, op.Value); err != nil {
				return err
			}
			keys = append(keys, Key{bpath: ps, k: op.Key})
		}
		return nil
	}

	ackC := make(chan rbolt.Ack, 1) // buffer 1, as Recv() doesn't wait when sending the ack
	transport := rbolt.NewChanTransport()
	go transport.Recv(dbt.DB, ackC)

	err := quick.Check(func(ops []Op) bool {
		if err := dbs.Update(func(txo *bolt.Tx) error {
			tx := rbolt.RTx(txo, transport)
			txo.OnCommit(func() {
				if err := tx.Flush(); err != nil {
					t.Error(err)
				}
			})
			for _, op := range ops {
				if err := doOp(tx, op); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Error(err)
			return false
		}

		// Wait for the Tx to sync
		ack := <-ackC
		if ack.Err != nil {
			t.Error(ack.Err)
			return false
		}

		_, ok, err := testDBEquals(dbs.DB, dbt.DB)
		if err != nil {
			t.Error(err)
			return false
		}
		if !ok {
			t.Error("dbs differ")
		}
		return true
	}, nil)
	if err != nil {
		t.Error("Failed to sync dbs")
	}
}

func TestTxBucket(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("bucket"))
			return err
		},
		`
bucket/
bucket/k => v
`,
		func(tx *rbolt.Tx) error {
			// Use the bucket to create a key
			return tx.Bucket([]byte("bucket")).Put([]byte("k"), []byte("v"))
		},
	)
}

func TestTxCreateBucket(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error { return nil },
		`bucket/`,
		func(tx *rbolt.Tx) error {
			_, err := tx.CreateBucket([]byte("bucket"))
			return err
		},
	)
}

func TestTxCreateBucketIfNotExists(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error { return nil },
		`bucket/`,
		func(tx *rbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte("bucket"))
			return err
		},
	)
}

func TestTxDeleteBucket(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("bucket"))
			return err
		},
		`
`,
		func(tx *rbolt.Tx) error {
			return tx.DeleteBucket([]byte("bucket"))
		},
	)
}

func TestTxForEach(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("bucket"))
			return err
		},
		`
bucket/
bucket/k => v
`,
		func(tx *rbolt.Tx) error {
			// Use a bucket retrieved with ForEach
			var bucket *rbolt.Bucket
			if err := tx.ForEach(func(name []byte, b *rbolt.Bucket) error {
				if bytes.Equal(name, []byte("bucket")) {
					bucket = b
				}
				return nil
			}); err != nil {
				return err
			}
			return bucket.Put([]byte("k"), []byte("v"))
		},
	)
}
