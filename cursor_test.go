package rbolt_test

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/vincent-petithory/rbolt"
)

func TestCursorForwardSession(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte("bucket"))
			if err != nil {
				return err
			}
			for i := 1; i < 10; i++ {
				if err := b.Put([]byte(fmt.Sprintf("k%d", i)), []byte(strconv.Itoa(i))); err != nil {
					return err
				}
			}
			return nil
		},
		`
bucket/
bucket/k2 => 2
bucket/k4 => 4
bucket/k6 => 6
bucket/k8 => 8
`,
		func(tx *rbolt.Tx) error {
			b := tx.Bucket([]byte("bucket"))
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				i, err := strconv.Atoi(string(v))
				if err != nil {
					return err
				}
				if i%2 == 1 {
					if err := c.Delete(); err != nil {
						return err
					}
				}
			}
			return nil
		},
	)
}

func TestCursorBackwardSession(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte("bucket"))
			if err != nil {
				return err
			}
			for i := 1; i < 10; i++ {
				if err := b.Put([]byte(fmt.Sprintf("k%d", i)), []byte(strconv.Itoa(i))); err != nil {
					return err
				}
			}
			return nil
		},
		`
bucket/
bucket/k2 => 2
bucket/k4 => 4
bucket/k6 => 6
bucket/k8 => 8
`,
		func(tx *rbolt.Tx) error {
			b := tx.Bucket([]byte("bucket"))
			c := b.Cursor()
			for k, v := c.Last(); k != nil; k, v = c.Prev() {
				i, err := strconv.Atoi(string(v))
				if err != nil {
					return err
				}
				if i%2 == 1 {
					if err := c.Delete(); err != nil {
						return err
					}
				}
			}
			return nil
		},
	)
}

func TestCursorSeekSession(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte("bucket"))
			if err != nil {
				return err
			}
			ks := []string{
				"a.b",
				"a.c",
				"b.a",
				"b.b",
				"b.c",
				"x.a",
				"x.b",
				"y.c",
			}
			for _, k := range ks {
				if err := b.Put([]byte(k), []byte("x")); err != nil {
					return err
				}
			}
			return nil
		},
		`
bucket/
bucket/a.b => x
bucket/a.c => x
bucket/x.a => x
bucket/x.b => x
bucket/y.c => x
`,
		func(tx *rbolt.Tx) error {
			b := tx.Bucket([]byte("bucket"))
			c := b.Cursor()

			for k, _ := c.Seek([]byte("b.")); k != nil && bytes.HasPrefix(k, []byte("b.")); k, _ = c.Seek([]byte("b.")) {
				if err := c.Delete(); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func TestCursorManyMovesSession(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte("bucket"))
			if err != nil {
				return err
			}
			for i := 1; i < 10; i++ {
				if err := b.Put([]byte(fmt.Sprintf("k%d", i)), []byte(strconv.Itoa(i))); err != nil {
					return err
				}
			}
			return nil
		},
		`
bucket/
bucket/k3 => 3
bucket/k4 => 4
bucket/k5 => 5
bucket/k6 => 6
bucket/k7 => 7
bucket/k8 => 8
bucket/k9 => 9
bucket/x.a => x
bucket/z.b => z
`,
		func(tx *rbolt.Tx) error {
			b := tx.Bucket([]byte("bucket"))
			c := b.Cursor()

			c.First()
			c.Next()
			if err := b.Put([]byte("x.a"), []byte("x")); err != nil {
				return err
			}
			if err := c.Delete(); err != nil { // removes k2
				return err
			}
			c.Next()
			if err := b.Put([]byte("y.b"), []byte("y")); err != nil {
				return err
			}
			if err := b.Put([]byte("y.c"), []byte("y")); err != nil {
				return err
			}
			if err := b.Put([]byte("z.a"), []byte("z")); err != nil {
				return err
			}
			if err := b.Put([]byte("z.b"), []byte("z")); err != nil {
				return err
			}
			if err := b.Put([]byte("z.c"), []byte("z")); err != nil {
				return err
			}
			c.Last()
			if err := c.Delete(); err != nil { // removes z.c
				return err
			}
			c.Last()
			c.Prev()
			if err := c.Delete(); err != nil { // removes z.a
				return err
			}
			c.First()
			if err := c.Delete(); err != nil { // removes k1
				return err
			}

			for k, _ := c.Seek([]byte("y.")); k != nil && bytes.HasPrefix(k, []byte("y.")); k, _ = c.Seek([]byte("y.")) {
				if err := c.Delete(); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func TestCursorBucket(t *testing.T) {
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
			b := tx.Bucket([]byte("bucket"))
			c := b.Cursor()
			bc := c.Bucket()
			// Use the bucket to create a key
			return bc.Put([]byte("k"), []byte("v"))
		},
	)
}
