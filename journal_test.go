package rbolt_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/vincent-petithory/rbolt"
)

func TestJournalBufferSequence(t *testing.T) {
	dbs := NewDB(t)
	defer dbs.Close()
	dbt := NewDB(t)
	defer dbt.Close()

	if err := dbs.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("incs"))
		return err
	}); err != nil {
		t.Error(err)
		return
	}
	if err := dbt.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("incs"))
		return err
	}); err != nil {
		t.Error(err)
		return
	}

	const N = 10

	var (
		txIDs []int
		lsn   = new(rbolt.MonotonicLSN)
	)

	jbuf := rbolt.NewJournalBuffer(dbt.DB)
	transport := &rbolt.LocalTransport{JournalBuffer: jbuf}

	var wg sync.WaitGroup

	for i := 1; i <= N; i++ {
		inc := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rbolt.Update(dbs.DB, transport, lsn, func(tx *rbolt.Tx) error {
				txIDs = append(txIDs, tx.ID())
				b := tx.Bucket([]byte("incs"))
				k := []byte(fmt.Sprintf("k%d", inc))
				v := []byte(fmt.Sprintf("v%d", inc))
				return b.Put(k, v)
			}); err != nil {
				t.Error(err)
				return
			}
		}()
	}

	wg.Wait()

	if err := jbuf.Flush(); err != nil {
		t.Error(err)
	}

	// Check dbs equal at the end
	_, ok, err := testDBEquals(dbs.DB, dbt.DB)
	if err != nil {
		t.Error(err)
		return
	}
	if !ok {
		t.Error("dbs differ")
	}
}

func TestJournalBufferNoSyncIfErrs(t *testing.T) {
	dbs := NewDB(t)
	defer dbs.Close()
	dbt := NewDB(t)
	defer dbt.Close()

	lsn := new(rbolt.MonotonicLSN)

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

	jbuf := rbolt.NewJournalBuffer(dbt.DB)
	transport := &rbolt.LocalTransport{JournalBuffer: jbuf}

	var errForce = errors.New("force")
	if err := rbolt.Update(dbs.DB, transport, lsn, func(tx *rbolt.Tx) error {
		b := tx.Bucket([]byte("bucket"))
		if err := b.Delete([]byte("k")); err != nil {
			return err
		}
		return errForce
	}); err != nil && err != errForce {
		t.Errorf("unexpected error %v", err)
	}

	if err := jbuf.Flush(); err != nil {
		t.Error(err)
	}

	if err := dbt.View(func(tx *bolt.Tx) error {
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
