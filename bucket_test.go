package rbolt_test

import (
	"testing"

	"github.com/boltdb/bolt"
	"github.com/vincent-petithory/rbolt"
)

func TestBucketBucket(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte("bucket"))
			if err != nil {
				return err
			}
			_, err = b.CreateBucket([]byte("subbucket"))
			return err
		},
		`
bucket/
bucket/subbucket/
bucket/subbucket/k => v
`,
		func(tx *rbolt.Tx) error {
			// Use the bucket to create a key
			return tx.Bucket([]byte("bucket")).Bucket([]byte("subbucket")).Put([]byte("k"), []byte("v"))
		},
	)
}

func TestBucketCreateBucket(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("bucket"))
			return err
		},
		`bucket/
bucket/subbucket/
`,
		func(tx *rbolt.Tx) error {
			_, err := tx.Bucket([]byte("bucket")).CreateBucket([]byte("subbucket"))
			return err
		},
	)
}

func TestBucketCreateBucketIfNotExists(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("bucket"))
			return err
		},
		`
bucket/
bucket/subbucket/
`,
		func(tx *rbolt.Tx) error {
			_, err := tx.Bucket([]byte("bucket")).CreateBucketIfNotExists([]byte("subbucket"))
			return err
		},
	)
}

func TestBucketDelete(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte("bucket"))
			if err != nil {
				return err
			}
			return b.Put([]byte("k"), []byte("v"))
		},
		`bucket/`,
		func(tx *rbolt.Tx) error {
			return tx.Bucket([]byte("bucket")).Delete([]byte("k"))
		},
	)
}

func TestBucketDeleteBucket(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte("bucket"))
			if err != nil {
				return err
			}
			_, err = b.CreateBucket([]byte("subbucket"))
			return err
		},
		`bucket/`,
		func(tx *rbolt.Tx) error {
			return tx.Bucket([]byte("bucket")).DeleteBucket([]byte("subbucket"))
		},
	)
}

func TestBucketPut(t *testing.T) {
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
			return tx.Bucket([]byte("bucket")).Put([]byte("k"), []byte("v"))
		},
	)
}

func TestBucketTx(t *testing.T) {
	runSyncTest(t,
		func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("bucket"))
			return err
		},
		`
bucket/
bucket2/
`,
		func(tx *rbolt.Tx) error {
			b := tx.Bucket([]byte("bucket"))
			// Use the tx to create a bucket
			_, err := b.Tx().CreateBucket([]byte("bucket2"))
			return err
		},
	)
}
