package rbolt_test

import (
	"bytes"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/vincent-petithory/rbolt"
)

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
