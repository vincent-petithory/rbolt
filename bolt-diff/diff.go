package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/boltdb/bolt"
)

type pair struct {
	Ks [][]byte
	V  []byte
}

func (p *pair) IsBucket() bool {
	return p.V == nil
}

func (p *pair) KString() string {
	var buf bytes.Buffer
	fmt.Fprint(&buf, "/")
	for i, k := range p.Ks {
		fmt.Fprintf(&buf, "%+q", k)
		if i < len(p.Ks)-1 {
			fmt.Fprint(&buf, "/")
		}
	}
	return buf.String()
}

func (p *pair) String() string {
	if p.IsBucket() {
		return p.KString()
	}
	h := sha1.New()
	_, _ = h.Write(p.V)
	v := h.Sum(nil)
	sv := hex.EncodeToString(v)
	return fmt.Sprintf("%s => sha1:%s len:%d", p.KString(), sv, len(p.V))
}

type diff struct {
	x    rune
	pair *pair
}

func (s diff) String() string { return fmt.Sprintf("%c %s", s.x, s.pair.String()) }

type byKey []diff

func (s byKey) Len() int      { return len(s) }
func (s byKey) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byKey) Less(i, j int) bool {
	p1 := s[i].pair.KString()
	p2 := s[j].pair.KString()
	if p1 != p2 {
		return p1 < p2
	}
	if s[i].x == '-' && s[j].x == '+' {
		return true
	}
	return false
}

func dbEqual(db1 *bolt.DB, db2 *bolt.DB) (bool, error) {
	tx1, err := db1.Begin(false)
	if err != nil {
		return false, err
	}
	defer tx1.Rollback()
	tx2, err := db2.Begin(false)
	if err != nil {
		return false, err
	}
	defer tx2.Rollback()

	// check removals
	c12 := make(chan *pair)
	var s12err error
	go func() {
		defer close(c12)
		s12err = sendPairs(tx1, c12)
	}()
	if s12err != nil {
		return false, s12err
	}
	ldiffs := checkPairs(tx2, c12, '-')
	if len(ldiffs) > 0 {
		return false, nil
	}

	// check additions
	c21 := make(chan *pair)
	var s21err error
	go func() {
		defer close(c21)
		s21err = sendPairs(tx2, c21)
	}()
	if s21err != nil {
		return false, s21err
	}
	adiffs := checkPairs(tx1, c21, '+')
	if len(adiffs) > 0 {
		return false, nil
	}
	return true, nil
}

func dbDiff(db1 *bolt.DB, db2 *bolt.DB) ([]diff, error) {
	tx1, err := db1.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx1.Rollback()
	tx2, err := db2.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx2.Rollback()

	// check removals
	c12 := make(chan *pair)
	var s12err error
	go func() {
		defer close(c12)
		s12err = sendPairs(tx1, c12)
	}()
	if s12err != nil {
		return nil, s12err
	}
	ldiffs := checkPairs(tx2, c12, '-')

	// check additions
	c21 := make(chan *pair)
	var s21err error
	go func() {
		defer close(c21)
		s21err = sendPairs(tx2, c21)
	}()
	if s21err != nil {
		return nil, s21err
	}
	adiffs := checkPairs(tx1, c21, '+')

	// merge, sort
	diffs := append(ldiffs, adiffs...)
	sort.Sort(byKey(diffs))
	return diffs, nil
}

func checkPairs(tx *bolt.Tx, c <-chan *pair, x rune) []diff {
	var diffs []diff
NextPair:
	for pair := range c {
		b := tx.Bucket(pair.Ks[0])
		if b == nil {
			diffs = append(diffs, diff{x: x, pair: pair})
			continue NextPair
		}
		if len(pair.Ks) > 1 {
			for _, k := range pair.Ks[1 : len(pair.Ks)-1] {
				b = b.Bucket(k)
				if b == nil {
					diffs = append(diffs, diff{x: x, pair: pair})
					continue NextPair
				}
			}
		}
		if !pair.IsBucket() {
			v := b.Get(pair.Ks[len(pair.Ks)-1])
			if !bytes.Equal(pair.V, v) {
				diffs = append(diffs, diff{x: x, pair: pair})
			}
		}
	}
	return diffs
}

func sendPairs(tx *bolt.Tx, c chan<- *pair) error {
	return tx.ForEach(func(k []byte, b *bolt.Bucket) error {
		c <- &pair{Ks: [][]byte{k}, V: nil}
		return sendBucketPairs(b, [][]byte{k}, c)
	})
}

func sendBucketPairs(b *bolt.Bucket, path [][]byte, c chan<- *pair) error {
	return b.ForEach(func(k []byte, v []byte) error {
		if v == nil {
			c <- &pair{Ks: append(path, k), V: nil}
			return sendBucketPairs(b.Bucket(k), append(path, k), c)
		}
		c <- &pair{Ks: append(path, k), V: v}
		return nil
	})
}
