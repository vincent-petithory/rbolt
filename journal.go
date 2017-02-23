package rbolt

import (
	"bytes"
	"sort"
	"sync"

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

func (t JournalType) String() string {
	switch t {
	case JournalTypeUpdate:
		return "update"
	case JournalTypeCommit:
		return "commit"
	case JournalTypeRollback:
		return "rollback"
	default:
		return ""
	}
}

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

type JournalBuffer struct {
	l        sync.Mutex
	updates  map[int]*Journal // txID => Journal
	journals []*Journal       // Journal commits or rollbacks sorted by LSN

	db *bolt.DB
}

type byLSN []*Journal

func (js byLSN) Len() int           { return len(js) }
func (js byLSN) Swap(i, j int)      { js[i], js[j] = js[j], js[i] }
func (js byLSN) Less(i, j int) bool { return js[i].LSN < js[j].LSN }

func NewJournalBuffer(db *bolt.DB) *JournalBuffer {
	return &JournalBuffer{updates: make(map[int]*Journal), db: db}
}

func (jb *JournalBuffer) Flush() error {
	jb.l.Lock()
	defer jb.l.Unlock()

	sort.Sort(byLSN(jb.journals))
	var journals []*Journal
	defer func() {
		jb.journals = journals
	}()

	for i, j := range jb.journals {
		txj, ok := jb.updates[j.TxID]
		if !ok {
			// we may have got the commit/rollback journal before the update journal
			// we must interrupt now to not mix the order journals are played
			journals = append(journals, jb.journals[i:]...)
			return nil
		}
		switch j.Type {
		case JournalTypeCommit:
			if ok {
				if err := jb.db.Update(func(tx *bolt.Tx) error { return txj.Play(tx) }); err != nil {
					// keep this journal and the remaining ones to retry later
					journals = append(journals, jb.journals[i:]...)
					return err
				}
				delete(jb.updates, j.TxID)
			}
		case JournalTypeRollback:
			delete(jb.updates, j.TxID)
		}
	}
	return nil
}

func (jb *JournalBuffer) WriteJournal(j *Journal) error {
	jb.l.Lock()
	defer jb.l.Unlock()
	switch j.Type {
	case JournalTypeUpdate:
		jb.updates[j.TxID] = j
	case JournalTypeCommit, JournalTypeRollback:
		jb.journals = append(jb.journals, j)
	}
	return nil
}

func Update(db *bolt.DB, transport Transport, lsn LSNGenerator, fn func(tx *Tx) error) error {
	var txID int
	if err := db.Update(func(tx *bolt.Tx) (err error) {
		rtx := RTx(tx)
		txID = rtx.ID()
		tx.OnCommit(func() {
			commitJournal := &Journal{
				TxID: txID,
				Type: JournalTypeCommit,
			}
			transport.Send(commitJournal, lsn.NextLSN())
		})
		defer func() {
			transport.Send(rtx.Journal(), lsn.NextLSN())
		}()
		return fn(rtx)
	}); err != nil {
		rollbackJournal := &Journal{
			TxID: txID,
			Type: JournalTypeRollback,
		}
		transport.Send(rollbackJournal, lsn.NextLSN())
		return err
	}
	return nil
}

type Transport interface {
	Send(*Journal, int)
}

type LocalTransport struct {
	JournalBuffer *JournalBuffer
}

func (t *LocalTransport) Send(j *Journal, lsn int) {
	j.LSN = lsn
	t.JournalBuffer.WriteJournal(j)
}

type LSNGenerator interface {
	NextLSN() int
}

type MonotonicLSN struct {
	l   sync.Mutex
	lsn int
}

func (l *MonotonicLSN) NextLSN() int {
	l.l.Lock()
	defer l.l.Unlock()
	l.lsn++
	return l.lsn
}
