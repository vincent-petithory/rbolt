package rbolt

import "github.com/boltdb/bolt"

type Transport interface {
	Send(*Journal, int)
	Recv(*bolt.DB, chan<- Ack)
}

type Ack struct {
	Err     error
	Journal *Journal
}

type NullTransport struct{}

func (t NullTransport) Send(j *Journal, lsn int) {
	j.LSN = lsn
}
func (t NullTransport) Recv(db *bolt.DB, ackC chan<- Ack) {
	if ackC != nil {
		close(ackC)
	}
}

type ChanTransport struct {
	c        chan *Journal
	journals map[int]*Journal
}

func NewChanTransport() *ChanTransport {
	return &ChanTransport{c: make(chan *Journal), journals: make(map[int]*Journal)}
}

func (t *ChanTransport) Send(j *Journal, lsn int) {
	j.LSN = lsn
	t.c <- j
}

func (t *ChanTransport) Recv(db *bolt.DB, ackC chan<- Ack) {
	for j := range t.c {
		var err error
		switch j.Type {
		case JournalTypeUpdate:
			t.journals[j.TxID] = j
		case JournalTypeCommit:
			txj, ok := t.journals[j.TxID]
			if ok {
				err = db.Update(func(tx *bolt.Tx) error { return txj.Play(tx) })
				if err == nil {
					delete(t.journals, j.TxID)
				}
			}
		case JournalTypeRollback:
			delete(t.journals, j.TxID)
		}
		ack := Ack{
			Err:     err,
			Journal: j,
		}
		ackC <- ack
	}
}

func DBUpdate(db *bolt.DB, transport Transport, lsn func() int, fn func(tx *Tx) error) error {
	var txID int
	if err := db.Update(func(tx *bolt.Tx) (err error) {
		rtx := RTx(tx)
		txID = rtx.ID()
		tx.OnCommit(func() {
			commitJournal := &Journal{
				TxID: txID,
				Type: JournalTypeCommit,
			}
			transport.Send(commitJournal, lsn())
		})
		defer func() {
			transport.Send(rtx.Journal(), lsn())
		}()
		return fn(rtx)
	}); err != nil {
		rollbackJournal := &Journal{
			TxID: txID,
			Type: JournalTypeRollback,
		}
		transport.Send(rollbackJournal, lsn())
		return err
	}
	return nil
}
