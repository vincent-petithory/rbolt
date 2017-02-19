package rbolt

import "github.com/boltdb/bolt"

type Transport interface {
	Send(*Journal) error
	Recv(*bolt.DB, chan<- Ack)
}

type Ack struct {
	Err     error
	Journal *Journal
}

type NullTransport struct{}

func (t NullTransport) Send(j *Journal) error { return nil }
func (t NullTransport) Recv(db *bolt.DB, ackC chan<- Ack) {
	if ackC != nil {
		close(ackC)
	}
}

type ChanTransport struct {
	c chan *Journal
}

func NewChanTransport() *ChanTransport {
	return &ChanTransport{c: make(chan *Journal)}
}

func (t *ChanTransport) Send(j *Journal) error {
	t.c <- j
	return nil
}

func (t *ChanTransport) Recv(db *bolt.DB, ackC chan<- Ack) {
	for j := range t.c {
		err := db.Update(func(tx *bolt.Tx) error {
			return j.Play(tx)
		})
		ack := Ack{
			Err:     err,
			Journal: j,
		}
		// Don't block on ack
		select {
		case ackC <- ack:
		default:
		}
	}
}
