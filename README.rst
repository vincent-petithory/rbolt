RBolt
=====

RBolt is a package for replicating `boltdb <https://github.com/boltdb/bolt>`_ databases.

Journal
-------

It provides a transaction type which builds a transaction journal. This journal can be played in another transaction to replicate the changes performed during the original transaction.
This is the basic building block for building replication.


To use it, call the `rbolt.RTx()` func in a writeable transaction::

  err := db.Update(func(tx *bolt.Tx) error {
      rtx := rbolt.RTx(tx)
      // ... use rtx
  })
  
  ...

Transport
---------

RBolt also provides a `Transport` interface and several implementations to forward the journal to another DB. It is optional, use whatever system suits you to play the journal elsewhere.
Example with the `ChanTransport`, for replication in the same go program::


  ackC := make(chan rbolt.Ack, 1)
  transport := rbolt.NewChanTransport()
  go transport.Recv(dbt.DB, ackC)

  err := db.Update(func(tx *bolt.Tx) error {
      rtx := rbolt.RTx(tx)
      tx.OnCommit(func() {
          if err := transport.Send(rtx.Journal()); err != nil {
              tb.Error(err)
          }
      })
      // ... use rtx
  })

  ...


Performance
-----------

The overhead is when recording what happens during a writeable transaction.
There is a cost in memory since key/values written in the journal are copies (byte-slices are valid only during the transaction)::

  BenchmarkRTx-8   	    5000	   4028813 ns/op	 1256485 B/op	   19522 allocs/op
  BenchmarkTx-8    	    5000	   3576751 ns/op	  997499 B/op	   16505 allocs/op

and that cost will depend on the size of the keys/values of your database.
