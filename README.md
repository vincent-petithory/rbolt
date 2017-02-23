RBolt
=====

RBolt is a package for replicating [boltdb](https://github.com/boltdb/bolt>) databases.

Journal
-------

It provides a transaction type which builds a transaction journal. This journal can be played in another transaction to replicate the changes performed during the original one.
This is the basic building block for replication.

To use it, call the `rbolt.RTx()` func in a writeable transaction:

```go
  err := db.Update(func(tx *bolt.Tx) error {
      rtx := rbolt.RTx(tx)
      // ... use rtx
  })
  ...
```

Transport and JournalBuffer
--------------------------

The `Transport` interface is the sender side. It forwards the journals somewhere.
The `Update()` function calls `boltdb.DB.Update()`, creates a journal update record, and either a commit or rollback journal record, and sends those two with the transport.
It is optional, use whatever system suits you to send the journal from `rbolt.RTx` elsewhere.

`JournalBuffer` acts on the receiver side. It buffers the journals it receives.
`JournalBuffer.Flush()` plays the journals and empties the buffer.
It ensures the journals are played in the correct order (not the order they are received, as transports may mix it) when a monotonic LSN is used. Use `rbolt.MonotonicLSN` for this.

Example with the `LocalTransport`, for replication in the same go program:

```go
  // db is the "master" db.
  // dbt is the target bolt.DB to synchronize
  jbuf := rbolt.NewJournalBuffer(dbt.DB)
  transport := &rbolt.LocalTransport{JournalBuffer: jbuf}
  lsn := new(rbolt.MonotonicLSN)

  if err := rbolt.DBUpdate(db, transport, lsn, func(tx *rbolt.Tx) error {
      // ... use tx
  }); err != nil {
      ...
  }

  if err := jbuf.Flush(); err != nil {
      ...
  }

```

Performance
-----------

The overhead is when recording what happens during a writeable transaction. Read-only transactions are not affected.
There is a cost in memory since key/values written in the journal are copies (byte-slices are valid only during the transaction):

```
  BenchmarkRTx-8   	    2000	   6208192 ns/op	 1256913 B/op	   19532 allocs/op
  BenchmarkTx-8    	    2000	   5982453 ns/op	  997624 B/op	   16509 allocs/op
```

and that cost will depend on the size of the keys/values of your database.

Status
------

Things are working and only ask for more thorough use to ensure it's solid.
However the package is still very young, so the API may still change in minor ways. Also, `Transport` implementations could be good to have to give more ready-to-use options.

People who want to test it and provide feedback are very welcome!
