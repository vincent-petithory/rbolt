// Command bolt-diff compares two bolt databases and reports if they are equal or not.
// A diff can be optionnaly printed. Keys are printed using fmt's %+q and values are sha1-ed
// for clarity.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
)

var showDiff bool

func init() {
	flag.BoolVar(&showDiff, "d", false, "show diff")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [OPTIONS] db1 db2\n\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	flag.Parse()
}

func main() {
	log.SetFlags(0)
	var exitCode int
	defer func() {
		os.Exit(exitCode)
	}()

	if flag.NArg() != 2 {
		flag.Usage()
		log.Printf("expected 2 args, got %d", flag.NArg())
		exitCode = 1
		return
	}

	var dbs [2]*bolt.DB
	for i, dbpath := range []string{flag.Arg(0), flag.Arg(1)} {
		if fi, err := os.Stat(dbpath); err != nil {
			log.Println(err)
			exitCode = 1
			return
		} else if fi.IsDir() {
			log.Printf("%s: not a regular file", dbpath)
			exitCode = 1
			return
		}
		db, err := bolt.Open(dbpath, 0444, &bolt.Options{ReadOnly: true})
		if err != nil {
			log.Println(err)
			exitCode = 1
			return
		}
		defer func() {
			_ = db.Close()
		}()
		dbs[i] = db
	}

	if showDiff {
		diffs, err := dbDiff(dbs[0], dbs[1])
		if err != nil {
			log.Println(err)
			exitCode = 1
			return
		}
		if len(diffs) > 0 {
			exitCode = 2
			for _, diff := range diffs {
				fmt.Println(diff.String())
			}
		}
	} else {
		ok, err := dbEqual(dbs[0], dbs[1])
		if err != nil {
			log.Println(err)
			exitCode = 1
			return
		}
		if !ok {
			exitCode = 2
		}
	}
}
