// an application that uses the rawdb in go-ethereum to access its underlying database

package main

import (
	"encoding/hex"
	"flag"
	"rawdb_access/rawdb"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func main() {
	// read the command from the arguments
	var (
		databaseType string
		databasePath string
		command      string
		key          string
		value        string
		tableName    string
		compact      bool
	)

	flag.StringVar(&databaseType, "databaseType", "leveldb", "freeze/leveldb")
	flag.StringVar(&databasePath, "databasePath", "", "the path to the database")
	flag.StringVar(&tableName, "tableName", "", "the name of the table (only used in ancient)")
	flag.StringVar(&command, "command", "", "the command to execute (only used in leveldb)")
	flag.StringVar(&key, "key", "", "the key to use (only used in leveldb)")
	flag.StringVar(&value, "value", "", "the value to use")
	flag.BoolVar(&compact, "compact", false, "compact the database (only used in leveldb")

	flag.Parse()

	if databaseType == "freeze" {
		table, err := rawdb.NewFreezerTable(databasePath, tableName, false, false)
		if err != nil {
			panic(err)
		}
		defer table.Close()

		batch := table.NewBatch()
		encItem, err := hex.DecodeString(value)
		if err != nil {
			panic(err)
		} else {
			batch.AppendItem(encItem)
		}
		batch.Commit()

	} else if databaseType == "leveldb" {
		db, err := leveldb.OpenFile(databasePath, nil)
		if err != nil {
			panic(err)
		}
		defer db.Close()

		if command == "put" {
			encKey, err := hex.DecodeString(key)
			if err != nil {
				panic(err)
			}
			encValue, err := hex.DecodeString(value)
			if err != nil {
				panic(err)
			}
			db.Put(encKey, encValue, nil)
		} else if command == "delete" {
			encKey, err := hex.DecodeString(key)
			if err != nil {
				panic(err)
			}
			db.Delete(encKey, nil)
		}

		if compact {
			db.CompactRange(util.Range{})
		}
	}
}
