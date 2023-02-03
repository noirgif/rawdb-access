// an application that uses the rawdb in go-ethereum to access its underlying database

package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"rawdb_access/rawdb"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	// chainFreezerHeaderTable indicates the name of the freezer header table.
	chainFreezerHeaderTable = "headers"

	// chainFreezerHashTable indicates the name of the freezer canonical hash table.
	chainFreezerHashTable = "hashes"

	// chainFreezerBodiesTable indicates the name of the freezer block body table.
	chainFreezerBodiesTable = "bodies"

	// chainFreezerReceiptTable indicates the name of the freezer receipts table.
	chainFreezerReceiptTable = "receipts"

	// chainFreezerDifficultyTable indicates the name of the freezer total difficulty table.
	chainFreezerDifficultyTable = "diffs"
)

// chainFreezerNoSnappy configures whether compression is disabled for the ancient-tables.
// Hashes and difficulties don't compress well.
var chainFreezerNoSnappy = map[string]bool{
	chainFreezerHeaderTable:     false,
	chainFreezerHashTable:       true,
	chainFreezerBodiesTable:     false,
	chainFreezerReceiptTable:    false,
	chainFreezerDifficultyTable: true,
}

// freezerTableSize defines the maximum size of freezer data files.
const freezerTableSize = 2 * 1000 * 1000 * 1000

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
	flag.StringVar(&value, "value", "", "the value to use, if not provided it will be read from stdin (only used in put(leveldb), or append(freeze)))")
	flag.BoolVar(&compact, "compact", false, "compact the database (only used in leveldb")

	flag.Parse()

	if databaseType == "freeze" {
		// find whether the table is snappy-enabled
		if command == "" || command == "append" {
			if _, ok := chainFreezerNoSnappy[tableName]; !ok {
				panic("unknown table name")
			}

			table, err := rawdb.NewFreezerTable(databasePath, tableName, chainFreezerNoSnappy[tableName], false)
			if err != nil {
				panic(err)
			}
			defer table.Close()

			batch := table.NewBatch()
			if value == "" {
				_, err := fmt.Scanf("%s", &value)
				if err != nil {
					panic(err)
				}
			}
			encItem, err := hex.DecodeString(value)
			if err != nil {
				panic(err)
			} else {
				batch.AppendItem(encItem)
			}
			batch.Commit()
			table.Sync()
		} else if command == "repair" {
			// will be automatically truncated if created with readonly=false
			freezer, err := rawdb.NewFreezer(databasePath, "", false, freezerTableSize, chainFreezerNoSnappy)
			if err != nil {
				panic(err)
			}
			defer freezer.Close()
			if err := freezer.Sync(); err != nil {
				panic(err)
			}
		}
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
			if value == "" {
				_, err := fmt.Scanf("%s", &value)
				if err != nil {
					panic(err)
				}
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
