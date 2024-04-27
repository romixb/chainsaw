package db

import (
	"database/sql"

	_ "github.com/lib/pq"
)

const (
	createTableBlocks = `
		CREATE TABLE IF NOT EXISTS blocks (
			id INTEGER NOT NULL PRIMARY KEY,
			hash VARCHAR(255),
			height INTEGER,
			merkleroot VARCHAR(255),
			time INTEGER,
			previousblock VARCHAR(255),
			nextblock VARCHAR(255)
		)
	`

	createTableTransaction = `
		CREATE TABLE IF NOT EXISTS transactions (
			id INTEGER NOT NULL PRIMARY KEY,
			block_id INTEGER, 
			FOREIGN KEY (block_id) REFERENCES blocks (id),
			hash VARCHAR(255),
			ins varchar[],
			out varchar[],
			value INTEGER,
			relayedBy VARCHAR(255)
		)
	`

	createTableUtils = `
		CREATE TABLE IF NOT EXISTS utils (
			last_harvested_block INTEGER
)
`
)

func StartDb(connectionString string) (*sql.DB, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createTableBlocks)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createTableTransaction)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createTableUtils)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// func getLastBlock(db *sql.DB) (Row, err error) {
// 	return db.QueryRow("SELECT last_harvested_block FROM utils FETCH FIRST")
// }
