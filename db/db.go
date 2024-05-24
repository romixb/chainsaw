package db

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"sync"

	_ "github.com/lib/pq"
)

type Data struct {
	DB      *sql.DB
	mux     sync.Mutex
	initRun bool
}

type product struct {
	id      int
	model   string
	company string
	price   int
}

type Blocks struct {
	ID            int    `json:"id" gorm:"column:id"`
	Hash          string `json:"hash" gorm:"column:hash"`
	Height        int    `json:"height" gorm:"column:height"`
	Time          int    `json:"time" gorm:"column:time"`
	Previousblock string `json:"previousblock" gorm:"column:previousblock"`
	Nextblock     string `json:"nextblock" gorm:"column:nextblock"`
}

func (d *Data) StartDb(connectionString string) (*Data, error) {

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}
	d.DB = db
	err = createTables(d.DB)
	if err != nil {
		log.Fatal(err)
	}

	return d, nil
}
func (d *Data) GetLastHeight(ctx context.Context) int {
	var height int

	stmt := "SELECT id FROM blocks WHERE height=(SELECT MAX(height) FROM blocks)"

	err := d.DB.QueryRowContext(ctx, stmt).Scan(&height)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0
	case err != nil:
		log.Fatalf("query error: %v\n", err)
	default:
		log.Printf("height is %d", height)
	}
	// err := d.DB.QueryRowContext(ctx, stmt).Scan(&height); err != nil {
	//	if errors.Is(err, sql.ErrNoRows) {
	//		return 0
	//	}
	//}
	return height
}
func (d *Data) GetLastTx(ctx context.Context, lb int) interface{} {
	var ltx string

	stmt := "WITH bt AS(SELECT FROM blocks WHERE block_id = ?), "

	err := d.DB.QueryRowContext(ctx, stmt, lb).Scan(&ltx)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0
	case err != nil:
		log.Fatalf("query error: %v\n", err)
	default:
		log.Printf("height is %d", height)
	}
	// err := d.DB.QueryRowContext(ctx, stmt).Scan(&height); err != nil {
	//	if errors.Is(err, sql.ErrNoRows) {
	//		return 0
	//	}
	//}
	return height
}
func createTables(db *sql.DB) error {
	sqlTables := `
  CREATE TABLE IF NOT EXISTS blocks (
	id bigserial PRIMARY KEY,
	hash varchar(255),
	height integer,
	time integer,
	previousblock varchar(255),
	nextblock varchar(255)
  );

  CREATE TABLE IF NOT EXISTS txs (
   id            bigserial PRIMARY KEY,
   hash          varchar(255) NOT NULL    
  );

  CREATE TABLE IF NOT EXISTS block_txs (   
   block_id      bigint REFERENCES blocks(id),
   tx_id         bigint REFERENCES txs(id)
  );

  CREATE TABLE IF NOT EXISTS addrs (
   id            bigserial PRIMARY KEY,
   hash 		 varchar(255)
  );

  CREATE TABLE IF NOT EXISTS txins (
   id            bigserial PRIMARY KEY,   
   tx_id         bigint REFERENCES txs(id) ,
   prevout_tx_id bigint REFERENCES txs(id),   -- can be NULL for coinbase
   prevout_n     smallint NOT NULL,
   value         bigint,
   prev_address  bigint REFERENCES addrs(id)
  );

  CREATE TABLE IF NOT EXISTS txouts (
  tx_id        bigint NOT NULL,
  n            smallint NOT NULL,
  value         bigint,
  address       bigint REFERENCES addrs(id)
  );
`
	_, err := db.Exec(sqlTables)
	return err
}
