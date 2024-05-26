package db

import (
	"context"
	"database/sql"
	"errors"
	"github.com/jmoiron/sqlx"
	"log"
	"sync"

	_ "github.com/lib/pq"
)

type Data struct {
	DB      *sqlx.DB
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
	ID            int    `json:"id" db:"column:id"`
	Hash          string `json:"hash" db:"column:hash"`
	Height        int    `json:"height" db:"column:height"`
	Time          int    `json:"time" db:"column:time"`
	Previousblock string `json:"previousblock" db:"column:previousblock"`
	Nextblock     string `json:"nextblock" db:"column:nextblock"`
}
type Tx struct {
	ID   int    `json:"id" db:"column:id"`
	Hash string `json:"hash" db:"column:hash"`
}

func (d *Data) StartDb(connectionString string) (*Data, error) {

	db, err := sqlx.Connect("postgres", connectionString)
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
func (d *Data) GetLastBlockId(ctx context.Context) (int64, int64) {
	var id int64
	var height int64

	stmt := "SELECT id, height FROM blocks WHERE height=(SELECT MAX(height) FROM blocks)"

	err := d.DB.QueryRowContext(ctx, stmt).Scan(&id, &height)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		log.Fatalf("No result")
	case err != nil:
		log.Fatalf("query error: %v\n", err)
	default:
		log.Printf("block id=%d, height = %d", id, height)
	}
	return id, height
}
func (d *Data) getTxQtyInBlock(ctx context.Context, block int64) int64 {
	var qty int64
	stmt := "WITH b(tx_id) as (SELECT * FROM block_txs WHERE block_id = ?) SELECT COUNT(*) FROM b JOIN txs ON block_txs.tx_id=txs.id"
	err := d.DB.QueryRowContext(ctx, stmt).Scan(&qty)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return -1
	case err != nil:
		log.Fatalf("query error: %v\n", err)
	default:
		log.Printf("block %d txs amount is %d", block, qty)
	}
	return qty
}
func (d *Data) GetLastProcessedTxFromBlock(ctx context.Context, block int64) int {
	var n int
	stmt := "WITH b(tx_id) as (SELECT * FROM block_txs WHERE block_id = ?) SELECT MAX(n) FROM b JOIN txs ON block_txs.tx_id=txs.id"
	err := d.DB.QueryRowContext(ctx, stmt).Scan(&n)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return -1
	case err != nil:
		log.Fatalf("query error: %v\n", err)
	default:
		log.Printf("block %d txs amount is %d", block, n)
	}
	return n
}

func (d *Data) GetTxsInBlock(ctx context.Context, lastblock int64) int64 {
	var qty int64
	stmt := "WITH b(tx_id) as (SELECT * FROM block_txs WHERE block_id = ?) SELECT COUNT(*) FROM b JOIN txs ON block_txs.tx.id=txs.id"
	err := d.DB.QueryRowContext(ctx, stmt).Scan(&qty)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return -1
	case err != nil:
		log.Fatalf("query error: %v\n", err)
	default:
		log.Printf("block %d txs amount is %d", lastblock, qty)
	}
	return qty
}
func createTables(db *sqlx.DB) error {
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
   tx_id         bigint REFERENCES txs(id),
   n             int 
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
