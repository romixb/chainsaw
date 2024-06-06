package db

import (
	"chainsaw/btcjson"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
	"sync"
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
	ID            int64  `json:"id" db:"column:id"`
	Hash          string `json:"hash" db:"column:hash"`
	Height        int64  `json:"height" db:"column:height"`
	Time          int    `json:"time" db:"column:time"`
	Previousblock string `json:"previousblock" db:"column:previousblock"`
	Nextblock     string `json:"nextblock" db:"column:nextblock"`
	processed     bool
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
func (d *Data) GetLastBlock(ctx context.Context) (*Blocks, error) {

	b := Blocks{}

	stmt := "SELECT * FROM blocks WHERE height=(SELECT MAX(height) FROM blocks)"

	err := d.DB.QueryRowContext(ctx, stmt).Scan(b.ID, b.Hash, b.Height, b.Time, b.Previousblock, &b.Nextblock, b.processed)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, err
	case err != nil:
		log.Fatalf("query error: %v\n", err)
	default:
		log.Printf("block id=%d, height = %d", b.ID, b.Height)
	}

	return &b, err

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
	stmt := "WITH b as (SELECT * FROM block_txs WHERE block_id = 1) SELECT id FROM b JOIN txs ON b.tx_id = txs.id WHERE id=(SELECT MAX(n) FROM b)"
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
func (d *Data) GetTxsInBlock(ctx context.Context, block int64) int64 {
	var qty int64
	stmt := "WITH b(tx_id) as (SELECT * FROM block_txs WHERE block_id = ?) SELECT COUNT(*) FROM b JOIN txs ON block_txs.tx.id=txs.id"
	err := d.DB.QueryRowContext(ctx, stmt, block).Scan(&qty)
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
func (d *Data) InsertTx(ctx context.Context, trx btcjson.TxRawResult, blockId int64) (err error) {

	// Create a helper function for preparing failure results.
	fail := func(err error) error {
		return fmt.Errorf("CreateOrder: %v", err)
	}
	// Get a Tx for making transaction requests.
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return fail(err)
	}
	// Defer a rollback in case anything fails.
	defer tx.Rollback()

	//txId and hash are equal always?
	result, err := tx.ExecContext(ctx, "INSERT INTO txs (hash) VALUES (?)", trx.Txid)
	if err != nil {
		return fail(err)
	}

	txid, err := result.LastInsertId()
	if err != nil {
		return fail(err)
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO block_tx (block_id, tx_id) VALUES (?, ?, ?, ?)", blockId, txid)
	if err != nil {
		return fail(err)
	}

	for _, in := range trx.Vin {
		ptxid, err := tx.ExecContext(ctx, "SELECT id FROM txs WHERE hash=?", in.Txid)
		if err != nil {
			return fail(err)
		}

		paddr, err := GetOrInsertAddr(ctx, tx, in.PrevOut.Addresses[0])

		_, err = tx.ExecContext(ctx, "INSERT INTO txins (tx_id, prevout_tx_id, prevout_n, value, prev_address) VALUES (?, ?, ?, ?, ?, ?)", txid, ptxid, in.Vout, in.PrevOut.Value, paddr)
		if err != nil {
			return fail(err)
		}

	}

	for _, out := range trx.Vout {

		addrId, err := GetOrInsertAddr(ctx, tx, out.ScriptPubKey.Address)

		_, err = tx.ExecContext(ctx, "INSERT INTO txins (tx_id, n, value, prev_address) VALUES (?, ?, ?, ?)", txid, out.N, out.Value, addrId)
		if err != nil {
			return fail(err)
		}

	}

	// Commit the transaction.
	if err = tx.Commit(); err != nil {
		return fail(err)
	}

	//TODO: finalize Block process, make processed true

	return err
}
func GetOrInsertAddr(ctx context.Context, tx *sql.Tx, hash string) (int64, error) {

	var addrId int64
	err := tx.QueryRowContext(ctx, "SELECT id FROM addr WHERE hash=?", hash).Scan(&addrId)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		res, err := tx.ExecContext(ctx, "INSERT INTO addrs (tx_id, prevout_tx_id) VALUES(?)", hash)
		if err != nil {
			log.Fatalf("query error: %v\n", err)
		}
		addrId, err = res.LastInsertId()
	case err != nil:
		addrId = -1
	}

	return addrId, err
}
func createTables(db *sqlx.DB) error {
	sqlTables := `
  CREATE TABLE IF NOT EXISTS blocks (
	id bigserial PRIMARY KEY,
	hash varchar(255),
	height integer,
	time integer,
	previousblock varchar(255),
	nextblock varchar(255),
    processed boolean
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
   prevout_tx_id bigint,   -- can be NULL for coinbase
   prevout_n     smallint NOT NULL,
   value         bigint,
   n			 int
   prev_address  bigint REFERENCES addrs(id)
  );

  CREATE TABLE IF NOT EXISTS txouts (
  tx_id            bigint REFERENCES txs(id) ,
  n               int NOT NULL,
  value           bigint,
  address         bigint REFERENCES addrs(id)
  );
`
	_, err := db.Exec(sqlTables)
	return err
}
