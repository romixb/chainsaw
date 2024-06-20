package db

import (
	"chainsaw/btcjson"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/jmoiron/sqlx"
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
	ID            int64  `json:"id" db:"column:id"`
	Hash          string `json:"hash" db:"column:hash"`
	Height        int64  `json:"height" db:"column:height"`
	Time          int    `json:"time" db:"column:time"`
	Previousblock string `json:"previousblock" db:"column:previousblock"`
	Nextblock     string `json:"nextblock" db:"column:nextblock"`
	Processed     bool   `json:"processed" db:"column:processed"`
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
func (d *Data) GetLastProcessedBlock(ctx context.Context) *Blocks {

	b := Blocks{}

	stmt := "SELECT * FROM blocks WHERE height=(SELECT MAX(height) FROM blocks)"

	err := d.DB.QueryRowContext(ctx, stmt).Scan(&b.ID, &b.Hash, &b.Height, &b.Time, &b.Previousblock, &b.Nextblock, &b.Processed)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		log.Fatalf("query error: %v\n", err)
	}
	log.Printf("block id=%d, height = %d", b.ID, b.Height)
	return &b

}

func (d *Data) GetLastProcessedBlockHash(ctx context.Context) string {

	var h string

	stmt := "SELECT * FROM blocks WHERE height=(SELECT MAX(height) FROM blocks WHERE processed=true)"

	err := d.DB.QueryRowContext(ctx, stmt).Scan(&h)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return ""
	case err != nil:
		log.Fatalf("query error: %v\n", err)
	}
	log.Printf("found block, hash = %s", h)
	return h

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
	stmt := "WITH b as (SELECT * FROM block_txs WHERE block_id = $1) SELECT id FROM b JOIN txs ON b.tx_id = txs.id WHERE id=(SELECT MAX(n) FROM b)"
	err := d.DB.QueryRowContext(ctx, stmt, block).Scan(&n)
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
func (d *Data) InsertBlock(ctx context.Context, hash string, height int64, time int64, prevblock string, nextblock string, processed bool) (b *Blocks, err error) {
	stmt := "INSERT INTO blocks VALUES (default, $1, $2, $3, $4, $5, $6) RETURNING *"

	bl := Blocks{}
	b = &bl
	err = d.DB.QueryRowContext(ctx, stmt, hash, height, time, prevblock, nextblock, processed).Scan(&b.ID, &b.Hash, &b.Height, &b.Time, &b.Previousblock, &b.Nextblock, &b.Processed)
	if err != nil {
		log.Fatalf("query error: %v\n", err)
	}
	return b, err
}
func (d *Data) InsertTx(ctx context.Context, trx btcjson.TxRawResult, blockId int64, n int32) (err error) {

	// Create a helper function for preparing failure results.
	fail := func(err error) error {
		return fmt.Errorf("query error: %v", err)
	}
	// Get a Tx for making transaction requests.
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return fail(err)
	}
	// Defer a rollback in case anything fails.
	//defer tx.Rollback()

	var txid int64
	//txId and hash are equal always?
	log.Print("inserting txs")
	err = tx.QueryRowContext(ctx, "INSERT INTO txs VALUES (default, $1) RETURNING id", trx.Txid).Scan(&txid)
	if err != nil {
		return fail(err)
	}

	log.Print("inserting block_txs")
	_, err = tx.ExecContext(ctx, "INSERT INTO block_txs VALUES ($1, $2, $3)", blockId, txid, n)
	if err != nil {
		return fail(err)
	}

	log.Print("inserting Vins")
	for i, in := range trx.Vin {
		if i == 0 && in.Coinbase != "" {
			_, err = tx.ExecContext(ctx, "INSERT INTO txins VALUES (default, $1, null, null, null, null ,null, true)", txid)
			if err != nil {
				return fail(err)
			}
			continue
		}
		var ptxid int64
		err := tx.QueryRowContext(ctx, "SELECT id FROM txs WHERE hash=$1", in.Txid).Scan(&ptxid)
		if err != nil {
			return fail(err)
		}

		paddr, err := GetOrInsertAddr(ctx, tx, in.PrevOut.Addresses[0])
		if err != nil {
			return fail(err)
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO txins VALUES (default, $1, $2, $3, $4, $5, $6, $7)", txid, ptxid, in.Vout, in.PrevOut.Value, i, paddr, false)
		if err != nil {
			return fail(err)
		}

	}

	for _, out := range trx.Vout {

		if out.ScriptPubKey.Address == "" {
			_, err = tx.ExecContext(ctx, "INSERT INTO txins VALUES (default, $1, $2, $3, null)", txid, out.N, out.Value)
			if err != nil {
				return fail(err)
			}

			continue
		}

		addrId, err := GetOrInsertAddr(ctx, tx, out.ScriptPubKey.Address)
		if err != nil {
			return fail(err)
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO txins VALUES (default, $1, $2, $3, $4)", txid, out.N, out.Value, addrId)
		if err != nil {
			return fail(err)
		}

	}
	// Commit the transaction.
	if err = tx.Commit(); err != nil {
		return fail(err)
	}

	return err
}
func (d *Data) MarkBlockAsProcessed(ctx context.Context, id int64) (*Blocks, error) {

	b := Blocks{}
	stmt := "UPDATE blocks SET processed = true WHERE id=$1 RETURNING *"

	err := d.DB.QueryRowContext(ctx, stmt, id).Scan(&b.ID, &b.Hash, &b.Height, &b.Time, &b.Previousblock, &b.Nextblock, &b.Processed)

	return &b, err
}
func GetOrInsertAddr(ctx context.Context, tx *sql.Tx, hash string) (int64, error) {

	var addrId int64
	err := tx.QueryRowContext(ctx, "SELECT id FROM addr WHERE hash=$1", hash).Scan(&addrId)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		err := tx.QueryRowContext(ctx, "INSERT INTO addrs VALUES(default, $1) RETURNING id", hash).Scan(&addrId)
		if err != nil {
			log.Fatalf("query error: %v\n", err)
		}

	case err != nil:
		return -1, err
	}

	return addrId, err
}
func createTables(db *sqlx.DB) error {
	sqlTables := `
  CREATE TABLE IF NOT EXISTS blocks (
	id           bigserial PRIMARY KEY,
	hash         varchar(255),
	height       integer,
	time         integer,
	previousblock varchar(255),
	nextblock    varchar(255),
    processed    boolean
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
   tx_id         bigint REFERENCES txs(id),
   prevout_tx_id bigint,   
   prevout_n     int,
   value         bigint,
   n			 int,
   prev_address  bigint REFERENCES addrs(id),
   coinbase      boolean
  );

  CREATE TABLE IF NOT EXISTS txouts (
  id             bigserial PRIMARY KEY,   
  tx_id          bigint REFERENCES txs(id),
  n              int NOT NULL,
  value          bigint,
  address        bigint REFERENCES addrs(id)
  );
`
	_, err := db.Exec(sqlTables)
	return err
}
