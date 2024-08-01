package db

import (
	"chainsaw/btcjson"
	"chainsaw/utils"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"log"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type Data struct {
	DB      *sqlx.DB
	mux     sync.Mutex
	initRun bool
}
type Blocks struct {
	ID            int64  `json:"id" db:"column:id"`
	Hash          string `json:"hash" db:"column:hash"`
	Height        int64  `json:"height" db:"column:height"`
	Time          int    `json:"time" db:"column:time"`
	Previousblock string `json:"previousblock" db:"column:previousblock"`
	Nextblock     string `json:"nextblock" db:"column:nextblock"`
	Processed     bool   `json:"processed" db:"column:processed"`
	Duration      int    `json:"duration" db:"column:ptime"`
}
type Tx struct {
	ID   int    `json:"id" db:"column:id"`
	Hash string `json:"hash" db:"column:hash"`
}

func (d *Data) StartDb(connectionString string) (*Data, error) {

	db, err := sqlx.Connect("postgres", connectionString)
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(time.Hour)
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

	err := d.DB.QueryRowContext(ctx, stmt).Scan(&b.ID, &b.Hash, &b.Height, &b.Time, &b.Nextblock, &b.Processed, &b.Duration)
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

func (d *Data) GetProcessedTxIndicesFromBlock(ctx context.Context, block int64) ([]int, error) {
	var indices []int
	stmt := "SELECT n FROM block_txs WHERE block_id=$1 ORDER BY n"
	rows, err := d.DB.QueryContext(ctx, stmt, block)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, err
	case err != nil:
		log.Fatalf("query error: %v\n", err)
	}

	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		indices = append(indices, n)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	log.Printf("block %d txs: %v", block, indices)

	return indices, nil
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
func (d *Data) InsertBlock(ctx context.Context, hash string, height int64, time int64, nextblock string, processed bool) (b *Blocks, err error) {
	stmt := "INSERT INTO blocks VALUES (default, $1, $2, $3, $4, $5, $6) RETURNING *"

	bl := Blocks{}
	b = &bl
	err = d.DB.QueryRowContext(ctx, stmt, hash, height, time, nextblock, processed, 0).Scan(&b.ID, &b.Hash, &b.Height, &b.Time, &b.Nextblock, &b.Processed, &b.Duration)
	if err != nil {
		log.Fatalf("query error: %v\n", err)
	}
	return b, err
}
func (d *Data) InsertTx(ctx context.Context, trx btcjson.TxRawResult, blockId int64, n int32) (err error) {
	fail := func(err error) error {
		log.Printf("query error: \"%v\" on tx %s # %d in block %d", err, trx.Txid, n, blockId)
		return err
	}
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return fail(err)
	}
	defer tx.Rollback()
	log.Printf("Started #%d with tx %s", n, trx.Txid)

	var txid int64
	//txId and hash are equal always?
	err = tx.QueryRowContext(ctx, "INSERT INTO txs VALUES (default, $1) ON CONFLICT (hash) DO NOTHING RETURNING id", trx.Txid).Scan(&txid)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return utils.NewRetryableError(trx.Txid + " tx is already in process lets retry it later")
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return fail(err)
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO block_txs VALUES ($1, $2, $3)", blockId, txid, n)
	if err != nil {
		return fail(err)
	}

	for i, in := range trx.Vin {
		if in.Coinbase != "" {
			_, err = tx.ExecContext(ctx, "INSERT INTO txins VALUES (default, $1, null, null, null, null ,null, true)", txid)
			if err != nil {
				return fail(err)
			}
			continue
		}

		//in some cases the previous transaction is in the same block and is not yet processed in a concurrent goroutine
		// In case there is no result for prev txid lets send this tx index to a retry channel to try and process later
		var ptxid int64
		err := tx.QueryRowContext(ctx, "SELECT id FROM txs WHERE hash=$1", in.Txid).Scan(&ptxid)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return utils.NewRetryableError(in.Txid + " tx not registered yet, sending to retry")
		case err != nil && !errors.Is(err, sql.ErrNoRows):
			return fail(err)
		}

		h, err := hex.DecodeString(in.PrevOut.ScriptPubKey.Hex)
		if err != nil {
			return fail(err)
		}

		script, a, _, err := txscript.ExtractPkScriptAddrs(h, &chaincfg.MainNetParams)
		if err != nil {
			return fail(err)
		}

		var straddr string
		if in.PrevOut.Addresses == nil {
			if a != nil {
				straddr = a[0].EncodeAddress()
			}
		} else {
			straddr = in.PrevOut.Addresses[0]
		}

		var paddrid *int64
		paddrid, err = GetOrInsertAddr(ctx, tx, straddr)
		if err != nil {
			return fail(err)
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO txins VALUES (default, $1, $2, $3, $4, $5, $6, $7, $8)", txid, ptxid, in.Vout, in.PrevOut.Value, i, &paddrid, false, script)
		if err != nil {
			return fail(err)
		}

	}

	for _, out := range trx.Vout {
		h, err := hex.DecodeString(out.ScriptPubKey.Hex)
		if err != nil {
			return fail(err)
		}

		script, a, _, err := txscript.ExtractPkScriptAddrs(h, &chaincfg.MainNetParams)
		if err != nil {
			return fail(err)
		}

		var strAddr string
		strAddr = out.ScriptPubKey.Address
		if strAddr == "" {
			if a != nil && len(a) > 0 {
				strAddr = a[0].EncodeAddress()
			}
		}

		var addrId *int64
		addrId, err = GetOrInsertAddr(ctx, tx, strAddr)
		if err != nil {
			return fail(err)
		}

		_, err = tx.ExecContext(ctx, "INSERT INTO txouts VALUES (default, $1, $2, $3, $4, $5)", txid, out.N, out.Value, addrId, script)
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
func (d *Data) MarkBlockAsProcessed(ctx context.Context, id int64, duration time.Duration) (*Blocks, error) {

	b := Blocks{}
	stmt := "UPDATE blocks SET processed = true, ptime = $1 WHERE id=$2 RETURNING *"

	err := d.DB.QueryRowContext(ctx, stmt, duration.Milliseconds(), id).Scan(&b.ID, &b.Hash, &b.Height, &b.Time, &b.Nextblock, &b.Processed, &b.Duration)

	return &b, err
}
func GetOrInsertAddr(ctx context.Context, tx *sql.Tx, hash string) (*int64, error) {
	var addrId *int64
	//err := tx.QueryRowContext(ctx, "SELECT id FROM addrs WHERE hash=$1", hash).Scan(&addrId)
	err := tx.QueryRowContext(ctx, "WITH ins AS (INSERT INTO addrs VALUES (default, $1) ON CONFLICT (hash) DO NOTHING RETURNING id) SELECT id FROM ins UNION ALL SELECT id FROM addrs WHERE hash = $1 LIMIT 1;", hash).Scan(&addrId)

	if err != nil {
		return nil, utils.NewRetryableError("Failed to insert %s on conflict", hash)
	}
	return addrId, nil
}
func createTables(db *sqlx.DB) error {
	sqlTables := `
  CREATE TABLE IF NOT EXISTS blocks (
	id           bigserial PRIMARY KEY,
	hash         varchar(255),
	height       integer,
	time         integer,
	nextblock    varchar(255),
    processed    boolean,
    ptime        bigint
  );

  CREATE TABLE IF NOT EXISTS txs (
   id            bigserial PRIMARY KEY,
   hash          varchar(255) UNIQUE NOT NULL      
  );

  CREATE TABLE IF NOT EXISTS block_txs (   
   block_id      bigint REFERENCES blocks(id),
   tx_id         bigint REFERENCES txs(id),
   n             int 
  );

  CREATE TABLE IF NOT EXISTS addrs (
   id            bigserial PRIMARY KEY,
   hash 		 varchar(255) UNIQUE
  );

  CREATE TABLE IF NOT EXISTS txins (
   id            bigserial PRIMARY KEY,   
   tx_id         bigint REFERENCES txs(id),
   prevout_tx_id bigint,   
   prevout_n     int,
   value         numeric,
   n			 int,
   prev_address  bigint REFERENCES addrs(id),
   coinbase      boolean,
   scripttype    smallint 
  );

  CREATE TABLE IF NOT EXISTS txouts (
  id             bigserial PRIMARY KEY,   
  tx_id          bigint REFERENCES txs(id),
  n              int NOT NULL,
  value          numeric,
  address        bigint REFERENCES addrs(id),
  scripttype     smallint
  );
`
	_, err := db.Exec(sqlTables)
	return err
}

func (d *Data) FilterTxByIndices(trx []btcjson.TxRawResult, indices []int) []int {
	indexMap := make(map[int]struct{})
	for _, index := range indices {
		indexMap[index] = struct{}{}
	}

	filteredtxs := make([]int, 0)
	for i := range trx {
		if _, exists := indexMap[i]; !exists {
			log.Printf("tx #%d is not in indexMap", i)
			filteredtxs = append(filteredtxs, i)
		}
	}

	log.Printf("txs: %v", filteredtxs)
	return filteredtxs
}
