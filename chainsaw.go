package main

import (
	"chainsaw/bclient"
	"chainsaw/btcjson"
	"chainsaw/db"
	"chainsaw/rpcclient"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	_ "github.com/lib/pq"
)

var (
	genblockhash  = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
	numGoroutines = 4
)

type Chainsaw struct {
	DB  *db.Data
	RPC *rpcclient.Client
	BC  *bclient.BlockchainClient
}

func (c *Chainsaw) InitDB(host, user, password, dbname string) {
	connectionString :=
		fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", host, user, password, dbname)

	var err error
	DB := new(db.Data)
	c.DB = DB
	c.DB, err = c.DB.StartDb(connectionString)

	if err != nil {
		log.Fatal(err)
	} else {
		log.Print("Connected to db successfully")
	}
}
func (c *Chainsaw) InitBlkObsClient() {
	bc := new(bclient.BlockchainClient)
	if !bc.IsAvailable(genblockhash) {
		log.Fatal(fmt.Errorf("blockchain.info api not available"))
	}
	c.BC = bc
}
func (c *Chainsaw) InitNodeClient(host, user, pass string) {
	config := &rpcclient.ConnConfig{
		Host:         host,
		User:         user,
		Pass:         pass,
		HTTPPostMode: true,
		DisableTLS:   true,
	}

	var err error
	c.RPC, err = rpcclient.New(config, nil)

	if err != nil {
		log.Fatal(err)
	} else {
		log.Print("BTC_RPC client started successfully")
	}
}
func (c *Chainsaw) StartHarvest() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	//get local best block height
	h, err := c.RPC.GetBestBlockHash()
	if err != nil {
		log.Fatal(err)
	}
	var stat *[]string
	bbs, err := c.RPC.GetBlockStats(h, stat)
	if err != nil {
		log.Fatal(err)
	}
	bbh := bbs.Height
	b := c.DB.GetLastProcessedBlock(ctx)

	var ctrl int64
	if b == nil {
		ctrl = 0
	} else {
		ctrl = b.Height
	}

	for i := ctrl; i <= bbh; i++ {
		start := time.Now()
		b, err = c.ProcessBlock(b)
		duration := time.Since(start)
		log.Print(duration.Milliseconds())
		if err != nil {
			log.Fatal(err)
		}
	}
}
func (c *Chainsaw) ProcessBlock(b *db.Blocks) (*db.Blocks, error) {
	//TODO remove global ctx WithTimeout
	ctx, cancel := context.WithTimeout(context.Background(), 10000*time.Second)
	defer cancel()

	switch {
	case b == nil:
		bhash, err := chainhash.NewHashFromStr(genblockhash)
		if err != nil {
			log.Fatal(err)
		}

		rblock := c.RPC.GetBlockVerboseTxAsync(bhash)
		bdata, err := rblock.Receive()
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("inserting block %s, height: %s", bdata.Hash, bdata.Height)
		b, err = c.DB.InsertBlock(ctx, bdata.Hash, bdata.Height, bdata.Time, bdata.NextHash, false)
		log.Print("block inserted")

		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < len(bdata.Tx); i++ {
			tx := bdata.Tx[i]
			log.Printf("inserting tx %s (# %d) from block %d ", tx.Txid, i, b.Height)
			err = c.DB.InsertTx(ctx, tx, b.ID, int32(i), nil)
			if err != nil {
				log.Fatal(err)
			}
		}
	case b.Processed != true:
		bhash, err := chainhash.NewHashFromStr(b.Hash)
		if err != nil {
			log.Fatal(err)
		}

		rblock := c.RPC.GetBlockVerboseTxAsync(bhash)
		bdata, err := rblock.Receive()
		if err != nil {
			log.Fatal(err)
		}

		ltx := c.DB.GetLastProcessedTxFromBlock(ctx, b.ID)
		ltx++
		for i := ltx; i < len(bdata.Tx); i++ {
			tx := bdata.Tx[i]
			log.Printf("inserting tx %s (# %d) from block %d ", tx.Txid, i, b.Height)

			err = c.DB.InsertTx(ctx, tx, b.ID, int32(i), nil)
			if err != nil {
				log.Fatal(err)
			}
		}
	case b.Processed == true:
		bhash, err := chainhash.NewHashFromStr(b.Nextblock)
		if err != nil {
			log.Fatal(err)
		}

		rblock := c.RPC.GetBlockVerboseTxAsync(bhash)
		bdata, err := rblock.Receive()
		if err != nil {
			log.Fatal(err)
		}
		b, err = c.DB.InsertBlock(ctx, bdata.Hash, bdata.Height, bdata.Time, bdata.NextHash, false)
		if err != nil {
			log.Fatal(err)
		}

		//
		retries := make(chan int32, 200)
		errors := make(chan error, numGoroutines)

		length := len(bdata.Tx)
		var wg sync.WaitGroup
		chunkSize := (length + numGoroutines - 1) / numGoroutines
		//TODO: add a channel with tx index for retry
		for i := 0; i < numGoroutines; i++ {
			start := i * chunkSize
			end := start + chunkSize
			if end > length {
				end = length
			}
			if start < length {
				wg.Add(1)
				//TODO: wrap in a worker func
				go c.processTx(ctx, &wg, bdata.Tx[start:end], b.ID, b.Height, retries, errors)
			}
		}

		wg.Add(1)
		go c.Retry(ctx, &wg, &bdata.Tx, b.ID, b.Height, retries, errors)

		go func() {
			wg.Wait()
			close(retries)
			close(errors)
		}()

		wg.Wait()

		for err := range errors {
			fmt.Printf("Error encountered: %v\n", err)
		}

	}

	b, err := c.DB.MarkBlockAsProcessed(ctx, b.ID)
	if err != nil {
		log.Print("from 215")

		log.Fatal(err)
	}

	return b, err
}

func (c *Chainsaw) processTx(ctx context.Context, wg *sync.WaitGroup, txs []btcjson.TxRawResult, blockid int64, height int64, retries chan int32, errors chan error) {
	defer wg.Done()
	for i, tx := range txs {
		log.Printf("inserting tx %s (# %d) from block %d ", tx.Txid, i, height)
		err := c.DB.InsertTx(ctx, tx, blockid, int32(i), retries)
		if err != nil {
			errors <- err
			return
		}
	}
}

func (c *Chainsaw) Retry(ctx context.Context, wg *sync.WaitGroup, txarr *[]btcjson.TxRawResult, blockid int64, height int64, retries chan int32, errors chan error) {
	defer wg.Done()
	txs := *txarr
	for i := range retries {
		log.Printf("Retry tx %s (# %d) from block %d ", txs[i].Txid, i, height)
		err := c.DB.InsertTx(ctx, txs[i], blockid, i, retries)
		if err != nil {
			errors <- err
			return
		}
	}
}

// func (c *Chainsaw) StopHarvest() {
// }
