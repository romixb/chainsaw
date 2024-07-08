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

const (
	genblockhash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
	workerNum    = 10
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

		txs := bdata.Tx[ltx:len(bdata.Tx)]
		length := len(bdata.Tx)
		chunkSize := (length + workerNum - 1) / workerNum
		currentWorkers := workerNum
		if length < workerNum {
			currentWorkers = length
		}

		var wg sync.WaitGroup
		retries := make(chan int32, 400)
		errors := make(chan error, currentWorkers)
		done := make(chan struct{}, currentWorkers)

		for i := 0; i < currentWorkers; i++ {
			start := i * chunkSize
			end := start + chunkSize
			if end > length {
				end = length
			}
			log.Printf("currentWorker %d to process txs %d to %d", i, start, end)
			if start < length {
				wg.Add(1)
				go func() {
					log.Printf("currentWorker %d starts", i)
					c.processTx(ctx, &wg, txs[start:end], b.ID, b.Height, retries, errors, done)
				}()
			} else {
				fmt.Printf("No work for currentWorker %n on block %n", i, b.Height)
				done <- struct{}{}
			}

		}

		wg.Add(1)
		go func() {
			c.retry(ctx, &wg, &bdata.Tx, b.ID, retries, errors, done, currentWorkers)
		}()

		wg.Wait()
		close(errors)

		for err := range errors {
			fmt.Printf("Error encountered: %v\n", err)
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

		length := len(bdata.Tx)
		chunkSize := (length + workerNum - 1) / workerNum
		currentWorkers := workerNum
		if length < workerNum {
			currentWorkers = length
		}

		var wg sync.WaitGroup
		retries := make(chan int32, 400)
		errors := make(chan error, workerNum)
		done := make(chan struct{}, currentWorkers)

		for i := 0; i < currentWorkers; i++ {
			start := i * chunkSize
			end := start + chunkSize
			if end > length {
				end = length
			}
			if start < length {
				wg.Add(1)
				go func() {
					c.processTx(ctx, &wg, bdata.Tx[start:end], b.ID, b.Height, retries, errors, done)
				}()
			} else {
				fmt.Printf("No work for currentWorker %n on block %n", i, b.Height)
				done <- struct{}{}
			}
		}

		wg.Add(1)
		go func() {
			c.retry(ctx, &wg, &bdata.Tx, b.ID, retries, errors, done, currentWorkers)
		}()

		wg.Wait()
		close(errors)
		for err := range errors {
			fmt.Printf("Error encountered: %v\n", err)
		}

	}

	b, err := c.DB.MarkBlockAsProcessed(ctx, b.ID)
	if err != nil {
		log.Fatal(err)
	}

	return b, err
}
func (c *Chainsaw) processTx(ctx context.Context, wg *sync.WaitGroup, txs []btcjson.TxRawResult, blockid int64, height int64, retries chan int32, errors chan<- error, done chan<- struct{}) {
	defer wg.Done()

	log.Printf("txs length %d", len(txs))
	for i, tx := range txs {
		log.Printf("inserting tx %s (# %d) from block %d ", tx.Txid, i, height)
		err := c.DB.InsertTx(ctx, tx, blockid, int32(i), retries)
		if err != nil {
			errors <- err
			return
		}
		log.Printf("finished inserting tx %s (# %d) from block %d ", tx.Txid, i, height)
	}
	//log.Print("try push signal")
	done <- struct{}{}
	log.Printf("signal pushed")
}
func (c *Chainsaw) retry(ctx context.Context, wg *sync.WaitGroup, txarr *[]btcjson.TxRawResult, blockid int64, retries chan int32, errors chan<- error, done chan struct{}, workers int) {
	defer wg.Done()
	txs := *txarr
	donesig := 0
	for {
		select {
		case val, ok := <-retries:
			if !ok {
				fmt.Println("retries closed, exiting.")
				return
			}
			err := c.DB.InsertTx(ctx, txs[val], blockid, val, retries)
			if err != nil {
				errors <- err
				return
			}
		case <-done:
			donesig++
			if donesig == workers {
				log.Print("all goroutines on tx processing are done")
				close(retries)
				close(done)
				return
			}
		}
	}
}

// func (c *Chainsaw) StopHarvest() {
// }
