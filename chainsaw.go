package main

import (
	"chainsaw/bclient"
	"chainsaw/btcjson"
	"chainsaw/db"
	"chainsaw/rpcclient"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	_ "github.com/lib/pq"
)

var (
	genblockhash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
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
func getCurrentTx(tx string, txs []btcjson.TxRawResult) int {
	if len(txs) == 1 {
		return -1
	}
	nextTxIndex := 0
	for index, value := range txs {
		if value.Hash == tx && index < len(txs)-1 {
			nextTxIndex = index + 1
		}

	}

	return nextTxIndex
}
func (c *Chainsaw) StartHarvest() {
	ctx, cancel := context.WithTimeout(context.Background(), 10000*time.Second)
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

	for i := int32(0); i < int32(bbh); i++ {
		b, err = c.ProcessBlock(b)
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
		b, err = c.DB.InsertBlock(ctx, bdata.Hash, bdata.Height, bdata.Time, bdata.PreviousHash, bdata.NextHash, false)
		log.Print("block inserted")

		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < len(bdata.Tx); i++ {
			tx := bdata.Tx[i]
			log.Printf("inserting tx %s (# %d) from block %d ", tx.Txid, i, b.Height)
			err = c.DB.InsertTx(ctx, tx, b.ID, int32(i))
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

			err = c.DB.InsertTx(ctx, tx, b.ID, int32(i))
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
		b, err = c.DB.InsertBlock(ctx, bdata.Hash, bdata.Height, bdata.Time, bdata.PreviousHash, bdata.NextHash, false)
		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < len(bdata.Tx); i++ {
			tx := bdata.Tx[i]
			log.Printf("inserting tx %s (# %d) from block %d ", tx.Txid, i, b.Height)

			err = c.DB.InsertTx(ctx, tx, b.ID, int32(i))
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	b, err := c.DB.MarkBlockAsProcessed(ctx, b.ID)
	if err != nil {
		log.Fatal(err)
	}

	return b, err
}

// func (c *Chainsaw) StopHarvest() {
// }
