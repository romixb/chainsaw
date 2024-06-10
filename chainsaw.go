package main

import (
	"chainsaw/bclient"
	"chainsaw/btcjson"
	"chainsaw/db"
	"chainsaw/rpcclient"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	_ "github.com/lib/pq"
	"log"
	"time"
)

var (
	genblockhash = "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048"
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
	b, err := c.DB.GetLastProcessedBlock(ctx)

	if errors.Is(err, sql.ErrNoRows) {
		bbh++
		for i := int32(0); i < int32(bbh); i++ {
			err = c.ProcessBlock(nil, 0)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	//ltx := c.DB.GetLastProcessedTxFromBlock(ctx, b.ID)
	err = c.ProcessBlock(b, 0)

}
func (c *Chainsaw) ProcessBlock(b *db.Blocks, txoffset int32) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if b == nil {
		bhash, err := chainhash.NewHashFromStr(genblockhash)
		if err != nil {
			return err
		}

		rblock := c.RPC.GetBlockVerboseTxAsync(bhash)
		bdata, err := rblock.Receive()
		if err != nil {
			return err
		}

		b, err = c.DB.InsertBlock(ctx, bdata.Hash, bdata.Height, bdata.Time, bdata.PreviousHash, bdata.NextHash)
		if err != nil {
			return err
		}

		for i := 0; i < len(bdata.Tx); i++ {
			tx := bdata.Tx[i]
			log.Printf("inserting tx %s from block %s (# %d)", tx.Txid, b.Height, i)

			err = c.DB.InsertTx(ctx, tx, b.ID)
			if err != nil {
				return err
			}
		}
	}

	bhash, err := chainhash.NewHashFromStr(b.Hash)
	if err != nil {
		return err
	}

	block := c.RPC.GetBlockVerboseTxAsync(bhash)
	bdata, err := block.Receive()
	if err != nil {
		return err
	}

	txoffset++
	for i := txoffset; i < int32(len(bdata.Tx)); i++ {
		tx := bdata.Tx[i]
		err = c.DB.InsertTx(ctx, tx, b.ID)
		if err != nil {
			return err
		}
	}

	err = c.DB.MarkBlockAsProcessed(ctx, b.ID)
	if err != nil {
		return err
	}

	return nil
}
func (c *Chainsaw) StopHarvest() {
}
func (c *Chainsaw) GetLastHandledBlock() {

}
