package main

import (
	"chainsaw/db"
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	_ "github.com/skydive-project/skydive/graffiti/storage/orientdb"
)

type Chainsaw struct {
	DB     *sql.DB
	Client *rpcclient.Client
	mux    sync.Mutex
}

func (c *Chainsaw) InitializeDB(host, user, password, dbname string) {

	connectionString :=
		fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", host, user, password, dbname)
	c.mux.Lock()
	defer c.mux.Unlock()
	var err error
	c.DB, err = db.StartDb(connectionString)

	if err != nil {
		log.Fatal(err)
	} else {
		log.Print("Connected to db successfully")
	}
}

func (c *Chainsaw) InitializeHarvester(host, user, pass string) {
	config := &rpcclient.ConnConfig{
		Host:         host,
		User:         user,
		Pass:         pass,
		HTTPPostMode: true,
		DisableTLS:   true,
	}

	var err error
	c.Client, err = rpcclient.New(config, nil)

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

func (c *Chainsaw) StartHarvest(height int64, txH string) {

	//Get last handled entities before start
	// lastBlock := c.getLastHandledBlock()
	// lastTx := c.getLastHanledTx()

	//get hash of current block
	bh, err := c.Client.GetBlockHash(height)
	if err != nil {
		log.Fatal(err)
	}

	// get the block
	b := c.Client.GetBlockVerboseTxAsync(bh)
	block, err := b.Receive()
	if err != nil {
		log.Fatal(err)
	}

	//get transaction list from current block
	//and get the next transacation for handling
	txs := block.Tx
	txIndex := getCurrentTx(txH, txs)
	tx := txs[txIndex]

	//Handle i
	for i, t := range tx.Vin {
		println(i, t.Txid)
		//if t.
	}

	// ccc:=txs[nextTx]

	// ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	// defer cancel()
	// Chainsaw
	// select{
	// 	case <- ctx.Done
	// }

	// hash, err := chainhash.NewHashFromStr("98f847a51f48e93c3d750f652b93882d64e0f48aab9326b70639ef2fe2b56820")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// res := c.Client.GetBlockVerboseTxAsync(hash)
	// tx, err := res.Receive()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// res1, err := c.Client.GetRawTransactionVerboseAsync(hash).Receive()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// fmt.Println(res1)

	// js, err := json.Marshal(tx)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println(string(js))

	client := BlockchainClient{}

	blocks := client.getBlocks(time.Now())

	for _, b := range blocks {

		block := client.getBlock(b.Hash)
		nb := strings.Join(block.NextBlock, "")
		res, err := c.DB.Exec("INSERT INTO blocks (id, hash, height, merkleroot, time, previousblock, nextblock) VALUES (?, ?, ?, ?, ?, ?, ?,)",
			nil, block.Hash, block.Height, block.MrklRoot, block.Time, block.PrevBlock, nb)
		if err != nil {
			log.Fatal(err)
		}
		blockId, _ := res.LastInsertId()

		for _, t := range block.Tx {

			transaction := client.getTransaction(t.Hash)

			h, err := chainhash.NewHashFromStr(t.Hash)
			if err != nil {
				log.Fatal(err)
			}

			r, err := c.Client.GetRawTransactionVerboseAsync(h).Receive()
			if err != nil {
				log.Fatal(err)
			}

			ins := []string{}
			for _, i := range r.Vin {
				ins = append(ins, i.Txid)
			}

			outs := []string{}
			for _, o := range r.Vout {
				outs = append(outs, o.ScriptPubKey.Hex)
			}

			ctx := context.Background()
			tx, err := c.DB.BeginTx(ctx, nil)
			if err != nil {
				log.Fatal(err)
			}

			_, err = tx.ExecContext(ctx, "INSERT INTO transactions (id, block_id, hash, ins, out, value, relayedBy) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
				nil, blockId, transaction.Hash, pq.Array(ins), pq.Array(outs), nil, nil, transaction.RelayedBy)

			if err != nil {
				tx.Rollback()
				return
			}

			err = tx.Commit()
			if err != nil {
				log.Fatal(err)
			}

		}
	}

	fmt.Println(blocks)
}
