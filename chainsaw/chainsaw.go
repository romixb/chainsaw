package chainsaw

import (
	"chainsaw/bclient"
	"chainsaw/db"
	"chainsaw/rpcclient"
	"chainsaw/utils"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	_ "github.com/lib/pq"
)

const (
	genblockhash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
)

type Chainsaw struct {
	DB  *db.Data
	RPC *rpcclient.Client
	BC  *bclient.BlockchainClient
}
type TxJob struct {
	utils.BaseJob
	Func func(ctx context.Context) error
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
		log.Printf("Block %d processed in %d ms", b.Height, duration.Milliseconds())
		if err != nil {
			log.Fatal(err)
		}
	}
}
func (c *Chainsaw) ProcessBlock(b *db.Blocks) (*db.Blocks, error) {
	//TODO remove global ctx WithTimeout
	ctx, cancel := context.WithTimeout(context.Background(), 10000*time.Second)
	defer cancel()

	workerCount := 10
	maxRetries := 10
	txWorkerPool := utils.NewWorkerPool[*TxJob](maxRetries)
	txWorkerPool.Run(ctx, workerCount)

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

		log.Printf("inserting block %s, height: %d", bdata.Hash, bdata.Height)
		b, err = c.DB.InsertBlock(ctx, bdata.Hash, bdata.Height, bdata.Time, bdata.NextHash, false)

		if err != nil {
			log.Fatal(err)
		}

		for i, tx := range bdata.Tx {
			job := &TxJob{
				BaseJob: utils.NewBaseJob(int64(i)),
				Func: func(ctx context.Context) error {
					return c.DB.InsertTx(ctx, tx, b.ID, int32(i))
				},
			}
			txWorkerPool.JobQueue <- job
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
		for i, tx := range txs {
			job := &TxJob{
				BaseJob: utils.NewBaseJob(int64(i)),
				Func: func(ctx context.Context) error {
					return c.DB.InsertTx(ctx, tx, b.ID, int32(i))
				},
			}
			txWorkerPool.JobQueue <- job
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
		log.Printf("inserting block %s, height: %d", bdata.Hash, bdata.Height)
		b, err = c.DB.InsertBlock(ctx, bdata.Hash, bdata.Height, bdata.Time, bdata.NextHash, false)

		for i, tx := range bdata.Tx {
			job := &TxJob{
				BaseJob: utils.NewBaseJob(int64(i)),
				Func: func(ctx context.Context) error {
					return c.DB.InsertTx(ctx, tx, b.ID, int32(i))
				},
			}
			txWorkerPool.JobQueue <- job
		}
	}

	close(txWorkerPool.JobQueue)
	txWorkerPool.Wait()

	b, err := c.DB.MarkBlockAsProcessed(ctx, b.ID)
	if err != nil {
		log.Fatal(err)
	}

	return b, err
}

func (job *TxJob) Process(ctx context.Context) error {
	return job.Func(ctx)
}
