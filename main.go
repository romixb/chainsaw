package main

import (
	"fmt"
	"syscall"

	// "encoding/json"
	// "fmt"
	"log"
	"os"
	"os/signal"

	// "os"
	"github.com/joho/godotenv"
	// "github.com/btcsuite/btcd/chaincfg/chainhash"
)

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	c := Chainsaw{}

	c.InitDB(
		os.Getenv("DB_HOST"),
		os.Getenv("DB_USERNAME"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_NAME"))
	c.InitBlkObsClient()
	c.InitNodeClient(
		os.Getenv("BTC_RPC_HOST"),
		os.Getenv("BTC_RPC_USER"),
		os.Getenv("BTC_RPC_PASS"))

	go c.StartHarvest()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-signalCh
		fmt.Printf("Received signal: %v\n", sig)
		// c.StopHarvest()
		os.Exit(0)
	}()

	select {}

}

func Handle(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// client, err := rpcclient.New(connCfg, nil)
// if err != nil {
// 	log.Fatal(err)
// }
// defer client.Shutdown()

// blockCount, err := client.GetBlockCount()
// if err != nil {
// 	log.Fatal(err)
// }
// fmt.Printf("%d\n", blockCount)

// blockHash, err := client.GetBlockHash(blockCount)
// if err != nil {
// 	log.Fatal(err)
// }
// fmt.Printf("%s\n", blockHash.String())

// blockVerb, err := client.GetBlockVerbose(blockHash)
// if err != nil {
// 	log.Fatal(err)
// }
// fmt.Printf("%+v\n", blockVerb)

// hash, err := chainhash.NewHashFromStr("00000000000000000002dc67d4dbc17cf3364d9a9a282625b717e958b3427f6f")
// if err != nil {
// 	log.Fatal(err)
// }

// res := client.GetBlockVerboseTxAsync(hash)
// tx, err := res.Receive()

// if err != nil {
// 	log.Fatal(err)
// }

// js, err := json.Marshal(tx)
// if err != nil {
// 	log.Fatal(err)
// }
// fmt.Println(string(js))

// _ = os.WriteFile("test.json", js, 0644)

// fmt.Printf("%+v\n", tx)

// res2B, _ := json.Marshal(&blockVerb)
// fmt.Println(res2B)
