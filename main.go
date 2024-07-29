package main

import (
	"chainsaw/chainsaw"
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
	c := chainsaw.Chainsaw{}

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
