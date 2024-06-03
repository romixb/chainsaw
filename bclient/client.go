package bclient

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type BlockchainClient struct {
}

type Blocks []struct {
	Hash       string `json:"hash"`
	Height     int    `json:"height"`
	Time       int    `json:"time"`
	BlockIndex int    `json:"block_index"`
}

type Inputs struct {
	Sequence int64  `json:"sequence"`
	Witness  string `json:"witness"`
	Script   string `json:"script"`
	Index    int    `json:"index"`
	PrevOut  struct {
		N                 int64  `json:"n"`
		Script            string `json:"script"`
		SpendingOutpoints []struct {
			N       int   `json:"n"`
			TxIndex int64 `json:"tx_index"`
		} `json:"spending_outpoints"`
		Spent   bool `json:"spent"`
		TxIndex int  `json:"tx_index"`
		Type    int  `json:"type"`
		Value   int  `json:"value"`
	} `json:"prev_out"`
}

type Out struct {
	Type              int  `json:"type"`
	Spent             bool `json:"spent"`
	Value             int  `json:"value"`
	SpendingOutpoints []struct {
		TxIndex int64 `json:"tx_index"`
		N       int   `json:"n"`
	} `json:"spending_outpoints"`
	N       int    `json:"n"`
	TxIndex int64  `json:"tx_index"`
	Script  string `json:"script"`
	Addr    string `json:"addr,omitempty"`
}

type Tx struct {
	Hash        string   `json:"hash"`
	Ver         int      `json:"ver"`
	VinSz       int      `json:"vin_sz"`
	VoutSz      int      `json:"vout_sz"`
	Size        int      `json:"size"`
	Weight      int      `json:"weight"`
	Fee         int      `json:"fee"`
	RelayedBy   string   `json:"relayed_by"`
	LockTime    int      `json:"lock_time"`
	TxIndex     int64    `json:"tx_index"`
	DoubleSpend bool     `json:"double_spend"`
	Time        int      `json:"time"`
	BlockIndex  int      `json:"block_index"`
	BlockHeight int      `json:"block_height"`
	Inputs      []Inputs `json:"inputs"`
	Out         []Out    `json:"out"`
}

type Block struct {
	Hash       string   `json:"hash"`
	Ver        int      `json:"ver"`
	PrevBlock  string   `json:"prev_block"`
	MrklRoot   string   `json:"mrkl_root"`
	Time       int      `json:"time"`
	Bits       int      `json:"bits"`
	NextBlock  []string `json:"next_block"`
	Fee        int      `json:"fee"`
	Nonce      int64    `json:"nonce"`
	NTx        int      `json:"n_tx"`
	Size       int      `json:"size"`
	BlockIndex int      `json:"block_index"`
	MainChain  bool     `json:"main_chain"`
	Height     int      `json:"height"`
	Weight     int      `json:"weight"`
	Tx         []Tx     `json:"tx"`
}
type Transaction struct {
	Hash        string `json:"hash"`
	Ver         int    `json:"ver"`
	VinSz       int    `json:"vin_sz"`
	VoutSz      int    `json:"vout_sz"`
	LockTime    string `json:"lock_time"`
	Size        int    `json:"size"`
	RelayedBy   string `json:"relayed_by"`
	BlockHeight int    `json:"block_height"`
	TxIndex     string `json:"tx_index"`
	Inputs      []struct {
		PrevOut struct {
			Hash    string `json:"hash"`
			Value   string `json:"value"`
			TxIndex string `json:"tx_index"`
			N       string `json:"n"`
		} `json:"prev_out"`
		Script string `json:"script"`
	} `json:"inputs"`
	Out []struct {
		Value  string `json:"value"`
		Hash   string `json:"hash"`
		Script string `json:"script"`
	} `json:"out"`
}

func (c *BlockchainClient) IsAvailable(hash string) bool {
	_url, err := url.Parse("https://blockchain.info/rawblock/")
	Handle(err)

	_, err = http.Get(_url.String() + hash)
	Handle(err)
	return true
}

func Handle(err error) {

}
func (c *BlockchainClient) getBlocks(t time.Time) Blocks {
	url, err := url.Parse("https://blockchain.info/blocks/")
	Handle(err)

	resp, err := http.Get(url.String() + strconv.FormatInt(t.UnixMilli(), 10) + "?format=json")
	Handle(err)

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	Handle(err)

	var result Blocks
	if err := json.Unmarshal(body, &result); err != nil { // Parse []byte to the go struct pointer
		fmt.Println("Can not unmarshal JSON")
	}

	return result
}
func (c *BlockchainClient) GetBlock(height int64) Block {
	_url, err := url.Parse("https://blockchain.info/rawblock/")
	Handle(err)

	resp, err := http.Get(_url.String() + strconv.FormatInt(height, 10))
	Handle(err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	Handle(err)

	var result Block
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Println("Can not unmarshal JSON")
	}

	return result
}
func (c *BlockchainClient) getTransaction(hash string) Transaction {
	url, err := url.Parse("https://blockchain.info/rawtx/")
	Handle(err)

	url.JoinPath(hash)

	resp, err := http.Get(url.String())
	Handle(err)

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	Handle(err)

	var result Transaction
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Println("Can not unmarshal JSON")
	}

	return result
}
