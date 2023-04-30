// Taoufik 03/2023
package storage

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"storage/utils"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
)

type State struct {
	TxHash      string
	BlockNumber int64
	Contract    []byte
	Location    []byte
	Value       []byte
	Timestamp   int64
}

type Data struct {
	States    []State
	Keccas    map[string]KeccaEntry
	Contracts []AccountEntry
	Hashes    map[string]void
}

type KeccaEntry struct {
	Pos     int
	Args    []string
	Indexes []int
}

type AccountEntry struct {
	BlockNumber int64
	Timestamp   int64
	Addr        []byte
	Balance     string
}

type void struct{}

var destination string // kafka, s3, postgres

var chanData = make(chan Data, 100)
var lock sync.RWMutex
var stateEntries []State = []State{}
var keccaEntries = make(map[string]KeccaEntry)
var accountEntries = []AccountEntry{}
var hashEntries = make(map[string]void)
var runningNode *node.Node
var writer StorageWriter

func (k *KeccaEntry) String() string {
	return fmt.Sprintf("%d args:%v indexes:%v", k.Pos, k.Args, k.Indexes)
}

func fatal(msg string, err error) {
	log.Error(msg, "error", err)
	runningNode.Close()
	log.Crit(msg, "error", err)
}
func Init(dest string, stack *node.Node) {
	runningNode = stack
	destination = dest
	switch destination {
	case "kafka":
		// TODO authentication
		host := os.Getenv("KAFKA_HOST")
		if host == "" {
			host = "localhost"
		}
		log.Info("init storage (kafka producer)", "host", host)

		writer = KafkaWriter

	case "s3":

		writer = S3Writer

	case "database":

		writer = DatabaseWriter

	default:
		fatal(fmt.Sprintf("--states=%s", destination), errors.New("invalid states cli argument, valid states are kafka ans s3"))
	}
	go uploadLoop()
}

func WriteBalance(txHash string, timestamp uint64, block_number int64, contract common.Address, balance string) {
	lock.Lock()
	defer lock.Unlock()
	hashEntries[txHash] = void{}
	accountEntries = append(accountEntries, AccountEntry{
		BlockNumber: block_number,
		Timestamp:   int64(timestamp),
		Addr:        contract[:],
		Balance:     balance,
	})
}

func Write(h string, timestamp uint64, blockNumber int64, contract common.Address, loc [32]byte, value [32]byte) {
	lock.Lock()
	defer lock.Unlock()
	stateEntries = append(stateEntries, State{
		TxHash:      h,
		BlockNumber: blockNumber,
		Contract:    contract[:],
		Location:    loc[:],
		Value:       value[:],
		Timestamp:   int64(timestamp),
	})
}

func WriteKecca256(newloc string, data string) {
	l := len(data)
	if l > 64 { // hex characters
		p := data[64:]
		arg := strings.TrimLeft(data[:64], "0")
		lock.Lock()
		defer lock.Unlock()
		if d, ok := keccaEntries[p]; ok {
			keccaEntries[newloc] = KeccaEntry{Pos: d.Pos, Indexes: d.Indexes, Args: append(d.Args, arg)}
		} else {
			if pos, err := strconv.ParseInt(p, 16, 32); err == nil {
				keccaEntries[newloc] = KeccaEntry{Pos: int(pos), Args: []string{arg}}
			}
		}
	} else {
		if l == 64 {
			lock.Lock()
			defer lock.Unlock()
			if d, ok := keccaEntries[data]; ok {
				keccaEntries[newloc] = d
			} else {
				if pos, err := strconv.ParseInt(data, 16, 32); err == nil {
					keccaEntries[newloc] = KeccaEntry{Pos: int(pos)}
				}
			}
		} //else {
		/*
			WARN [04-17|22:55:37.162] kecca newloc=b052050c6ed400a931c7e68cb2c3fd217e53c8cfa638ee94c32b17947c0f6ea4 data=55524c11

			log.Warn("kecca", "newloc", newloc, "data", data)
		*/
		//}
	}
}

func WriteKecca256Add(newloc string, oldloc string, data [32]byte) {
	lock.Lock()
	defer lock.Unlock()
	if d, ok := keccaEntries[oldloc]; ok {
		if _, ok := keccaEntries[newloc]; !ok {
			if index, err := strconv.ParseInt(hex.EncodeToString(data[:]), 16, 32); err == nil {
				keccaEntries[newloc] = KeccaEntry{Pos: d.Pos, Indexes: append(d.Indexes, int(index)), Args: d.Args}
			}
		}
	}
}

func Upload() {
	lock.Lock()
	defer lock.Unlock()
	if len(stateEntries) > 0 {
		kentries := map[string]KeccaEntry{}
		for k, v := range keccaEntries {
			kentries[k] = v
		}
		chanData <- Data{States: stateEntries, Keccas: kentries, Contracts: accountEntries, Hashes: hashEntries}
		stateEntries = []State{}
		keccaEntries = make(map[string]KeccaEntry)
		accountEntries = []AccountEntry{}
		hashEntries = make(map[string]void)
	}
}

func uploadLoop() {
	for {
		data := <-chanData
		start := time.Now()
		records := uploadAccounts(data.Contracts)
		records = append(records, uploadStates(data.States, data.Keccas, data.Hashes)...)
		writer.UploadRecords(records)
		log.Info("upload", "chanData", len(chanData), "records", len(records), "elapsed", time.Since(start))
	}
}

func uploadStates(sEntries []State, kEntries map[string]KeccaEntry, hEntries map[string]void) []Record {
	states := map[string]State{}
	for _, e := range sEntries {
		if _, ok := hEntries[e.TxHash]; ok {
			addr := hex.EncodeToString(e.Contract)
			states[fmt.Sprintf("%d,%s,%s", e.BlockNumber, addr, hex.EncodeToString(e.Location))] = e
		}
	}

	records := []utils.Record{}

	for _, s := range states {
		var location *string = nil
		var position *int = nil
		var arg1 *string = nil
		var arg2 *string = nil
		var index1 *int = nil
		var index2 *int = nil
		var index3 *int = nil

		loc := hex.EncodeToString(s.Location)
		if kentry, ok := kEntries[loc]; ok {
			var posDone bool = false
			switch len(kentry.Args) {
			case 1:
				posDone = true
				position = &kentry.Pos
				arg1 = &kentry.Args[0]
				arg2 = nil
			case 2:
				posDone = true
				position = &kentry.Pos
				arg1 = &kentry.Args[0]
				arg2 = &kentry.Args[1]
			default:
				arg1 = nil
				arg2 = nil
			}
			switch len(kentry.Indexes) {
			case 1:
				if !posDone {
					position = &kentry.Pos
					posDone = true
				}
				index1 = &kentry.Indexes[0]
				index2 = nil
				index3 = nil
			case 2:
				if !posDone {
					position = &kentry.Pos
					posDone = true
				}
				index1 = &kentry.Indexes[0]
				index2 = &kentry.Indexes[1]
				index3 = nil
			case 3:
				if !posDone {
					position = &kentry.Pos
					posDone = true
				}
				index1 = &kentry.Indexes[0]
				index2 = &kentry.Indexes[1]
				index3 = &kentry.Indexes[2]
			default:
				index1 = nil
				index2 = nil
				index3 = nil
			}
			if !posDone {
				position = nil
				location = &loc
			} else {
				location = nil
			}
		} else {
			pos64, err := strconv.ParseInt(loc, 16, 32)
			pos := int(pos64)
			if err == nil {
				location = nil
				position = &pos
			} else {
				location = &loc
				position = nil
			}
			arg1 = nil
			arg2 = nil
			index1 = nil
			index2 = nil
			index3 = nil
		}

		records = append(records, utils.Record{
			StateChange: &utils.StateChangeRecord{
				BlockNumber: s.BlockNumber,
				Timestamp:   s.Timestamp,
				Contract:    hex.EncodeToString(s.Contract),
				Location:    location,
				Position:    position,
				Arg1:        arg1,
				Arg2:        arg2,
				Index1:      index1,
				Index2:      index2,
				Index3:      index3,
			}})
	}
	return records
}

func uploadAccounts(cEntries []AccountEntry) []utils.Record {
	centries := map[string]AccountEntry{}
	for _, e := range cEntries {
		addr := hex.EncodeToString(e.Addr)
		centries[fmt.Sprintf("%d,%s", e.BlockNumber, addr)] = e
	}
	records := []utils.Record{}
	for _, s := range centries {
		records = append(records, utils.Record{
			AccountBalance: &utils.AccountBalanceRecord{
				BlockNumber: s.BlockNumber,
				Timestamp:   s.Timestamp,
				Account:     hex.EncodeToString(s.Addr),
				Balance:     s.Balance,
			}})
	}
	return records
}
