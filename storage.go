// Package storage Taoufik 03/2023
package storage

import (
	"encoding/hex"
	"fmt"
	"github.com/mazeboard/storage/dbWriter"
	"github.com/mazeboard/storage/fileWriter"
	"github.com/mazeboard/storage/kafkaWriter"
	"github.com/mazeboard/storage/s3Writer"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mazeboard/storage/utils"
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
	States    []*State
	keccaks   map[string]*KeccakEntry
	Contracts []*AccountEntry
	Hashes    map[string]void
}

type KeccakEntry struct {
	Pos  string
	Args []string
}

type AccountEntry struct {
	BlockNumber int64
	Timestamp   int64
	Addr        []byte
	Balance     string
}

type void struct{}

type Storage struct {
	dataChannel    chan *Data
	ErrorChannel   chan error
	EndChannel     chan bool
	lock           sync.RWMutex
	stateEntries   []*State
	keccakEntries  map[string]*KeccakEntry
	accountEntries []*AccountEntry
	hashEntries    map[string]void
	writer         utils.StorageWriter
	upload         bool
	ts             string
	writerData     string
}

var CurrentStorage *Storage

func (s *Storage) watch(close func()) {
	err := <-s.ErrorChannel
	log.Printf("storage failed - error %s\n", err)
	s.dataChannel <- nil
	close()
}
func (s *Storage) Wait() {
	s.dataChannel <- nil
	<-s.EndChannel // block until storage is done
	log.Printf("storage end\n")
}

// TOD save block number in s3
func New(close func(), setHead func(uint64)) *Storage {
	destination := os.Getenv("STORAGE_DESTINATION")
	if destination == "" {
		destination = "file"
	}
	st := Storage{
		dataChannel:    make(chan *Data, 100),
		ErrorChannel:   make(chan error),
		EndChannel:     make(chan bool),
		lock:           sync.RWMutex{},
		stateEntries:   []*State{},
		keccakEntries:  make(map[string]*KeccakEntry),
		accountEntries: []*AccountEntry{},
		hashEntries:    make(map[string]void),
		upload:         false,
	}
	go st.watch(close)
	var err error
	var writer utils.StorageWriter
	switch destination {
	case "kafka":
		log.Printf("new kafka storage\n")

		writer, err = kafkaWriter.New(&st.ErrorChannel)

	case "s3":
		log.Printf("new s3 storage\n")

		writer, err = s3Writer.New(&st.ErrorChannel)

	case "file":
		log.Printf("new file storage\n")

		writer, err = fileWriter.New(&st.ErrorChannel)

	case "database":
		log.Printf("new database storage\n")

		writer, err = dbWriter.New(&st.ErrorChannel)
	}

	if err != nil {
		st.ErrorChannel <- err
	} else {
		if err != nil {
			st.ErrorChannel <- err
		} else {
			setHead(uint64(writer.LastBlockNumber()))
			st.writer = writer
			go st.uploadLoop()
		}
	}
	return &st
}

func (s *Storage) UploadStorage() {
	if len(s.stateEntries) > 0 {
		s.lock.Lock()
		defer s.lock.Unlock()
		s.dataChannel <- &Data{States: s.stateEntries, keccaks: s.keccakEntries, Contracts: s.accountEntries, Hashes: s.hashEntries}
		s.stateEntries = []*State{}
		s.keccakEntries = make(map[string]*KeccakEntry)
		s.accountEntries = []*AccountEntry{}
		s.hashEntries = make(map[string]void)
	}
}

func (s *Storage) uploadLoop() {
	for {
		data := <-s.dataChannel
		if data == nil { // received signal to stop (on error nil is added to dataChannel)
			s.writer.Stop()      // wait for writer to stop
			s.EndChannel <- true // send signal to geth to stop
			return
		}
		start := time.Now()
		//s.saveKeccaks(start, data.keccaks, data.States) // for debug
		records := append(uploadAccounts(data.Contracts), uploadStates(data.States, data.keccaks, data.Hashes)...)
		if err := s.writer.UploadRecords(records); err != nil {
			s.ErrorChannel <- err
		} else {
			l := len(s.dataChannel)
			if l == 0 {
				log.Printf("upload - records %d - elapsed %s\n", len(records), time.Since(start).String())
			} else {
				log.Printf("upload - dataChannel %d - records %d - elapsed %s\n", l, len(records), time.Since(start).String())
			}
		}
	}
}

func uploadStates(sEntries []*State, kEntries map[string]*KeccakEntry, hEntries map[string]void) []utils.Record {
	states := map[string]*State{}
	for _, e := range sEntries {
		if _, ok := hEntries[e.TxHash]; ok {
			states[fmt.Sprintf("%d,%s,%s", e.BlockNumber, hex.EncodeToString(e.Contract), hex.EncodeToString(e.Location))] = e
		}
	}

	var records []utils.Record

	for _, s := range states {
		var position *string = nil
		var args []string

		location := hex.EncodeToString(s.Location)
		if kentry, ok := kEntries[location]; ok {
			position = &kentry.Pos
			args = kentry.Args
		}
		location = strings.TrimLeft(location, "0")
		if location == "" {
			location = "0"
		}
		records = append(records, utils.Record{
			StateChange: &utils.StateChangeRecord{
				BlockNumber: s.BlockNumber,
				Timestamp:   s.Timestamp,
				Contract:    hex.EncodeToString(s.Contract),
				Location:    location,
				Position:    position,
				Args:        args,
				Value:       hex.EncodeToString(s.Value),
			}})
	}
	return records
}

func uploadAccounts(cEntries []*AccountEntry) []utils.Record {
	centries := map[string]*AccountEntry{}
	for _, e := range cEntries {
		addr := hex.EncodeToString(e.Addr)
		centries[fmt.Sprintf("%d,%s", e.BlockNumber, addr)] = e
	}
	var records []utils.Record
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

func (s *Storage) WriteBalance(txHash string, timestamp uint64, blockNumber int64, contract []byte, balance string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.hashEntries[txHash] = void{}
	s.accountEntries = append(s.accountEntries, &AccountEntry{
		BlockNumber: blockNumber,
		Timestamp:   int64(timestamp),
		Addr:        contract,
		Balance:     balance,
	})
}

func (s *Storage) Write(h string, timestamp uint64, blockNumber int64, contract []byte, loc [32]byte, value [32]byte) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.stateEntries = append(s.stateEntries, &State{
		TxHash:      h,
		BlockNumber: blockNumber,
		Contract:    contract,
		Location:    loc[:],
		Value:       value[:],
		Timestamp:   int64(timestamp),
	})
}

func (s *Storage) WriteKeccak256(newloc string, data string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.keccakEntries[newloc]; !ok { // new location
		l := len(data)
		switch true {
		case l <= 64:
			if d, ok := s.keccakEntries[data]; !ok { // data is not a hash, then data is the position of a variable
				pos := strings.TrimLeft(data, "0")
				if data == "" {
					data = "0"
				}
				s.keccakEntries[newloc] = &KeccakEntry{Pos: pos}
			} else { // k(k(x)+0)
				s.keccakEntries[newloc] = &KeccakEntry{Pos: d.Pos, Args: append(d.Args, "0")}
			}
		default:
			// h(k) . pos
			pos := data[64:]
			if d, ok := s.keccakEntries[pos]; ok { // pos is a hash
				// h(k) . keccak256(X)
				s.keccakEntries[newloc] = &KeccakEntry{Pos: d.Pos, Args: append(d.Args, data[:64])}
			} else {
				// h(k) . p  // TODO check p sometimes is a long hex string (> 32 bytes)
				if l == 128 {
					pos = strings.TrimLeft(pos, "0")
					if pos == "" {
						pos = "0"
					}
					s.keccakEntries[newloc] = &KeccakEntry{Pos: pos, Args: []string{data[:64]}}
				} /*else { //p is not a position nor a hash (p must be 32 bytes), can be very long data
					s.keccakEntries[newloc] = &KeccakEntry{Pos: pos, Args: []string{data[:64]}}
				}*/
			}
		}
	}
}
func (s *Storage) WriteKeccak256Add(newloc string, oldloc string, data string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	if d, ok := s.keccakEntries[oldloc]; ok { // add computing new hash from old hash
		if _, ok := s.keccakEntries[newloc]; !ok { // new location
			s.keccakEntries[newloc] = &KeccakEntry{Pos: d.Pos, Args: append(d.Args, data[:])}
			return true
		}
	}
	return false
}

func (s *Storage) saveKeccaks(t time.Time, keccaks map[string]*KeccakEntry, states []*State) {
	if dir, err := os.UserHomeDir(); err == nil {
		datadir := dir + "/.storage"
		name := fmt.Sprintf("%s/k_%d", datadir, t.UnixNano())
		f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0755)
		if err == nil {
			defer func() {
				err := f.Close()
				if err != nil {
					log.Println("error", err)
				}
			}()
			_, err := f.WriteString(fmt.Sprintf("kentries:\n"))
			if err != nil {
				log.Println("error", err)
			}
			for k, v := range keccaks {
				_, err := f.WriteString(fmt.Sprintf("   %s,%v\n", k, v))
				if err != nil {
					log.Println("error", err)
				}
			}
			_, err = f.WriteString(fmt.Sprintf("locations:\n"))
			if err != nil {
				log.Println("error", err)
			}
			for _, v := range states {
				r, ok := keccaks[hex.EncodeToString(v.Location)]
				if !ok {
					r = nil
				}
				_, err := f.WriteString(fmt.Sprintf("   addr:%s loc:%s = %v\n", hex.EncodeToString(v.Contract), hex.EncodeToString(v.Location), r))
				if err != nil {
					log.Println("error", err)
				}
			}
		} else {
			log.Println("error", err)
		}
	}
}
