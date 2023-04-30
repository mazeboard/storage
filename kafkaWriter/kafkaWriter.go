// Taoufik 03/2023
package kafkaWriter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/hamba/avro"
	"github.com/mazeboard/storage/utils"
	"github.com/segmentio/kafka-go"
)

type KafkaWriter struct {
	utils.StorageWriter
	nconnections    int
	connectionLocks []sync.RWMutex
	kafkaWriters    []*kafka.Writer
	dataFile        string
	lastBlockNumber int64
}

func New(errorChannel *chan error) (utils.StorageWriter, error) {
	w := KafkaWriter{
		nconnections:    20,
		kafkaWriters:    []*kafka.Writer{},
		connectionLocks: []sync.RWMutex{},
	}
	host := os.Getenv("KAFKA_HOST")
	if host == "" {
		host = "localhost"
	}
	for i := 0; i < w.nconnections; i++ {
		w.connectionLocks = append(w.connectionLocks, sync.RWMutex{})
	}
	for i := 0; i < w.nconnections; i++ {
		writer := &kafka.Writer{
			Addr:     kafka.TCP(fmt.Sprintf("%s:9092", host)), //, fmt.Sprintf("%s:9093", host), fmt.Sprintf("%s:9094", host)),
			Balancer: &kafka.LeastBytes{},
		}
		w.kafkaWriters = append(w.kafkaWriters, writer)
	}
	var dir string
	var err error
	if dir, err = os.UserHomeDir(); err == nil {
		w.dataFile = dir + "/.s3WriterData"
	}
	w.readData()
	return &w, nil
}
func (w *KafkaWriter) readData() {
	buf, err := os.ReadFile(w.dataFile)
	if err != nil {
	} else {
		if bn, err := strconv.Atoi(string(buf)); err == nil {
			w.lastBlockNumber = int64(bn)
		}

	}
}
func (s *KafkaWriter) writeData(data string) {
	err := os.WriteFile(s.dataFile, []byte(data), 0755)
	if err != nil {
		log.Printf("failed to save block number - error %s\n", err)
	}
}

func (w *KafkaWriter) LastBlockNumber() int64 {
	return w.lastBlockNumber
}

func (w *KafkaWriter) Stop() {

}
func (w *KafkaWriter) UploadRecords(records []utils.Record) error {
	msgs := []kafka.Message{}
	var maxBlockNumber int64 = 0
	for _, record := range records {
		if record.StateChange != nil && record.StateChange.BlockNumber > maxBlockNumber {
			maxBlockNumber = record.StateChange.BlockNumber
		} else if record.AccountBalance != nil && record.AccountBalance.BlockNumber > maxBlockNumber {
			maxBlockNumber = record.AccountBalance.BlockNumber
		}
		if msg, err := kafkaMessage(record); err == nil {
			msgs = append(msgs, msg)
		} else {
			return err
		}
	}
	err := w.writeKafkaMessages(msgs)
	if err == nil {
		w.writeData(fmt.Sprintf("%d", maxBlockNumber))
	}
	return err
}

var stateChangeSchema = avro.MustParse(`{
	"type": "record",
	"name": "state_change",
	"namespace": "org.hamba.avro",
	"fields" : [
		{"name": "block_number", "type": "long"},
		{"name": "timestamp", "type": "long"},
		{"name": "contract", "type": "string"},
		{"name": "location", "type": "string"},
		{"name": "position", "type": ["null", "int"]},
		{"name": "args", "type": { 
      "type": "array",
      "items": "string"
   }},
		{"name": "value", "type": "string"}
	]
}`)
var accountBalanceSchema = avro.MustParse(`{
	"type": "record",
	"name": "account_balance",
	"namespace": "org.hamba.avro",
	"fields" : [
		{"name": "block_number", "type": "long"},
		{"name": "timestamp", "type": "long"},
		{"name": "account", "type": "string"},
		{"name": "balance", "type": "string"}
	]
}`)

func (w *KafkaWriter) writeKafkaMessages(msgs []kafka.Message) error {
	parts := utils.Partition(w.nconnections, msgs)
	var wg sync.WaitGroup
	wg.Add(w.nconnections)
	var err error
	for i := 0; i < w.nconnections; i++ {
		go func(i int, msgs []kafka.Message) {
			w.connectionLocks[i].Lock()
			defer w.connectionLocks[i].Unlock()
			defer wg.Done()
			err = w.kafkaWriters[i].WriteMessages(context.Background(), msgs...)
		}(i, parts[i])
	}
	wg.Wait()
	return err
}

// TODO Unknown Topic Or Partition: the request is for a topic or partition that does not exist on this broker
func kafkaMessage(record utils.Record) (kafka.Message, error) {
	var err error
	var bytes []byte
	if record.StateChange != nil {
		// TODO may fail and err is nil
		if bytes, err = avro.Marshal(stateChangeSchema, *record.StateChange); err == nil {
			return kafka.Message{
				Value: bytes,
				Topic: "eth_state_change",
			}, nil
		}
	} else {
		if record.AccountBalance != nil {
			// TODO may fail (len(bytes)=0) and err is nil
			if bytes, err = avro.Marshal(accountBalanceSchema, *record.AccountBalance); err == nil {
				return kafka.Message{
					Value: bytes,
					Topic: "eth_account_balance",
				}, nil
			}
		}
	}
	return kafka.Message{}, errors.New(fmt.Sprintf("failed to create avro %v", record))
}
