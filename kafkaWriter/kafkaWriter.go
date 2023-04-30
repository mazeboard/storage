// Taoufik 03/2023
package kafkaWriter

import (
	"context"
	"errors"
	"fmt"
	"storage/utils"
	"sync"

	"github.com/hamba/avro"
	"github.com/segmentio/kafka-go"
)

var nconnections = 20
var connectionLocks []sync.RWMutex
var kafkaWriters = []*kafka.Writer{}
kafkaWriters = []*kafka.Writer{}
for i := 0; i < nconnections; i++ {
writer := &kafka.Writer{
Addr:     kafka.TCP(fmt.Sprintf("%s:9092", host)), //, fmt.Sprintf("%s:9093", host), fmt.Sprintf("%s:9094", host)),
Balancer: &kafka.LeastBytes{},
}
kafkaWriters = append(kafkaWriters, writer)
}

var stateChangeSchema = avro.MustParse(`{
	"type": "record",
	"name": "state_change",
	"namespace": "org.hamba.avro",
	"fields" : [
		{"name": "block_number", "type": "long"},
		{"name": "timestamp", "type": "long"},
		{"name": "contract", "type": "string"},
		{"name": "location", "type": ["null", "string"]},
		{"name": "position", "type": ["null", "int"]},
		{"name": "arg1", "type": ["null", "string"]},
		{"name": "arg2", "type": ["null", "string"]},
		{"name": "index1", "type": ["null", "int"]},
		{"name": "index2", "type": ["null", "int"]},
		{"name": "index3", "type": ["null", "int"]}
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

func writeKafkaMessages(msgs []kafka.Message) error {
	parts := utils.Partition(nconnections, msgs)
	var wg sync.WaitGroup
	wg.Add(nconnections)
	var err error
	for i := 0; i < nconnections; i++ {
		go func(i int, msgs []kafka.Message) {
			connectionLocks[i].Lock()
			defer connectionLocks[i].Unlock()
			defer wg.Done()
			err = kafkaWriters[i].WriteMessages(context.Background(), msgs...)
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
func (*KafkaWriter) UploadRecords(records []utils.Record) error {
	msgs := []kafka.Message{}
	for _, record := range records {
		if msg, err := kafkaMessage(record); err == nil {
			msgs = append(msgs, msg)
		} else {
			return err
		}
	}
	return writeKafkaMessages(msgs)
}
