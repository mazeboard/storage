package utils

type StorageWriter interface {
	New(errorChannel *chan error) (StorageWriter, error)
	Stop()
	LastBlockNumber() int64
	UploadRecords(records []Record) error // last saved block, error
}
type StateChangeRecord struct {
	BlockNumber int64    `avro:"block_number"`
	Timestamp   int64    `avro:"timestamp"`
	Contract    string   `avro:"contract"`
	Location    string   `avro:"location"`
	Position    *string  `avro:"position"`
	Args        []string `avro:"args"`
	Value       string   `avro:"value"`
}
type AccountBalanceRecord struct {
	BlockNumber int64  `avro:"block_number"`
	Timestamp   int64  `avro:"timestamp"`
	Account     string `avro:"account"`
	Balance     string `avro:"balance"`
}

type Record struct {
	StateChange    *StateChangeRecord
	AccountBalance *AccountBalanceRecord
}

func Partition[T any](n int, arr []T) [][]T {
	parts := [][]T{}
	for i := 0; i < n; i++ {
		parts = append(parts, []T{})
	}
	size := len(arr)
	s := 0
	l := size / n
	z := size - n*l
	e := l
	for i := 0; i < n; i++ {
		if z > 0 {
			e += 1
		}
		if e > size {
			parts[i] = arr[s:]
		} else {
			parts[i] = arr[s:e]
			s += l
			e += l
			if z > 0 {
				s += 1
				z -= 1
			}
		}
	}
	return parts
}
