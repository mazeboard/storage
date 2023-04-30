package utils

type StorageWriter struct {
}
type StateChangeRecord struct {
	BlockNumber int64   `avro:"block_number"`
	Timestamp   int64   `avro:"timestamp"`
	Contract    string  `avro:"contract"`
	Location    *string `avro:"location"`
	Position    *int    `avro:"position"`
	Arg1        *string `avro:"arg1"`
	Arg2        *string `avro:"arg2"`
	Index1      *int    `avro:"index1"`
	Index2      *int    `avro:"index2"`
	Index3      *int    `avro:"index3"`
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
