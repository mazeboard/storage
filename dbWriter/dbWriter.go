// Taoufik 03/2023
package dbWriter

import (
	"errors"
	"github.com/mazeboard/storage/utils"
)

type dbWriter struct {
	utils.StorageWriter
	writeData       func(string)
	lastBlockNumber int64
}

func New(errorChannel *chan error) (utils.StorageWriter, error) {
	return nil, errors.New("not implemented")
}
func (w *dbWriter) LastBlockNumber() int64 {
	return w.lastBlockNumber
}

func (w *dbWriter) UploadRecords(records []utils.Record) error {
	return errors.New("dbWriter not implemented")
}
