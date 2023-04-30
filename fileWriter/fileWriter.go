// Taoufik 03/2023
package fileWriter

import (
	"errors"
	"fmt"
	"github.com/mazeboard/storage/utils"
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type FileRecord struct {
	file           string
	content        []string
	maxBlockNumber int64
}
type fileWriter struct {
	utils.StorageWriter
	datadir         string
	nconnections    int
	connectionLocks []sync.RWMutex
	files           map[string]*FileRecord
	filesLock       sync.RWMutex
	running         bool
	stopped         chan bool
	lastBlockNumber int64 // last block saved to s3
	dataFile        string
	errorChannel    *chan error
}

func New(errorChannel *chan error) (utils.StorageWriter, error) {
	datadir := os.Getenv("STORAGE_DATADIR")
	if datadir == "" {
		if dir, err := os.UserHomeDir(); err == nil {
			datadir = dir + "/.storage"
		} else {
			return nil, err
		}
	}
	stat, err := os.Stat(datadir)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.Mkdir(datadir, 0755); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		if !stat.IsDir() {
			return nil, errors.New(fmt.Sprintf("%s is not a directory", datadir))
		}
	}

	w := fileWriter{
		errorChannel:    errorChannel,
		nconnections:    32,
		connectionLocks: []sync.RWMutex{},
		datadir:         datadir,
		files:           make(map[string]*FileRecord),
		filesLock:       sync.RWMutex{},
		running:         true,
		stopped:         make(chan bool),
		lastBlockNumber: 0,
	}
	w.dataFile = datadir + "/.fileWriterData"

	for i := 0; i < w.nconnections; i++ {
		w.connectionLocks = append(w.connectionLocks, sync.RWMutex{})
	}
	w.readData()
	go w.watch()
	return &w, nil
}

func (w *fileWriter) LastBlockNumber() int64 {
	return w.lastBlockNumber
}

func (w *fileWriter) readData() {
	buf, err := os.ReadFile(w.dataFile)
	if err == nil {
		data := string(buf)
		if bn, err := strconv.Atoi(data); err == nil {
			w.lastBlockNumber = int64(bn)
		}
	}
}
func (s *fileWriter) writeData(data string) {
	err := os.WriteFile(s.dataFile, []byte(data), 0755)
	if err != nil {
		log.Printf("failed to save block number - error %s\n", err)
	}
}
func (w *fileWriter) Stop() {
	w.running = false
	<-w.stopped
}
func (w *fileWriter) watch() {
	log.Println("fileWriter watch started")
	delay := 30 * time.Second
	for {
		// TODO if current month then save records that are old by 1 minute
		time.Sleep(delay)
		saveFiles := []*FileRecord{}
		w.filesLock.Lock()
		start := time.Now()
		var maxBlockNumber int64 = 0
		for _, rec := range w.files {
			if rec.maxBlockNumber > maxBlockNumber {
				maxBlockNumber = rec.maxBlockNumber
			}
		}
		for _, rec := range w.files {
			if !w.running || rec.maxBlockNumber < maxBlockNumber-2048 {
				if rec.maxBlockNumber > w.lastBlockNumber {
					w.lastBlockNumber = rec.maxBlockNumber
				}
				saveFiles = append(saveFiles, rec)
				delete(w.files, rec.file)
			}
		}
		if err := w.saveFiles(saveFiles); err != nil {
			w.filesLock.Unlock()
			*w.errorChannel <- errors.New(fmt.Sprintf("fileWriter save files failed - error %s\n", err))
			return
		} else {
			if len(saveFiles) > 0 {
				w.writeData(fmt.Sprintf("%d", w.lastBlockNumber))
			}
			log.Printf("fileWriter saved files %d (%d) - lastBlock %d - elapsed %s\n", len(saveFiles), len(w.files), w.lastBlockNumber, time.Since(start))
		}
		if len(w.files) == 0 && !w.running {
			w.filesLock.Unlock()
			w.stopped <- true
			return
		}
		w.filesLock.Unlock()
	}
}

func (w *fileWriter) writeFile(file string, content []byte) error {
	err := os.MkdirAll(path.Dir(file), 0755)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(file+"_temp", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	} else {
		defer func(f *os.File) {
			err := f.Close()
			if err != nil {
				log.Printf("failed to close file %s", file)
			}
		}(f)
		_, err = f.Write(content)
		if err != nil {
			return err
		}
		err := os.Rename(file+"_temp", file)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *fileWriter) saveFiles(records []*FileRecord) error {
	parts := utils.Partition(w.nconnections, records)
	var wg sync.WaitGroup
	wg.Add(w.nconnections)
	var err error
	for i := 0; i < w.nconnections; i++ {
		go func(i int, records []*FileRecord) {
			w.connectionLocks[i].Lock()
			defer w.connectionLocks[i].Unlock()
			defer wg.Done()
			for _, rec := range records {
				if err == nil {
					buf := w.distinct(rec)
					e := w.writeFile(rec.file, buf)
					if e != nil {
						err = e
						return
					}
				}
			}
		}(i, parts[i])
	}
	wg.Wait()
	return err
}

// rows are ordered by block numbers
func (w *fileWriter) distinctKey(content []string, key func(s string) string) []string {
	ss := make(map[string]string)
	for _, row := range content {
		ss[key(row)] = row
	}
	res := []string{}
	for _, row := range ss {
		res = append(res, row)
	}
	return res
}
func (w *fileWriter) distinct(rec *FileRecord) []byte {
	isBalance := strings.HasSuffix(rec.file, "balance.csv")
	r := w.distinctKey(rec.content, func(s string) string {
		if isBalance {
			// for balance.csv block_number, timestamp, balance
			//    key is block_number
			i := strings.IndexByte(s, ',')
			sbn := s[:i]
			return sbn
		} else {
			// for value.csv block_number, timestamp, location, value, args...
			//    key is block_number, location
			i := strings.IndexByte(s, ',')
			sbn := s[:i]
			j := strings.IndexByte(s[i:], ',')
			k := strings.IndexByte(s[j:], ',')
			return sbn + s[j:k]
		}
	})
	return []byte(strings.Join(r, "\n"))
}

func (w *fileWriter) UploadRecords(records []utils.Record) error {
	w.filesLock.Lock()
	defer w.filesLock.Unlock()
	fileRecords := []*FileRecord{}
	for _, record := range records {
		var month string
		if record.AccountBalance != nil {
			rec := record.AccountBalance
			t := time.Unix(rec.Timestamp, 0)
			month = fmt.Sprintf("%d%02d", t.Year(), t.Month()) // by hour creates a huge number of files
			file := fmt.Sprintf("%s/%s/%s_balance.csv", w.datadir, rec.Account, month)
			fileRecord := w.addRow(rec.BlockNumber, file, w.accountBalanceRow(rec))
			if fileRecord != nil {
				fileRecords = append(fileRecords, fileRecord)
			}
		} else if record.StateChange != nil {
			rec := record.StateChange
			t := time.Unix(rec.Timestamp, 0)
			var fileRecord *FileRecord
			if rec.Position == nil {
				month = fmt.Sprintf("%d/%02d", t.Year(), t.Month())
				file := fmt.Sprintf("%s%s/%s/%s/value.csv", w.datadir, rec.Contract, month, rec.Location)
				fileRecord = w.addRow(rec.BlockNumber, file, w.stateChangeRow(rec))
			} else {
				month = fmt.Sprintf("%d/%02d", t.Year(), t.Month())
				file := fmt.Sprintf("%s%s/%s/%s/value.csv", w.datadir, rec.Contract, *rec.Position, month)
				fileRecord = w.addRow(rec.BlockNumber, file, w.stateChangeRow(rec))
			}
			if fileRecord != nil {
				fileRecords = append(fileRecords, fileRecord)
			}
			if fileRecord != nil {
				fileRecords = append(fileRecords, fileRecord)
			}
		} else {
			return errors.New(fmt.Sprintf("invalid record type %v", record))
		}
	}
	return w.loadFiles(fileRecords)
}

func (w *fileWriter) addRow(blockNumber int64, file string, row string) *FileRecord {
	if rec, ok := w.files[file]; ok {
		if rec.maxBlockNumber < blockNumber {
			rec.maxBlockNumber = blockNumber
		}
		rec.content = append(rec.content, row)
	} else {
		record := FileRecord{
			file:           file,
			content:        []string{row},
			maxBlockNumber: blockNumber,
		}
		w.files[file] = &record
		// loadFiles will load existing file from storage and add row
		return &record
	}
	return nil
}

func (w *fileWriter) loadFile(record *FileRecord) error {
	if _, err := os.Stat(record.file); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	} else {
		f, err := os.OpenFile(record.file, os.O_RDONLY, 0755)
		if err != nil {
			return err
		} else {
			defer func(f *os.File) {
				err := f.Close()
				if err != nil {
					log.Printf("failed to close file %s", record.file)
				}
			}(f)
			b := make([]byte, 2048, 2048)
			oldContent := ""
			for {
				if n, err := f.Read(b); err != nil {
					if err == io.EOF {
						break
					}
					return err
				} else {
					oldContent += string(b[:n])
				}
			}
			record.content = append(strings.Split(oldContent, "\n"), record.content...)
			return nil
		}
	}
}

func (w *fileWriter) loadFiles(fileRecords []*FileRecord) error {
	start := time.Now()
	parts := utils.Partition(w.nconnections, fileRecords)
	var wg sync.WaitGroup
	wg.Add(w.nconnections)
	var err error
	for i := 0; i < w.nconnections; i++ {
		go func(i int, records []*FileRecord) {
			w.connectionLocks[i].Lock()
			defer w.connectionLocks[i].Unlock()
			defer wg.Done()
			for _, record := range records {
				if err != nil {
					return
				}
				e := w.loadFile(record)
				if e != nil {
					err = e
					return
				}
			}
		}(i, parts[i])
	}
	wg.Wait()
	if err != nil {
		return err
	}
	log.Printf("loadFiles %d (%d) - elapsed %s\n", len(fileRecords), len(w.files), time.Since(start))
	return nil
}

// save records to files and upload files to s3
// save all file names into table files.lst (help in speeding up queries)
// path structure for state changes:
//
//	    s3://<bucket>/<contract>/<position>/<year>/<month>/<day>/<hour>/value.csv
//		where value.csv file format: block_number, timestamp, [key,]+ value
//
//	    for some locations we could not extract position/[keys]+, so we save them in the following s3 structure
//	    s3://<bucket>/<contract>/location/<year>/<month>/<day>/<hour>/value.csv
//		where value.csv file format: block_number, timestamp, location, value
//
// path structure for account balances: s3://<bucket>/<account>/<year>/<month>/<day>/<hour>/balance.csv
//
//	where balance.csv file format: block_number, timestamp, balance
//
// key names in values.csv depend on the variable of the contract (ie. position)

func (w *fileWriter) accountBalanceRow(record *utils.AccountBalanceRecord) string {
	return fmt.Sprintf("%d,%d,%s", record.BlockNumber, record.Timestamp, record.Balance)
}
func (w *fileWriter) stateChangeRow(record *utils.StateChangeRecord) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d,%d,", record.BlockNumber, record.Timestamp))
	if record.Position == nil {
		sb.WriteString(record.Value)
	} else {
		if len(record.Args) > 0 {
			sb.WriteString(record.Value + ",")
			for _, arg := range record.Args[:len(record.Args)-1] {
				sb.WriteString(arg + ",")
			}
			sb.WriteString(record.Args[len(record.Args)-1])
		} else {
			sb.WriteString(record.Value)
		}
	}
	return sb.String()
}
