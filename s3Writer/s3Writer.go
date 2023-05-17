// Taoufik 03/2023
package s3Writer

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/mazeboard/storage/utils"
	"io"
	"log"
	"os"
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
type s3Writer struct {
	utils.StorageWriter
	s3Services      []*s3.S3
	bucket          string
	prefix          string
	region          string
	profile         string
	nconnections    int
	connectionLocks []sync.RWMutex
	files           map[string]*FileRecord
	filesLock       sync.RWMutex
	filesS3Svc      *s3.S3
	uploader        *s3manager.Uploader
	running         bool
	stopped         chan bool
	lastBlockNumber int64 // last block saved to s3
	dataFile        string
	errorChannel    *chan error
	retries         int
}

func New(errorChannel *chan error) (utils.StorageWriter, error) {
	bucket := os.Getenv("STORAGE_AWS_S3_BUCKET") //
	prefix := os.Getenv("STORAGE_AWS_S3_PREFIX")
	region := os.Getenv("AWS_REGION") // "eu-west-1"
	//profile := os.Getenv("AWS_PROFILE") // "649502643044_DeveloperAccess"
	if bucket == "" || region == "" {
		return nil, errors.New("must define environement variables STORAGE_AWS_S3_BUCKET, AWS_REGION")
	}
	w := s3Writer{
		errorChannel:    errorChannel,
		nconnections:    128,
		connectionLocks: []sync.RWMutex{},
		bucket:          bucket,
		prefix:          prefix,
		files:           make(map[string]*FileRecord),
		filesLock:       sync.RWMutex{},
		running:         true,
		stopped:         make(chan bool),
		lastBlockNumber: 0,
		retries:         10,
	}
	var dir string
	var err error
	if dir, err = os.UserHomeDir(); err == nil {
		w.dataFile = dir + "/.s3WriterData"
	}

	w.createConnections()
	w.readData()
	go w.watch()
	return &w, nil
}

func (w *s3Writer) createConnections() {
	for i := 0; i < w.nconnections; i++ {
		w.connectionLocks = append(w.connectionLocks, sync.RWMutex{})
		w.s3Services = append(w.s3Services, s3.New(w.createSession()))
	}
}

func (w *s3Writer) createSession() *session.Session {
	if sess, err := session.NewSessionWithOptions(session.Options{
		//SharedConfigState: session.SharedConfigEnable,
		//Profile:           w.profile,
		Config: aws.Config{Region: &w.region},
	}); err != nil {
		*w.errorChannel <- err
		panic(err)
	} else {
		return sess
	}
}
func (w *s3Writer) recreateConnection(s3SvcIndex int) {
	w.s3Services[s3SvcIndex] = s3.New(w.createSession())
}

func (w *s3Writer) LastBlockNumber() int64 {
	return w.lastBlockNumber
}

func (w *s3Writer) readData() {
	buf, err := os.ReadFile(w.dataFile)
	if err == nil {
		data := string(buf)
		if bn, err := strconv.Atoi(data); err == nil {
			w.lastBlockNumber = int64(bn)
		}
	}
}
func (s *s3Writer) writeData(data string) {
	err := os.WriteFile(s.dataFile, []byte(data), 0755)
	if err != nil {
		log.Printf("failed to save block number - error %s\n", err)
	}
}
func (w *s3Writer) Stop() {
	w.running = false
	<-w.stopped
}
func (w *s3Writer) watch() {
	log.Println("s3Writer watch started")
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
			*w.errorChannel <- errors.New(fmt.Sprintf("s3Writer save files failed - error %s\n", err))
			w.stopped <- true
			return
		} else {
			if len(saveFiles) > 0 {
				w.writeData(fmt.Sprintf("%d", w.lastBlockNumber))
			}
			log.Printf("s3Writer saved files %d (%d) - lastBlock %d - elapsed %s\n", len(saveFiles), len(w.files), w.lastBlockNumber, time.Since(start))
		}
		if len(w.files) == 0 && !w.running {
			w.filesLock.Unlock()
			w.stopped <- true
			return
		}
		w.filesLock.Unlock()
	}
}

func (w *s3Writer) getObject(s3SvcIndex int, file string) (io.ReadCloser, error) {
	s3Svc := w.s3Services[s3SvcIndex]
	if rawObject, err := s3Svc.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(w.bucket),
			Key:    aws.String(file),
		}); err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				return nil, nil
			}
		}
		time.Sleep(5 * time.Second)
		log.Printf("s3Writer recreate connection %d - error %s\n", s3SvcIndex, err)
		w.recreateConnection(s3SvcIndex)
		return w.getObject(s3SvcIndex, file)
	} else {
		return rawObject.Body, nil
	}
}
func (w *s3Writer) putObject(s3SvcIndex int, file string, body []byte) error {
	s3Svc := w.s3Services[s3SvcIndex]
	if _, err := s3Svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(file),
		Body:   bytes.NewReader(body),
	}); err != nil {
		time.Sleep(5 * time.Second)
		log.Printf("s3Writer recreate connection %d - error %s\n", s3SvcIndex, err)
		w.recreateConnection(s3SvcIndex)
		return w.putObject(s3SvcIndex, file, body)
	}
	return nil
}
func (w *s3Writer) saveFiles(records []*FileRecord) error {
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
					if err = w.putObject(i, rec.file, buf); err != nil {
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
func (w *s3Writer) distinctKey(content []string) []string {
	ss := make(map[string]string)
	for _, row := range content {
		i := strings.IndexByte(row, ',')
		ss[row[:i]] = row
	}
	res := []string{}
	for _, row := range ss {
		res = append(res, row)
	}
	return res
}
func (w *s3Writer) distinct(rec *FileRecord) []byte {
	r := w.distinctKey(rec.content)
	return []byte(strings.Join(r, "\n"))
}

func (w *s3Writer) UploadRecords(records []utils.Record) error {
	w.filesLock.Lock()
	defer w.filesLock.Unlock()
	fileRecords := []*FileRecord{}
	for _, record := range records {
		var month string
		if record.AccountBalance != nil {
			rec := record.AccountBalance
			t := time.Unix(rec.Timestamp, 0)
			month = fmt.Sprintf("%d/%02d", t.Year(), t.Month()) // by hour creates a huge number of files
			file := fmt.Sprintf("%s%s/%s/balance.csv", w.prefix, rec.Account, month)
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
				file := fmt.Sprintf("%s%s/s%s/%s/value.csv", w.prefix, rec.Contract, rec.Location, month)
				fileRecord = w.addRow(rec.BlockNumber, file, w.stateChangeRow(rec))
			} else {
				month = fmt.Sprintf("%d/%02d", t.Year(), t.Month())
				file := fmt.Sprintf("%s%s/p%s/%s/value.csv", w.prefix, rec.Contract, *rec.Position, month)
				fileRecord = w.addRow(rec.BlockNumber, file, w.stateChangeRow(rec))
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

func (w *s3Writer) addRow(blockNumber int64, file string, row string) *FileRecord {
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
		return &record
	}
	return nil
}

func (w *s3Writer) loadFile(i int, record *FileRecord) error {
	if body, err := w.getObject(i, record.file); err != nil {
		return err
	} else {
		if body != nil {
			buf := new(bytes.Buffer)
			if _, err := buf.ReadFrom(body); err != nil {
				return err
			} else {
				record.content = append(strings.Split(buf.String(), "\n"), record.content...)
			}
		}
		return nil
	}
}

func (w *s3Writer) loadFiles(fileRecords []*FileRecord) error {
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
				e := w.loadFile(i, record)
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

func (w *s3Writer) accountBalanceRow(record *utils.AccountBalanceRecord) string {
	return fmt.Sprintf("%d,%d,%s", record.BlockNumber, record.Timestamp, record.Balance)
}
func (w *s3Writer) stateChangeRow(record *utils.StateChangeRecord) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d,%d,", record.BlockNumber, record.Timestamp))
	value := strings.TrimLeft(record.Value, "0")
	if value == "" {
		value = "0"
	}
	sb.WriteString(value)
	for _, arg := range record.Args {
		arg = strings.TrimLeft(arg, "0")
		if arg == "" {
			arg = "0"
		}
		sb.WriteString("," + arg)
	}
	return sb.String()
}
