// Taoufik 03/2023
package s3Writer

import (
	"errors"
	"fmt"
	"os"
	"path"
	"storage/utils"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// aws s3
var s3Services = []*s3.S3{}
var s3Bucket = aws.String("/haas-data-dev/data/dev/ethereum/storage")
var awsRegion = "eu-west-1"
var awsProfile = "649502643044_DeveloperAccess"
var s3FilesContent = map[string][]string{}

var nconnections = 20
var connectionLocks []sync.RWMutex

for i := 0; i < nconnections; i++ {
s3Services = append(s3Services, s3.New(session.Must(session.NewSessionWithOptions(session.Options{
SharedConfigState: session.SharedConfigEnable,
Profile:           awsProfile,
Config:            aws.Config{Region: &awsRegion},
}))))
}

func uploadFileToS3(s3Svc *s3.S3, file string) error {
	f, err := os.OpenFile(file, os.O_RDONLY, 0)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to open file %s", file))
	} else {
		defer f.Close()
		_, err = s3Svc.PutObject(&s3.PutObjectInput{
			Bucket: s3Bucket,
			Key:    aws.String(path.Base(file)),
			Body:   f,
		})
		if err != nil {
			return errors.New(fmt.Sprintf("failed to upload S3 %s", file))
		} else {
			if err = os.Remove(file); err != nil {
				return errors.New(fmt.Sprintf("failed to remove file %s", file))
			}
		}
	}
	return nil
}

func uploadFilesS3() error {
	// TODO use s3FilesContent map[string][]string
	files := []string{}
	parts := utils.Partition(nconnections, files)
	var wg sync.WaitGroup
	wg.Add(nconnections)
	var err error
	for i := 0; i < nconnections; i++ {
		go func(i int, files []string) {
			connectionLocks[i].Lock()
			defer connectionLocks[i].Unlock()
			defer wg.Done()
			s3Svc := s3Services[i]
			for _, file := range files {
				err = uploadFileToS3(s3Svc, file)
				return
			}
		}(i, parts[i])
	}
	wg.Wait()
	return err
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

func uploadAccountBalance(record *utils.AccountBalanceRecord) []string {
	return []string{}
}
func uploadStateChange(record *utils.StateChangeRecord) []string {
	return []string{}
}
func (*S3Writer) UploadRecords(records []utils.Record) error {
	for _, record := range records {
		if record.AccountBalance != nil {
			rec := record.AccountBalance
			t := time.Unix(rec.Timestamp, 0)
			file := fmt.Sprintf("%s/%d/%d/%d/%d/balance.csv", rec.Account, t.Year(), t.Month(), t.Day(), t.Hour())
			rows := uploadAccountBalance(rec)
			if v, ok := s3FilesContent[file]; ok {
				s3FilesContent[file] = append(v, rows...)
			} else {
				s3FilesContent[file] = rows
			}
		} else if record.StateChange != nil {
			rec := record.StateChange
			t := time.Unix(rec.Timestamp, 0)
			var file string
			if rec.Position != nil {
				file = fmt.Sprintf("%s/%d/%d/%d/%d/%d/value.csv", rec.Contract, *rec.Position, t.Year(), t.Month(), t.Day(), t.Hour())
			} else {
				file = fmt.Sprintf("%s/location/%d/%d/%d/%d/value.csv", rec.Contract, t.Year(), t.Month(), t.Day(), t.Hour())
			}
			rows := uploadStateChange(rec)
			if v, ok := s3FilesContent[file]; ok {
				s3FilesContent[file] = append(v, rows...)
			} else {
				s3FilesContent[file] = rows
			}
		} else {
			return errors.New(fmt.Sprintf("invalid record type %v", record))
		}
	}
	return uploadFilesS3()
}
