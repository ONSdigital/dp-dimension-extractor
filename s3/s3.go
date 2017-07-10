package s3

import (
	"bytes"
	"strings"

	"github.com/ONSdigital/go-ns/log"
	"gopkg.in/amz.v1/aws"
	"gopkg.in/amz.v1/s3"
)

// GetCSV gets a csv file from S3 bucket
func GetCSV(s3URL string) (*bytes.Reader, error) {
	urlSplit := strings.SplitAfterN(strings.TrimPrefix(s3URL, "s3://"), "/", 2)
	bucketName := urlSplit[0]
	bucketName = trimSuffix(bucketName, "/")
	path := urlSplit[1]

	log.Info("bucketname and path are: \n", log.Data{"bucket-name": bucketName, "path": path})

	auth, err := aws.EnvAuth()
	if err != nil {
		return nil, err
	}
	conn := s3.New(auth, aws.EUWest)
	buck := conn.Bucket(bucketName)

	byteData, err := buck.Get(path)
	if err != nil {
		return nil, err
	}

	data := bytes.NewReader(byteData)

	return data, nil
}

func trimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}
