package amazons3

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"strings"
)

type AmazonS3 struct {
	Log    *log.Logger
	Svc    *s3.S3
	Region string
	Bucket string
}

type UploadErr struct {
	Code     string
	Message  string
	UploadID string
}

type downloader struct {
	AmazonS3

	File       *os.File
	FileOffset int64
	Err        error
}

type filePart struct {
	Key        string
	Range      string
	Etag       string
	Offset     int64
	Length     int64
	PartNumber int64
	Body       []byte
}

func (self UploadErr) Error() string {
	return fmt.Sprintf("Upload error. Code: %s, Message: %s, Upload ID: %s", self.Code, self.Message, self.UploadID)
}

// String returns the string representation of the error.
// Alias for Error to satisfy the stringer interface.
func (self UploadErr) String() string {
	return self.Error()
}

func (self AmazonS3) GetRegions() []string {
	var AwsRegions = []string{
		"us-east-1",
		"us-west-1",
		"us-west-2",
		"eu-west-1",
		"eu-central-1",
		"ap-southeast-1",
		"ap-southeast-2",
		"ap-northeast-1",
		"sa-east-1",
	}
	return AwsRegions
}

func (self AmazonS3) IsRegionValid(name string) error {
	regions := self.GetRegions()
	sort.Strings(regions)
	i := sort.SearchStrings(regions, name)
	if i < len(regions) && regions[i] == name {
		self.Log.Println("Region valid:", name)
		return nil
	}

	return fmt.Errorf("Failed to validate region: %s", name)
}

func (self AmazonS3) CreateBucket(name string) error {
	buckets, err := self.GetBucketsList()
	if err != nil {
		return err
	}

	sort.Strings(buckets)
	i := sort.SearchStrings(buckets, name)
	if i < len(buckets) && buckets[i] == name {
		self.Log.Println("Bucket already exists:", name)
		return nil
	}

	_, err = self.Svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: &name,
	})
	if err != nil {
		return fmt.Errorf("Failed to create bucket %s: %s", name, err)
	}

	if err = self.Svc.WaitUntilBucketExists(&s3.HeadBucketInput{Bucket: &name}); err != nil {
		return fmt.Errorf("Failed to wait for bucket to exist %s: %s\n", name, err)
	}

	self.Log.Println("Create bucket:", name)
	return nil
}

func (self AmazonS3) CreateFolder(path string) error {
	req := &s3.PutObjectInput{
		Bucket: aws.String(self.Bucket),
		Key:    aws.String(path + "/"),
	}
	_, err := self.Svc.PutObject(req)

	return err
}

// List available buckets
func (self AmazonS3) GetBucketsList() (list []string, err error) {
	result, err := self.Svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return list, fmt.Errorf("Failed to list buckets: %s\n", err)
	}

	for _, bucket := range result.Buckets {
		list = append(list, *bucket.Name)
	}

	self.Log.Println("Get buckets:", list)
	return
}

// List files and folders.
// SubFolder can be ""
func (self AmazonS3) GetBucketFilesList(subFolder string) (list []string, err error) {
	if subFolder != "" {
		if !strings.HasSuffix(subFolder, "/") {
			subFolder = subFolder + "/"
		}
		subFolder = strings.Replace(subFolder, "//", "/", -1)
	}
	result, err := self.Svc.ListObjects(&s3.ListObjectsInput{Bucket: &self.Bucket, Prefix: &subFolder})
	if err != nil {
		return list, fmt.Errorf("Failed to list objects: %s\n", err)
	}
	list = []string{}
	for _, object := range result.Contents {
		list = append(list, strings.TrimPrefix(*object.Key, subFolder))
	}

	self.Log.Println("Get bucket files:", list)
	return
}

// Get file info
// Returns http://docs.aws.amazon.com/sdk-for-go/api/service/s3/#HeadObjectOutput
func (self AmazonS3) GetFileInfo(path string) (resp *s3.HeadObjectOutput, err error) {
	resp, err = self.Svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(self.Bucket), // Required
		Key:    aws.String(path),        // Required
	})

	if err != nil {
		self.Log.Println("Failed to get file info error:", path, err)
		return nil, err
	}
	self.Log.Println("Get file info:", path, resp)

	//_, _ = self.GetFilePartInfo(path, fmt.Sprintf("bytes=%d-%d", 0, s3manager.DefaultUploadPartSize))
	return
}

// Get File Part
// partRange "bytes=0-100"
func (self AmazonS3) GetFilePart(path string, partRange string) (resp *s3.GetObjectOutput, err error) {
	resp, err = self.Svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(self.Bucket), // Required
		Key:    aws.String(path),        // Required
		Range:  aws.String(partRange),
	})

	if err != nil {
		self.Log.Println("Failed to get file part:", path, err)
		return nil, err
	}
	self.Log.Println("Get file part:", path, resp)
	return
}

func (self AmazonS3) Delete(path string) (err error) {
	resp, err := self.Svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(self.Bucket), // Required
		Key:    aws.String(path),        // Required
	})

	if err != nil {
		self.Log.Println("Failed to delete:", path, err)
		return err
	}
	self.Log.Println("Delete path:", path, resp)
	return
}

func (self AmazonS3) Download(fileName, destinationPath string) error {
	fileInfo, err := self.GetFileInfo(fileName)
	if err != nil {
		return fmt.Errorf("Failed to get file %s: %s\n", fileName, err)
	}
	var file *os.File

	if _, err = os.Stat(destinationPath); os.IsNotExist(err) {
		file, err = self.createEmptyFile(destinationPath, *fileInfo.ContentLength)
		if err != nil {
			return fmt.Errorf("Failed to allocate %s bytes on disk for destination file %s: %s\n", *fileInfo.ContentLength, destinationPath, err)
		}
		self.IoClose(file)
		err = os.Remove(destinationPath)
		if err != nil {
			return fmt.Errorf("Failed to remove temporary file %s: %s\n", destinationPath, err)
		}
		file, err = os.Create(destinationPath)
		if err != nil {
			return fmt.Errorf("Failed to create destination file %s: %s\n", destinationPath, err)
		}

	} else {
		file, err = os.OpenFile(destinationPath, os.O_WRONLY, 666)
		if err != nil {
			return fmt.Errorf("Failed to create destination file %s: %s\n", destinationPath, err)
		}
	}
	defer self.IoClose(file)

	downloader := s3manager.NewDownloader(session.New(&aws.Config{Region: aws.String(self.Region)}))
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(self.Bucket),
			Key:    aws.String(fileName),
		})
	if err != nil {
		return fmt.Errorf("Failed to download file %s to destination path %s with error: %s\n", fileName, destinationPath, err)
	}

	self.Log.Printf("Downloaded file %s with size %s\n", file.Name(), numBytes)
	return nil
}

func (self AmazonS3) ResumeDownload(fileName, destinationPath string) error {
	remoteFileInfo, err := self.GetFileInfo(fileName)
	if err != nil {
		return fmt.Errorf("Failed to get file %s: %s\n", fileName, err)
	}

	file, err := os.OpenFile(destinationPath, os.O_WRONLY, 666)
	if err != nil {
		return fmt.Errorf("Failed to create destination file %s: %s\n", destinationPath, err)
	}
	defer self.IoClose(file)

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("Failed to stat destination file %s: %s\n", destinationPath, err)
	}

	if *remoteFileInfo.ContentLength < stat.Size() {
		return fmt.Errorf("Failed to compare size of remote %s and destination file %s: %d <= %d\n", fileName, destinationPath, *remoteFileInfo.ContentLength, stat.Size())
	}

	if *remoteFileInfo.ContentLength == stat.Size() {
		self.Log.Printf("Size of remote %s and destination %s file match: %d == %d. Nothing to do.\n", fileName, destinationPath, *remoteFileInfo.ContentLength, stat.Size())
		return nil
	}

	d := downloader{
		AmazonS3:   self,
		File:       file,
		FileOffset: stat.Size(),
	}

	taskPartChan := make(chan filePart, s3manager.DefaultDownloadConcurrency)
	var wg sync.WaitGroup
	for i := 0; i < s3manager.DefaultUploadConcurrency; i++ {
		wg.Add(1)
		go d.asyncDownloadPart(taskPartChan, &wg)
	}

	partOffset := stat.Size()
	leftBytes := *remoteFileInfo.ContentLength - stat.Size()
	go func() {
		for {
			self.Log.Printf("Resume download: Left bytes %d\n", leftBytes)
			if leftBytes <= s3manager.DefaultDownloadPartSize {
				partRange := fmt.Sprintf("bytes=%d-%d", partOffset, partOffset+leftBytes-1)
				self.Log.Printf("Resume download: File range %s\n", partRange)
				taskPartChan <- filePart{
					Key:    fileName,
					Range:  partRange,
					Offset: partOffset,
					Length: leftBytes,
					Body:   make([]byte, leftBytes),
				}
				close(taskPartChan)
				self.Log.Println("Resume download: All parts send to download. Close channel.")
				return
			}
			fileRange := fmt.Sprintf("bytes=%d-%d", partOffset, partOffset+s3manager.DefaultDownloadPartSize-1)
			self.Log.Printf("Resume download: Part range %s\n", fileRange)
			self.Log.Printf("Resume download: Part offset %d\n", partOffset)

			taskPartChan <- filePart{
				Key:    fileName,
				Range:  fileRange,
				Offset: partOffset,
				Length: s3manager.DefaultDownloadPartSize,
				Body:   make([]byte, s3manager.DefaultDownloadPartSize),
			}
			partOffset = partOffset + s3manager.DefaultDownloadPartSize
			leftBytes = leftBytes - s3manager.DefaultDownloadPartSize
		}
	}()

	wg.Wait()

	return nil
}

func (self *downloader) asyncDownloadPart(taskPartChan <-chan filePart, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		if part, ok := <-taskPartChan; ok {
			if self.Err != nil {
				self.Log.Printf("Failed to start download: %s\n", self.Err)
				return
			}
			self.Log.Printf("Start to download part for key %s: Range: %s, Offset: %d, Length: %d\n", part.Key, part.Range, part.Offset, part.Length)

			resp, err := self.Svc.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(self.Bucket), // Required
				Key:    aws.String(part.Key),    // Required
				Range:  aws.String(fmt.Sprintf("bytes=%d-%d", part.Offset, part.Offset+part.Length-1)),
			})
			self.Log.Printf("Request sent for %s range %s\n", part.Key, part.Range)
			if err != nil {
				self.Log.Printf("Failed to download file %s range %s: %s\n", part.Key, part.Range, err)
				return
			}
			self.Log.Printf("Responce for %s range %s: %s\n", part.Key, part.Range, resp)

			n, err := io.ReadFull(resp.Body, part.Body)
			if err != nil {
				self.Log.Printf("Failed to read responce for %s range %s: %s\n", part.Key, part.Range, err)
				return
			}
			self.IoClose(resp.Body)

			self.Log.Printf("Download %d bytes part range %s for key %s \n", n, part.Range, part.Key)
			self.Log.Printf("File offset: %d\n", self.FileOffset)
			self.Log.Printf("Part offset: %d\n", part.Offset)
			for {
				if self.FileOffset == part.Offset {
					n, err = self.File.WriteAt(part.Body, part.Offset)
					if err != nil {
						self.Err = err
						self.Log.Printf("Failed to write file %s range %s: %s\n", part.Key, part.Range, err)
						return
					}

					self.FileOffset = part.Offset + part.Length
					self.Log.Printf("New file offset: %d\n", self.FileOffset)
					break
				}
				time.Sleep(10 * time.Millisecond)
			}

			self.Log.Printf("Finish write %d bytes part range %s for key %s \n", n, part.Range, part.Key)
		} else {
			self.Log.Println("Download channel closed. Return.")

			return
		}
	}
}

// Upload filePath to destinationPath, where destinationPath contains only folders like /folder/folder2
func (self AmazonS3) Upload(filePath, destinationPath string, useGzip bool) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("Failed to open file %s for upload: %s\n", filePath, err)
	}

	key := destinationPath + "/" + filepath.Base(filePath)

	// Not required, but you could zip the file before uploading it
	// using io.Pipe read/writer to stream gzip'd file contents.
	reader, writer := io.Pipe()

	if useGzip {
		go func() {
			gw := gzip.NewWriter(writer)
			written, err := io.Copy(gw, file)
			if err != nil {
				self.Log.Printf("AmazonS3 Upload gzip io.Copy error: %s\n", err)
			}
			self.Log.Printf("AmazonS3 Upload gzip io.Copy written: %s\n", written)

			self.IoClose(file)
			self.IoClose(gw)
			self.IoClose(writer)
		}()

		key = key + ".gz"
	} else {
		go func() {
			bw := bufio.NewWriter(writer)
			written, err := io.Copy(bw, file)
			if err != nil {
				self.Log.Printf("AmazonS3 Upload buffer io.Copy error: %s\n", err)
			}
			self.Log.Printf("AmazonS3 Upload buffer io.Copy written: %s\n", written)

			self.IoClose(file)
			err = bw.Flush()
			if err != nil {
				self.Log.Printf("bufio flush error: %s\n", err)
			}
			self.IoClose(writer)
		}()
	}

	self.Log.Printf("Upload %s to %s with Gzip: %s\n", filePath, key, useGzip)

	uploader := s3manager.NewUploader(
		session.New(
			&aws.Config{
				Region: aws.String(self.Region),
			},
		),
		func(u *s3manager.Uploader) {
			u.LeavePartsOnError = true // Leave good uploaded parts on Storage in case of failure
		},
	)

	result, err := uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(self.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if multiErr, ok := err.(s3manager.MultiUploadFailure); ok {
			// Process error and its associated uploadID
			self.Log.Printf("Error code: %s, Message: %s, UploadID: %s\n", multiErr.Code(), multiErr.Message(), multiErr.UploadID())
			uploadErr := UploadErr{
				Code:     multiErr.Code(),
				Message:  multiErr.Message(),
				UploadID: multiErr.UploadID(),
			}
			return uploadErr
		}
		return fmt.Errorf("Failed upload file %s: %s\n", filePath, err)
	}

	self.Log.Println("Successfully uploaded to", result.Location)
	return nil
}

func (self AmazonS3) ResumeUpload(filePath, key, uploadId string, useGzip bool) (err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("Failed to open file %s for upload: %s\n", filePath, err)
	}

	// Not required, but you could zip the file before uploading it
	// using io.Pipe read/writer to stream gzip'd file contents.
	pipeReader, writer := io.Pipe()

	if useGzip {
		go func() {
			gw := gzip.NewWriter(writer)
			written, err := io.Copy(gw, file)
			if err != nil {
				self.Log.Printf("AmazonS3 Upload gzip io.Copy error: %s\n", err)
			}
			self.Log.Printf("AmazonS3 Upload gzip io.Copy written: %s\n", written)

			self.IoClose(file)
			self.IoClose(gw)
			self.IoClose(writer)
		}()
	} else {
		go func() {
			bw := bufio.NewWriter(writer)
			written, err := io.Copy(bw, file)
			if err != nil {
				self.Log.Printf("AmazonS3 Upload buffer io.Copy error: %s\n", err)
			}
			self.Log.Printf("AmazonS3 Upload buffer io.Copy written: %s\n", written)

			self.IoClose(file)
			err = bw.Flush()
			if err != nil {
				self.Log.Printf("bufio flush error: %s\n", err)
			}
			self.IoClose(writer)
		}()
	}

	self.Log.Printf("Resume Upload %s to %s with Gzip: %s\n", filePath, key, useGzip)

	resp, err := self.ListParts(key, uploadId)
	if err != nil {
		return fmt.Errorf("Failed to list uploaded parts for key %s of upload id %s: %s\n", key, uploadId, err)
	}

	partQueue := make(chan filePart, s3manager.DefaultUploadConcurrency)
	var wg sync.WaitGroup

	for i := 0; i < s3manager.DefaultUploadConcurrency; i++ {
		wg.Add(1)
		go self.asyncUploadPart(key, uploadId, partQueue, &wg)
	}

	go self.getFileParts(partQueue, pipeReader, resp.Parts)

	self.Log.Println("Wait for all parts are uploading...")
	wg.Wait()

	err = self.CompleteUpload(key, uploadId)
	if err != nil {
		return fmt.Errorf("Failed to complete upload with key %s: %s\n", key, err)
	}

	self.Log.Println("Successfully resumed upload to", key)

	return nil
}

func (self AmazonS3) getFileParts(partChan chan<- filePart, reader io.Reader, uploadedParts []*s3.Part) {
	var lastPartNumber int64
	var offset int64
	lastPartNumber = 1
	offset = 0

	for {
		part := make([]byte, s3manager.DefaultUploadPartSize)
		partSize, errRead := io.ReadFull(reader, part)
		if errRead != nil && errRead != io.EOF && errRead != io.ErrUnexpectedEOF {
			self.Log.Fatalf("Failed to read part number %s from reader at offset %s: %s\n", lastPartNumber, offset, errRead)
		}
		self.Log.Printf("Read bytes %s for part number %d with size: %s\n", partSize, lastPartNumber, len(part))

		if int64(partSize) != s3manager.DefaultUploadPartSize {
			lastPart := make([]byte, partSize)
			copy(lastPart, part)
			part = lastPart
		}

		partEtag, err := self.getPartEtag(part)
		if err != nil {
			self.Log.Fatalf("Failed to get Etag for part number %s with size %s: %s\n", lastPartNumber, partSize, err)
		}

		self.Log.Printf("Part number %s size bytes %s has ETag: %s\n", lastPartNumber, len(part), partEtag)

		if true == self.needToUpload(uploadedParts, lastPartNumber, partEtag) {
			partChan <- filePart{
				Body:       part,
				PartNumber: lastPartNumber,
			}
		}

		offset = offset + int64(len(part))
		lastPartNumber = lastPartNumber + 1

		if errRead == io.EOF || errRead == io.ErrUnexpectedEOF {
			self.Log.Printf("EOF or ErrUnexpectedEOF. All parts are read and send to upload. Last part is %s, offset is %d", lastPartNumber, offset)
			close(partChan)
			return
		}
	}
}

func (self AmazonS3) needToUpload(uploadedParts []*s3.Part, partNumber int64, partEtag string) bool {
	for _, part := range uploadedParts {
		if *part.PartNumber == partNumber {
			self.Log.Printf("Part number %s with ETag %s found\n", *part.PartNumber, string(*part.ETag))

			if *part.ETag == partEtag {
				self.Log.Printf("Match Etag for part number %s with size %s ETag %s == %s.\n", *part.PartNumber, *part.Size, string(*part.ETag), partEtag)
				return false
			} else {
				self.Log.Printf("Mismatch Etag for part number %s with size %s ETag %s != %s. Reuploading...\n", *part.PartNumber, *part.Size, string(*part.ETag), partEtag)
				return true
			}
		}
	}
	self.Log.Printf("Part number %s not found\n", partNumber)

	return true
}

func (self AmazonS3) asyncUploadPart(key string, uploadId string, partChan <-chan filePart, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		if part, ok := <-partChan; ok {
			self.Log.Printf("Start to upload part number %s for key %s\n", part.PartNumber, key)

			_, err := self.Svc.UploadPart(&s3.UploadPartInput{
				Bucket:     aws.String(self.Bucket),    // Required
				Key:        aws.String(key),            // Required
				PartNumber: aws.Int64(part.PartNumber), // Required
				UploadId:   aws.String(uploadId),       // Required
				Body:       bytes.NewReader(part.Body),
			})
			if err != nil {
				self.Log.Printf("Failed to upload part number %s for key %s: %s\n", part.PartNumber, key, err)
				return
			}
			self.Log.Printf("Finished upload part number %s for key %s\n", part.PartNumber, key)
		} else {
			self.Log.Println("Upload channel closed. Return.")

			return
		}
	}

	return
}

func (self AmazonS3) uploadPart(key string, partNumber int64, uploadId string, body []byte) (err error) {
	self.Log.Printf("Start upload part number %d of key %s for upload id %s\n", partNumber, key, uploadId)

	_, err = self.Svc.UploadPart(&s3.UploadPartInput{
		Bucket:     aws.String(self.Bucket), // Required
		Key:        aws.String(key),         // Required
		PartNumber: aws.Int64(partNumber),   // Required
		UploadId:   aws.String(uploadId),    // Required
		Body:       bytes.NewReader(body),
	})

	return
}

// List bucket's unfinished uploads
// http://docs.aws.amazon.com/sdk-for-go/api/service/s3/#MultipartUpload
func (self AmazonS3) ListUnfinishedUploads() ([]*s3.MultipartUpload, error) {
	resp, err := self.Svc.ListMultipartUploads(&s3.ListMultipartUploadsInput{
		Bucket: aws.String(self.Bucket), // Required
	})
	if err != nil {
		return nil, fmt.Errorf("Failed list unfinised uploads: %s\n", err)
	}
	self.Log.Println("List bucket's unfinished uploads", resp)

	return resp.Uploads, nil
}

// List parts of unfinished uploads
// http://docs.aws.amazon.com/sdk-for-go/api/service/s3/#ListPartsOutput
// Parts []*Part - can be empty
// Part.PartNumber
// Part.Size
func (self AmazonS3) ListParts(key string, uploadId string) (resp *s3.ListPartsOutput, err error) {
	resp, err = self.Svc.ListParts(&s3.ListPartsInput{
		Bucket:   aws.String(self.Bucket), // Required
		Key:      aws.String(key),         // Required
		UploadId: aws.String(uploadId),    // Required
	})
	self.Log.Printf("List parts for key %s of upload id %s: %s\n", key, uploadId, resp)

	return
}

// Abort upload
func (self AmazonS3) AbortUpload(key string, uploadId string) (err error) {
	resp, err := self.Svc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(self.Bucket), // Required
		Key:      aws.String(key),         // Required
		UploadId: aws.String(uploadId),    // Required
	})
	self.Log.Printf("Abort upload for key %s of upload id %s: %s\n", key, uploadId, resp)

	return
}

// Complete upload
func (self AmazonS3) CompleteUpload(key string, uploadId string) (err error) {
	respParts, err := self.ListParts(key, uploadId) // Just for debug

	var completedParts []*s3.CompletedPart
	for _, part := range respParts.Parts {
		completedPart := &s3.CompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		}
		completedParts = append(completedParts, completedPart)
	}
	resp, err := self.Svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(self.Bucket), // Required
		Key:      aws.String(key),         // Required
		UploadId: aws.String(uploadId),    // Required
		MultipartUpload: &s3.CompletedMultipartUpload{ // Required
			Parts: completedParts,
		},
	})
	if err != nil {
		self.Log.Printf("Failed to complete upload for key %s of upload id %s: %s\n", key, uploadId, err)
		return
	}
	self.Log.Printf("Complete upload for key %s of upload id %s: %s\n", key, uploadId, resp)

	return
}

func (self AmazonS3) IoClose(c io.Closer) {
	err := c.Close()
	if err != nil {
		self.Log.Println(err)
	}
}

func (self AmazonS3) getPartEtag(part []byte) (etag string, err error) {
	hasher := md5.New()
	_, err = hasher.Write(part)
	if err != nil {
		return "", fmt.Errorf("Failed to write part to hasher: %s", err)
	}
	etag = fmt.Sprintf("\"%s\"", hex.EncodeToString(hasher.Sum(nil)))

	return
}

func (self AmazonS3) createEmptyFile(filePath string, size int64) (f *os.File, err error) {
	f, err = os.Create(filePath)
	if err != nil {
		return nil, err
	}

	chunkSize := int64(1024 * 1024 * 25)

	self.Log.Printf("Start creating empty file %s with size %s\n", filePath, size)
	for {
		if size <= chunkSize {
			s := make([]byte, size)
			n, err := f.Write(s)
			self.Log.Printf("Bytes written %s\n", n)
			_, err = f.Seek(0, 0)
			return f, err
		}

		size = size - chunkSize

		s := make([]byte, chunkSize)

		n, err := f.Write(s)
		if err != nil {
			return nil, err
		}
		self.Log.Printf("Bytes written %s\n", n)
	}
}
