package antbird

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"syscall"

	"github.com/kshlm/gogfapi/gfapi"
	"github.com/openstack/swift/go/hummingbird"
)

const METADATA_CHUNK_SIZE = 65536

func GetXAttr(volume *gfapi.Volume, fileNameOrFd interface{}, attr string, value []byte) (int64, error) {
	var err error
	var ret int64

	switch v := fileNameOrFd.(type) {
	case string:
		ret, err = volume.Getxattr(v, attr, value)
	case *gfapi.File:
		ret, err = v.Getxattr(attr, value)
	case gfapi.Fd:
		ret, err = v.Fgetxattr(attr, value)
	}
	return ret, err
}

func RawReadMetadata(volume *gfapi.Volume, fileNameOrFd interface{}) ([]byte, error) {
	var pickledMetadata []byte
	var offset, length int64
	var err error
	offset = 0
	for index := 0; ; index += 1 {
		var metadataName string
		// get name of next xattr
		if index == 0 {
			metadataName = "user.swift.metadata"
		} else {
			metadataName = "user.swift.metadata" + strconv.Itoa(index)
		}
		// get size of xattr
		length, err = GetXAttr(volume, fileNameOrFd, metadataName, nil)
		if err != nil || length <= 0 {
			if err.(syscall.Errno) == syscall.ENODATA && index == 0 {
				// xattr does not exist
				return nil, err
			}
			break
		}
		// grow buffer to hold xattr
		for int64(cap(pickledMetadata)) < offset+length {
			pickledMetadata = append(pickledMetadata, 0)
		}
		pickledMetadata = pickledMetadata[0 : offset+length]
		if _, err := GetXAttr(volume, fileNameOrFd, metadataName, pickledMetadata[offset:]); err != nil {
			return nil, err
		}
		if index == 0 && length < METADATA_CHUNK_SIZE {
			// all metadata is retrieved as single xattr
			break
		}
		offset += length
	}
	return pickledMetadata, nil
}

func ReadMetadata(volume *gfapi.Volume, fileNameOrFd interface{}) (map[string]string, error) {
	pickledMetadata, err := RawReadMetadata(volume, fileNameOrFd)
	if err != nil {
		return nil, err
	}
	v, err := hummingbird.PickleLoads(pickledMetadata)
	if err != nil {
		return nil, err
	}
	if v, ok := v.(map[interface{}]interface{}); ok {
		metadata := make(map[string]string, len(v))
		for mk, mv := range v {
			var mks, mvs string
			if mks, ok = mk.(string); !ok {
				return nil, fmt.Errorf("Metadata key not string: %v", mk)
			} else if mvs, ok = mv.(string); !ok {
				return nil, fmt.Errorf("Metadata value not string: %v", mv)
			}
			metadata[mks] = mvs
		}
		return metadata, nil
	}
	return nil, fmt.Errorf("Unpickled metadata not correct type")
}

func RawWriteMetadata(volume *gfapi.Volume, file interface{}, buf []byte) error {
	var err error
	for index := 0; len(buf) > 0; index++ {
		var metadataName string
		if index == 0 {
			metadataName = "user.swift.metadata"
		} else {
			metadataName = "user.swift.metadata" + strconv.Itoa(index)
		}
		writelen := METADATA_CHUNK_SIZE
		if len(buf) < writelen {
			writelen = len(buf)
		}
		switch v := file.(type) {
		case string:
			err = volume.Setxattr(v, metadataName, buf[0:writelen], 0)
		case *gfapi.File:
			err = v.Setxattr(metadataName, buf[0:writelen], 0)
		case gfapi.Fd:
			err = v.Fsetxattr(metadataName, buf[0:writelen], 0)
		}
		if err != nil {
			return err
		}
		buf = buf[writelen:len(buf)]
	}
	return nil
}

func WriteMetadata(volume *gfapi.Volume, file interface{}, v map[string]string) error {
	return RawWriteMetadata(volume, file, hummingbird.PickleDumps(v))
}

func GenerateObjectMetadata(file *gfapi.File, stat os.FileInfo) (map[string]string, error) {
	ts := float64(float64(stat.ModTime().UnixNano()) / 1000000000)
	hash := md5.New()
	io.Copy(hash, file)
	metadata := map[string]string{
		"name":           file.Name(),
		"X-Timestamp":    strconv.FormatFloat(ts, 'f', 5, 64),
		"Content-Type":   "application/octet-stream",
		"Content-Length": strconv.FormatInt(stat.Size(), 10),
		"ETag":           hex.EncodeToString(hash.Sum(nil)),
	}
	return metadata, nil
}
