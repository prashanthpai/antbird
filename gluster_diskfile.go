package antbird

import (
	"errors"
	"fmt"
	"io"
	"log/syslog"
	"net/http"
	"os"
	"path"
	"strconv"
	"syscall"

	"github.com/kshlm/gogfapi/gfapi"
	"github.com/openstack/swift/go/hummingbird"
	"github.com/satori/go.uuid"
)

// Get a list of devices from ring file and virtual mount them using libgfapi
func SetupGlusterDiskFile(serverconf *hummingbird.IniFile, logger *syslog.Writer) (map[string]interface{}, error) {
	hashPathPrefix, hashPathSuffix, _ := hummingbird.GetHashPrefixAndSuffix()
	objRing, _ := hummingbird.GetRing("object", hashPathPrefix, hashPathSuffix)
	bindPort := int(serverconf.GetInt("app:object-server", "bind_port", 6000))
	localDevices, _ := objRing.LocalDevices(bindPort)

	globals := make(map[string]interface{})
	globals["glusterVolumes"] = make(map[string]*gfapi.Volume)

	var ret int
	var err error
	for _, dev := range localDevices {
		// TODO: Error handling. All the following APIs return int
		globals["glusterVolumes"].(map[string]*gfapi.Volume)[dev.Device] = new(gfapi.Volume)

		ret = globals["glusterVolumes"].(map[string]*gfapi.Volume)[dev.Device].Init("localhost", dev.Device)
		if ret < 0 {
			return nil, errors.New(fmt.Sprintf("Volume %s: Init() failed with ret = %d", dev.Device, ret))
		}

		ret, err = globals["glusterVolumes"].(map[string]*gfapi.Volume)[dev.Device].SetLogging("", gfapi.LogDebug)
		if ret < 0 {
			//FIXME: There's a bug in SetLogging: err != nil even when ret = 0
			return nil, errors.New(fmt.Sprintf("Volume %s: SetLogging() failed with ret = %d, error = %s", dev.Device, ret, err.Error()))
		}

		ret = globals["glusterVolumes"].(map[string]*gfapi.Volume)[dev.Device].Mount()
		if ret < 0 {
			return nil, errors.New(fmt.Sprintf("Volume %s: Mount() failed with ret = %d", dev.Device, ret))
		}

		logger.Info(fmt.Sprintf("GlusterFS volume %s sucessfully virtual mounted.", dev.Device))
	}

	return globals, nil
}

// The following struct and member fields are implementation specific
type GlusterDiskFile struct {
	dataFile string
	dataDir  string
	request  *http.Request
	vars     map[string]string
	stat     os.FileInfo
	file     *gfapi.File
	volume   *gfapi.Volume

	// PUT
	tempFileName    string
	commitSucceeded bool
}

func (d *GlusterDiskFile) Init(globals map[string]interface{}, request *http.Request, vars map[string]string) error {
	d.request = request
	d.vars = vars

	d.volume = globals["glusterVolumes"].(map[string]*gfapi.Volume)[vars["device"]]
	d.dataFile = "/" + vars["account"] + "/" + vars["container"] + "/" + vars["obj"]
	d.dataDir = path.Dir(d.dataFile)
	d.stat, _ = d.volume.Stat(d.dataFile)
	return nil
}

func (d *GlusterDiskFile) GetObjectState() hummingbird.ObjectState {
	if d.stat != nil {
		return hummingbird.ObjectConsumable
	}
	return hummingbird.ObjectNotExists
}

func (d *GlusterDiskFile) Open(a ...interface{}) (io.ReadSeeker, error) {
	var err error
	d.file, err = d.volume.Open(d.dataFile)
	return d.file, err
}

func (d *GlusterDiskFile) GetMetadata() (map[string]string, error) {
	var metadata map[string]string
	var err error

	if d.file != nil {
		metadata, err = ReadMetadata(d.volume, d.file) // GET, HEAD
	} else {
		metadata, err = ReadMetadata(d.volume, d.dataFile) //PUT, DELETE
	}

	if err != nil {
		if err.(syscall.Errno) == syscall.ENODATA {
			// Generate object metadata
			metadata, _ = GenerateObjectMetadata(d.file, d.stat)
			d.PutMetadata(metadata)
			d.file.Seek(int64(os.SEEK_SET), 0)
			err = nil
		}
	} else {
		// check if file was modified from backend over other interfaces
		if cL, _ := strconv.ParseInt(metadata["Content-Length"], 10, 64); d.stat.Size() != cL {
			metadata, _ = GenerateObjectMetadata(d.file, d.stat)
			d.PutMetadata(metadata)
			d.file.Seek(int64(os.SEEK_SET), 0)
		}
	}
	return metadata, err
}

func (d *GlusterDiskFile) PutMetadata(metadata map[string]string) error {
	return WriteMetadata(d.volume, d.file, metadata)
}

func (d *GlusterDiskFile) Commit(a ...interface{}) error {
	d.file.Sync()

	err := d.file.Close()
	if err != nil {
		hummingbird.GetLogger(d.request).LogError("file.Close() failed: %s", err.Error())
		return err
	}
	d.file = nil

	err = d.volume.Rename(d.tempFileName, d.dataFile)
	if err != nil {
		hummingbird.GetLogger(d.request).LogError("Error renaming file: %s -> %s", d.tempFileName, d.dataFile)
		return err
	}
	d.commitSucceeded = true

	return nil
}

func (d *GlusterDiskFile) Quarrantine(a ...interface{}) error {
	return nil
}

func (d *GlusterDiskFile) Close(a ...interface{}) error {

	if d.file != nil {
		d.file.Close()
		d.file = nil
	}

	if !d.commitSucceeded && d.request.Method == "PUT" {
		// PUT did not finish, delete the incomplete temp file
		// TODO: Remove empty directories all the way up to parent
		d.volume.Unlink(d.tempFileName)
	}

	return nil
}

func (d *GlusterDiskFile) Create(a ...interface{}) (io.Writer, error) {

	err := d.volume.MkdirAll(d.dataDir, 0755)
	if err != nil {
		hummingbird.GetLogger(d.request).LogError("Error creating directory %s:%s - %s", d.vars["device"], d.dataDir, err.Error())
		return nil, hummingbird.ResponseToReturn{http.StatusInternalServerError}
	}

	u := uuid.NewV4()
	tempFile := d.dataDir + "/" + "." + path.Base(d.dataFile) + "." + fmt.Sprintf("%x", u[0:16])
	d.file, err = d.volume.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		hummingbird.GetLogger(d.request).LogError("Error creating temporary file in %s:%s - %s", d.vars["device"], tempFile, err.Error())
		return nil, hummingbird.ResponseToReturn{http.StatusInternalServerError}
	}
	d.tempFileName = d.file.Name()

	//TODO: Do fallocate and return 507 on ENOSPC
	return d.file, nil
}

func (d *GlusterDiskFile) Delete(metadata map[string]string) error {
	//TODO: Remove empty directories all the way up to parent
	err := d.volume.Unlink(d.dataFile)
	if err != nil {
		return hummingbird.ResponseToReturn{http.StatusInternalServerError}
	}
	return nil
}
