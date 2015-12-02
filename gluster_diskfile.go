package antbird

import (
	"io"
	"net/http"
	"os"

	"github.com/kshlm/gogfapi/gfapi"
	"github.com/openstack/swift/go/hummingbird"
)

// Get a list of devices from ring file and virtual mount them using libgfapi
func SetupGlusterDiskFile(serverconf *hummingbird.IniFile) (map[string]interface{}, error) {
	hashPathPrefix, hashPathSuffix, _ := hummingbird.GetHashPrefixAndSuffix()
	objRing, _ := hummingbird.GetRing("object", hashPathPrefix, hashPathSuffix)
	bindPort := int(serverconf.GetInt("app:object-server", "bind_port", 6000))
	localDevices, _ := objRing.LocalDevices(bindPort)

	globals := make(map[string]interface{})
	globals["glusterVolumes"] = make(map[string]*gfapi.Volume)

	for _, dev := range localDevices {
		// TODO: Error handling. All the following APIs return int
		globals["glusterVolumes"].(map[string]*gfapi.Volume)[dev.Device] = new(gfapi.Volume)
		globals["glusterVolumes"].(map[string]*gfapi.Volume)[dev.Device].Init("localhost", dev.Device)
		globals["glusterVolumes"].(map[string]*gfapi.Volume)[dev.Device].Mount()
	}

	return globals, nil
}

// The following struct and member fields are implementation specific
type GlusterDiskFile struct {
	dataFile string
	request  *http.Request
	vars     map[string]string
	stat     os.FileInfo
	file     *gfapi.File
	volume   *gfapi.Volume
}

func (d *GlusterDiskFile) Init(globals map[string]interface{}, request *http.Request, vars map[string]string) error {
	d.request = request
	d.vars = vars

	d.volume = globals["glusterVolumes"].(map[string]*gfapi.Volume)[vars["device"]]
	d.dataFile = "/" + vars["account"] + "/" + vars["container"] + "/" + vars["obj"]
	d.stat, _ = d.volume.Stat(d.dataFile)
	return nil
}

func (d *GlusterDiskFile) GetObjectState() hummingbird.ObjectState {
	if d.stat != nil {
		return hummingbird.ObjectConsumable
	}
	return hummingbird.ObjectStateUnknown
}

func (d *GlusterDiskFile) Open(a ...interface{}) (hummingbird.ReadSeekCloser, error) {
	file, err := d.volume.Open(d.dataFile)
	d.file = file
	return file, err
}

func (d *GlusterDiskFile) GetMetadata() (map[string]string, error) {
	var metadata map[string]string
	var err error

	if d.file != nil {
		// GET, HEAD
		metadata, err = ReadMetadata(d.volume, d.file)
	} else {
		// PUT, DELETE
		metadata, err = ReadMetadata(d.volume, d.dataFile)
	}

	return metadata, err
}

func (d *GlusterDiskFile) PutMetadata(metadata map[string]string) error {
	return nil
}

func (d *GlusterDiskFile) Commit(a ...interface{}) error {
	return nil
}

func (d *GlusterDiskFile) Quarrantine(a ...interface{}) error {
	return nil
}

func (d *GlusterDiskFile) Cleanup(a ...interface{}) error {
	return nil
}

func (d *GlusterDiskFile) Create(a ...interface{}) (io.WriteCloser, error) {
	return nil, nil
}
