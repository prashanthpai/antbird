Antbird
=======

Antbird enables [Hummingbird](https://github.com/openstack/swift/tree/feature/hummingbird/go) to use [GlusterFS](https://github.com/gluster/glusterfs) as storage backend over [Libgfapi](https://github.com/kshlm/gogfapi). This project aims to be Go implementation of [SwiftOnFile](https://github.com/openstack/swiftonfile) project.

Installation
------------
1. Set up the hummingbird environment with SAIO as described [here](https://github.com/openstack/swift/tree/feature/hummingbird/go#installation)
2. Use [this branch](https://github.com/prashanthpai/swift/tree/hummingbird-diskfile) to install hummingbird. This will make hummingbird pluggable.
3. Create GlusterFS volumes and [export it over libgfapi](https://gist.github.com/prashanthpai/8b36761668c0273ef056)
4. Install gogfapi: `go get github.com/kshlm/gogfapi`
5. Add GlusterFS volume names to object ring as devices.
6. Apply following diff to hummingbird code referred in Step 2:

```diff
diff --git a/go/objectserver/main.go b/go/objectserver/main.go
index 7c428be..afb1451 100644
--- a/go/objectserver/main.go
+++ b/go/objectserver/main.go
@@ -39,6 +39,7 @@ import (
        "github.com/justinas/alice"
        "github.com/openstack/swift/go/hummingbird"
        "github.com/openstack/swift/go/middleware"
+       "github.com/prashanthpai/antbird"
 )
 
 type ObjectServer struct {
@@ -730,9 +731,9 @@ func GetServer(conf string, flags *flag.FlagSet) (bindIP string, bindPort int, s
 
        // Change this section to use a different backend implementation
        server.getDiskFile = func() hummingbird.DiskFile {
-               return &DefaultDiskFile{}
+               return &antbird.GlusterDiskFile{}
        }
-       server.DiskFileGlobals, err = SetupDefaultDiskFile(&serverconf, server.logger)
+       server.DiskFileGlobals, err = antbird.SetupGlusterDiskFile(&serverconf, server.logger)
        if err != nil {
                server.logger.Err(fmt.Sprintf("DiskFile Setup failed: %s", err.Error()))
                os.Exit(-1)
```
As hummingbird is not storage policy aware yet, `GlusterFS volume == Swift device == Swift account`

Debugging
---------
It's helpful to see a trace of libgfapi calls made by hummingbird:

~~~
# ltrace -T -ff -p <pid>
~~~
