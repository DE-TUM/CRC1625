# Virtuoso Docker container folder

This is the folder for the (optional) local `Virtuoso` docker container. It will employ this directory as the mountpoint for `/data` inside
the container. This is **required** as the system will store and attempt to upload serialized RDF triples from there.

It needs to expose a SPARQL endpoint at http://127.0.0.1:8891, and accept ODBC requests at the `1111` port for user/password dba/dba (pending to change on production settings)

We employed the following [image](https://hub.docker.com/r/openlink/virtuoso-opensource-7).

Setup example:

```
docker run -d \
  --name virtuoso_CRC_1625 \
  -p 8891:8890 \
  -p 1111:1111 \
  -v $(pwd)/virtuoso-db:/database \
  -v $(pwd)/data:/data \
   openlink/virtuoso-opensource-7
```

#### The following configuration tweaks are **required**:
  - [Make virtuoso correctly treat untyped and typed xsd:string literals as the same during comparisons](https://github.com/openlink/virtuoso-opensource/issues/728#issuecomment-1937376203)
  - Add `/data` under `DirsAllowed` in the `virtuoso.ini` file
  - Increase `MaxVectorSize` in the `virtuoso.ini` file to, e.g., 4000000

#### The following configuration tweaks are **recommended**:
  - General performance tweaks are also recommended, such as increasing its maximum allowed memory usage. 


The `virtuoso.ini` file we employed for all our experiments is with the above-mentioned tweaks [available in this folder](virtuoso.ini). 
Note that you will need to modify `NumberOfBuffers` and `MaxDirtyBuffers` according to your server's memory. 4 or 8 GiB is sufficient.
