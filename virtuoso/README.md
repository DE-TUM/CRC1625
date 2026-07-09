# Virtuoso Docker container folder

**Warning: We recommend using the docker-compose deployment. This setup is only used for debugging and performance testing.**

This is the folder for the **optional** local `Virtuoso` docker container. It will employ this directory as the mountpoint for `/data` inside
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
  -e DBA_PASSWORD=your_dba_password_as_in_the_env_file \
   openlink/virtuoso-opensource-7
```

#### The following configuration tweaks are **required**:
  - [Make virtuoso correctly treat untyped and typed xsd:string literals as the same during comparisons](https://github.com/openlink/virtuoso-opensource/issues/728#issuecomment-1937376203)
  - Add `/data` under `DirsAllowed` in the `virtuoso.ini` file
  - Increase `MaxVectorSize` in the `virtuoso.ini` file to, e.g., 4000000
  - Set up proper SPARQL UPDATE and INSERT user permissions using conductor. 
    An easy (but more unsafe) way to do this is to execute the following via ISQL:

```
DB.DBA.RDF_DEFAULT_USER_PERMS_SET ('nobody', 7);

-- Set default permissions
UPDATE DB.DBA.SYS_USERS 
   SET U_DEF_PERMS = '110100005R' 
 WHERE U_NAME = 'SPARQL';

-- Grant required EXECUTE rights
GRANT EXECUTE ON DB.DBA.SPARQL_EVAL TO "SPARQL";
GRANT EXECUTE ON DB.DBA.SPARQL_UPDATE TO "SPARQL";
GRANT EXECUTE ON DB.DBA.SPARQL_INSERT_DICT_CONTENT TO "SPARQL";
GRANT EXECUTE ON DB.DBA.SPARQL_DELETE_DICT_CONTENT TO "SPARQL";

-- Grant triple-level graph write access
GRANT INSERT, DELETE ON DB.DBA.RDF_QUAD TO "SPARQL";

-- Grant internal role required for SPARQL Update support
GRANT "SPARQL_UPDATE" TO "SPARQL";

-- Finalize with a checkpoint
checkpoint;
```
  - If, even after running the above script, there are permission issues or strange errors when running materialization UPDATE/DELETE queries, the workaround mentioned in https://github.com/openlink/virtuoso-opensource/issues/1094 can be applied to remedy this.

#### The following configuration tweaks are **recommended**:
  - General performance tweaks are also recommended, such as increasing its maximum allowed memory usage. 


The `virtuoso.ini` file we employed for all our experiments is with the above-mentioned tweaks [available in this folder](virtuoso.ini). 
Note that you will need to modify `NumberOfBuffers` and `MaxDirtyBuffers` according to your server's memory. 4 or 8 GiB is sufficient.
