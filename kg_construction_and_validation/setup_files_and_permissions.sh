# Setup RMLMapper
wget https://github.com/RMLio/rmlmapper-java/releases/download/v8.0.1/rmlmapper-8.0.1-r379-all.jar
mv rmlmapper-8.0.1-r379-all.jar ./materialization/rmlmapper.jar

# Setup RMLStreamer
wget https://github.com/RMLio/RMLStreamer/releases/download/v2.5.0/RMLStreamer-v2.5.0-standalone.jar
mv RMLStreamer-v2.5.0-standalone.jar ./materialization/RMLStreamer.jar

# Write permissions on the DB backups dir (needed for docker to write into it if running performance tests)
chmod o+w ./datastores/sql/db_dumps