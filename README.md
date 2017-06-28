# Firehose

This node js server came up with the need to create 1GB buffer files for MapReduce modeling - 
Some buffer to stand between our data streaming server to the DB server, bufferd the logs into a batch file and drop the data to some safe zone (like S3) with size/time triggers.
The server append each log into 2 buffers:
Small Buffer  - 20MB  - for Backup.
Big Buffer    - 1GB   - for MapReduce.

 
