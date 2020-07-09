# Athena example data
* See AWS official document for [Create database and table](https://docs.aws.amazon.com/athena/latest/ug/work-with-data.html)
* See AWS official document for [WorkGroup information](https://docs.aws.amazon.com/athena/latest/ug/workgroups-create-update-delete.html)
* Put [Sample dataset](dataset/) files in a S3 bucket ```FixityOutputBucket```
	* Example table schema
	```
	CREATE EXTERNAL TABLE IF NOT EXISTS fixitycost.fixityoutput (
	  `bucket` string,
	  `key` string,
	  `elapsed` int,
	  `etag` string,
	  `filesize` bigint,
	  `state` string,
	  `status` string,
	  `restorestatus` string,
	  `restorerequest` array<string>,
	  `algorithm` string,
	  `chunksize` bigint,
	  `bytesread` bigint,
	  `nextbytestart` bigint,
	  `computed` string,
	  `comparedwith` string,
	  `comparedresult` string,
	  `storechecksumontagging` boolean,
	  `tagupdated` boolean,
	  `timestamp` timestamp
	)
	ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
	WITH SERDEPROPERTIES (
	  'serialization.format' = '1'
	) LOCATION 's3://fixitycost-output/'
	TBLPROPERTIES ('has_encrypted_data'='false');
	```
* Create a S3 bucket ```ResultBucket```  to store Athena query results
	* Configure workgroup to save query result to ```ResultBucket```
