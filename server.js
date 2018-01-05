/*********************/
/* Express Packages: */
/*********************/
var express 	= require('express');
var bodyParser 	= require('body-parser');
var fs 		= require('fs');
var aws 	= require('aws-sdk');
var datetime 	= require('node-datetime');
var request 	= require('request');
var app   	= express();
var serverPort 	= 8081;

//For extract the entire body portion of an incoming request 
app.use(bodyParser.urlencoded({
	extended: true
}));

//S3 connection conf
var s3 = new aws.S3({signatureVersion: 'v4'});

/******************/
/* Log Timestamp: */
/******************/
var dt, formatted;

var time = new Date().toISOString()
.replace(/T/, ' ')		// replace T with a space
.replace(/\..+/, '');   // delete the dot and everything after

/*************************/
/* Environment Variables */
/*************************/
var S3_BUCKET_NAME		= process.env.S3_BUCKET_NAME;

/**************/
/* Constants: */
/**************/
var BF_NAME		= "/var/log/firehose/bigFileBuffer.txt";
var SF_NAME		= "/var/log/firehose/smallFileBuffer.txt";
var BF_TO_DUMP		= "/var/log/firehose/bigFileToDump.txt";
var SF_TO_DUMP		= "/var/log/firehose/smallFileToDump.txt";
var FAILED_TO_POST	= "/var/log/firehose/failedToPostBuffer.log";

var BIG_BUFFER_SIZE 	= 1073741824; 	//1GB
var SMALL_BUFFER_SIZE 	= 20971520;	//20MB


//set the headers to the post request to django
var headers = { 'User-Agent'	 : 'FirehoseServer/0.0.1',
				'Content-Type' 	 : 'application/json'
	};

/**************/
/* Functions: */
/**************

/**
 * Dump fileToDump to the remoteFolder on the relevant bucket.
 * @param {String} fileToDump 
 * @param {String} remoteFolder
 */
var dumpFileToS3 = function( fileToDump, remoteFolder){
	var fileBuffer 		= fs.readFileSync(fileToDump);
	var remoteFileName	= remoteFolder + formatted + '.txt';
	s3.putObject({
		ACL		: 'public-read',
		Bucket		: S3_BUCKET_NAME,
		Key		: remoteFileName,
		Body		: fileBuffer,
		ContentType	: "text/html"
	}, function(error, res){
		console.log("dumpFileToS3 FUNCTION: " + fileToDump);
			if(error) {
				console.log("Failed to upload big file to S3 ||| ERROR : " + error);
			}
			else{
				fs.unlink(fileToDump, (err) => {
					if (err) console.log('Failed to delete ' + fileToDump + ' ||| ERROR : ' + err);
				});
			}
	});
}

/**
 * Check the existance of file
 * @param {String} file
 * @param {Function} callback
 */
function checkIfFile(file, callback) {
	  fs.stat(file, function fsStat(err, stats) {
	    if (err) {
	      if (err.code === 'ENOENT') {
	        return callback(null, false);
	      } else {
	        return callback(err);
	      }
	    }
	    return callback(null, stats.isFile());
	  });
	}
/**
 * create watcher on fileToWatch and dump it to s3 when it exceed bufferSize.
 * @param {String} fileToWatch 
 * @param {String} fileToDump 
 * @param {String} bufferSize
 * @param {String} bucketPrefix
 */
var watchFile = function(fileToWatch, fileToDump, bufferSize, bucketPrefix ){
	require('file-size-watcher').watch(fileToWatch).on('sizeChange', function callback(newSize, oldSize){
		if(newSize > bufferSize){	
			checkIfFile(fileToDump, function(err, isFile) {
				  if (isFile) {
					  console.log("Concurrency ERROR - Failed to dump " + fileToDump + " to S3.");
					  dumpFileToS3(fileToDump, bucketPrefix);
				  }
				  else
					  fs.rename(fileToWatch, fileToDump, function(err) {
							//callback - rename buffer file:
							if ( err ) console.log('Failed to rename buffer file ||| ERROR: ' + err);
							dumpFileToS3(fileToDump, bucketPrefix);
						});
				});
		}
	});
}

/*******************/
/* Files Watchers: */
/*******************/

//Raw-Data:
watchFile(SF_NAME, SF_TO_DUMP, SMALL_BUFFER_SIZE, "small-files/");
watchFile(BF_NAME, BF_TO_DUMP, BIG_BUFFER_SIZE, "big-files/");


/***********/
/* SERVER: */
/***********/
app.get("/", function (req, res) {
	res.end("Firehose is up and running");
});

///sendjson url gets log as json, append it to small&big buffers and return response to the client:
app.post("/sendjson", function (req, res) {
	
	dt              = datetime.create();
	formatted       = dt.format('Y/m/d/H-M-S');
	
	var body = '';
	req.on('data', function(data) {
		body += data;
	});
	
	//write logs to small and big buffers
	req.on('end', function (){
		fs.appendFile(BF_NAME, body + '\n', function(err){
			if (err){
				console.log("Failed to write log to big file buffer. ERR: " + err + " | " + time);
			}
		});
		fs.appendFile(SF_NAME, body + '\n', function(err){
			if (err){
				console.log("Failed to write log to small file buffer. ERR: " + err + " | " + time);
			}
		});
		
		res.end("The log inserted successfully");
		
	//Response already get to the requested server.
	//Continue with async tasks here.

	});

});

app.listen(serverPort, function () {
	console.log('Firehose is running on port ' + serverPort + ' from: ' + time);
});

