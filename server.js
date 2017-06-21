/*********************/
/* Express Packages: */
/*********************/
var express 	= require('express'),
bodyParser 	= require('body-parser'),
fs 		= require('fs'),
aws 		= require('aws-sdk'),
datetime 	= require('node-datetime'),
request 	= require('request'),
app   		= express(),
serverPort 	= 8081;

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
var FUNKEY_DJANGO_SERVER_URL		= process.env.FUNKEY_DJANGO_SERVER_URL;
var FUNKEY_MONGO_RAW_VIEW_NAME		= process.env.FUNKEY_MONGO_RAW_VIEW_NAME;
var FUNKEY_MONGO_PARSED_VIEW_NAME	= process.env.FUNKEY_MONGO_PARSED_VIEW_NAME;
var FUNKEY_ES_RAW_VIEW_NAME		= process.env.FUNKEY_ES_RAW_VIEW_NAME;
var FUNKEY_ES_PARSED_VIEW_NAME		= process.env.FUNKEY_ES_PARSED_VIEW_NAME;
var FUNKEY_S3_BUCKET_NAME		= process.env.FUNKEY_S3_BUCKET_NAME;

/**************/
/* Constants: */
/**************/
var BF_NAME		= "/var/log/firehose/bigFileBuffer.txt";
var SF_NAME		= "/var/log/firehose/smallFileBuffer.txt";
var BF_TO_DUMP		= "/var/log/firehose/bigFileToDump.txt";
var SF_TO_DUMP		= "/var/log/firehose/smallFileToDump.txt";
var FAILED_TO_POST	= "/var/log/firehose/failedToPostBuffer.log";

//KBD-Reports files:
var KBD_BF_NAME		= "/var/log/firehose/KBD_bigFileBuffer.txt";
var KBD_SF_NAME		= "/var/log/firehose/KBD_smallFileBuffer.txt";
var KBD_BF_TO_DUMP	= "/var/log/firehose/KBD_bigFileToDump.txt";
var KBD_SF_TO_DUMP	= "/var/log/firehose/KBD_smallFileToDump.txt";

var BIG_BUFFER_SIZE 	= 1073741824;
var SMALL_BUFFER_SIZE 	= 20971520;


//set the headers to the post request to django
var headers = { 'User-Agent'	 : 'FunkeyFirehoseServer/0.0.1',
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
		ACL			: 'public-read',
		Bucket		: FUNKEY_S3_BUCKET_NAME,
		Key			: remoteFileName,
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
 * Send log to django view
 * @param {String} viewName 
 * @param {json} log
 */
var sendLogToDjangoView = function( viewName, log ){
	var options = {
					url: FUNKEY_DJANGO_SERVER_URL + viewName,
					method: 'POST',
					headers: headers,
					json: log
				};

	// Start the request
	request(options, function (error, resp, post_body) {
		if (!(!error && resp.statusCode == 200)) {
			//Error / statusCode != 200:
			if(error){
				console.log("Django failed accepting the request! ||| ERROR : " + 
						error + " | " + time);
				return;
			}
			else { 
				console.log("Django View : " + viewName + " | " + "TIME: "  + time);
				console.log("Response Code : " + resp.statusCode + " | The log added to failedToPostBuffer.log | " + JSON.stringify(log));				
				fs.appendFile(FAILED_TO_POST, JSON.stringify(log) + '\n', function(err){
					if (err) {
						console.log("Failed to write log to failedToPostBuffer.log ||| ERROR : " + 
								err + " | " + time);
					}
				});
			}
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

//KBD-Reports:
watchFile(KBD_SF_NAME, KBD_SF_TO_DUMP, SMALL_BUFFER_SIZE, "kbd-reports/small-files/");
watchFile(KBD_BF_NAME, KBD_BF_TO_DUMP, BIG_BUFFER_SIZE, "kbd-reports/big-files/");


/***********/
/* SERVER: */
/***********/
app.get("/", function (req, res) {
	res.end("Funkey Firehose is up and running");
});

app.post("/sendKBDReports", function (req, res) {
	
	var body = '';
	req.on('data', function(data) {
		body += data;
	});
	
	//write logs to small and big buffers
	req.on('end', function (){
		fs.appendFile(KBD_BF_NAME, body + '\n', function(err){
			if (err){
				console.log("Failed to write KBD report to big file buffer. ERR: " + err + " | " + time);
			}
		});
		fs.appendFile(KBD_SF_NAME, body + '\n', function(err){
			if (err){
				console.log("Failed to write KBD report to small file buffer. ERR: " + err + " | " + time);
			}
		});
		
		res.end("KBD Report has successfully handled");

	});

});

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
		
//		sendLogToDjangoView(FUNKEY_MONGO_RAW_VIEW_NAME, JSON.parse(body));
		sendLogToDjangoView(FUNKEY_ES_RAW_VIEW_NAME, JSON.parse(body));
//		sendLogToDjangoView(FUNKEY_MONGO_PARSED_VIEW_NAME, JSON.parse(body));
		sendLogToDjangoView(FUNKEY_ES_PARSED_VIEW_NAME, JSON.parse(body));

	});

});

app.listen(serverPort, function () {
	console.log('FunkeyFirehose is running on port ' + serverPort + ' from: ' + time);
});

