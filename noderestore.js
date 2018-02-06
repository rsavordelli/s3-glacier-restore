// for statistics
var inflight = 0, delayed = 0, listed = 0, processed = 0, tstart = Date.now();
var success = 0, already = 0, errors = 0, bytes = 0;

// for rate limit
const MB = 1048576;
var baseh = 0, bytesh = 0, bph = 0;

// for TPS limit
const max_tps = 50;
var nextreq = Date.now();

// command line parameters
var bucket = process.argv[2];
var days   = process.argv[3];
var fbph   = '500000';
//var fbph   = process.argv[4];
var nbph   = '500000';
//var nbph   = process.argv[5];
var restoretype   = process.argv[6];
var prefix = process.argv[7];
var marker = process.argv[8];

//console.assert(bucket && days > 0 && fbph > 5000000 && nbph >= fbph, 'Syntax: node %s <bucket> <days> <MB/firsth> <MB/nexth> <restoretype(Standard | Bulk | Expedited)> [<prefix> [<marker>]]', process.argv[1]);

var AWS = require('aws-sdk');
AWS.config.region = 'us-east-1'; // any valid region will do as S3 is region-agnostic

// default limit is 5 sockets, we need more to get 50 TPS
var https = require('https');
https.globalAgent.maxSockets = 20;
https.globalAgent.rejectUnauthorized = true;
AWS.config.httpOptions = {agent: https.globalAgent};

// Create an S3 client
var s3 = new AWS.S3();

process.on('exit', function(code) {
  console.log('Finally processed %d objects, success = %d, errors = %d, already in progress = %d', processed, success, errors, already);
});

// run the loop
listObjects(marker);

function listObjects(marker)
{
  s3.listObjects({Bucket: bucket, Prefix: prefix, Marker: marker, MaxKeys: 1000}, onList);
}

function onList(err, data) {
  console.assert(!err, err, data);
  listed += data.Contents.length;

  var now = Date.now() / 1000;
  var nowh = Math.floor(now / 3600);
  if (nowh > baseh) {
    // setup for next hour
    baseh = nowh;
    bytes += bytesh;
    bytesh = 0;
    bph = bph ? nbph : fbph;
  }
  else if (bytesh > bph) {
    // call myself again 10 seconds after next hour (safety margin for clock skew)
    var sleep = (nowh+1) * 3600 - now + 10;
    console.log('Handled %d bytes this hour, sleeping until next hour (%d seconds)', bytesh, Math.floor(sleep));
    setTimeout(onList, sleep * 1000, err, data);
    return;
  }

  // continue listing in backgroud
  if (data.IsTruncated)
    setTimeout(listObjects, (delayed+inflight) / max_tps * 1000, data.Contents[data.Contents.length-1].Key);

  data.Contents.forEach(function(item) {
    if (item.StorageClass === 'GLACIER') {
      processed++;
      bytesh += item.Size;
      processObject(item.Key, 1);
    }
  });

  var t = (Date.now() - tstart) / 1000;
  var mb = (bytes+bytesh) / MB;
  console.log('Listed %d (%d tps), processed %d (%d tps), success %d (%d tps), %d MB (%d MB/h), errors = %d, already = %d, time = %d',
    listed, Math.floor(listed / t),
    processed, Math.floor(processed / t),
    success, Math.floor(success / t),
    Math.floor(mb), Math.floor(mb * 3600 / t),
    errors, already, t);
  if (data.IsTruncated)
    console.log('Marker: %s', data.Contents[data.Contents.length-1].Key);
  else
    console.log('Finished ListObjects');
}

function processObject(key, retry)
{
  // increase even on retry, to maintain tps
  nextreq += 1000 / max_tps;
  var wait = (retry === 1) ?
    nextreq - Date.now() :
    Math.random() * (500 << retry);
  delayed++;
  setTimeout(restoreObject, wait, key, retry);
  // dont burst more than 1s of requests if we are slower than tps
  if (wait < -1000) nextreq = Date.now() - 1000;
}

function restoreObject(key, retry)
{
  delayed--;
  inflight++;
//  simulate({
  s3.restoreObject({
    Bucket: bucket,
    Key: key,
    RestoreRequest: {
      Days: days,
      GlacierJobParameters: {
      Tier: restoretype
     }
    },
  }, function(err, data) {
    inflight--;
    if (!err) success++;
    else if (err.code == 'RestoreAlreadyInProgress')
      already++;
    else if (retry < 5 && (err.retryable || err.statusCode == 500 || err.code == 'UnknownEndpoint')) {
      console.log('Retry', retry, 'for', err.code, 'on', key);
      console.error('DEBUG', retry, 'delayed', delayed, 'inflight', inflight, 'err', err);
      processObject(key, retry+1);
    }
    else {
      errors++;
      console.error('ERROR', key);
      console.error('DEBUG error delayed', delayed, 'inflight', inflight, 'err', err);
    }
  });
}

function simulate(args, callback)
{
  err = Math.random() > 0.01 ? false : {
    code: Math.random() > 0.1 ? 'RestoreAlreadyInProgress' : 'UnknownEndpoint'
  }
  setTimeout(callback, 500, err, 'data');
}
3
