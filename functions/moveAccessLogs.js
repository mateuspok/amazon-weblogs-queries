// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
const aws = require('aws-sdk');
const s3 = new aws.S3({ apiVersion: '2006-03-01' });

// prefix to copy partitioned data to w/o leading but w/ trailing slash
//const targetKeyPrefix = process.env.TARGET_KEY_PREFIX;

// regex for filenames by Amazon CloudFront access logs. Groups:
// - 1.	year
// - 2.	month
// - 3.	day 
// - 4.	hour
const datePattern = '^.*\/(.*)[^\\d](\\d{4})-(\\d{2})-(\\d{2})-(\\d{2})[^\\d]';
const filenamePattern = '[^/]+$';
//const typePattern = '[^/]+$';

exports.handler = async (event, context, callback) => {
  const moves = event.Records.map(record => {
    const bucket = record.s3.bucket.name;
    const sourceKey = record.s3.object.key;

    const sourceRegex = new RegExp(datePattern, 'g');
    //const typeRegex = new RegExp(typePattern, 'g');
    //const type = typeRegex.exec(sourceKey);
    
    const match = sourceRegex.exec(sourceKey);
    
    if (match == null) {
      console.log(`Object key ${sourceKey} does not look like an access log file, so it will not be moved.`);
    } else {
      const [,type, year, month, day, hour] = match;
      var typer;
      switch (type.substring(0,5)) {
        case "amazo":
          typer = "apig";
          break;
        case "aws-w":
          typer = "waf";
          break;
        default:
          typer = "cf";
      }

      const filenameRegex = new RegExp(filenamePattern, 'g');
      const filename = filenameRegex.exec(sourceKey)[0];

      const targetKey = `${typer}-partitioned-gz/year=${year}/month=${month}/day=${day}/hour=${hour}/${filename}`;
      console.log(`Copying ${sourceKey} to ${typer}_${targetKey}.`);

      const copyParams = {
        CopySource: bucket + '/' + sourceKey,
        Bucket: bucket,
        Key: targetKey
      };
      
      const copy = s3.copyObject(copyParams).promise();

      const deleteParams = { Bucket: bucket, Key: sourceKey };

      return copy.then(function () {
        console.log(`Copied. Now deleting ${sourceKey}.`);
        const del = s3.deleteObject(deleteParams).promise();
        console.log(`Deleted ${sourceKey}.`);
        return del;
      }, function (reason) {
        var error = new Error(`Error while copying ${sourceKey}: ${reason}`);
        callback(error);
      });

    }
  });
  await Promise.all(moves);
};
