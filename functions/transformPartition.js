// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
const util = require('./util');

// AWS Glue Data Catalog database and tables
const sourceTable = process.env.SOURCE_TABLE;
const targetTable = process.env.TARGET_TABLE;
const database = process.env.DATABASE;

// get the partition of 2hours ago
exports.handler = async (event, context, callback) => {
  var partitionHour = new Date(Date.now() - 120 * 60 * 1000);
  var year = partitionHour.getUTCFullYear();
  var month = (partitionHour.getUTCMonth() + 1).toString().padStart(2, '0');
  var day = partitionHour.getUTCDate().toString().padStart(2, '0');
  var hour = partitionHour.getUTCHours().toString().padStart(2, '0');

  console.log('Transforming Partition', { year, month, day, hour });

  var ctasStatement = `
    INSERT INTO ${database}.waf_partitioned_parquet
    SELECT timestamp,
         formatversion,
         webaclid,
         terminatingruleid,
         terminatingruletype,
         action,
         array_join(terminatingrulematchdetails,' eee ') as terminatingrulematchdetails, 
         httpsourcename, 
         httpsourceid, 
         array_join(ratebasedrulelist,' eee ') as ratebasedrulelist, 
         array_join(nonterminatingmatchingrules,' eee ') as nonterminatingmatchingrules, 
         httprequest, 
         year, 
         month, 
         day, 
         hour
    FROM ${database}.waf_partitioned_gz
    WHERE year = '${year}'
        AND month = '${month}'
        AND day = '${day}'
        AND hour = '${hour}';`;

  await util.runQuery(ctasStatement);
  
  var ctasStatement2 = `
    INSERT INTO ${database}.cf_partitioned_parquet
    SELECT *
    FROM ${database}.cf_partitioned_gz
    WHERE year = '${year}'
        AND month = '${month}'
        AND day = '${day}'
        AND hour = '${hour}';`;

  await util.runQuery(ctasStatement2);
  
  var ctasStatement3 = `
    INSERT INTO ${database}.apig_partitioned_parquet
    SELECT *
    FROM ${database}.apig_partitioned_gz
    WHERE year = '${year}'
        AND month = '${month}'
        AND day = '${day}'
        AND hour = '${hour}';`;

  await util.runQuery(ctasStatement3);
  console.log('Transformed ',ctasStatement);
  console.log('Transformed ',ctasStatement2);
  console.log('Transformed ',ctasStatement3);
 
}
