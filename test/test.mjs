import { createServer } from 'node:http';
import duckdb from 'duckdb';

const db = new duckdb.Database(':memory:');
var sql_tables = [];

import { DeltaSharingProfile, SharingClient, DataSharingRestClient } from 'delta-sharing';

const sharingProfile = DeltaSharingProfile.readFromFile('/Users/richardkooijman/Downloads/config.share');
const client = new SharingClient(sharingProfile);
const restClient = new DataSharingRestClient(sharingProfile);

client.listAllTablesAsync().then(function(tables) {
  console.log('Listing tables...');
  tables.map(function(table) {
    console.log(table.toString());
    const schema = table.schema;
    sql_tables.push(`${schema}.${table.tableName}`);
    db.all(`create schema if not exists ${schema};`);
    restClient.queryTableMetadataAsync(table).then(function(metaData) {
      console.log('Listing table metaData...');
      console.log(metaData.toString());
      var read_parquet_urls = [];
      restClient.listFilesInTable(table).then(function(files) {
        console.log('Listing all files...');
        console.log(JSON.stringify(files.addFiles, undefined, 2));
        files.addFiles.map(function(file) {
          read_parquet_urls.push(`select * from read_parquet('${file.url}')`);
        });
        const read_parquet_queries = read_parquet_urls.join(" union all ");
        // CREATE TABLE people AS SELECT * FROM read_parquet('test.parquet');
        console.log(`create table ${schema}.${table.tableName} as ${read_parquet_queries}`);
        db.all(`create table ${schema}.${table.tableName} as ${read_parquet_queries}`, function(err, res) {
          if (err) {
            console.warn(err);
          }
        });
        db.all(`select count(*) _count from ${schema}.${table.tableName}`, function(err, res) {
          if (err) {
            console.warn(err);
          }
          console.log(`count: ${res[0]._count}`);
        });
        console.log("create done");
      })
      .catch(function(error) {
        console.log(error.toString());
      });
    })
    .catch(function(error) {
      console.log(error.toString());
    });
  })
})
.catch(function(error) {
  console.log(error.toString());
});

const hostname = '127.0.0.1';
const port = 3000;

const server = createServer((req, res) => {
  var response = "";
  res.statusCode = 200;
  res.setHeader('Content-Type', 'text/plain');
  console.log(sql_tables);
  sql_tables.map(function (table) {
    db.all(`select count(*) _count from ${table}`, function (err, result) {
      if (err) {
        console.warn(err);
      }
      response += `count: ${result[0]._count}`;
      response += "\n";
      console.log("response: " + response);
    });
  });
  console.log("response: " + response);
  res.end('Hello World' + '\n' + response);
});

server.listen(port, hostname, () => {
  console.log(`Server running at http://${hostname}:${port}/`);
});
