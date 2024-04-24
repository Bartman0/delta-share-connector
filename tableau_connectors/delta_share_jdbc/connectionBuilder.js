const os = require('os');
const path = require('path');
const duckdb = require('duckdb');
const ds = require('delta-sharing');

function dsbuilder(attr) {
    const connectionProfile = attr[connectionHelper.attributeConnectionProfile];

    const dbFile = path.join(`${os.tmpdir()}`, `${attr[connectionHelper.attributeDatabase]}.db`);
    var url = `jdbc:duckdb:${dbFile}`;

    ingestDeltaShare(url, connectionProfile);

    return [url];
}

function ingestDeltaShare(url, connectionProfile) {
    const db = new duckdb.Database(url);

    const sharingProfile = ds.DeltaSharingProfile.readFromFile(connectionProfile);
    const client = new ds.SharingClient(sharingProfile);
    const restClient = new ds.DataSharingRestClient(sharingProfile);
    
    var sql_tables = [];
    var read_parquet_urls = [];

    client.listAllTablesAsync().then(function (tables) {
        console.log('Listing tables...');
        tables.map(function (table) {
            console.log(`table: ${table.toString()}`);
            const schema = table.schema;
            const tableName = table.tableName;
            sql_tables.push(`${schema}.${tableName}`);

            db.all(`create schema if not exists ${schema};`);

            restClient.listFilesInTable(table).then(function (files) {
                // example: CREATE TABLE people AS SELECT * FROM read_parquet('test.parquet');
                console.log('Listing all files...');
                files.addFiles.map(function (file) {
                    console.log(`file: ${file.url}`);
                    read_parquet_urls.push(`select * from read_parquet('${file.url}')`);
                });
                const read_parquet_queries = read_parquet_urls.join(" union all ");
                db.all(`create table ${schema}.${tableName} as ${read_parquet_queries}`, function (err, res) {
                    if (err) {
                        console.warn(err);
                    }
                });
                console.log("create done");
            })
            .catch(function (error) {
                console.log(error.toString());
            });
        })
    })
    .catch(function (error) {
        console.log(error.toString());
    });
}
