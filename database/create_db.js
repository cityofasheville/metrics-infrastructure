const { Client } = require('pg');
const fs = require('fs');

lambda_handler = async function (event, context) {
  let sql = fs.readFileSync('./createMetricsTable.sql').toString();

  if (process.argv.length <= 2) {
    console.log('Usage: node create_db.js DB-ENDPOINT-ADDRESS');
    process.exit(1);
  }
  const host = process.argv[2];

  console.log('The host is ', host);


  const client = new Client({
    host: host,
    user: 'bedrock',
    password: 'test-bedrock',
    database: 'bedrock',
    max: 10,
    idleTimeoutMillis: 10000,
  });
  await client.connect()
  const res = await client.query(sql);

  await client.end()
  return res;
}

lambda_handler()

