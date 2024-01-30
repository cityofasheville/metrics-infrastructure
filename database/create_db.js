const { Client } = require('pg');
const fs = require('fs');
const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require("@aws-sdk/client-secrets-manager");

const region = 'us-east-1';

async function getConnection(secretName) {
  const client = new SecretsManagerClient({
    region
  });

  let response;
  try {
    response = await client.send(
      new GetSecretValueCommand({
        SecretId: secretName,
        VersionStage: "AWSCURRENT", // VersionStage defaults to AWSCURRENT if unspecified
      })
    );
  } catch (error) {
    // For a list of exceptions thrown, see
    // https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    throw error;
  }
  return JSON.parse(response.SecretString);
}

async function getDBConnection(dbLocation) {
  let connection = Promise.resolve({
    host: process.env.METRIC_DB_HOST,
    port: 5432,
    user: process.env.METRIC_DB_USER,
    password: process.env.METRIC_DB_PASSWORD,
    database: process.env.METRIC_DB_NAME,
    max: 10,
    idleTimeoutMillis: 10000,
  });

  // If METRIC_DB_HOST is not in the environment, use location
  if (!('METRIC_DB_HOST' in process.env)
      || process.env.METRIC_DB_HOST === null
      || process.env.METRIC_DB_HOST.trim().length === 0) {
    return getConnection(dbLocation)
      .then(
        (cpValue) => {
          connection = {
            host: cpValue.host,
            port: cpValue.port,
            user: cpValue.username,
            password: cpValue.password,
            database: cpValue.database,
            max: 10,
            idleTimeoutMillis: 10000,
          };
          return connection;
        },
      )
      .catch((err) => { // Just pass it on.
        throw err;
      });
  }
  return connection;
}

lambda_handler = async function (event, context) {
  let dbLocation = null;
  if (process.argv.length > 2) {
    dbLocation = process.argv[2];
    console.log('Using db location ', dbLocation);
  }
  const connection = await getDBConnection(dbLocation);

  let sql = fs.readFileSync('./createMetricsTable.sql').toString();

  const client = new Client(connection);
  await client.connect()
  const res = await client.query(sql);

  await client.end()
  return res;
}

lambda_handler()

