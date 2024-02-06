const axios = require('axios');
const { MongoClient } = require('mongodb');
const crypto = require('crypto');


// MongoDB connection URL
//const url = 'mongo://solar-logger-module-mongo-1:27017';
const url = 'mongodb://localhost:27019';

// Function to fetch inverter list from MongoDB
async function fetchInverterList() {
  process.stdout.write('connecting to mongo on '+url+'...');
  const client = new MongoClient(url);
  console.log('DONE!');

  process.stdout.write('fetching inverter list...');
  try {
    await client.connect();
    const db = client.db('info');
    const collection = db.collection('inverters');

    const inverters = await collection.find({}).toArray();
    return inverters;
  } finally {
    await client.close();
  }
  console.log('DONE!');
}

async function processInverter(inverter) {
  try {
    const response = await axios.get(inverter.location, { params: { Scope: 'System' }});
    let inverterData = { id: inverter.id, data: response.data, timestamp: new Date()};

    // Create a SHA256 hash of the inverterData
    const checksum = crypto.createHash('sha256').update(JSON.stringify(inverterData.data)).digest('hex');
    inverterData.checksum = checksum;

    console.log(inverterData);

    const client = new MongoClient(url);
    await client.connect();
    const db = client.db('data');
    const collection = db.collection('inverter_logs');

    await collection.insertOne(inverterData);

    await client.close();  // Close the connection after insertion
  } catch (error) {
    console.error(`Error fetching data for inverter ${inverter.id}: ${error.message}`);
  }
}


// Main function to execute the flow
async function main() {
  try {
    const inverters = await fetchInverterList();
    inverters.forEach(inverter => processInverter(inverter));
  } catch (error) {
    console.error('Error in main function:', error.message);
  }
}

// Schedule the main function to run every 3 minutes
setInterval(main, 3 * 60 * 1000);

main();

