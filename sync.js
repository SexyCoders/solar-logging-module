const axios = require('axios');
const { MongoClient } = require('mongodb');
const crypto = require('crypto');

const loggerID='test';
const loggerGroup='test_group'
// MongoDB connection URLs
const localUrl = 'mongodb://localhost:27019';
const cloudServerUrl = 'http://localhost:9999'; // Replace with actual cloud server URL

// Function to calculate checksum for a batch
function calculateBatchChecksum(batch) {
  const hash = crypto.createHash('sha256');
  hash.update(JSON.stringify(batch));
  return hash.digest('hex');
}

// Function to send batch to cloud
async function sendBatch(batchId) {
  const client = new MongoClient(localUrl);
  try {
    await client.connect();
    const db = client.db('data');
    const bufferCollection = db.collection('send_buffer');

    const batch = await bufferCollection.findOne({ id: batchId });
    if (!batch) {
      console.error(`Batch ${batchId} not found in send buffer`);
      return false;
    }

    console.log(`sending batch ${JSON.stringify(batch)}`);

    try {
      const response = await axios.post(cloudServerUrl+'/sync', batch);
      return response.data.success;
    } catch (error) {
      console.error('Error sending batch:', error.message);
      return false;
    }
  } finally {
    await client.close();
  }
}

// Function to create batches and add them to send buffer
async function createBatchesAndAddToSendBuffer(lastSyncTimestamp) {
  const client = new MongoClient(localUrl);
  try {
    await client.connect();
    const db = client.db('data');
    const collection = db.collection('inverter_logs');
    const bufferCollection = db.collection('send_buffer');

    const unsyncedData = await collection.find({ timestamp: { $gt: new Date(lastSyncTimestamp) }}).toArray();

    console.log('Data out of sync. Local changes are: ');
    console.log(unsyncedData)

    while (unsyncedData.length) {
      const batch = unsyncedData.splice(0, 1000); // Adjust batch size as needed
      const batchId = `${loggerID}-${new Date().toISOString()}`;
      const checksum = calculateBatchChecksum(batch);
      const timestamp = new Date();
      const batchPayload = { id: batchId, data: batch, checksum, loggerGroup, timestamp};

      // Add batch to send buffer
      await bufferCollection.insertOne(batchPayload);
    }
  } finally {
    await client.close();
  }
}

// Function to process send buffer
async function processSendBuffer() {
  const client = new MongoClient(localUrl);
  try {
    await client.connect();
    const db = client.db('data');
    const bufferCollection = db.collection('send_buffer');

    const batches = await bufferCollection.find({}).toArray();
    for (const batch of batches) {
      const success = await sendBatch(batch.id);
      if (success) {
        console.log(`Batch ${batch.id} synced successfully`);
        await bufferCollection.deleteOne({ id: batch.id });
      } else {
        console.error(`Failed to sync batch ${batch.id}`);
        // Optionally, implement a retry mechanism
      }
    }
  } finally {
    await client.close();
  }
}

// Function to check connection and get last sync timestamp
async function checkConnectionAndGetLastSync() {
  try {
    const response = await axios.get(cloudServerUrl + '/lastSync');
    if (response.data.lastSync) {
      console.log('Connection established. Last sync:', response.data.lastSync);
      return response.data.lastSync;
    }
  } catch (error) {
    console.error('Error checking connection or getting last sync:', error.message);
    return null;
  }
}

// Main function to execute the flow
async function main() {
  const lastSyncTimestamp = await checkConnectionAndGetLastSync();
  if (lastSyncTimestamp) {
    await createBatchesAndAddToSendBuffer(lastSyncTimestamp);
    await processSendBuffer();
  }
}

// Schedule the main function to run periodically
setInterval(main, 10 * 60 * 1000); // Adjust the interval as needed
main();

