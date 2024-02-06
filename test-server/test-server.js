const express = require('express');
const bodyParser = require('body-parser');
const crypto = require('crypto');
const { MongoClient } = require('mongodb');

const app = express();
app.use(bodyParser.json());

// MongoDB setup
const mongoUri = 'mongodb://localhost:27020'; // Replace with your MongoDB URI
const client = new MongoClient(mongoUri, { useNewUrlParser: true, useUnifiedTopology: true });
let db;

client.connect(err => {
  if (err) throw err;
  db = client.db('inverter_data');
  console.log("Connected to MongoDB");
});

// Function to calculate checksum for a batch
function calculateBatchChecksum(batch) {
  const hash = crypto.createHash('sha256');
  hash.update(JSON.stringify(batch));
  return hash.digest('hex'); 
}

// Endpoint to receive batch data
app.post('/sync', async (req, res) => {
  const batch = req.body;
  console.log(batch);
  const checksum = calculateBatchChecksum(batch.data);

  if (checksum === batch.checksum) {
    console.log(`Received valid batch ${batch.id}`);
    const db = client.db('data');
    const collection = db.collection(batch.loggerGroup);

    // Array to hold promises for each insert operation
    let insertPromises = [];

    // Iterate over each data item and try to insert it
    batch.data.forEach(dataItem => {
      insertPromises.push(
        collection.insertOne(dataItem).catch(error => {
          if (error.code === 11000) {
            console.log(`Duplicate entry for batch ${batch.id}, ignored.`);
            return null; // Return null for duplicates to filter them out later
          } else {
            throw error; // Rethrow other errors to handle them in Promise.all
          }
        })
      );
    });

    try {
      // Wait for all insert operations to complete
      await Promise.all(insertPromises);
      res.json({ success: true });
    } catch (error) {
      console.error(`Error processing batch ${batch.id}: ${error}`);
      res.status(500).json({ success: false, error: 'Internal server error' });
    }
  } else {
    console.error(`Checksum mismatch for batch ${batch.id}`);
    res.status(400).json({ success: false, error: 'Checksum mismatch' });
  }
});


// Endpoint to provide last sync timestamp
app.get('/lastSync', (req, res) => {
  // Logic to retrieve the last sync timestamp from the cloud DB
  res.json({ lastSync: '2023-12-24T00:00:00Z' }); // Example timestamp
});

const port = 9999;
app.listen(port, () => {
  console.log(`Cloud server running on port ${port}`);
});

