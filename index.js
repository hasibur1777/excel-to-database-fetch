const mysql = require('mysql2/promise');
const fs = require('fs');
const csv = require('fast-csv');

// MySQL Connection Configuration
const pool = mysql.createPool({
  host: '',
  user: '',
  password: '',
  database: 'wbst',
  connectionLimit: 2, // Limit connections for efficiency
});

// File paths
const inputFile = 'files/2024.csv'; // Convert Excel to CSV first
const outputPrefix = 'files/2024_results'; // Base name for output files
const maxRowsPerFile = 100000; // Max rows per CSV file before splitting
const batchSize = 5000; // Fetch 5000 records at a time

let fileCount = 1;
let totalRowsWritten = 0;
let writeStream = createNewCSVFile();

// Function to create a new CSV file
function createNewCSVFile() {
  const fileName = `${outputPrefix}_${fileCount}.csv`;
  const stream = fs.createWriteStream(fileName);
  stream.write('barcode,entry_date\n'); // Write CSV header
  console.log(`Created new CSV file: ${fileName}`);
  return stream;
}

// Read barcodes in chunks and fetch from MySQL
async function processBarcodes() {
  const readStream = fs.createReadStream(inputFile);
  const csvStream = csv.parse({ headers: true });

  let batch = [];

  csvStream
    .on('data', (row) => {
      batch.push(row.barcode);
      if (batch.length >= batchSize) {
        csvStream.pause(); // Pause reading until the batch is processed
        fetchAndWrite(batch).then(() => {
          batch = []; // Clear batch after processing
          csvStream.resume(); // Resume reading the file
        });
      }
    })
    .on('end', async () => {
      if (batch.length > 0) {
        await fetchAndWrite(batch);
      }
      console.log('Processing complete.');
      writeStream.end(); // Close last file
    });

  readStream.pipe(csvStream);
}

// Fetch barcode entry dates from MySQL in batches
async function fetchAndWrite(batch) {
  if (batch.length === 0) return;

  try {
    const placeholders = batch.map(() => '?').join(',');
    const query = `SELECT serial_no AS barcode, DATE_FORMAT(actproddate, '%Y-%m-%d') AS entry_date 
                   FROM wbcsm_barcode_serial_track2 
                   WHERE serial_no IN (${placeholders})`;

    const [rows] = await pool.query(query, batch);

    for (const { barcode, entry_date } of rows) {
      writeStream.write(`${barcode},${entry_date}\n`);
      totalRowsWritten++;

      // If 65,000 rows are reached, create a new file
      if (totalRowsWritten >= maxRowsPerFile) {
        writeStream.end(); // Close current file
        fileCount++;
        totalRowsWritten = 0;
        writeStream = createNewCSVFile(); // Create new file
      }
    }

    console.log(`Processed ${rows.length} barcodes...`);
  } catch (error) {
    console.error('Error processing batch:', error.message);
  }
}

// Run the process
processBarcodes();
