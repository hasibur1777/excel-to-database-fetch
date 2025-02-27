const mysql = require('mysql2/promise');
const fs = require('fs');
const csv = require('fast-csv');

// MySQL Connection Configuration
const pool = mysql.createPool({
  host: '',
  user: '',
  password: '',
  database: 'wbst',
  connectionLimit: 2,
});

// File paths
const outputPrefix = 'files/tv/TV-Production';
const maxRowsPerFile = 150000; // Max rows per CSV file before splitting
const batchSize = 5000; // Fetch records at a time

let fileCount = 1;
let totalRowsWritten = 0;
let writeStream = createNewCSVFile();

// Function to create a new CSV file
function createNewCSVFile() {
  const fileName = `${outputPrefix}_${fileCount}.csv`;
  const stream = fs.createWriteStream(fileName);
  //   stream.write('item_id,serial_no,item_serial_no,date,time\n'); // Write CSV header
  console.log(`Created new CSV file: ${fileName}`);
  return stream;
}

// Fetch and write data from MySQL
async function fetchAndWriteData() {
  let offset = 0;

  try {
    while (true) {
      //   const query = `SELECT 'Panel-with-Supplier' item_id, serial_no panel, item_serial_no supplier_barcode, open_cel_list, lc_number, led_bar_lc_number, date track_date, time track_time
      //                  FROM wbcsm_additional_data
      //                  WHERE item_id = 7 AND isActive = 1
      //                  LIMIT ?, ?`;

      const query = `select
                        m.serial_no,
                        DATE_FORMAT(m.actproddate, '%Y-%m-%d') production_datee,
                        wm.ebs_item_code,
                        p.itemcode,
                        p.description
                    from
                        wbcsm_barcode_serial_track2 as m
                    left join wbcsm_product_code as p on
                        m.itemcode = p.itemcode
                        and m.isActive = 1
                    left join wbcsm_model wm on
                        wm.id = p.modelID
                    where
                        m.assembly_line_id in ('51', '19', '10', '50', '7', '73', '72', '71', '64')
                        and m.actproddate BETWEEN '2020-01-11' AND '2025-02-25'
                        and wm.isActive = 1
                     LIMIT ?, ?`;

      const [rows] = await pool.query(query, [offset, batchSize]);

      if (rows.length === 0) break; // Stop if no more data

      for (const row of rows) {
        if (totalRowsWritten === 0 && fileCount === 1) {
          // Write header dynamically (only for the first file)
          writeStream.write(Object.keys(row).join(',') + '\n');
        }
        writeStream.write(Object.values(row).join(',') + '\n');
        totalRowsWritten++;

        // Create a new file if maxRowsPerFile limit is reached
        if (totalRowsWritten >= maxRowsPerFile) {
          writeStream.end(); // Close current file
          fileCount++;
          totalRowsWritten = 0;
          writeStream = createNewCSVFile();
        }
      }

      offset += batchSize;
      console.log(`Processed ${offset} records...`);
    }

    console.log('Processing complete.');
    writeStream.end(); // Close last file
  } catch (error) {
    console.error('Error processing data:', error.message);
  }
}

// Run the process
fetchAndWriteData();
