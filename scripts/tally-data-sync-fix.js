const { spawn } = require('child_process');
const path = require('path');

function runPythonScript(userId, companyId, tallyCompany) {
  return new Promise((resolve, reject) => {
    const pythonProcess = spawn('python', [
      path.join(__dirname, 'tally-data-sync.py'),
      userId,
      companyId,
      tallyCompany
    ]);

    let stdoutData = '';
    let stderrData = '';

    pythonProcess.stdout.on('data', (data) => {
      stdoutData += data.toString();
      console.log(`Python output: ${data}`);
    });

    pythonProcess.stderr.on('data', (data) => {
      stderrData += data.toString();
      console.error(`Python error: ${data}`);
    });

    pythonProcess.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`Python script exited with code ${code}\n${stderrData}`));
      } else {
        resolve(stdoutData);
      }
    });
  });
}

// Example usage
runPythonScript('3', '4', 'URBAN CREATION (Chovisawadi)')
  .then(output => console.log('Python script executed successfully:', output))
  .catch(error => console.error('Error executing Python script:', error.message));

// Modifications to be made in the Python script:

/*
1. In the `parse_tally_data_xml` function, modify the ledger handling:

    ledger = allocation.findtext("LEDGER")
    if ledger is None or not ledger.strip():
        self.logger.warning(f"Empty or missing ledger found for voucher {voucher_number}. Setting to 'Unspecified'.")
        ledger = "Unspecified"
    else:
        ledger = ledger.strip()

2. In the `insert_tally_data_into_postgres` function, modify the data validation and insertion process:

    def validate_and_clean_data(df):
        required_fields = ['date', 'voucher_type', 'voucher_number', 'ledger', 'amount']
        for field in required_fields:
            if field not in df.columns:
                self.logger.error(f"Required field '{field}' is missing from the DataFrame.")
                raise ValueError(f"Required field '{field}' is missing from the DataFrame.")
            
            df[field] = df[field].fillna('Unspecified' if field == 'ledger' else '')
            if field in ['date', 'amount']:
                df = df.dropna(subset=[field])
        
        return df

    try:
        df = validate_and_clean_data(df)
        
        # Convert DataFrame to CSV string
        csv_data = df.to_csv(index=False, header=False, sep='\t', na_rep='\\N')
        
        # Use StringIO to create a file-like object
        from io import StringIO
        csv_buffer = StringIO(csv_data)
        
        # Create temporary table
        cur.execute(f"CREATE TEMP TABLE temp_{table_name} (LIKE {table_name} INCLUDING ALL)")
        
        # Copy data to temporary table
        cur.copy_expert(f"COPY temp_{table_name} FROM STDIN WITH CSV DELIMITER E'\\t' NULL '\\N'", csv_buffer)
        
        # Insert data from temporary table to main table
        cur.execute(f"""
            INSERT INTO {table_name}
            SELECT * FROM temp_{table_name}
            ON CONFLICT (subscribe_id, date, voucher_type, voucher_number, ledger, amount) 
            DO NOTHING
        """)
        
        # Drop temporary table
        cur.execute(f"DROP TABLE temp_{table_name}")
        
        conn.commit()
        self.logger.info(f"Data inserted into '{table_name}' successfully.")
    except Exception as e:
        conn.rollback()
        self.logger.error(f"Error inserting data into {table_name}: {str(e)}")
        raise

3. Add more detailed logging in the `sync_tally_data` method:

    self.logger.info(f"Starting sync_tally_data for subscribe_id: {subscribe_id}, tally_company: {tally_company}")
    self.logger.info(f"Constructing payload for date range: {self.tally_data_config['from_date']} to {self.tally_data_config['to_date']}")
    
    # After parsing XML
    self.logger.info(f"Parsed {len(data)} records from XML.")
    if data:
        self.logger.debug(f"Sample data (first 5 records): {data[:5]}")
    
    # Before inserting data
    self.logger.info(f"Attempting to insert {len(data)} records into {self.tally_data_config['table_name']}")

4. Implement error handling for specific database errors:

    from psycopg2 import errors

    try:
        # ... (existing insertion code)
    except errors.UniqueViolation as e:
        self.logger.warning(f"Unique constraint violation: {str(e)}")
        # Handle duplicate data (e.g., skip or update)
    except errors.NotNullViolation as e:
        self.logger.error(f"Not-null constraint violation: {str(e)}")
        # Handle missing required data
    except Exception as e:
        self.logger.error(f"Unexpected error during data insertion: {str(e)}")
        raise

These modifications should help resolve the "missing data for column 'ledger'" error by ensuring that the 'ledger' field always has a value, even if it's 'Unspecified'. The changes also improve error handling, data validation, and logging throughout the script.
*/

console.log("Implement these changes in your Python script to resolve the ledger column error and improve overall data handling and error reporting.");