const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

/**
 * Synchronizes Tally data table for a given user, company, and Tally company.
 * @param {string|number} userId - The user ID
 * @param {string|number} companyId - The company ID
 * @param {string} tallyCompany - The Tally company name
 * @returns {Promise<SyncResult>} A promise that resolves with the sync result
 * @throws {Error} If the Python script fails or times out
 */
function syncTallyTable(userId, companyId, tallyCompany) {
  return new Promise((resolve, reject) => {
    const possiblePaths = [
      path.join(__dirname, 'tally_data.py'),
      path.join(process.cwd(), 'scripts', 'tally_data.py'),
      path.join(process.cwd(), 'tally_data.py')
    ];

    let pythonScriptPath;
    for (const p of possiblePaths) {
      if (fs.existsSync(p)) {
        pythonScriptPath = p;
        break;
      }
    }

    if (!pythonScriptPath) {
      reject(new Error('Unable to find tally_data.py script'));
      return;
    }

    console.log(`Executing Python script: ${pythonScriptPath}`);
    console.log(`Parameters: userId=${userId}, companyId=${companyId}, tallyCompany=${tallyCompany}`);

    const pythonProcess = spawn('python', [
      pythonScriptPath,
      userId.toString(),
      companyId.toString(),
      tallyCompany
    ]);

    let stdoutData = '';
    let stderrData = '';

    pythonProcess.stdout.on('data', (data) => {
      stdoutData += data.toString();
      console.log(`Python script output: ${data}`);
    });

    pythonProcess.stderr.on('data', (data) => {
      stderrData += data.toString();
      console.error(`Python script error: ${data}`);
    });

    // Set a timeout for the Python script execution
    const timeout = setTimeout(() => {
      pythonProcess.kill();
      reject(new Error('Python script execution timed out after 10 minutes'));
    }, 10 * 60 * 1000); // 10 minutes timeout

    pythonProcess.on('close', (code) => {
      clearTimeout(timeout);
      console.log(`Python script exited with code ${code}`);
      if (code === 0) {
        resolve({ stdout: stdoutData, stderr: stderrData });
      } else {
        console.error('Python script failed. Full stderr output:');
        console.error(stderrData);
        reject(new Error(`Python script failed with exit code ${code}. Error details: ${stderrData}`));
      }
    });

    pythonProcess.on('error', (err) => {
      clearTimeout(timeout);
      console.error('Failed to start Python script:', err);
      reject(new Error(`Failed to start Python script: ${err.message}`));
    });
  });
}

module.exports = { syncTallyTable };