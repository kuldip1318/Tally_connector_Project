// const { spawn } = require('child_process');
// const path = require('path');
// const fs = require('fs');

// function syncTallyData() {
//   return new Promise((resolve, reject) => {
//     // Resolve the path relative to the project root
//     const scriptPath = path.join(process.cwd(), 'scripts', 'tally-data-sync.py');
//     console.log(`Attempting to run Python script at: ${scriptPath}`);

//     // Validate if the file exists
//     if (!fs.existsSync(scriptPath)) {
//       return reject(new Error(`Python script not found at: ${scriptPath}`));
//     }

//     const pythonProcess = spawn('python', [scriptPath], {
//       env: {
//         ...process.env,
//         DB_HOST: process.env.DB_HOST,
//         DB_PORT: process.env.DB_PORT,
//         DB_NAME: process.env.DB_NAME,
//         DB_USER: process.env.DB_USER,
//         DB_PASSWORD: process.env.DB_PASSWORD,
//         TALLY_URL: process.env.TALLY_URL,
//       },
//     });

//     let output = '';
//     let errorOutput = '';

//     pythonProcess.stdout.on('data', (data) => {
//       output += data.toString();
//       console.log(`Python script output: ${data}`);
//     });

//     pythonProcess.stderr.on('data', (data) => {
//       errorOutput += data.toString();
//       console.error(`Python script error: ${data}`);
//     });

//     pythonProcess.on('close', (code) => {
//       if (code !== 0) {
//         reject(new Error(`Python script exited with code ${code}\nError: ${errorOutput}`));
//       } else {
//         resolve(output);
//       }
//     });
//   });
// }

// module.exports = { syncTallyData };
