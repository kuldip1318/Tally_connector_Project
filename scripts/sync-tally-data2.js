const { spawn } = require("child_process")
const path = require("path")
const fs = require("fs")

async function runPythonScript(scriptName, args, progressCallback) {
  return new Promise((resolve, reject) => {
    const pythonProcess = spawn("python", [scriptName, ...args])

    pythonProcess.stdout.on("data", (data) => {
      console.log(`${scriptName} output: ${data}`)

      // Parse progress information from the Python script output
      const progressMatch = data.toString().match(/Progress: (\d+)%/)
      if (progressMatch) {
        const scriptProgress = Number.parseInt(progressMatch[1])
        progressCallback({ yearProgress: null, overallProgress: scriptProgress })
      }
    })

    pythonProcess.stderr.on("data", (data) => {
      console.error(`${scriptName} error: ${data}`)
    })

    pythonProcess.on("close", (code) => {
      if (code === 0) {
        resolve()
      } else {
        reject(new Error(`${scriptName} exited with code ${code}`))
      }
    })
  })
}

async function syncTallyData(userId, companyId, tallyCompany, subscribeId, progressCallback) {
  const scripts = ["tally-data-sync.py", "tally_data.py"]

  const totalScripts = scripts.length
  let completedScripts = 0

  for (const script of scripts) {
    try {
      await runPythonScript(script, [userId, companyId, tallyCompany, subscribeId], (scriptProgress) => {
        const overallProgress = ((completedScripts + scriptProgress.overallProgress / 100) / totalScripts) * 100
        progressCallback({ yearProgress: null, overallProgress })
      })
      completedScripts++
    } catch (error) {
      console.error(`Error running ${script}:`, error)
      throw error
    }
  }

  // Final progress update
  progressCallback({ yearProgress: null, overallProgress: 100 })
}

// Helper function to find the Python script
function findPythonScript(scriptName) {
  const possiblePaths = [
    path.join(__dirname, scriptName),
    path.join(process.cwd(), "scripts", scriptName),
    path.join(process.cwd(), scriptName),
  ]

  for (const p of possiblePaths) {
    if (fs.existsSync(p)) {
      return p
    }
  }

  throw new Error(`Unable to find ${scriptName} script`)
}

module.exports = { syncTallyData }

