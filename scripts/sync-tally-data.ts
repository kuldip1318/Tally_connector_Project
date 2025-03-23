import { spawn } from "child_process"
import path from "path"
import fs from "fs"
import { promisify } from "util"

const sleep = promisify(setTimeout)

interface Progress {
  monthProgress: number | null
  overallProgress: number
}

export type ProgressCallback = (progress: Progress) => void

interface SessionData {
  userId: string | number
  userCompanyId: string | number
  tallyCompanyId: string
  subscribeId: string | number
  start_date: string
  end_date: string
  [key: string]: string | number
}

async function runPythonScript(scriptName: string, sessionData: SessionData): Promise<void> {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(process.cwd(), "scripts", scriptName)
    const args = Object.entries(sessionData).map(([key, value]) => {
      // Convert value to string and escape spaces
      const stringValue = String(value)
      const escapedValue = stringValue.includes(" ") ? `"${stringValue}"` : stringValue
      return `${key}=${escapedValue}`
    })
    const fullCommand = `python ${scriptPath} ${args.join(" ")}`
    console.log(`Executing command: ${fullCommand}`)

    if (!fs.existsSync(scriptPath)) {
      reject(new Error(`Python script not found at ${scriptPath}`))
      return
    }

    const pythonProcess = spawn("python", [scriptPath, ...args])

    let stdoutData = ""
    let stderrData = ""

    pythonProcess.stdout.on("data", (data) => {
      stdoutData += data.toString()
      console.log(`${scriptName} output: ${data}`)
    })

    pythonProcess.stderr.on("data", (data) => {
      stderrData += data.toString()
      console.error(`${scriptName} error: ${data}`)
    })

    pythonProcess.on("close", async (code) => {
      if (code === 0) {
        console.log(`${scriptName} executed successfully`)
        // Add a delay after each script execution
        await sleep(5000) // 5 seconds delay, adjust as needed
        resolve()
      } else {
        console.error(`${scriptName} exited with code ${code}`)
        reject(new Error(`${scriptName} exited with code ${code}. Stdout: ${stdoutData}. Stderr: ${stderrData}`))
      }
    })
  })
}

export async function syncTallyData(sessionData: SessionData, progressCallback: ProgressCallback): Promise<void> {
  const scripts = [ "tally-data-sync.py"]
  const totalScripts = scripts.length

  for (let i = 0; i < scripts.length; i++) {
    const script = scripts[i]
    try {
      await runPythonScript(script, sessionData)
      progressCallback({
        monthProgress: null,
        overallProgress: ((i + 1) / totalScripts) * 100,
      })
      // Add a delay between script executions
      if (i < scripts.length - 1) {
        await sleep(10000) // 10 seconds delay between scripts, adjust as needed
      }
    } catch (error) {
      console.error(`Error running ${script}:`, error)
      throw error
    }
  }
}

