import { NextResponse } from "next/server"
import { syncTallyData } from "../../../scripts/sync-tally-data"
import { EventEmitter } from "events"

const emitter = new EventEmitter()

export async function POST(req: Request) {
  try {
    const sessionData = await req.json()

    const requiredKeys = ["userId", "userCompanyId", "tallyCompanyId", "subscribeId","start_date","end_date"]
    const missingKeys = requiredKeys.filter((key) => !(key in sessionData))
    if (missingKeys.length > 0) {
      return NextResponse.json({ error: `Missing required session data: ${missingKeys.join(", ")}` }, { status: 400 })
    }

    console.log("Received session data:", JSON.stringify(sessionData, null, 2))

    const jobId = `${sessionData.userId}-${sessionData.userCompanyId}-${Date.now()}`

    // Start the sync process in the background
    // Start the sync process in the background
    syncTallyData(sessionData, (progress) => {
      emitter.emit(jobId, progress)
    })
      .then(() => {
        emitter.emit(jobId, {
          monthProgress: null,
          overallProgress: 100,
          message: "Sync completed"
        })
      })
      .catch((error) => {
        console.error("Sync process failed:", error)
        emitter.emit(jobId, {
          monthProgress: null,
          overallProgress: -1,
          message: error.message
        })
      })

    return NextResponse.json({ jobId, message: "Sync process initiated" })
  } catch (error) {
    console.error("Error in sync route:", error)
    return NextResponse.json(
      {
        error: "Internal server error",
        details: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined,
      },
      { status: 500 },
    )
  }
}

export async function GET(req: Request) {
  const jobId = new URL(req.url).searchParams.get("jobId")

  if (!jobId) {
    return NextResponse.json({ error: "Job ID is required" }, { status: 400 })
  }

  const stream = new ReadableStream({
    start(controller) {
      const listener = (data: any) => {
        controller.enqueue(`data: ${JSON.stringify(data)}\n\n`)
        if (data.overallProgress === 100 || data.overallProgress === -1) {
          controller.close()
          emitter.removeListener(jobId, listener)
        }
      }
      emitter.on(jobId, listener)
    },
  })

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    },
  })
}

