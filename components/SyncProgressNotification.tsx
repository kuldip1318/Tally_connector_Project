"use client"

import * as React from "react"
import { X } from "lucide-react"
import { cn } from "@/lib/utils"

interface SyncProgressNotificationProps {
  yearProgress: {
    year: string
    progress: number
  } | null
  overallProgress: number | null
  onCancel?: () => void
  className?: string
}

export function SyncProgressNotification({
  yearProgress,
  overallProgress,
  onCancel,
  className,
}: SyncProgressNotificationProps) {
  if (!yearProgress && overallProgress === null) return null

  return (
    <div className={cn("fixed right-4 top-4 z-50 w-80 rounded-lg bg-zinc-900 p-4 text-white shadow-lg", className)}>
      <div className="mb-3 flex items-center justify-between">
        <h3 className="text-sm font-medium">
          {yearProgress ? `Processing ${yearProgress.year}...` : "Syncing Data..."}
        </h3>
        {onCancel && (
          <button onClick={onCancel} className="rounded p-1 hover:bg-zinc-800">
            <X className="h-4 w-4" />
            <span className="sr-only">Cancel</span>
          </button>
        )}
      </div>
      <div className="space-y-3">
        {yearProgress && (
          <div>
            <div className="mb-1 flex items-center justify-between text-xs">
              <span>Year Progress</span>
              <span>{Math.round(yearProgress.progress)}%</span>
            </div>
            <div className="h-1.5 w-full overflow-hidden rounded-full bg-zinc-800">
              <div
                className="h-full bg-blue-500 transition-all duration-300"
                style={{ width: `${yearProgress.progress}%` }}
              />
            </div>
          </div>
        )}
        {overallProgress !== null && (
          <div>
            <div className="mb-1 flex items-center justify-between text-xs">
              <span>Overall Progress</span>
              <span>{Math.round(overallProgress)}%</span>
            </div>
            <div className="h-1.5 w-full overflow-hidden rounded-full bg-zinc-800">
              <div
                className="h-full bg-blue-500 transition-all duration-300"
                style={{ width: `${overallProgress}%` }}
              />
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

