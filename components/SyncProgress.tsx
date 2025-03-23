import React from 'react'
import { Progress } from "@/components/ui/progress"
import { Card } from "@/components/ui/card"

interface SyncProgressProps {
  yearProgress: number | null
  overallProgress: number | null
}

export const SyncProgress: React.FC<SyncProgressProps> = ({ yearProgress, overallProgress }) => {
  if (yearProgress === null && overallProgress === null) {
    return null
  }

  return (
    <Card className="mt-4 p-4">
      <h3 className="text-lg font-semibold mb-2">Sync Progress</h3>
      {yearProgress !== null && (
        <div className="mb-4">
          <label className="text-sm font-medium">Year Progress</label>
          <Progress value={yearProgress} className="w-full" />
          <p className="mt-1 text-sm text-gray-600">{yearProgress.toFixed(2)}% Complete</p>
        </div>
      )}
      {overallProgress !== null && (
        <div>
          <label className="text-sm font-medium">Overall Progress</label>
          <Progress value={overallProgress} className="w-full" />
          <p className="mt-1 text-sm text-gray-600">{overallProgress.toFixed(2)}% Complete</p>
        </div>
      )}
    </Card>
  )
}

