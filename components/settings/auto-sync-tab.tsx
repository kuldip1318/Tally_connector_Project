'use client'

import { useState } from 'react'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { useToast } from '@/components/ui/use-toast'

export function AutoSyncTab() {
  const [autoSync, setAutoSync] = useState(false)
  const { toast } = useToast()

  const handleToggle = async (value: boolean) => {
    try {
      setAutoSync(value)
      // TODO: Implement actual save logic
      toast({
        title: "Auto sync updated",
        description: `Auto sync has been ${value ? 'enabled' : 'disabled'}.`,
      })
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to update auto sync settings.",
      })
    }
  }

  return (
    <Card>
      <CardContent className="p-6">
        <h2 className="text-lg font-semibold mb-4">Exe Auto Sync</h2>
        <div className="flex gap-4">
          <Button
            variant={autoSync ? "ghost" : "default"}
            onClick={() => handleToggle(false)}
          >
            No
          </Button>
          <Button
            variant={autoSync ? "default" : "ghost"}
            onClick={() => handleToggle(true)}
          >
            Yes
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}

