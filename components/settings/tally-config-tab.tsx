'use client'

import { useState } from 'react'
import { Card, CardContent } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { useToast } from '@/components/ui/use-toast'

export function TallyConfigTab() {
  const [host, setHost] = useState('localhost')
  const [port, setPort] = useState('9000')
  const { toast } = useToast()

  const handleSave = async () => {
    try {
      // TODO: Implement actual save logic
      toast({
        title: "Settings saved",
        description: "Tally configuration has been updated successfully.",
      })
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to save Tally configuration.",
      })
    }
  }

  return (
    <Card>
      <CardContent className="p-6">
        <h2 className="text-lg font-semibold mb-4">
          Connect your Tally to Suvit using ODBC Configuration.
        </h2>
        <div className="grid gap-6">
          <div className="grid gap-2">
            <label className="text-sm font-medium">
              Tally Host<span className="text-red-500">*</span>
            </label>
            <Input
              value={host}
              onChange={(e) => setHost(e.target.value)}
              placeholder="localhost"
            />
          </div>
          <div className="grid gap-2">
            <label className="text-sm font-medium">
              Tally Port<span className="text-red-500">*</span>
            </label>
            <Input
              value={port}
              onChange={(e) => setPort(e.target.value)}
              placeholder="9000"
            />
          </div>
          <Button onClick={handleSave}>Save Settings</Button>
        </div>
      </CardContent>
    </Card>
  )
}

