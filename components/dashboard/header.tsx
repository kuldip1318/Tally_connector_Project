'use client'

import { RefreshCw, Info, Settings } from 'lucide-react'
import { Button } from "@/components/ui/button"
import { useRouter } from 'next/navigation'
import { useToast } from '@/components/ui/use-toast'
import AuthService from '@/lib/AuthService'

interface DashboardHeaderProps {
  onRefresh: () => Promise<void>
}

export function DashboardHeader({ onRefresh }: DashboardHeaderProps) {
  const router = useRouter()
  const { toast } = useToast()

  const handleRefresh = async () => {
    try {
      await onRefresh()
      toast({
        title: "Refreshed",
        description: "Company list has been refreshed successfully.",
      })
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to refresh company list.",
      })
    }
  }

  return (
    <div className="flex items-center justify-between p-4 border-b bg-white">
      <div className="flex items-center gap-3">
        <h1 className="text-xl font-semibold text-gray-900">My Companies</h1>
        <Button 
          variant="ghost" 
          size="icon" 
          className="h-8 w-8"
          onClick={handleRefresh}
        >
          <RefreshCw className="h-4 w-4 text-gray-600" />
        </Button>
        <Button variant="ghost" size="icon" className="h-8 w-8">
          <Info className="h-4 w-4 text-gray-600" />
        </Button>
      </div>
      
      <Button 
        variant="ghost" 
        size="icon" 
        className="h-9 w-9"
        onClick={() => router.push('/settings')}
      >
        <Settings className="h-4 w-4 text-gray-600" />
      </Button>
    </div>
  )
}

