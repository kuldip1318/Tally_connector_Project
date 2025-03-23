'use client'

import { RefreshCw, Info, Settings, LogOut } from 'lucide-react'
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"

export function DashboardHeader() {
  return (
    <div className="flex items-center justify-between p-4 border-b bg-white">
      <div className="flex items-center gap-3">
        <h1 className="text-xl font-semibold text-gray-900">My Companies</h1>
        <Button variant="ghost" size="icon" className="h-8 w-8">
          <RefreshCw className="h-4 w-4 text-gray-600" />
        </Button>
        <Button variant="ghost" size="icon" className="h-8 w-8">
          <Info className="h-4 w-4 text-gray-600" />
        </Button>
      </div>
      
      <div className="flex items-center gap-3">
        <Input 
          type="search" 
          placeholder="Search by Name or Tally ID" 
          className="w-[300px] h-9"
        />
        <Button 
          variant="default" 
          className="bg-blue-600 hover:bg-blue-700 h-9"
        >
          Add Companies
        </Button>
        <Button variant="ghost" size="icon" className="h-9 w-9">
          <Settings className="h-4 w-4 text-gray-600" />
        </Button>
        <Button variant="ghost" size="icon" className="h-9 w-9 text-red-500">
          <LogOut className="h-4 w-4" />
        </Button>
      </div>
    </div>
  )
}

