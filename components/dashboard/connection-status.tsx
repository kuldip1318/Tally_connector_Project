'use client'

import { Wifi, Database } from 'lucide-react'

interface ConnectionStatusProps {
  isTallyConnected: boolean;
  isInternetConnected: boolean;
  version: string;
}

export function ConnectionStatus({ 
  isTallyConnected, 
  isInternetConnected, 
  version 
}: ConnectionStatusProps) {
  return (
    <div className="fixed bottom-0 left-0 right-0 flex items-center justify-between px-6 py-2 bg-white border-t text-sm">
      <div className="flex items-center gap-2">
        <Database className="h-4 w-4" />
        <span>Tally: {isTallyConnected ? 'CONNECTED' : 'DISCONNECTED'}</span>
        <span className="text-gray-400 ml-2">{version}</span>
      </div>
      <div className="flex items-center gap-2">
        <Wifi className="h-4 w-4" />
        <span>Internet: {isInternetConnected ? 'CONNECTED' : 'DISCONNECTED'}</span>
      </div>
    </div>
  )
}

