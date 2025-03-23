import { Wifi, WifiOff } from 'lucide-react'

interface StatusIndicatorProps {
  connected: boolean;
  label: string;
}

export function StatusIndicator({ connected, label }: StatusIndicatorProps) {
  return (
    <div className="flex items-center gap-2">
      {connected ? (
        <Wifi size={16} className="text-green-500" />
      ) : (
        <WifiOff size={16} className="text-red-500" />
      )}
      <span>{label}: {connected ? 'CONNECTED' : 'DISCONNECTED'}</span>
    </div>
  )
}

