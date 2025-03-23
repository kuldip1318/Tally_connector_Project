import React, { forwardRef } from 'react';
import { Wifi, WifiOff, Database, ServerOffIcon as DatabaseOff } from 'lucide-react';

interface ConnectionStatusProps {
  internetConnected: boolean;
  tallyConnected: boolean;
}

export const ConnectionStatus = forwardRef<HTMLDivElement, ConnectionStatusProps>(
  ({ internetConnected, tallyConnected }, ref) => {
    return (
      <div ref={ref} className="flex space-x-4">
        <div className="flex items-center">
          {internetConnected ? (
            <Wifi className="text-green-500 mr-2" size={20} />
          ) : (
            <WifiOff className="text-red-500 mr-2" size={20} />
          )}
          <span>{internetConnected ? 'Internet Connected' : 'No Internet'}</span>
        </div>
        <div className="flex items-center">
          {tallyConnected ? (
            <Database className="text-green-500 mr-2" size={20} />
          ) : (
            <DatabaseOff className="text-red-500 mr-2" size={20} />
          )}
          <span>{tallyConnected ? 'Tally Connected' : 'Tally Disconnected'}</span>
        </div>
      </div>
    );
  }
);

ConnectionStatus.displayName = 'ConnectionStatus';

