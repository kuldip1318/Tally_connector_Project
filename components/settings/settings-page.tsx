'use client'

import { useState } from 'react'
import { ArrowLeft } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { TallyConfigTab } from './tally-config-tab'
import { AutoSyncTab } from './auto-sync-tab'
import { ProfileTab } from './profile-tab'

type Tab = 'tally' | 'autosync' | 'profile'

export function SettingsPage() {
  const [activeTab, setActiveTab] = useState<Tab>('tally')

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="border-b bg-white p-4">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => window.history.back()}>
            <ArrowLeft className="h-4 w-4" />
          </Button>
          <h1 className="text-xl font-semibold">Settings</h1>
        </div>
        <div className="mt-4 flex gap-4 border-b">
          <Button
            variant={activeTab === 'tally' ? 'default' : 'ghost'}
            onClick={() => setActiveTab('tally')}
            className="rounded-none border-b-2 border-transparent px-4 py-2 hover:border-primary"
          >
            Tally Configuration
          </Button>
          <Button
            variant={activeTab === 'autosync' ? 'default' : 'ghost'}
            onClick={() => setActiveTab('autosync')}
            className="rounded-none border-b-2 border-transparent px-4 py-2 hover:border-primary"
          >
            Auto Sync
          </Button>
          <Button
            variant={activeTab === 'profile' ? 'default' : 'ghost'}
            onClick={() => setActiveTab('profile')}
            className="rounded-none border-b-2 border-transparent px-4 py-2 hover:border-primary"
          >
            Profile
          </Button>
        </div>
      </div>

      <div className="container mx-auto p-6">
        {activeTab === 'tally' && <TallyConfigTab />}
        {activeTab === 'autosync' && <AutoSyncTab />}
        {activeTab === 'profile' && <ProfileTab />}
      </div>
    </div>
  )
}

