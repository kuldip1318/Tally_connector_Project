"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import AuthService from "@/lib/AuthService"
import { DashboardHeader } from "@/components/dashboard/header"
import { CompanyList } from "@/components/dashboard/company-list"
import { ConnectionStatus } from "@/components/dashboard/connection-status"
import { Card } from "@/components/ui/card"
import { SyncProgressNotification } from "@/components/SyncProgressNotification"
import { Label } from "@/components/ui/label"
import { Input } from "@/components/ui/input"
import { Calendar } from "lucide-react"


import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { checkAuth } from "@/lib/authUtils"
import { useToast } from "@/components/ui/use-toast"
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select"
import { AlertTriangle } from "lucide-react"
import { Progress } from "@/components/ui/progress"

interface LoadingStates {
  [key: string]: boolean
}

interface UserCompany {
  id: string
  name: string
  gstNumber?: string
}

interface User {
  id: string
  name: string
  email: string
}

interface ActiveTallyCompany {
  tally_company_id: number
  user_id: number
  tally_company_name: string
  active_status: number
  created_at: string
}

const MAX_RETRY_ATTEMPTS = 3
const RETRY_DELAY = 1000

export default function Dashboard() {
  const [user, setUser] = useState<User | null>(null)
  const [availableCompanies, setAvailableCompanies] = useState<Array<{ id: string; name: string }>>([])
  const [subscribedCompanies, setSubscribedCompanies] = useState<Array<{ id: string; name: string }>>([])
  const [activeTallyCompany, setActiveTallyCompany] = useState<ActiveTallyCompany | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [message, setMessage] = useState("")
  const [internetConnected, setInternetConnected] = useState(true)
  const [tallyConnected, setTallyConnected] = useState(false)
  const [userCompanies, setUserCompanies] = useState<UserCompany[]>([])
  const [selectedUserCompany, setSelectedUserCompany] = useState<UserCompany | null>(null)
  const [loadingStates, setLoadingStates] = useState<LoadingStates>({})
  const [dialogOpen, setDialogOpen] = useState(false)
  const [companyToSubscribe, setCompanyToSubscribe] = useState<{ id: string; name: string } | null>(null)
  const [syncConfirmationOpen, setSyncConfirmationOpen] = useState(false)
  const [companySyncDetails, setCompanySyncDetails] = useState<{ id: string; name: string } | null>(null)
  const [syncInProgress, setSyncInProgress] = useState(false)
  const [yearProgress, setYearProgress] = useState<{ year: string; progress: number } | null>(null)
  const [overallProgress, setOverallProgress] = useState<number | null>(null)
  const [startDate, setStartDate] = useState<string>("")
  const [endDate, setEndDate] = useState<string>("")


  const router = useRouter()
  const { toast } = useToast()

  useEffect(() => {
    const checkAuthentication = async () => {
      const { isAuthenticated, userId } = await checkAuth()
      if (!isAuthenticated) {
        router.push("/auth/login")
        return
      }

      const userStr = localStorage.getItem("user")
      if (userStr) {
        const userData = JSON.parse(userStr)
        setUser(userData)
        await fetchCompanies(userData.id)
        await fetchActiveTallyCompany(userData.id)
      } else {
        router.push("/auth/login")
      }
      setIsLoading(false)
    }

    checkAuthentication()
    checkInternetConnection()
    checkTallyConnection()
  }, [router])

  const checkInternetConnection = () => {
    setInternetConnected(navigator.onLine)
    const handleOnline = () => setInternetConnected(true)
    const handleOffline = () => setInternetConnected(false)

    window.addEventListener("online", handleOnline)
    window.addEventListener("offline", handleOffline)

    return () => {
      window.removeEventListener("online", handleOnline)
      window.removeEventListener("offline", handleOffline)
    }
  }

  const checkTallyConnection = async () => {
    const isConnected = await AuthService.checkTallyConnection()
    setTallyConnected(isConnected)
  }

  const fetchCompanies = async (userId: string) => {
    try {
      const userCompanies = await AuthService.getUserCompanies(userId)
      setUserCompanies(userCompanies)

      if (userCompanies.length > 0) {
        const firstCompany = userCompanies[0]
        setSelectedUserCompany(firstCompany)
        await updateCompanyLists(userId, firstCompany.id)
      }
    } catch (error) {
      console.error("Failed to fetch companies:", error)
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to fetch companies. Please try again.",
      })
    }
  }

  const updateCompanyLists = async (userId: string, companyId: string) => {
    const subscribed = await AuthService.getSubscribedCompanies(userId, companyId)
    setSubscribedCompanies(subscribed)

    const allTallyCompanies = await AuthService.getTallyCompanies()
    const subscribedIds = new Set(subscribed.map((company) => company.id))
    const available = allTallyCompanies.filter((company) => !subscribedIds.has(company.id))
    setAvailableCompanies(available)
  }

  const fetchActiveTallyCompany = async (userId: string) => {
    try {
      const company = await AuthService.getActiveTallyCompany(userId)
      setActiveTallyCompany(company)
    } catch (error) {
      console.error("Failed to fetch active tally company:", error)
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to fetch active tally company. Please try again.",
      })
    }
  }

  const handleUserCompanyChange = async (companyId: string) => {
    try {
      const selected = userCompanies.find((company) => company.id === companyId)
      if (selected && user) {
        setSelectedUserCompany(selected)
        await updateCompanyLists(user.id, selected.id)
      }
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Failed to update company selection. Please try again.",
      })
    }
  }

  const setCompanyLoading = (companyId: string, loading: boolean) => {
    setLoadingStates((prev) => ({
      ...prev,
      [companyId]: loading,
    }))
  }

  const initializeSubscription = (companyId: string) => {
    const company = availableCompanies.find((c) => c.id === companyId)
    if (company) {
      setCompanyToSubscribe(company)
      setDialogOpen(true)
    }
  }

  const handleSubscribeConfirmed = async () => {
    if (!companyToSubscribe) return
    setDialogOpen(false)
    await handleSubscribe(companyToSubscribe.id)
    setCompanyToSubscribe(null)
  }

  const handleSubscribe = async (companyId: string) => {
    if (!user || !selectedUserCompany) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Please select a company first.",
      })
      return
    }

    setCompanyLoading(companyId, true)

    try {
      await AuthService.subscribeToTallyCompany(user.id, selectedUserCompany.id, companyId)

      await updateCompanyLists(user.id, selectedUserCompany.id)

      toast({
        title: "Success",
        description: "Successfully subscribed to company.",
      })
    } catch (error) {
      console.error("Subscription error:", error)
      toast({
        variant: "destructive",
        title: "Subscription Failed",
        description: error instanceof Error ? error.message : "An unknown error occurred",
      })
    } finally {
      setCompanyLoading(companyId, false)
    }
  }

  const handleSync = async (companyId: string) => {
    if (!user || !selectedUserCompany || !startDate || !endDate) {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Please select a company and date range first.",
      })
      return
    }
  
    setCompanyLoading(companyId, true)
    setSyncInProgress(true)
    setOverallProgress(0)
    setYearProgress(null)
  
    try {
      const subscribeIdResponse = await AuthService.getSubscribeId(user.id, selectedUserCompany.id, companyId)
  
      if (!subscribeIdResponse.subscribeId) {
        throw new Error("Invalid subscribe ID")
      }
  
      const response = await AuthService.syncTallyCompanyData(
        user.id,
        selectedUserCompany.id,
        companyId,
        subscribeIdResponse.subscribeId,
        startDate,
        endDate
      )
  
      if (response.jobId) {
        const eventSource = new EventSource(`/api/sync?jobId=${response.jobId}`)
  
        eventSource.onmessage = (event) => {
          const data = JSON.parse(event.data)
  
          if (data.year && data.yearProgress !== undefined) {
            setYearProgress({
              year: data.year,
              progress: data.yearProgress,
            })
          }
  
          if (data.overallProgress !== null) {
            setOverallProgress(data.overallProgress)
          }
  
          if (data.overallProgress === 100) {
            eventSource.close()
            setSyncInProgress(false)
            setYearProgress(null)
            toast({
              title: "Success",
              description: "Sync completed successfully.",
            })
            updateCompanyLists(user.id, selectedUserCompany.id)
          } else if (data.overallProgress === -1) {
            eventSource.close()
            setSyncInProgress(false)
            setYearProgress(null)
            toast({
              variant: "destructive",
              title: "Sync Failed",
              description: data.message || "An unknown error occurred during sync",
            })
          }
        }
  
        eventSource.onerror = (error) => {
          eventSource.close()
          setSyncInProgress(false)
          setYearProgress(null)
          console.error("EventSource error:", error)
          toast({
            variant: "destructive",
            title: "Sync Failed",
            description: "An error occurred while receiving sync updates",
          })
        }
      } else {
        throw new Error("Invalid response from server")
      }
    } catch (error) {
      console.error("Sync error:", error)
      setSyncInProgress(false)
      setYearProgress(null)
      toast({
        variant: "destructive",
        title: "Sync Failed",
        description: error instanceof Error ? error.message : "An unknown error occurred",
      })
    } finally {
      setCompanyLoading(companyId, false)
    }
  }

  const handleRefresh = async () => {
    if (!user || !selectedUserCompany) return
    try {
      await updateCompanyLists(user.id, selectedUserCompany.id)
      await fetchActiveTallyCompany(user.id)
      toast({
        title: "Success",
        description: "Successfully refreshed company data.",
      })
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Refresh Failed",
        description: error instanceof Error ? error.message : "An unknown error occurred",
      })
    }
  }

  const initializeSync = (companyId: string) => {
    const company = subscribedCompanies.find((c) => c.id === companyId)
    if (company && activeTallyCompany) {
      if (company.name === activeTallyCompany.tally_company_name) {
        handleSync(companyId)
      } else {
        setCompanySyncDetails(company)
        setSyncConfirmationOpen(true)
      }
    } else {
      toast({
        variant: "destructive",
        title: "Error",
        description: "Unable to find company details or active Tally company.",
      })
    }
  }
  

  const handleSyncConfirmed = async () => {
    if (!companySyncDetails) return
    setSyncConfirmationOpen(false)
    await handleSync(companySyncDetails.id)
    setCompanySyncDetails(null)
  }

  if (isLoading) {
    return <div>Loading...</div>
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <DashboardHeader onRefresh={handleRefresh} />

      <div className="container mx-auto p-6">
        <div className="flex items-center space-x-4 mb-6">
          <div className="flex-1">
            <Label htmlFor="userCompany" className="text-sm font-medium mb-1 block text-blue-600">
              Select User Company
            </Label>
            <Select value={selectedUserCompany?.id || ""} onValueChange={handleUserCompanyChange} disabled>
              <SelectTrigger id="userCompany" className="w-full">
                <SelectValue placeholder="Select a company" />
              </SelectTrigger>
              <SelectContent>
                {userCompanies.map((company) => (
                  <SelectItem key={company.id} value={company.id}>
                    {company.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          
          <div className="flex-1">
            <Label className="text-sm font-medium mb-1 block text-blue-600">Active Tally Company</Label>
            <Card className="p-2 h-10 flex items-center">
              <span className="text-sm truncate">
                {activeTallyCompany ? (
                  activeTallyCompany.tally_company_name
                ) : (
                  <span className="text-sm font-medium mb-1 block text-blue-600">No active Tally company</span>
                )}
              </span>
            </Card>
          </div>
        </div>

        <div className="flex-1">
  <Label className="text-sm font-medium text-gray-700 mb-2">Select Date Range</Label>
  <div className="flex gap-2 w-96 bg-white p-1.5 rounded-md border border-gray-200 shadow-sm">
    <div className="relative flex-1">
      <Input
        id="start-date"
        type="date"
        value={startDate}
        onChange={(e) => setStartDate(e.target.value)}
        className="pl-8 h-8 text-sm focus-visible:ring-blue-500 border-gray-200 hover:border-blue-300"
      />
      <Calendar className="absolute left-2 top-2 h-3.5 w-3.5 text-blue-500" />
    </div>
    <div className="relative flex-1">
      <Input
        id="end-date"
        type="date"
        value={endDate}
        onChange={(e) => setEndDate(e.target.value)}
        className="pl-8 h-8 text-sm focus-visible:ring-blue-500 border-gray-200 hover:border-blue-300"
        min={startDate}
      />
      <Calendar className="absolute left-2 top-2 h-3.5 w-3.5 text-blue-500" />
    </div>
  </div>
</div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <Card className="p-6">
            <h3 className="text-sm font-medium mb-1 block text-blue-600">Available Companies</h3>
            <CompanyList
              companies={availableCompanies}
              isSubscribed={false}
              onSubscribe={initializeSubscription}
              loadingStates={loadingStates}
            />
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium mb-1 block text-blue-600">Subscribed Companies</h3>
            <CompanyList
              companies={subscribedCompanies}
              isSubscribed={true}
              onSync={initializeSync}
              loadingStates={loadingStates}
            />
          </Card>
        </div>

        {syncInProgress && (
          <Card className="mt-4 p-4">
            <h3 className="text-sm font-medium mb-1 block text-blue-600 ">Sync Progress</h3>
            {overallProgress !== null && (
              <div>
                <Label>Overall Progress</Label>
                <Progress value={overallProgress} className="w-full" />
                <p className="mt-1 text-sm text-gray-600">{overallProgress.toFixed(2)}% Complete</p>
              </div>
            )}
          </Card>
        )}
      </div>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Confirm Subscription</DialogTitle>
            <DialogDescription>
              Are you sure you want to subscribe to {companyToSubscribe?.name}? You will be able to sync data after
              subscribing.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleSubscribeConfirmed}>Subscribe</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={syncConfirmationOpen} onOpenChange={setSyncConfirmationOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2 text-amber-500">
              <AlertTriangle className="h-5 w-5" />
              Confirm Sync
            </DialogTitle>
            <DialogDescription>
              <div className="space-y-2">
                <p>The Tally company names do not match:</p>
                <ul className="list-disc list-inside space-y-1">
                  <li>
                    Active Tally company:{" "}
                    <span className="font-semibold">{activeTallyCompany?.tally_company_name}</span>
                  </li>
                  <li>
                    Selected company to sync: <span className="font-semibold">{companySyncDetails?.name}</span>
                  </li>
                </ul>
                <p>Do you want to proceed with the sync?</p>
              </div>
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setSyncConfirmationOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleSyncConfirmed}>Proceed with Sync</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <ConnectionStatus isTallyConnected={tallyConnected} isInternetConnected={internetConnected} version="v 1.4.0" />
    </div>
  )
}

