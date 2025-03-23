'use client'

import { useState, useEffect } from 'react'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useRouter } from 'next/navigation'
import { useToast } from '@/components/ui/use-toast'
import AuthService from '@/lib/AuthService'

interface UserData {
  full_name: string;
  email: string;
  mobile_number: string;
}

export function ProfileTab() {
  const router = useRouter()
  const { toast } = useToast()
  const [userData, setUserData] = useState<UserData | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    const fetchUserData = async () => {
      setIsLoading(true);
      try {
        const user = JSON.parse(localStorage.getItem('user') || '{}');
        console.log('User from localStorage:', user);
        if (user.id) {
          console.log('Fetching user profile for ID:', user.id);
          const data = await AuthService.getUserProfile(user.id);
          console.log('Fetched user data:', data);
          setUserData(data);
        } else {
          console.error('User ID not found in localStorage');
          toast({
            variant: "destructive",
            title: "Error",
            description: "User ID not found. Please log in again.",
          });
        }
      } catch (error) {
        console.error('Failed to fetch user data:', error);
        toast({
          variant: "destructive",
          title: "Error",
          description: "Failed to load user profile. Please try again.",
        });
      } finally {
        setIsLoading(false);
      }
    };

    fetchUserData();
  }, [toast]);

  const handleLogout = () => {
    localStorage.removeItem('user')
    router.push('/auth/login')
  }

  const handleChangePassword = () => {
    toast({
      title: "Coming soon",
      description: "Password change functionality will be available soon.",
    })
  }

  if (isLoading) {
    return <div className="flex justify-center items-center h-64">Loading...</div>
  }

  return (
    <Card className="w-full max-w-4xl mx-auto">
      <CardContent className="p-6">
        <div className="flex flex-col md:flex-row justify-between items-start md:items-center mb-6">
          <h2 className="text-2xl font-semibold mb-4 md:mb-0">Personal Information</h2>
          <div className="space-y-2 md:space-y-0 md:space-x-2">
            <Button onClick={handleChangePassword} variant="outline" className="w-full md:w-auto">
              Change Password
            </Button>
            <Button 
              variant="destructive" 
              className="w-full md:w-auto"
              onClick={handleLogout}
            >
              Logout
            </Button>
          </div>
        </div>
        
        <div className="grid gap-6">
          <div className="grid md:grid-cols-2 gap-6">
            <div className="space-y-2">
              <label className="text-sm font-medium text-gray-700">Full Name:</label>
              <Input value={userData?.full_name || ''} disabled className="bg-gray-100" />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium text-gray-700">Email:</label>
              <Input value={userData?.email || ''} disabled className="bg-gray-100" />
            </div>
          </div>

          <div className="grid md:grid-cols-2 gap-6">
            <div className="space-y-2">
              <label className="text-sm font-medium text-gray-700">Mobile:</label>
              <Input value={userData?.mobile_number || ''} disabled className="bg-gray-100" />
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

