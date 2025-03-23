export async function checkAuth(): Promise<{ isAuthenticated: boolean; userId?: string }> {
  const user = localStorage.getItem('user')
  if (!user) {
    return { isAuthenticated: false }
  }
  
  try {
    const userData = JSON.parse(user)
    if (userData && userData.id) {
      // You might want to add an API call here to verify the token with your backend
      // For now, we'll just check if the user data exists
      return { isAuthenticated: true, userId: userData.id.toString() }
    }
  } catch (error) {
    console.error('Error parsing user data:', error)
  }

  return { isAuthenticated: false }
}

