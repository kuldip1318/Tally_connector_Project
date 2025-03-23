import axios from "axios"

interface AuthService {
  fetchWithErrorHandling(url: string, options?: RequestInit): Promise<any>;
  login(identifier: string, password: string): Promise<any>;
  getUserCompanies(userId: string): Promise<Array<{ id: string; name: string; gstNumber?: string }>>;
  getTallyCompanies(): Promise<Array<{ id: string; name: string }>>;
  getSubscribedCompanies(userId: string, userCompanyId: string): Promise<Array<{ id: string; name: string }>>;  
  // getActiveTallyCompany(userId: string): Promise<ActiveTallyCompany | null>;
  subscribeToTallyCompany(userId: string, userCompanyId: string, tallyCompany: string): Promise<any>;
  // syncTallyCompanyData(userId: string, userCompanyId: string, tallyCompany: string): Promise<any>;
  checkTallyConnection(): Promise<boolean>;
  getUserProfile(userId: string): Promise<any>;
  getSubscribeId: (userId: string, userCompanyId: string, tallyCompanyId: string) => Promise<any>
  syncTallyCompanyData: (
    userId: string,
    userCompanyId: string,
    tallyCompanyId: string,
    subscribeId: string,
    startDate: string,
    endDate: string,
  ) => Promise<{ jobId: any }>
  getActiveTallyCompany(userId: string): Promise<{
    tally_company_id: number;
    user_id: number;
    tally_company_name: string;
    user_companies_names: string;
    active_status: number;
    created_at: string;
  } | null>;
}

interface ActiveTallyCompany {
  tally_company_id: number;
  user_id: number;
  tally_company_name: string;
  user_companies_names: string;
  active_status: number;
  created_at: string;
}
const AuthService: AuthService = {
  async fetchWithErrorHandling(url: string, options?: RequestInit) {
    try {
      const response = await fetch(url, options);
      
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || `HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      return data || [];
    } catch (error) {
      throw new Error(error instanceof Error ? error.message : 'An error occurred during the request');
    }
  },

  async login(identifier: string, password: string) {
    try {
      return await this.fetchWithErrorHandling('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ identifier, password }),
      });
    } catch (error) {
      throw new Error('An error occurred during login. Please try again.');
    }
  },

  async getUserCompanies(userId: string): Promise<Array<{ id: string; name: string; gstNumber?: string }>> {
    try {
      const data = await this.fetchWithErrorHandling(`/api/user-companies?userId=${userId}`);
      const companies = Array.isArray(data) ? data : [];
      
      return companies.map((company: any) => ({
        id: (company?.id || company?.company_id || '').toString(),
        name: company?.name || company?.business_name || 'Unnamed Company',
        gstNumber: company?.gstNumber || company?.gst_number || '',
      }));
    } catch (error) {
      return [];
    }
  },

  async getTallyCompanies(): Promise<Array<{ id: string; name: string }>> {
    try {
      const response = await this.fetchWithErrorHandling('/api/tally-companies');
      const companies = Array.isArray(response) ? response : [];
      
      return companies
        .filter((company: any) => {
          // Only return companies that have an actual name
          return company?.name && !company.name.includes('unknown-guid');
        })
        .map((company: any) => ({
          id: company.name, // Use company name as ID since it's unique in Tally
          name: company.name // Use the actual Tally company name
        }));
    } catch (error) {
      return [];
    }
  },
  
  async getSubscribedCompanies(userId: string, userCompanyId: string): Promise<Array<{ id: string; name: string }>> {
    try {
      const response = await this.fetchWithErrorHandling(
        `/api/subscribed-companies?userId=${userId}&userCompanyId=${userCompanyId}`
      );
      
      const companies = Array.isArray(response) ? response : [];
      
      console.log('Raw subscribed companies response:', response);
      
      const transformedCompanies = companies
        .filter((company: any) => {
          // Filter out any companies with 'unknown-guid'
          const name = company?.tally_company || company?.name || '';
          return name && !name.includes('unknown-guid');
        })
        .map((company: any) => ({
          id: company.tally_company || company.name, // Use tally_company name as ID
          name: company.tally_company || company.name // Use tally_company name for display
        }));
      
      console.log('Transformed subscribed companies:', transformedCompanies);
      
      return transformedCompanies;
    } catch (error) {
      console.error('Error fetching subscribed companies:', error);
      return [];
    }
  },


  async getActiveTallyCompany(userId: string) {
    try {
      const response = await fetch(`/api/tally-companies-active?userId=${userId}`);
      if (!response.ok) {
        throw new Error('Failed to fetch active Tally company');
      }
      return await response.json();
    } catch (error) {
      console.error('Error fetching active Tally company:', error);
      throw error;
    }
  },
  async subscribeToTallyCompany(userId: string, userCompanyId: string, tallyCompany: string) {
    try {
      // Here tallyCompany should already be the correct company name
      return await this.fetchWithErrorHandling('/api/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          userId, 
          userCompanyId, 
          tallyCompany // This will be the actual company name from Tally
        }),
      });
    } catch (error) {
      throw new Error('Failed to subscribe to Tally company. Please try again.');
    }
  },
  
  async getSubscribeId(userId: string, userCompanyId: string, tallyCompanyId: string) {
    try {
      const response = await axios.get(
        `/api/subscribe-id?userId=${userId}&userCompanyId=${userCompanyId}&tallyCompany=${tallyCompanyId}`,
      )
      if (!response.data || !response.data.subscribeId) {
        throw new Error("Invalid response format: subscribeId not found")
      }
      return response.data
    } catch (error) {
      console.error("Error fetching subscribeId:", error)
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        throw new Error("Subscribe ID not found for the given parameters")
      }
      throw new Error("Failed to fetch subscribeId: " + (error instanceof Error ? error.message : "Unknown error"))
    }
  },

  async syncTallyCompanyData(
    userId: string,
    userCompanyId: string,
    tallyCompanyId: string,
    subscribeId: string,
    startDate: string,
    endDate: string,
  ): Promise<{ jobId: any }> {
    if (!userId || !userCompanyId || !tallyCompanyId || !subscribeId) {
      throw new Error("userId, userCompanyId, tallyCompanyId, and subscribeId are required")
    }
  
    try {
      const response = await axios.post(
        "/api/sync",
        {
          userId,
          userCompanyId,
          tallyCompanyId,
          subscribeId,
          start_date: startDate,
          end_date: endDate
        },
        {
          headers: {
            "Content-Type": "application/json",
          },
        },
      )
  
      if (response.data.jobId) {
        return { jobId: response.data.jobId }
      } else {
        throw new Error("No job ID returned from the server")
      }
    } catch (error) {
      console.error("Sync error:", error)
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(`Failed to initiate Tally company data sync: ${error.response.data.error || error.message}`)
      }
      throw new Error("Failed to initiate Tally company data sync. Please try again.")
    }
  },

  async checkTallyConnection(): Promise<boolean> {
    try {
      await this.fetchWithErrorHandling('/api/check-tally-connection');
      return true;
    } catch (error) {
      return false;
    }
  },

  async getUserProfile(userId: string) {
    try {
      return await this.fetchWithErrorHandling(`/api/user-profile?userId=${userId}`);
    } catch (error) {
      throw new Error('Failed to fetch user profile. Please try again.');
    }
  },
};


export default AuthService;



