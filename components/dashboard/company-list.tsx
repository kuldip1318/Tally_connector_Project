'use client'

import { BarChart3, Loader2 } from 'lucide-react'
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"

interface CompanyItemProps {
  name: string
  id: string
  isSubscribed: boolean
  onSync?: () => void
  onSubscribe?: () => void
  isLoading?: boolean
}

function CompanyItem({ 
  name, 
  id, 
  isSubscribed, 
  onSync, 
  onSubscribe,
  isLoading 
}: CompanyItemProps) {
  return (
    <Card className="p-4 hover:bg-gray-50 transition-colors">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-gray-100 rounded">
            <BarChart3 className="h-5 w-5 text-gray-500" />
          </div>
          <div>
            <span className="font-medium">{name}</span>
            <span className="text-gray-500 ml-2">({id})</span>
          </div>
        </div>
        {isSubscribed ? (
          <Button 
            onClick={onSync} 
            size="sm" 
            variant="outline"
            disabled={isLoading}
          >
            {isLoading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Syncing...
              </>
            ) : (
              'Sync Data'
            )}
          </Button>
        ) : (
          <Button
            onClick={onSubscribe}
            size="sm"
            className="bg-blue-600 hover:bg-blue-700 text-white"
            disabled={isLoading}
          >
            {isLoading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Subscribing...
              </>
            ) : (
              'Subscribe'
            )}
          </Button>
        )}
      </div>
    </Card>
  )
}

interface CompanyListProps {
  companies: Array<{ name: string; id: string }>
  isSubscribed: boolean
  onSync?: (companyId: string) => void
  onSubscribe?: (companyId: string) => void
  loadingStates?: { [key: string]: boolean }
}

export function CompanyList({ 
  companies, 
  isSubscribed, 
  onSync,
  onSubscribe,
  loadingStates = {}
}: CompanyListProps) {
  return (
    <div className="space-y-2">
      {companies.length === 0 ? (
        <div className="text-center py-12">
          <img 
            src="/placeholder.svg?height=100&width=100" 
            alt="No companies" 
            className="mx-auto mb-4 opacity-50"
          />
          <p className="text-gray-500">
            {isSubscribed ? "No subscribed companies." : "No available companies."}
          </p>
        </div>
      ) : (
        companies.map((company) => (
          <CompanyItem 
            key={company.id}
            name={company.name}
            id={company.id}
            isSubscribed={isSubscribed}
            onSync={onSync ? () => onSync(company.id) : undefined}
            onSubscribe={onSubscribe ? () => onSubscribe(company.id) : undefined}
            isLoading={loadingStates[company.id]}
          />
        ))
      )}
    </div>
  )
}

// 'use client'

// import { BarChart3 } from 'lucide-react'
// import { Card } from "@/components/ui/card"
// import { Button } from "@/components/ui/button"

// interface CompanyItemProps {
//   name: string
//   id: string
//   isSubscribed: boolean
//   onSync?: () => void
// }

// function CompanyItem({ name, id, isSubscribed, onSync }: CompanyItemProps) {
//   return (
//     <Card className="p-4 hover:bg-gray-50 transition-colors">
//       <div className="flex items-center justify-between">
//         <div className="flex items-center gap-3">
//           <div className="p-2 bg-gray-100 rounded">
//             <BarChart3 className="h-5 w-5 text-gray-500" />
//           </div>
//           <div>
//             <span className="font-medium">{name}</span>
//             <span className="text-gray-500 ml-2">({id})</span>
//           </div>
//         </div>
//         {isSubscribed && (
//           <Button onClick={onSync} size="sm" variant="outline">
//             Sync Data
//           </Button>
//         )}
//       </div>
//     </Card>
//   )
// }

// interface CompanyListProps {
//   companies: Array<{ name: string; id: string }>
//   isSubscribed: boolean
//   onSync?: (companyId: string) => void
// }

// export function CompanyList({ companies, isSubscribed, onSync }: CompanyListProps) {
//   return (
//     <div className="space-y-2">
//       {companies.length === 0 ? (
//         <div className="text-center py-12">
//           <img 
//             src="/placeholder.svg?height=100&width=100" 
//             alt="No companies" 
//             className="mx-auto mb-4 opacity-50"
//           />
//           <p className="text-gray-500">
//             {isSubscribed ? "No subscribed companies." : "No available companies."}
//           </p>
//         </div>
//       ) : (
//         companies.map((company) => (
//           <CompanyItem 
//             key={company.id}
//             name={company.name}
//             id={company.id}
//             isSubscribed={isSubscribed}
//             onSync={isSubscribed ? () => onSync && onSync(company.id) : undefined}
//           />
//         ))
//       )}
//     </div>
//   )
// }

