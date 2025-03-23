"use client"

import type * as React from "react"
import * as Progress from "@radix-ui/react-progress"
import { X } from "lucide-react"
import { cva, type VariantProps } from "class-variance-authority"

const notificationVariants = cva("fixed z-50 flex w-80 flex-col gap-2 rounded-lg p-4 shadow-lg", {
  variants: {
    position: {
      "top-right": "top-4 right-4",
      "top-left": "top-4 left-4",
      "bottom-right": "bottom-4 right-4",
      "bottom-left": "bottom-4 left-4",
    },
    variant: {
      default: "bg-slate-800 text-slate-100",
      secondary: "bg-slate-100 text-slate-900",
    },
  },
  defaultVariants: {
    position: "top-right",
    variant: "default",
  },
})

interface ProgressNotificationProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof notificationVariants> {
  title?: string
  progress: number
  showCancel?: boolean
  onCancel?: () => void
}

export function ProgressNotification({
  className,
  variant,
  position,
  title = "Processing...",
  progress,
  showCancel = true,
  onCancel,
  ...props
}: ProgressNotificationProps) {
  return (
    <div className={notificationVariants({ variant, position, className })} {...props}>
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium">
          {title} {Math.round(progress)}%
        </span>
        {showCancel && (
          <button
            onClick={onCancel}
            className="rounded-sm opacity-70 ring-offset-background transition-opacity hover:opacity-100 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:pointer-events-none data-[state=open]:bg-accent data-[state=open]:text-muted-foreground"
          >
            <X className="h-4 w-4" />
            <span className="sr-only">Cancel</span>
          </button>
        )}
      </div>
      <Progress.Root
        className="relative h-2 w-full overflow-hidden rounded-full bg-slate-600"
        style={{
          transform: "translateZ(0)",
        }}
      >
        <Progress.Indicator
          className="h-full w-full bg-blue-500 transition-transform duration-500 ease-in-out"
          style={{ transform: `translateX(-${100 - progress}%)` }}
        />
      </Progress.Root>
    </div>
  )
}

