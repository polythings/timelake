"use client"

import { Moon, Sun } from "lucide-react"
import { useTheme } from "next-themes"

import { Button } from "@/components/ui/button"

/**
 * Switches between the light and dark themes. Picking the theme the system prefers goes back to following the system.
 * The icon comes from CSS, so the server render matches whatever theme the page loads in.
 */
export function ThemeToggle({ className }: { className?: string }) {
  const { resolvedTheme, systemTheme, setTheme } = useTheme()
  const toggle = () => {
    const next = resolvedTheme === "dark" ? "light" : "dark"
    setTheme(next === systemTheme ? "system" : next)
  }
  return (
    <Button variant="ghost" size="icon" aria-label="Switch light or dark theme" onClick={toggle} className={className}>
      <Sun className="hidden size-4 dark:block" />
      <Moon className="size-4 dark:hidden" />
    </Button>
  )
}
