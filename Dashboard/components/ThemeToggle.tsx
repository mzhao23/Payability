"use client";

import { useEffect, useState } from "react";
import { THEME_STORAGE_KEY } from "@/lib/theme";

export function ThemeToggle() {
  const [theme, setTheme] = useState<"light" | "dark">("light");
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    const stored = localStorage.getItem(THEME_STORAGE_KEY) as "light" | "dark" | null;
    const root = document.documentElement;
    if (stored === "dark") {
      root.classList.add("dark");
      setTheme("dark");
    } else {
      root.classList.remove("dark");
      setTheme("light");
    }
  }, []);

  function toggle() {
    const root = document.documentElement;
    const next: "light" | "dark" = root.classList.contains("dark") ? "light" : "dark";
    if (next === "dark") {
      root.classList.add("dark");
      localStorage.setItem(THEME_STORAGE_KEY, "dark");
    } else {
      root.classList.remove("dark");
      localStorage.setItem(THEME_STORAGE_KEY, "light");
    }
    setTheme(next);
  }

  if (!mounted) {
    return (
      <span className="inline-block h-8 min-w-[5.5rem]" aria-hidden />
    );
  }

  return (
    <button
      type="button"
      onClick={toggle}
      className="text-sm px-3 py-1.5 rounded-md border border-gray-300 dark:border-zinc-600 bg-white dark:bg-zinc-800 text-gray-800 dark:text-zinc-100 hover:bg-gray-50 dark:hover:bg-zinc-700 transition-colors"
    >
      {theme === "dark" ? "Light mode" : "Dark mode"}
    </button>
  );
}
