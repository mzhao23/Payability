"use client";
import { useState } from "react";
import { getSupabaseBrowser } from "@/lib/supabase-browser";
import { useRouter } from "next/navigation";
import { ThemeToggle } from "@/components/ThemeToggle";

export default function LoginPage() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const router = useRouter();
  const supabase = getSupabaseBrowser();

  async function handleLogin(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError("");

    const { error } = await supabase.auth.signInWithPassword({ email, password });
    if (error) {
      setError(error.message);
      setLoading(false);
    } else {
      router.push("/dashboard");
    }
  }

  async function handleRegister() {
    setLoading(true);
    setError("");

    if (!email.endsWith("@payability.com")) {
      setError("Only @payability.com emails can register.");
      setLoading(false);
      return;
    }

    const { error } = await supabase.auth.signUp({ email, password });
    if (error) {
      setError(error.message);
    } else {
      setError("");
      alert("Check your email for a confirmation link.");
    }
    setLoading(false);
  }

  const inputClass =
    "w-full px-3 py-2 border border-gray-300 dark:border-zinc-600 rounded-md text-gray-900 dark:text-zinc-100 bg-white dark:bg-zinc-800 placeholder:text-gray-600 dark:placeholder:text-zinc-400";

  return (
    <div className="min-h-screen flex flex-col bg-gray-50 dark:bg-zinc-950">
      <header className="flex justify-end items-center px-4 py-3 border-b border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 shrink-0">
        <ThemeToggle />
      </header>
      <div className="flex-1 flex items-center justify-center p-4">
        <div className="w-full max-w-md bg-white dark:bg-zinc-900 rounded-lg shadow-md p-8 border border-gray-200 dark:border-zinc-800">
          <h1 className="text-2xl font-bold text-gray-900 dark:text-zinc-100 mb-6 text-center">
            Payability Risk Dashboard
          </h1>
          <form onSubmit={handleLogin} className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-zinc-300 mb-1">
                Email
              </label>
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className={inputClass}
                placeholder="you@payability.com"
                required
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-zinc-300 mb-1">
                Password
              </label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className={inputClass}
                required
              />
            </div>
            {error && <p className="text-red-600 dark:text-red-400 text-sm">{error}</p>}
            <button
              type="submit"
              disabled={loading}
              className="w-full py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
            >
              {loading ? "Loading..." : "Sign In"}
            </button>
            <button
              type="button"
              onClick={handleRegister}
              disabled={loading}
              className="w-full py-2 border border-gray-300 dark:border-zinc-600 text-gray-700 dark:text-zinc-200 rounded-md hover:bg-gray-50 dark:hover:bg-zinc-800"
            >
              Register (payability.com only)
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}