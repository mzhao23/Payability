import { createClient, SupabaseClient } from "@supabase/supabase-js";

let _client: SupabaseClient | null = null;

export function supabaseAdmin(): SupabaseClient {
  if (_client) return _client;

  const url = process.env.SUPABASE_URL;
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!url) throw new Error("SUPABASE_URL is required.");
  if (!serviceRoleKey) throw new Error("SUPABASE_SERVICE_ROLE_KEY is required.");

  _client = createClient(url, serviceRoleKey, {
    auth: { persistSession: false },
  });

  return _client;
}