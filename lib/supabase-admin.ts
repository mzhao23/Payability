// lib/supabase-admin.ts
import { createClient, SupabaseClient } from "@supabase/supabase-js";

let _client: SupabaseClient | null = null;

export function supabaseAdmin(): SupabaseClient {
  if (_client) return _client;

  const url = process.env.SUPABASE_URL;
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

  // Don't crash at import/build time. Only throw when actually used.
  if (!url) {
    throw new Error("SUPABASE_URL is required (missing env var).");
  }
  if (!serviceRoleKey) {
    throw new Error("SUPABASE_SERVICE_ROLE_KEY is required (missing env var).");
  }

  _client = createClient(url, serviceRoleKey, {
    auth: { persistSession: false },
  });

  return _client;
}