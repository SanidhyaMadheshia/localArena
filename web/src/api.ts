// Identity (Keycloak OIDC, authorization code + PKCE) and the /v2 API client.
// The browser never sees a client secret; tokens live in sessionStorage and
// are sent only as Authorization headers (no cookies, so no CSRF surface).
import { UserManager, WebStorageStateStore, type User } from "oidc-client-ts";
import { config } from "./config";

const here = location.origin + location.pathname;

export const users = new UserManager({
  authority: config.oidcAuthority,
  client_id: config.oidcClientId,
  redirect_uri: here,
  post_logout_redirect_uri: here,
  response_type: "code", // PKCE S256 is automatic and enforced by the realm
  scope: "openid profile",
  automaticSilentRenew: true, // uses the refresh token before expiry
  userStore: new WebStorageStateStore({ store: sessionStorage }),
});

/** Completes a login redirect if present; returns the signed-in user, if any. */
export async function initAuth(): Promise<User | null> {
  const q = new URLSearchParams(location.search);
  if (q.has("code") && q.has("state")) {
    try {
      const u = await users.signinRedirectCallback();
      return u;
    } catch (e) {
      console.warn("login callback failed", e);
    } finally {
      history.replaceState(null, "", location.pathname); // drop code/state from the URL
    }
  }
  const u = await users.getUser();
  return u && !u.expired ? u : null;
}

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * fetch wrapper. Retries only when it is safe (GET, or a write carrying an
 * Idempotency-Key) and only on network errors / 502-504, with capped
 * exponential backoff and full jitter (adr/0007): retries never duplicate a
 * purchase because the server deduplicates by key.
 */
async function call<T>(path: string, init: RequestInit = {}, auth = false): Promise<T> {
  const headers = new Headers(init.headers);
  if (init.body) headers.set("Content-Type", "application/json");
  if (auth) {
    const u = await users.getUser();
    if (!u || u.expired) throw new ApiError(401, "please sign in");
    headers.set("Authorization", `Bearer ${u.access_token}`);
  }
  const method = init.method ?? "GET";
  const retryable = method === "GET" || headers.has("Idempotency-Key");
  const attempts = retryable ? 3 : 1;
  let last: unknown;
  for (let i = 0; i < attempts; i++) {
    if (i > 0) {
      const retryAfter = last instanceof ApiError && last.status === 503 ? 1000 : 0;
      await sleep(Math.max(retryAfter, Math.random() * 250 * 2 ** i));
    }
    try {
      const res = await fetch(config.apiBase + path, { ...init, method, headers, signal: AbortSignal.timeout(5000) });
      const body = res.headers.get("Content-Type")?.includes("json") ? await res.json() : await res.text();
      if (res.ok) return body as T;
      const msg = typeof body === "object" && body && "error" in body ? String(body.error) : `${res.status} ${res.statusText}`;
      last = new ApiError(res.status, msg);
      if (![502, 503, 504].includes(res.status)) break;
    } catch (e) {
      last = e instanceof ApiError ? e : new ApiError(0, "network error: API unreachable");
    }
  }
  throw last;
}

export interface Profile {
  player_id: number;
  username: string;
  display_name: string;
  credits: number;
  items: string[];
}
export interface Me { profile: Profile; roles: string[]; banned: boolean }
export interface Cosmetic { id: number; sku: string; name: string; price: number; stock: number | null }
export interface LeaderRow { player_id: number; name: string; total: number }
export interface PurchaseResult { replayed?: boolean; [k: string]: unknown }

export const api = {
  me: () => call<Me>("/v2/me", {}, true),
  rename: (display_name: string) =>
    call<unknown>("/v2/me/profile", { method: "PUT", body: JSON.stringify({ display_name }) }, true),
  cosmetics: () => call<Cosmetic[]>("/v2/cosmetics"),
  /** The caller keeps `key` for retries of the same click: exactly one purchase. */
  buy: (cosmetic_id: number, key: string) =>
    call<PurchaseResult>("/v2/purchases", {
      method: "POST",
      headers: { "Idempotency-Key": key },
      body: JSON.stringify({ cosmetic_id }),
    }, true),
  ticket: () => call<{ ticket: string; player_id: number; expires_in: number }>("/v2/game-tickets", { method: "POST" }, true),
  leaderboard: (n = 10) => call<{ top: LeaderRow[] }>(`/v2/leaderboard?n=${n}`),
  publicProfile: (id: number) => call<Profile>(`/v2/players/${id}`),
};
