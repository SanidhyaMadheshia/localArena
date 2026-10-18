// Runtime endpoints. Defaults match the local Compose stack; override at build
// time with VITE_* variables (see GETTING_STARTED.md, "Frontend").
const host = location.hostname;
const wsProto = location.protocol === "https:" ? "wss" : "ws";

export const config = {
  /** Public API through Envoy (TLS edge is https://localhost:8443). */
  apiBase: import.meta.env.VITE_API_URL ?? `http://${host}:10000`,
  /** Keycloak realm; must equal the issuer the API trusts (OIDC_ISSUER). */
  oidcAuthority: import.meta.env.VITE_OIDC_AUTHORITY ?? `http://${host}:8180/realms/localarena`,
  oidcClientId: import.meta.env.VITE_OIDC_CLIENT_ID ?? "localarena-web",
  /** Game socket: same origin as the page (gameserver serves the client, Vite proxies /ws). */
  gameWs: import.meta.env.VITE_GAME_WS ?? `${wsProto}://${location.host}/ws`,
  /** Interpolation delay in ticks: render this far behind the newest snapshot. */
  interpTicks: 2,
};

/** Sessions without an account get IDs from this base (backend match.GuestIDBase). */
export const GUEST_ID_BASE = 1_000_000_000;
export const PROTOCOL_VERSION = 1;
