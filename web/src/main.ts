// LocalArena web client: wires the game connection, the 3D view, identity and
// the shop/leaderboard UI. The server is authoritative (adr/0001): the client
// sends only movement intent and renders server snapshots.
import "./style.css";
import type { User } from "oidc-client-ts";
import { api, ApiError, initAuth, users, type Cosmetic, type Me } from "./api";
import { GUEST_ID_BASE } from "./config";
import { GameClient, type Snapshot } from "./net";
import { Arena3D, webglAvailable, type Cosmetics } from "./scene";

const $ = <T extends HTMLElement = HTMLElement>(id: string) => document.getElementById(id) as T;

let user: User | null = null;
let me: Me | null = null;
let catalog: Cosmetic[] = [];
const names = new Map<number, string>();
const nameLookups = new Set<number>();

// ---------- toasts ----------
function toast(text: string, kind: "ok" | "bad" | "info" = "info"): void {
  const el = document.createElement("div");
  el.className = `toast ${kind}`;
  el.textContent = text;
  $("toasts").append(el);
  setTimeout(() => el.remove(), 4000);
}

// ---------- names for labels ----------
function nameOf(id: number): string {
  if (id >= GUEST_ID_BASE) return `Guest ${id - GUEST_ID_BASE}`;
  if (me && id === me.profile.player_id) return me.profile.display_name;
  const n = names.get(id);
  if (n) return n;
  if (!nameLookups.has(id)) { // lazy, cached, once per id
    nameLookups.add(id);
    api.publicProfile(id).then((p) => names.set(id, p.display_name)).catch(() => {});
  }
  return `Player ${id}`;
}

// ---------- game ----------
const game = new GameClient(async () => {
  if (!user) return null;
  return (await api.ticket()).ticket; // short-lived HMAC ticket binds the socket to the account
});

let arena: Arena3D | null = null;
if (webglAvailable()) arena = new Arena3D($("stage"));
else $("nogl").hidden = false;

game.onState = (state, detail) => {
  const el = $("conn");
  el.dataset.state = state;
  el.textContent = detail;
};
game.onWelcome = (w) => {
  arena?.setWorld(w.w, w.h);
  $("mode").textContent = w.mode || "classic";
  if (w.mode === "frenzy") toast("Frenzy mode: more gems, faster players!");
};
game.onSnapshot = (prev: Snapshot | null, next: Snapshot) => {
  $("tick").textContent = String(next.tick);
  $("count").textContent = String(next.players.length);
  $("rtt").textContent = game.rtt ? `${game.rtt.toFixed(0)} ms` : "-";
  if (!prev || !arena) return;
  // Effects come from diffs between authoritative snapshots.
  const now = new Set(next.items.map(([x, y]) => `${x},${y}`));
  for (const [x, y] of prev.items) if (!now.has(`${x},${y}`)) arena.burst(x, y);
  const before = new Map(prev.players.map((p) => [p.id, p.s]));
  for (const p of next.players) {
    const s0 = before.get(p.id);
    if (s0 !== undefined && p.s > s0) arena.popup(p.x, p.y, `+${p.s - s0}`);
  }
  renderScores(next);
};

let lastScores = "";
function renderScores(s: Snapshot): void {
  const top = [...s.players].sort((a, b) => b.s - a.s).slice(0, 8);
  const key = top.map((p) => `${p.id}:${p.s}`).join();
  if (key === lastScores) return;
  lastScores = key;
  $("scores").replaceChildren(...top.map((p) => {
    const li = document.createElement("li");
    li.textContent = `${nameOf(p.id)}: ${p.s}`;
    if (p.id === game.me) li.className = "me";
    return li;
  }));
}

function cosmetics(): Cosmetics {
  const items = new Set(me?.profile.items ?? []);
  return { hat: items.has("hat-basic"), trail: items.has("trail-gold"), legend: items.has("skin-legend") };
}

let lastFrame = performance.now();
function loop(now: number): void {
  requestAnimationFrame(loop);
  const dt = Math.min(0.1, (now - lastFrame) / 1000);
  lastFrame = now;
  if (!arena) return;
  arena.frame(dt, game.sample(now), game.latest?.items ?? [], game.me, game.predicted(), nameOf, cosmetics());
}

// ---------- input: keyboard + touch joystick ----------
const held = new Set<string>();
const KEYS: Record<string, string> = {
  ArrowUp: "up", KeyW: "up", ArrowDown: "down", KeyS: "down", ArrowLeft: "left", KeyA: "left", ArrowRight: "right", KeyD: "right",
};
let stick = { x: 0, y: 0 };
function pushInput(): void {
  const dx = (held.has("right") ? 1 : 0) - (held.has("left") ? 1 : 0) + stick.x;
  const dy = (held.has("down") ? 1 : 0) - (held.has("up") ? 1 : 0) + stick.y;
  game.setInput(dx, dy);
}
const typing = (e: Event) => e.target instanceof HTMLInputElement;
addEventListener("keydown", (e) => {
  if (typing(e)) return;
  if (e.code === "KeyP") { togglePanel(); return; }
  const k = KEYS[e.code];
  if (k) { held.add(k); pushInput(); e.preventDefault(); }
});
addEventListener("keyup", (e) => { const k = KEYS[e.code]; if (k) { held.delete(k); pushInput(); } });
addEventListener("blur", () => { held.clear(); stick = { x: 0, y: 0 }; pushInput(); }); // no stuck keys

const joy = $("joystick");
const knob = joy.querySelector<HTMLElement>(".knob")!;
function joyMove(e: PointerEvent): void {
  const r = joy.getBoundingClientRect();
  const x = e.clientX - (r.left + r.width / 2), y = e.clientY - (r.top + r.height / 2);
  const len = Math.min(45, Math.hypot(x, y)), ang = Math.atan2(y, x);
  knob.style.transform = `translate(${Math.cos(ang) * len}px, ${Math.sin(ang) * len}px)`;
  // Protocol inputs are -1/0/1 per axis; dead zone of 12 px.
  stick = len < 12 ? { x: 0, y: 0 } : { x: Math.round(Math.cos(ang)), y: Math.round(Math.sin(ang)) };
  pushInput();
}
joy.addEventListener("pointerdown", (e) => { joy.setPointerCapture(e.pointerId); joyMove(e); });
joy.addEventListener("pointermove", (e) => { if (joy.hasPointerCapture(e.pointerId)) joyMove(e); });
joy.addEventListener("pointerup", () => { knob.style.transform = ""; stick = { x: 0, y: 0 }; pushInput(); });

function togglePanel(): void {
  const panel = $("panel"), btn = $("toggle");
  panel.hidden = !panel.hidden;
  btn.setAttribute("aria-expanded", String(!panel.hidden));
  arena?.resize();
}
$("toggle").addEventListener("click", togglePanel);
if (matchMedia("(max-width: 760px)").matches) togglePanel();

// ---------- account ----------
async function refreshMe(): Promise<void> {
  if (!user) { me = null; renderAccount(); return; }
  try {
    me = await api.me();
  } catch (e) {
    if (e instanceof ApiError && e.status === 401) { await users.removeUser(); user = null; me = null; }
    else toast(`Profile unavailable: ${(e as Error).message}`, "bad");
  }
  renderAccount();
}

function renderAccount(): void {
  $("guest").hidden = !!user;
  $("account").hidden = !user;
  if (me) {
    $("who").textContent = me.profile.display_name;
    $("roles").textContent = me.roles.length ? `(${me.roles.join(", ")})` : "";
    $("banned").hidden = !me.banned;
    $("credits").textContent = String(me.profile.credits);
    const owned = me.profile.items.map((sku) => catalog.find((c) => c.sku === sku)?.name ?? sku);
    $("items").textContent = owned.length ? owned.join(", ") : "nothing yet";
    const input = $<HTMLInputElement>("name");
    if (document.activeElement !== input) input.value = me.profile.display_name;
  }
  renderShop();
}

$("login").addEventListener("click", () => {
  users.signinRedirect().catch(() => toast("Sign-in is unavailable: is the security profile (Keycloak) running?", "bad"));
});
$("logout").addEventListener("click", async () => {
  try { await users.signoutRedirect(); } catch { await users.removeUser(); user = null; me = null; renderAccount(); game.reconnect(); }
});
users.events.addSilentRenewError(async () => {
  toast("Session expired; please sign in again", "bad");
  await users.removeUser();
  user = null; me = null; renderAccount();
});
$("rename").addEventListener("submit", async (e) => {
  e.preventDefault();
  const name = $<HTMLInputElement>("name").value.trim();
  try {
    await api.rename(name);
    toast("Name updated", "ok");
    await refreshMe();
  } catch (err) { toast(`Rename failed: ${(err as Error).message}`, "bad"); }
});

// ---------- shop ----------
async function loadShop(): Promise<void> {
  try { catalog = await api.cosmetics(); } catch { /* keep the last catalog */ }
  renderShop();
}

function renderShop(): void {
  const owned = new Set(me?.profile.items ?? []);
  $("shop").replaceChildren(...catalog.map((c) => {
    const li = document.createElement("li");
    const label = document.createElement("span");
    label.textContent = `${c.name} · ${c.price} cr${c.stock === null ? "" : ` · ${c.stock} left`}`;
    const btn = document.createElement("button");
    const has = owned.has(c.sku);
    btn.textContent = has ? "Owned" : c.stock === 0 ? "Sold out" : "Buy";
    btn.disabled = has || !me || me.banned || c.stock === 0 || (me?.profile.credits ?? 0) < c.price;
    btn.title = !me ? "Sign in to buy" : c.stock === 0 ? "Sold out" : "";
    btn.setAttribute("aria-label", `${has ? "Owned" : "Buy"} ${c.name} for ${c.price} credits`);
    btn.addEventListener("click", () => void buy(c, btn));
    li.append(label, btn);
    return li;
  }));
}

async function buy(c: Cosmetic, btn: HTMLButtonElement): Promise<void> {
  btn.disabled = true;
  btn.textContent = "Buying…";
  const key = crypto.randomUUID(); // one key per click; automatic retries reuse it
  try {
    const res = await api.buy(c.id, key);
    toast(res.replayed ? `${c.name}: already purchased (deduplicated)` : `Bought ${c.name}!`, "ok");
  } catch (e) {
    const s = e instanceof ApiError ? e.status : 0;
    const why = s === 402 ? "not enough credits" : s === 409 ? "sold out or already owned" : s === 403 ? "not allowed"
      : s === 429 ? "slow down" : s === 503 ? "shop temporarily unavailable, nothing was charged; try again" : (e as Error).message;
    toast(`Could not buy ${c.name}: ${why}`, "bad");
  }
  await Promise.all([refreshMe(), loadShop()]);
}

// ---------- leaderboard ----------
async function loadLeaderboard(): Promise<void> {
  try {
    const { top } = await api.leaderboard(10);
    $("leaderboard").replaceChildren(...top.map((r) => {
      const li = document.createElement("li");
      const n = document.createElement("span");
      n.textContent = r.name;
      const t = document.createElement("b");
      t.textContent = String(r.total);
      if (me && r.player_id === me.profile.player_id) li.className = "me";
      li.append(n, t);
      return li;
    }));
    $("lb-note").textContent = top.length ? "" : "No scores yet: sign in and collect gems.";
  } catch (e) {
    const s = e instanceof ApiError ? e.status : 0;
    $("lb-note").textContent = s === 503 ? "Leaderboard is catching up (streaming profile down or rebalancing)." : "Leaderboard unavailable.";
  }
}

// ---------- boot ----------
async function boot(): Promise<void> {
  try { user = await initAuth(); } catch { user = null; }
  await Promise.all([loadShop(), refreshMe()]);
  if (me) toast(`Welcome, ${me.profile.display_name}`, "ok");
  void game.connect();
  void loadLeaderboard();
  setInterval(() => void loadLeaderboard(), 5000);
  setInterval(() => { if (user) void refreshMe(); }, 15000); // credits from rewards arrive asynchronously
  requestAnimationFrame(loop);
}
void boot();
