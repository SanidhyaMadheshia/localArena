// Game connection: WebSocket protocol v1 (backend/internal/protocol), snapshot
// interpolation for remote players and client-side prediction with server
// reconciliation for the local player. The server stays authoritative: the
// client only *displays* predictions and always snaps back to server state.
import { config, PROTOCOL_VERSION } from "./config";

export interface Welcome { t: "welcome"; v: number; id: number; match: string; tickHz: number; w: number; h: number; mode?: string }
export interface PlayerState { id: number; x: number; y: number; s: number }
export interface Snapshot { t: "snap"; tick: number; ack: number; players: PlayerState[]; items: [number, number][] }
type ServerMsg = Welcome | Snapshot;

export type ConnState = "connecting" | "open" | "closed" | "unsupported";

const PLAYER_R = 12;

export class GameClient {
  me = 0;
  match = "";
  mode = "classic";
  tickHz = 20;
  world = { w: 1000, h: 1000 };
  rtt = 0;
  latest: Snapshot | null = null;

  onWelcome: (w: Welcome) => void = () => {};
  onSnapshot: (prev: Snapshot | null, next: Snapshot) => void = () => {};
  onState: (s: ConnState, detail: string) => void = () => {};

  private ws: WebSocket | null = null;
  private timer = 0;
  private seq = 0;
  private dx = 0;
  private dy = 0;
  private buf: Snapshot[] = [];
  private offset: number | null = null; // ms: receive time minus server tick time
  private pending: { seq: number; dx: number; dy: number; at: number }[] = [];
  private attempt = 0;
  private stopped = false;

  /** ticket() returns a game ticket for account play, or null for guest play. */
  constructor(private ticket: () => Promise<string | null>) {}

  get speed(): number {
    return this.mode === "frenzy" ? 14 : 10; // backend match.ApplyMode
  }

  async connect(): Promise<void> {
    this.stopped = false;
    this.onState("connecting", "connecting…");
    let url = config.gameWs;
    try {
      const t = await this.ticket();
      if (t) url += `?ticket=${encodeURIComponent(t)}`;
    } catch (e) {
      this.onState("connecting", `account play unavailable (${(e as Error).message}); joining as guest`);
    }
    const ws = new WebSocket(url);
    this.ws = ws;
    ws.onopen = () => { this.attempt = 0; };
    ws.onmessage = (ev) => this.handle(ws, JSON.parse(ev.data as string) as ServerMsg);
    ws.onclose = (ev) => {
      clearInterval(this.timer);
      if (this.ws !== ws || this.stopped) return;
      // Exponential backoff with full jitter avoids synchronized reconnect storms (Exp 001).
      const delay = Math.random() * Math.min(10_000, 500 * 2 ** this.attempt++);
      this.onState("closed", `disconnected (${ev.code || "network"}), retrying in ${(delay / 1000).toFixed(1)} s`);
      setTimeout(() => void this.connect(), delay);
    };
  }

  /** Reconnect now (e.g. after login, to play as the account). */
  reconnect(): void {
    const old = this.ws;
    this.ws = null;
    old?.close();
    clearInterval(this.timer);
    this.reset();
    void this.connect();
  }

  setInput(dx: number, dy: number): void {
    this.dx = Math.sign(dx);
    this.dy = Math.sign(dy);
  }

  private reset(): void {
    this.buf = [];
    this.pending = [];
    this.offset = null;
    this.latest = null;
  }

  private handle(ws: WebSocket, msg: ServerMsg): void {
    if (msg.t === "welcome") {
      if (msg.v !== PROTOCOL_VERSION) {
        this.stopped = true;
        this.onState("unsupported", `server speaks protocol v${msg.v}; please reload`);
        ws.close();
        return;
      }
      this.reset();
      this.me = msg.id;
      this.match = msg.match;
      this.mode = msg.mode || "classic";
      this.tickHz = msg.tickHz;
      this.world = { w: msg.w, h: msg.h };
      this.onWelcome(msg);
      this.onState("open", `in ${msg.match}`);
      // Send held input at the tick rate; the server applies the latest one per tick.
      this.timer = window.setInterval(() => this.sendInput(ws), 1000 / msg.tickHz);
      return;
    }
    const now = performance.now();
    const dt = 1000 / this.tickHz;
    const o = now - msg.tick * dt;
    // Track the earliest arrival (lowest-latency path), drift up slowly.
    this.offset = this.offset === null || o < this.offset ? o : this.offset + (o - this.offset) * 0.02;
    const acked = this.pending.find((p) => p.seq === msg.ack);
    if (acked) this.rtt = now - acked.at;
    this.pending = this.pending.filter((p) => p.seq > msg.ack);
    const prev = this.latest;
    if (prev && msg.tick < prev.tick) { // new round restarted the tick counter
      this.reset();
      this.offset = o;
    }
    this.latest = msg;
    this.buf.push(msg);
    if (this.buf.length > 40) this.buf.shift();
    this.onSnapshot(prev, msg);
  }

  private sendInput(ws: WebSocket): void {
    if (ws.readyState !== WebSocket.OPEN) return;
    this.seq++;
    this.pending.push({ seq: this.seq, dx: this.dx, dy: this.dy, at: performance.now() });
    if (this.pending.length > 60) this.pending.shift();
    ws.send(JSON.stringify({ t: "input", seq: this.seq, dx: this.dx, dy: this.dy, ct: Date.now(), st: this.latest?.tick ?? 0 }));
  }

  /** Remote players interpolated `interpTicks` behind the newest snapshot. */
  sample(now: number): Map<number, PlayerState> {
    const out = new Map<number, PlayerState>();
    if (!this.buf.length || this.offset === null) return out;
    const dt = 1000 / this.tickHz;
    const target = (now - this.offset) / dt - config.interpTicks;
    let a = this.buf[0], b = this.buf[this.buf.length - 1];
    for (let i = this.buf.length - 1; i > 0; i--) {
      if (this.buf[i - 1].tick <= target) { a = this.buf[i - 1]; b = this.buf[i]; break; }
    }
    const span = b.tick - a.tick;
    const alpha = span > 0 ? Math.min(1, Math.max(0, (target - a.tick) / span)) : 1;
    const from = new Map(a.players.map((p) => [p.id, p]));
    for (const p of b.players) {
      const q = from.get(p.id);
      out.set(p.id, q ? { id: p.id, x: q.x + (p.x - q.x) * alpha, y: q.y + (p.y - q.y) * alpha, s: p.s } : p);
    }
    return out;
  }

  /** Local player: last server position plus not-yet-acknowledged inputs (reconciliation). */
  predicted(): { x: number; y: number } | null {
    const p = this.latest?.players.find((q) => q.id === this.me);
    if (!p) return null;
    const clamp = (v: number, max: number) => Math.min(max - PLAYER_R, Math.max(PLAYER_R, v));
    let x = p.x, y = p.y;
    for (const i of this.pending) {
      x = clamp(x + i.dx * this.speed, this.world.w);
      y = clamp(y + i.dy * this.speed, this.world.h);
    }
    return { x, y };
  }
}
