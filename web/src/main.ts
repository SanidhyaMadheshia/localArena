// LocalArena Canvas client: renders server snapshots and sends held input.
// It never computes authoritative state; positions and scores come only from
// the server (adr/0001). Protocol v1 is documented in backend/internal/protocol.

interface Welcome { t: "welcome"; v: number; id: number; match: string; tickHz: number; w: number; h: number }
interface PlayerState { id: number; x: number; y: number; s: number }
interface Snapshot { t: "snap"; tick: number; ack: number; players: PlayerState[]; items: [number, number][] }
type ServerMsg = Welcome | Snapshot;

const PROTOCOL_VERSION = 1;
const PLAYER_R = 12, ITEM_R = 8;

const canvas = document.getElementById("arena") as HTMLCanvasElement;
const ctx = canvas.getContext("2d")!;
const $ = (id: string) => document.getElementById(id)!;

let me = 0;
let snap: Snapshot | null = null;
let seq = 0;
const sentAt = new Map<number, number>(); // seq -> performance.now()
const held = new Set<string>();

function connect(attempt = 0): void {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  let timer = 0;

  ws.onopen = () => { attempt = 0; };
  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data as string) as ServerMsg;
    if (msg.t === "welcome") {
      if (msg.v !== PROTOCOL_VERSION) { setStatus(`protocol v${msg.v} unsupported`, "closed"); ws.close(); return; }
      me = msg.id;
      canvas.width = msg.w; canvas.height = msg.h;
      setStatus(`player ${msg.id} in ${msg.match}`, "open");
      // Send held input at the server tick rate; the server keeps the last one.
      timer = window.setInterval(() => sendInput(ws), 1000 / msg.tickHz);
    } else if (msg.t === "snap") {
      const t = sentAt.get(msg.ack);
      if (t !== undefined) $("rtt").textContent = `${(performance.now() - t).toFixed(0)} ms`;
      for (const s of sentAt.keys()) if (s <= msg.ack) sentAt.delete(s);
      snap = msg;
    }
  };
  ws.onclose = (ev) => {
    clearInterval(timer);
    // Exponential backoff with full jitter avoids synchronized reconnect storms.
    const delay = Math.random() * Math.min(10_000, 250 * 2 ** attempt);
    setStatus(`disconnected (${ev.code}); retrying in ${(delay / 1000).toFixed(1)} s`, "closed");
    setTimeout(() => connect(attempt + 1), delay);
  };
}

function sendInput(ws: WebSocket): void {
  if (ws.readyState !== WebSocket.OPEN) return;
  const dx = (held.has("right") ? 1 : 0) - (held.has("left") ? 1 : 0);
  const dy = (held.has("down") ? 1 : 0) - (held.has("up") ? 1 : 0);
  seq++;
  sentAt.set(seq, performance.now());
  ws.send(JSON.stringify({ t: "input", seq, dx, dy, ct: Date.now(), st: snap?.tick ?? 0 }));
}

const KEYS: Record<string, string> = {
  ArrowUp: "up", KeyW: "up", ArrowDown: "down", KeyS: "down",
  ArrowLeft: "left", KeyA: "left", ArrowRight: "right", KeyD: "right",
};
addEventListener("keydown", (e) => { const k = KEYS[e.code]; if (k) { held.add(k); e.preventDefault(); } });
addEventListener("keyup", (e) => { const k = KEYS[e.code]; if (k) held.delete(k); });
addEventListener("blur", () => held.clear()); // avoid stuck keys when focus leaves

function setStatus(text: string, state: "open" | "closed"): void {
  const el = $("status");
  el.textContent = text;
  el.dataset.state = state;
}

function draw(): void {
  requestAnimationFrame(draw);
  if (!snap) return;
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "#ffd54f";
  for (const [x, y] of snap.items) ctx.fillRect(x - ITEM_R, y - ITEM_R, ITEM_R * 2, ITEM_R * 2);
  for (const p of snap.players) {
    ctx.beginPath();
    ctx.arc(p.x, p.y, PLAYER_R, 0, Math.PI * 2);
    ctx.fillStyle = p.id === me ? "#4fc3f7" : `hsl(${(p.id * 47) % 360} 60% 60%)`;
    ctx.fill();
  }
  $("tick").textContent = String(snap.tick);
  const list = $("scores");
  list.replaceChildren(...[...snap.players].sort((a, b) => b.s - a.s).slice(0, 10).map((p) => {
    const li = document.createElement("li");
    li.textContent = `player ${p.id}: ${p.s}`;
    if (p.id === me) li.className = "me";
    return li;
  }));
}

connect();
requestAnimationFrame(draw);
