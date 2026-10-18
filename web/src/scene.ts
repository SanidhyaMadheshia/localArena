// three.js view of the arena. Pure presentation: it renders whatever state it
// is given and owns no game rules. World units (1000 x 1000, y down) map to
// scene units with scale S on the XZ plane.
import * as THREE from "three";
import { CSS2DObject, CSS2DRenderer } from "three/examples/jsm/renderers/CSS2DRenderer.js";

const S = 0.1;
const PLAYER_R = 12 * S;
const ITEM_R = 8 * S;
const reducedMotion = matchMedia("(prefers-reduced-motion: reduce)").matches;

export interface Cosmetics { hat: boolean; trail: boolean; legend: boolean }

interface Avatar {
  group: THREE.Group;
  body: THREE.Mesh;
  label: CSS2DObject;
  labelEl: HTMLDivElement;
  last: THREE.Vector3;
  heading: number;
  hat?: THREE.Mesh;
}

interface Burst { points: THREE.Points; vel: Float32Array; life: number; max: number }
interface Popup { obj: CSS2DObject; life: number }

export class Arena3D {
  readonly renderer: THREE.WebGLRenderer;
  private labels = new CSS2DRenderer();
  private scene = new THREE.Scene();
  private camera = new THREE.PerspectiveCamera(50, 1, 0.1, 1000);
  private floor = new THREE.Group();
  private avatars = new Map<number, Avatar>();
  private gems: THREE.Mesh[] = [];
  private gemGeo = new THREE.OctahedronGeometry(ITEM_R * 1.3);
  private gemMat = new THREE.MeshStandardMaterial({ color: 0xffd54f, emissive: 0xffa000, emissiveIntensity: 0.9, metalness: 0.3, roughness: 0.25 });
  private bodyGeo = new THREE.CapsuleGeometry(PLAYER_R * 0.8, PLAYER_R * 0.9, 6, 16);
  private ring: THREE.Mesh;
  private bursts: Burst[] = [];
  private popups: Popup[] = [];
  private trail: THREE.Points;
  private trailPos: Float32Array;
  private trailHead = 0;
  private camTarget = new THREE.Vector3();
  private zoom = 1;
  private size = { w: 1000, h: 1000 };
  private time = 0;

  constructor(private container: HTMLElement) {
    this.renderer = new THREE.WebGLRenderer({ antialias: true });
    this.renderer.setPixelRatio(Math.min(devicePixelRatio, 2));
    this.renderer.shadowMap.enabled = true;
    this.renderer.shadowMap.type = THREE.PCFSoftShadowMap;
    this.renderer.toneMapping = THREE.ACESFilmicToneMapping;
    this.renderer.domElement.setAttribute("aria-hidden", "true");
    container.append(this.renderer.domElement);
    this.labels.domElement.className = "labels";
    container.append(this.labels.domElement);

    this.scene.background = new THREE.Color(0x0b1020);
    this.scene.fog = new THREE.Fog(0x0b1020, 60, 180);
    this.scene.add(new THREE.HemisphereLight(0x9fc4ff, 0x1a1030, 1.1));
    const sun = new THREE.DirectionalLight(0xffffff, 2.2);
    sun.position.set(30, 60, 20);
    sun.castShadow = true;
    sun.shadow.mapSize.set(2048, 2048);
    Object.assign(sun.shadow.camera, { left: -60, right: 60, top: 60, bottom: -60, near: 1, far: 160 });
    this.scene.add(sun, this.floor);

    this.ring = new THREE.Mesh(
      new THREE.RingGeometry(PLAYER_R * 1.25, PLAYER_R * 1.6, 40),
      new THREE.MeshBasicMaterial({ color: 0x4fc3f7, transparent: true, opacity: 0.8, side: THREE.DoubleSide }),
    );
    this.ring.rotation.x = -Math.PI / 2;
    this.ring.visible = false;
    this.scene.add(this.ring);

    const n = 60;
    this.trailPos = new Float32Array(n * 3).fill(-999);
    const tg = new THREE.BufferGeometry();
    tg.setAttribute("position", new THREE.BufferAttribute(this.trailPos, 3));
    this.trail = new THREE.Points(tg, new THREE.PointsMaterial({ color: 0xffc107, size: 0.5, transparent: true, opacity: 0.8 }));
    this.trail.visible = false;
    this.scene.add(this.trail);

    this.camera.position.set(0, 90, 70);
    this.setWorld(1000, 1000);
    addEventListener("resize", () => this.resize());
    container.addEventListener("wheel", (e) => {
      this.zoom = Math.min(2.2, Math.max(0.45, this.zoom * (e.deltaY > 0 ? 1.1 : 0.9)));
      e.preventDefault();
    }, { passive: false });
    this.resize();
  }

  /** Converts world coordinates to the scene's XZ plane. */
  private at(x: number, y: number, h = 0): THREE.Vector3 {
    return new THREE.Vector3(x * S - (this.size.w * S) / 2, h, y * S - (this.size.h * S) / 2);
  }

  setWorld(w: number, h: number): void {
    this.size = { w, h };
    this.floor.clear();
    const W = w * S, H = h * S;
    const ground = new THREE.Mesh(new THREE.PlaneGeometry(W, H), new THREE.MeshStandardMaterial({ color: 0x1b2340, roughness: 0.9 }));
    ground.rotation.x = -Math.PI / 2;
    ground.receiveShadow = true;
    const grid = new THREE.GridHelper(Math.max(W, H), 20, 0x3d4f8a, 0x27325a);
    grid.position.y = 0.02;
    this.floor.add(ground, grid);
    const wallMat = new THREE.MeshStandardMaterial({ color: 0x5c6bc0, emissive: 0x283593, emissiveIntensity: 0.6 });
    for (const [sx, sz, px, pz] of [[W + 2, 1, 0, -H / 2 - 0.5], [W + 2, 1, 0, H / 2 + 0.5], [1, H, -W / 2 - 0.5, 0], [1, H, W / 2 + 0.5, 0]]) {
      const wall = new THREE.Mesh(new THREE.BoxGeometry(sx, 1.6, sz), wallMat);
      wall.position.set(px, 0.8, pz);
      wall.castShadow = wall.receiveShadow = true;
      this.floor.add(wall);
    }
    // A few decorative pillars outside the playfield for depth cues.
    const pillarMat = new THREE.MeshStandardMaterial({ color: 0x303a66 });
    for (let i = 0; i < 12; i++) {
      const a = (i / 12) * Math.PI * 2;
      const p = new THREE.Mesh(new THREE.CylinderGeometry(1.5, 2, 3 + (i % 3) * 2, 8), pillarMat);
      p.position.set(Math.cos(a) * W * 0.95, 2, Math.sin(a) * H * 0.95);
      p.castShadow = true;
      this.floor.add(p);
    }
  }

  private avatar(id: number, me: boolean): Avatar {
    let a = this.avatars.get(id);
    if (a) return a;
    const color = me ? new THREE.Color(0x4fc3f7) : new THREE.Color().setHSL(((id * 47) % 360) / 360, 0.6, 0.6);
    const body = new THREE.Mesh(this.bodyGeo, new THREE.MeshStandardMaterial({ color, roughness: 0.4 }));
    body.position.y = PLAYER_R * 1.25;
    body.castShadow = true;
    const eye = new THREE.Mesh(new THREE.SphereGeometry(PLAYER_R * 0.28, 12, 8), new THREE.MeshStandardMaterial({ color: 0xffffff, emissive: 0x333333 }));
    eye.position.set(0, PLAYER_R * 0.35, PLAYER_R * 0.65); // faces +Z (heading)
    body.add(eye);
    const group = new THREE.Group();
    group.add(body);
    const labelEl = document.createElement("div");
    labelEl.className = me ? "label me" : "label";
    const label = new CSS2DObject(labelEl);
    label.position.y = PLAYER_R * 3.2;
    group.add(label);
    this.scene.add(group);
    a = { group, body, label, labelEl, last: new THREE.Vector3(), heading: 0 };
    this.avatars.set(id, a);
    return a;
  }

  /** Draws one frame. positions are world units; names map player id to a label. */
  frame(dt: number, players: Map<number, { x: number; y: number; s: number }>, items: [number, number][],
    me: number, mePos: { x: number; y: number } | null, names: (id: number) => string, cos: Cosmetics): void {
    this.time += dt;
    for (const [id, a] of this.avatars) {
      if (!players.has(id) && !(id === me && mePos)) {
        this.scene.remove(a.group);
        a.label.element.remove();
        this.avatars.delete(id);
      }
    }
    for (const [id, p] of players) {
      const isMe = id === me;
      const a = this.avatar(id, isMe);
      const target = isMe && mePos ? this.at(mePos.x, mePos.y) : this.at(p.x, p.y);
      // Smooth the local prediction's corrections; remote players are already interpolated.
      if (isMe && a.last.lengthSq() > 0) a.group.position.lerp(target, 1 - Math.exp(-dt * 18));
      else a.group.position.copy(target);
      const v = a.group.position.clone().sub(a.last);
      const moving = v.lengthSq() > 1e-4;
      if (moving) {
        const want = Math.atan2(v.x, v.z);
        let d = want - a.heading;
        d = Math.atan2(Math.sin(d), Math.cos(d));
        a.heading += d * Math.min(1, dt * 12);
      }
      a.group.rotation.y = a.heading;
      a.body.position.y = PLAYER_R * 1.25 + (moving && !reducedMotion ? Math.abs(Math.sin(this.time * 14)) * 0.35 : 0);
      a.last.copy(a.group.position);
      a.labelEl.textContent = `${names(id)} · ${p.s}`;
      if (isMe) this.dressMe(a, cos, moving);
    }
    const meA = this.avatars.get(me);
    this.ring.visible = !!meA;
    if (meA) {
      this.ring.position.set(meA.group.position.x, 0.05, meA.group.position.z);
      const k = reducedMotion ? 1 : 1 + Math.sin(this.time * 4) * 0.08;
      this.ring.scale.set(k, k, k);
    }

    while (this.gems.length < items.length) {
      const g = new THREE.Mesh(this.gemGeo, this.gemMat);
      g.castShadow = true;
      this.scene.add(g);
      this.gems.push(g);
    }
    this.gems.forEach((g, i) => {
      g.visible = i < items.length;
      if (!g.visible) return;
      const [x, y] = items[i];
      g.position.copy(this.at(x, y, ITEM_R * 2 + (reducedMotion ? 0 : Math.sin(this.time * 3 + i) * 0.3)));
      g.rotation.y = this.time * 1.5 + i;
    });

    this.tickEffects(dt);
    this.follow(dt, meA?.group.position ?? null);
    this.renderer.render(this.scene, this.camera);
    this.labels.render(this.scene, this.camera);
  }

  private dressMe(a: Avatar, cos: Cosmetics, moving: boolean): void {
    const mat = a.body.material as THREE.MeshStandardMaterial;
    if (cos.legend) { mat.color.set(0xb388ff); mat.metalness = 0.9; mat.roughness = 0.15; mat.emissive.set(0x311b92); }
    else { mat.color.set(0x4fc3f7); mat.metalness = 0; mat.roughness = 0.4; mat.emissive.set(0x000000); }
    if (cos.hat && !a.hat) {
      a.hat = new THREE.Mesh(new THREE.ConeGeometry(PLAYER_R * 0.7, PLAYER_R * 1.2, 16), new THREE.MeshStandardMaterial({ color: 0xe53935 }));
      a.hat.position.y = PLAYER_R * 1.55;
      a.hat.castShadow = true;
      a.body.add(a.hat);
    }
    if (a.hat) a.hat.visible = cos.hat;
    this.trail.visible = cos.trail;
    if (cos.trail && moving) {
      const p = a.group.position;
      this.trailPos.set([p.x + (Math.random() - 0.5) * 0.6, 0.3 + Math.random() * 0.6, p.z + (Math.random() - 0.5) * 0.6], this.trailHead * 3);
      this.trailHead = (this.trailHead + 1) % (this.trailPos.length / 3);
      (this.trail.geometry.attributes.position as THREE.BufferAttribute).needsUpdate = true;
    }
  }

  private follow(dt: number, me: THREE.Vector3 | null): void {
    const focus = me ?? new THREE.Vector3();
    this.camTarget.lerp(focus, 1 - Math.exp(-dt * 6));
    const off = me ? new THREE.Vector3(0, 30, 24) : new THREE.Vector3(0, 95, 75);
    const want = this.camTarget.clone().add(off.multiplyScalar(this.zoom));
    this.camera.position.lerp(want, 1 - Math.exp(-dt * 4));
    this.camera.lookAt(this.camTarget);
  }

  /** Gem pickup: particle burst at world position. */
  burst(x: number, y: number, color = 0xffd54f): void {
    if (reducedMotion) return;
    const n = 28;
    const pos = new Float32Array(n * 3);
    const vel = new Float32Array(n * 3);
    const c = this.at(x, y, ITEM_R * 2);
    for (let i = 0; i < n; i++) {
      pos.set([c.x, c.y, c.z], i * 3);
      const a = Math.random() * Math.PI * 2, up = 4 + Math.random() * 6, r = 3 + Math.random() * 5;
      vel.set([Math.cos(a) * r, up, Math.sin(a) * r], i * 3);
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute("position", new THREE.BufferAttribute(pos, 3));
    const points = new THREE.Points(geo, new THREE.PointsMaterial({ color, size: 0.45, transparent: true }));
    this.scene.add(points);
    this.bursts.push({ points, vel, life: 0, max: 0.7 });
  }

  /** Floating text (e.g. "+1") above a world position. */
  popup(x: number, y: number, text: string): void {
    const el = document.createElement("div");
    el.className = "popup";
    el.textContent = text;
    const obj = new CSS2DObject(el);
    obj.position.copy(this.at(x, y, PLAYER_R * 4));
    this.scene.add(obj);
    this.popups.push({ obj, life: 0 });
  }

  private tickEffects(dt: number): void {
    this.bursts = this.bursts.filter((b) => {
      b.life += dt;
      const attr = b.points.geometry.attributes.position as THREE.BufferAttribute;
      const p = attr.array as Float32Array;
      for (let i = 0; i < p.length; i += 3) {
        b.vel[i + 1] -= 18 * dt;
        p[i] += b.vel[i] * dt; p[i + 1] = Math.max(0.05, p[i + 1] + b.vel[i + 1] * dt); p[i + 2] += b.vel[i + 2] * dt;
      }
      attr.needsUpdate = true;
      (b.points.material as THREE.PointsMaterial).opacity = 1 - b.life / b.max;
      if (b.life < b.max) return true;
      this.scene.remove(b.points);
      b.points.geometry.dispose();
      (b.points.material as THREE.Material).dispose();
      return false;
    });
    this.popups = this.popups.filter((p) => {
      p.life += dt;
      p.obj.position.y += dt * 4;
      p.obj.element.style.opacity = String(1 - p.life / 0.9);
      if (p.life < 0.9) return true;
      this.scene.remove(p.obj);
      p.obj.element.remove();
      return false;
    });
  }

  resize(): void {
    const w = this.container.clientWidth, h = this.container.clientHeight;
    this.renderer.setSize(w, h);
    this.labels.setSize(w, h);
    this.camera.aspect = w / Math.max(1, h);
    this.camera.updateProjectionMatrix();
  }
}

export function webglAvailable(): boolean {
  try {
    return !!document.createElement("canvas").getContext("webgl2");
  } catch {
    return false;
  }
}
