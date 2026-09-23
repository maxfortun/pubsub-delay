// Generates the .excalidraw sources in docs/diagrams. Render them with scripts/render-diagrams.mjs.
//
// Usage: node scripts/generate-diagrams.mjs [outDir]   (default docs/diagrams)
import { writeFileSync, mkdirSync } from 'fs';

const OUT = process.argv[2] || 'docs/diagrams';
mkdirSync(OUT, { recursive: true });

const C = {
  blue: ['#1971c2', '#a5d8ff'],
  green: ['#2f9e44', '#b2f2bb'],
  yellow: ['#f08c00', '#ffec99'],
  red: ['#e03131', '#ffc9c9'],
  violet: ['#6741d9', '#d0bfff'],
  gray: ['#495057', '#e9ecef'],
  teal: ['#0c8599', '#99e9f2'],
  none: ['#1e1e1e', 'transparent'],
};

function diagram() {
  const els = [];
  let n = 0;
  const id = (p) => `${p}-${++n}`;
  // Deterministic seeds so regenerating does not churn the files
  let rng = 42;
  const seed = () => (rng = (rng * 1103515245 + 12345) % 2 ** 31);
  const base = (type, x, y, w, h, color, extra = {}) => ({
    id: id(type), type, x, y, width: w, height: h, angle: 0,
    strokeColor: color[0], backgroundColor: color[1], fillStyle: 'solid',
    strokeWidth: 2, strokeStyle: 'solid', roughness: 1, opacity: 100,
    groupIds: [], frameId: null, roundness: null, seed: seed(), version: 1,
    versionNonce: seed(), isDeleted: false, boundElements: [], updated: 1,
    link: null, locked: false, ...extra,
  });
  const measure = (text, size) => {
    const lines = text.split('\n');
    return { w: Math.max(...lines.map((l) => l.length)) * size * 0.55, h: lines.length * size * 1.25 };
  };
  const text = (x, y, str, { size = 20, color = '#1e1e1e', align = 'left', container = null, w, h } = {}) => {
    const m = measure(str, size);
    const el = base('text', x, y, w ?? m.w, h ?? m.h, [color, 'transparent'], {
      text: str, originalText: str, fontSize: size, fontFamily: 5, textAlign: align,
      verticalAlign: container ? 'middle' : 'top', containerId: container, lineHeight: 1.25,
      autoResize: true, boundElements: null,
    });
    els.push(el);
    return el;
  };
  const shape = (type, x, y, w, h, label, color, { size = 20, dashed = false, rounded = true } = {}) => {
    const el = base(type, x, y, w, h, color, {
      roundness: rounded && type === 'rectangle' ? { type: 3 } : type === 'diamond' ? { type: 2 } : null,
      strokeStyle: dashed ? 'dashed' : 'solid',
    });
    els.push(el);
    if (label) {
      const m = measure(label, size);
      const t = text(x + w / 2 - m.w / 2, y + h / 2 - m.h / 2, label, { size, align: 'center', container: el.id });
      el.boundElements.push({ type: 'text', id: t.id });
    }
    return el;
  };
  const box = (x, y, w, h, label, color, o) => shape('rectangle', x, y, w, h, label, color, o);
  const diamond = (x, y, w, h, label, color, o) => shape('diamond', x, y, w, h, label, color, o);
  const ellipse = (x, y, w, h, label, color, o) => shape('ellipse', x, y, w, h, label, color, o);
  const frame = (x, y, w, h, color) => box(x, y, w, h, null, [color[0], 'transparent'], { dashed: true });
  const arrow = (pts, { color = '#1e1e1e', dashed = false, label, labelAt = 0.5, size = 16, dx = 8, dy = -24 } = {}) => {
    const [x0, y0] = pts[0];
    const rel = pts.map(([x, y]) => [x - x0, y - y0]);
    const xs = rel.map((p) => p[0]), ys = rel.map((p) => p[1]);
    els.push(base('arrow', x0, y0, Math.max(...xs) - Math.min(...xs), Math.max(...ys) - Math.min(...ys), [color, 'transparent'], {
      points: rel, lastCommittedPoint: null, startBinding: null, endBinding: null,
      startArrowhead: null, endArrowhead: 'arrow', strokeStyle: dashed ? 'dashed' : 'solid',
      roundness: pts.length > 2 ? null : { type: 2 }, elbowed: false,
    }));
    if (label) {
      // Label sits beside the segment containing labelAt
      const seg = Math.min(pts.length - 2, Math.floor(labelAt * (pts.length - 1)));
      const [ax, ay] = pts[seg], [bx, by] = pts[seg + 1];
      text((ax + bx) / 2 + dx, (ay + by) / 2 + dy, label, { size, color });
    }
  };
  const save = (name) => {
    writeFileSync(`${OUT}/${name}.excalidraw`, JSON.stringify({
      type: 'excalidraw', version: 2, source: 'https://excalidraw.com', elements: els,
      appState: { gridSize: null, viewBackgroundColor: '#ffffff' }, files: {},
    }, null, 2));
  };
  return { box, diamond, ellipse, frame, arrow, text, save };
}

// ---------------------------------------------------------------- Architecture
{
  const d = diagram();
  d.text(40, 20, 'pubsub-delay: the broker is the storage', { size: 36 });
  d.text(40, 72, 'One bucket topic per distinct delay. Every message in a bucket has the same delay,\nso each bucket is already sorted by deliverAt: its head is always the next one due.', { size: 18, color: '#495057' });

  d.box(40, 250, 180, 110, 'Producers', C.gray);
  d.text(20, 380, 'headers:\nDELAY_DURATION=PT30S\nDELAY_DESTINATION=orders', { size: 16, color: '#495057' });
  d.arrow([[220, 305], [300, 305]]);
  d.box(300, 260, 190, 90, 'delay-ingest\n(topic)', C.blue);
  d.arrow([[490, 305], [600, 305]]);

  d.frame(560, 150, 820, 600, C.violet);
  d.text(580, 160, 'pubsub-delay pod  (× N replicas, one consumer group)', { size: 18, color: C.violet[0] });

  d.box(600, 250, 220, 110, 'Router\nstamps\nDELAY_ENQUEUED_AT', C.violet, { size: 16 });

  const bx = 880;
  d.box(bx, 200, 230, 60, 'delay-ingest-PT1S', C.blue, { size: 18 });
  d.box(bx, 280, 230, 60, 'delay-ingest-PT30S', C.blue, { size: 18 });
  d.box(bx, 360, 230, 60, 'delay-ingest-PT1H', C.blue, { size: 18 });
  d.text(bx + 10, 430, 'bucket topics (FIFO = deliverAt order)', { size: 15, color: '#495057' });
  d.arrow([[820, 290], [880, 230]]);
  d.arrow([[820, 305], [880, 310]]);
  d.arrow([[820, 320], [880, 390]]);

  d.box(1170, 250, 190, 120, 'Scheduler\n\nstrategy:\nBoundedPool |\nTimeWheel', C.green, { size: 17 });
  d.arrow([[1110, 230], [1170, 290]]);
  d.arrow([[1110, 310], [1170, 310]]);
  d.arrow([[1110, 390], [1170, 330]]);

  d.box(1450, 230, 190, 70, 'orders', C.yellow);
  d.box(1450, 320, 190, 70, 'billing.reminders', C.yellow, { size: 18 });
  d.text(1450, 400, 'DELAY_DESTINATION topics', { size: 15, color: '#495057' });
  d.arrow([[1360, 295], [1450, 265]], { label: 'at deliverAt', dx: -60, dy: -48, size: 15 });
  d.arrow([[1360, 325], [1450, 355]]);

  d.box(600, 520, 200, 110, 'Bucket\nAdvisory', C.teal);
  d.box(880, 535, 230, 80, 'delay-ingest-advisory\nbucket:add / remove', C.blue, { size: 16 });
  d.arrow([[800, 575], [880, 575]]);
  d.arrow([[700, 360], [700, 520]], { dashed: true, color: C.teal[0], label: 'new bucket', dx: 8, dy: -10, size: 14 });
  d.arrow([[1110, 575], [1265, 575], [1265, 370]], { dashed: true, color: C.teal[0], label: '(re)subscribe', labelAt: 0.9, dx: 10, dy: 0, size: 14 });
  d.text(880, 650, 'Cleanup: bucket idle for\nBUCKET_IDLE_TIMEOUT_MS and\nscheduler lag = 0 → unsubscribe,\nbroadcast remove, delete topic', { size: 15, color: C.teal[0] });
  d.save('architecture');
}

// ---------------------------------------------------------------- BoundedPool
{
  const d = diagram();
  d.text(40, 20, 'BoundedPool: at most N live timers, everything else stays in the broker', { size: 32 });
  d.text(40, 68, 'N = TIMEOUT_POOL_SIZE.  Kafka: ack = commit offset+1, nack = seek back to the offset.  ActiveMQ: nack = do not ack.', { size: 17, color: '#495057' });

  d.box(40, 170, 220, 60, 'bucket PT1S', C.blue, { size: 18 });
  d.box(40, 245, 220, 60, 'bucket PT30S', C.blue, { size: 18 });
  d.box(40, 320, 220, 60, 'bucket PT1H', C.blue, { size: 18 });
  d.arrow([[260, 275], [340, 275]], { label: 'peek', dx: -20, dy: -28, size: 16 });

  d.box(340, 230, 220, 90, 'deliverAt =\nENQUEUED_AT + delay', C.gray, { size: 17 });
  d.arrow([[560, 275], [630, 275]]);
  d.diamond(630, 205, 220, 140, 'pool\nfull?', C.yellow);

  // Room in pool
  d.arrow([[850, 275], [960, 275]], { label: 'no', dx: -20, dy: -28, color: C.green[0] });
  d.box(960, 225, 250, 100, 'add to pool\nsetTimeout(deliverAt − now)', C.green, { size: 17 });
  d.arrow([[1210, 275], [1290, 275]]);

  // The pool
  d.box(1290, 150, 300, 300, null, C.green);
  d.text(1305, 160, 'timer pool (≤ N)', { size: 20, color: C.green[0] });
  ['t+0.2s', 't+0.9s', 't+1.4s', 't+3.0s', '… t+29s (furthest)'].forEach((l, i) =>
    d.box(1310, 200 + i * 48, 260, 38, l, i === 4 ? C.red : [C.green[0], '#ebfbee'], { size: 16 }));

  d.arrow([[1590, 225], [1680, 225]], { label: 'fires at\ndeliverAt', dx: -35, dy: -52, size: 15 });
  d.box(1680, 180, 230, 100, 'produce to\nDELAY_DESTINATION\nthen ack', C.yellow, { size: 17 });

  // Pool full
  d.arrow([[740, 345], [740, 430]], { label: 'yes', dx: 10, dy: -12, color: C.red[0] });
  d.diamond(620, 430, 240, 150, 'sooner than\nfurthest in\npool?', C.yellow, { size: 18 });

  d.arrow([[860, 505], [960, 505]], { label: 'yes', dx: -20, dy: -28, color: C.green[0] });
  d.box(960, 450, 280, 110, 'evict furthest:\ncache its deliverAt,\npause its bucket, nack it\nthen add new message', C.red, { size: 16 });
  d.arrow([[1240, 470], [1330, 440]], { dashed: true, color: C.red[0] });

  d.arrow([[740, 580], [740, 660]], { label: 'no', dx: 10, dy: -12, color: C.red[0] });
  d.box(610, 660, 260, 100, 'cache deliverAt,\npause this bucket,\nnack (seek back)', C.red, { size: 16 });

  // Resume path
  d.box(960, 660, 330, 100, 'resume timer fires at\ndeliverAt − BUCKET_RESUME_LEAD_MS\nresume bucket', C.violet, { size: 16 });
  d.arrow([[870, 710], [960, 710]]);
  d.arrow([[1100, 560], [1100, 660]], { dashed: true });
  d.arrow([[1125, 760], [1125, 820], [150, 820], [150, 380]], { dashed: true, color: C.violet[0], label: 're-read the same message: its 2nd and final look, now due', labelAt: 0.5, dx: -250, dy: 8, size: 16 });

  d.text(1330, 520, 'Why it scales:\n• ≤ N uncommitted messages per pod\n• rebalance/crash replays ≤ N\n• paused buckets cost nothing\n• each message read ≤ ~2 times\n• head of a bucket is the next due,\n  so one cached deliverAt per bucket\n  is enough to know when to wake', { size: 16, color: '#1e1e1e' });
  d.save('bounded-pool');
}

// ---------------------------------------------------------------- TimeWheel
{
  const d = diagram();
  d.text(40, 20, 'TimeWheel: hashed timing wheel with a sorted overflow list', { size: 32 });
  d.text(40, 68, 'span = WHEEL_SLOTS × WHEEL_RESOLUTION_MS.  Messages are held in memory and acked only after delivery.', { size: 17, color: '#495057' });

  d.box(40, 250, 200, 60, 'bucket topics', C.blue, { size: 18 });
  d.arrow([[240, 280], [310, 280]], { label: 'receive', dx: -30, dy: -28, size: 16 });
  d.diamond(310, 200, 230, 160, 'delay ≤\nspan?', C.yellow);

  d.arrow([[425, 360], [425, 450]], { label: 'no', dx: 10, dy: -12, color: C.red[0] });
  d.box(290, 450, 270, 120, 'overflow\n(sorted by deliverAt)\ne.g. PT1H messages', C.red, { size: 17 });
  d.arrow([[560, 510], [820, 470]], { dashed: true, color: C.red[0], label: 'promote after each tick\nonce within span', dx: -110, dy: 20, size: 15 });

  d.arrow([[540, 280], [760, 280]], { label: 'yes', dx: -30, dy: -28, color: C.green[0] });
  d.text(555, 300, 'slot = (cursor +\n⌈delay / resolution⌉)\nmod WHEEL_SLOTS', { size: 15, color: '#495057' });

  // Wheel
  const cx = 1060, cy = 380, r = 230, slots = 12;
  d.ellipse(cx - r - 60, cy - r - 60, 2 * r + 120, 2 * r + 120, null, [C.gray[0], '#f8f9fa']);
  for (let i = 0; i < slots; i++) {
    const a = -Math.PI / 2 + (i * 2 * Math.PI) / slots;
    const x = cx + r * Math.cos(a), y = cy + r * Math.sin(a);
    const load = [3, 0, 1, 2, 0, 4, 1, 0, 2, 0, 1, 0][i];
    const color = i === 0 ? C.green : load ? [C.blue[0], '#d0ebff'] : [C.gray[0], '#ffffff'];
    d.box(x - 42, y - 30, 84, 60, `${i}\n${'●'.repeat(Math.min(load, 4)) || '·'}`, color, { size: 16 });
  }
  d.ellipse(cx - 95, cy - 55, 190, 110, 'cursor', C.green, { size: 18 });
  d.arrow([[cx, cy - 55], [cx, cy - r + 32]], { color: C.green[0] });
  d.text(cx - 150, cy + 70, 'advances one slot every\nWHEEL_RESOLUTION_MS', { size: 15, color: C.green[0], align: 'center', w: 300 });

  d.arrow([[cx + 42, cy - r - 10], [1480, cy - r - 10]], { label: 'tick: deliver every entry\nin the slot, in order', dx: 0, dy: -56, size: 15 });
  d.box(1480, cy - r - 60, 240, 100, 'produce to\nDELAY_DESTINATION\nthen ack', C.yellow, { size: 17 });

  d.text(1420, 330, 'Trade-offs:\n• one read per message, no pause/nack\n• precision ≈ one tick\n  (WHEEL_RESOLUTION_MS)\n• memory grows with ALL in-flight\n  messages, not bounded\n• nothing committed until delivered:\n  rebalance or crash replays the\n  whole in-flight set as duplicates\n• scales vertically, not horizontally', { size: 16 });
  d.save('time-wheel');
}
