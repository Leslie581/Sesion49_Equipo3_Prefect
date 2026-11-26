// Capturas reales con Edge/Chrome sin interfaz vía DevTools Protocol (Node >= 22, sin dependencias).
// Uso: node capturar.mjs <navegador.exe> <lista.json>
//   lista.json: [{"url": "...", "archivo": "x.png", "ancho": 1600, "alto": 1000, "espera": 6000}]
// Cierra el aviso de la comunidad y el banner de Prefect (botones "Skip"/"Dismiss") antes de capturar.
import { spawn } from "node:child_process";
import { readFileSync, writeFileSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const [nav, lista] = process.argv.slice(2);
const tareas = JSON.parse(readFileSync(lista, "utf8"));
const PUERTO = 9333;
const proc = spawn(nav, ["--headless=new", "--disable-gpu", "--hide-scrollbars", `--remote-debugging-port=${PUERTO}`,
  `--user-data-dir=${mkdtempSync(join(tmpdir(), "cdp-"))}`, "--force-device-scale-factor=1.5", "about:blank"], { stdio: "ignore" });
const dormir = (ms) => new Promise((r) => setTimeout(r, ms));

let destino;
for (let i = 0; i < 40 && !destino; i++) {
  await dormir(500);
  try { destino = (await (await fetch(`http://127.0.0.1:${PUERTO}/json/list`)).json()).find((t) => t.type === "page"); } catch {}
}
const ws = new WebSocket(destino.webSocketDebuggerUrl);
await new Promise((r) => (ws.onopen = r));
let id = 0; const pend = new Map();
ws.onmessage = (e) => { const m = JSON.parse(e.data); if (pend.has(m.id)) { pend.get(m.id)(m.result); pend.delete(m.id); } };
const cdp = (method, params = {}) => new Promise((r) => { pend.set(++id, r); ws.send(JSON.stringify({ id, method, params })); });
const clic = `(() => { let n = 0; for (const b of document.querySelectorAll('button')) {
  const t = b.textContent.trim(); if (t === 'Skip' || t === 'Dismiss') { b.click(); n++; } } return n; })()`;

await cdp("Page.enable");
for (const t of tareas) {
  await cdp("Emulation.setDeviceMetricsOverride", { width: t.ancho || 1600, height: t.alto || 1000, deviceScaleFactor: t.escala || 1.5, mobile: false });
  await cdp("Page.navigate", { url: t.url });
  await dormir(t.espera || 6000);
  await cdp("Runtime.evaluate", { expression: clic });
  await dormir(1200);
  if (t.js) { await cdp("Runtime.evaluate", { expression: t.js }); await dormir(t.esperaJs || 2500); }
  const opc = { format: "png" };
  if (t.completa) {  // página completa: recorta al tamaño real del contenido
    const { cssContentSize: c } = await cdp("Page.getLayoutMetrics");
    Object.assign(opc, { captureBeyondViewport: true, clip: { x: 0, y: 0, width: c.width, height: c.height, scale: 1 } });
  }
  const r = await cdp("Page.captureScreenshot", opc);
  writeFileSync(t.archivo, Buffer.from(r.data, "base64"));
  console.log("ok", t.archivo);
}
ws.close(); proc.kill();
