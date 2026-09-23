// Renders every docs/diagrams/*.excalidraw to a sibling .jpg using Excalidraw's own
// exporter in headless Chrome, so the images match what excalidraw.com shows.
//
// Usage: npm run docs:diagrams
// Env:   DIAGRAMS_DIR (default docs/diagrams), DIAGRAM_SCALE (default 2),
//        DIAGRAM_JPEG_QUALITY (default 0.92), EXCALIDRAW_VERSION (default 0.18.0),
//        CHROME_CHANNEL (default chrome)
import { chromium } from 'playwright-core';
import { readdirSync, readFileSync, writeFileSync } from 'fs';
import { join } from 'path';

const dir = process.env.DIAGRAMS_DIR || 'docs/diagrams';
const scale = parseFloat(process.env.DIAGRAM_SCALE || '2');
const quality = parseFloat(process.env.DIAGRAM_JPEG_QUALITY || '0.92');
const version = process.env.EXCALIDRAW_VERSION || '0.18.0';
const excalidrawUrl = `https://esm.sh/@excalidraw/excalidraw@${version}?deps=react@19.0.0,react-dom@19.0.0`;
const assetPath = `https://esm.sh/@excalidraw/excalidraw@${version}/dist/prod/`;
const channel = process.env.CHROME_CHANNEL || 'chrome';

const files = readdirSync(dir).filter((f) => f.endsWith('.excalidraw'));
if (files.length === 0) {
  console.log(`No .excalidraw files in ${dir}`);
  process.exit(0);
}

const browser = await chromium.launch({ channel });
const page = await browser.newPage();
await page.setContent('<html><body></body></html>');
await page.addScriptTag({
  type: 'module',
  content: `window.EXCALIDRAW_ASSET_PATH = '${assetPath}';
    window.excalidrawUtils = await import('${excalidrawUrl}');`,
});
await page.waitForFunction(() => window.excalidrawUtils, null, { timeout: 60000 });

for (const file of files) {
  const scene = JSON.parse(readFileSync(join(dir, file), 'utf8'));
  const base64 = await page.evaluate(
    async ({ scene, scale, quality }) => {
      const { exportToBlob, restoreElements } = window.excalidrawUtils;
      // Re-measure text and re-center bound labels exactly as the editor does on load
      const elements = restoreElements(scene.elements, null, { refreshDimensions: true, repairBindings: true });
      const blob = await exportToBlob({
        elements,
        appState: { ...scene.appState, exportBackground: true, viewBackgroundColor: '#ffffff' },
        files: scene.files || {},
        mimeType: 'image/jpeg',
        quality,
        exportPadding: 30,
        getDimensions: (w, h) => ({ width: w * scale, height: h * scale, scale }),
      });
      const bytes = new Uint8Array(await blob.arrayBuffer());
      let bin = '';
      for (const b of bytes) bin += String.fromCharCode(b);
      return btoa(bin);
    },
    { scene, scale, quality }
  );
  const out = join(dir, file.replace(/\.excalidraw$/, '.jpg'));
  writeFileSync(out, Buffer.from(base64, 'base64'));
  console.log(`Rendered ${out}`);
}

await browser.close();
