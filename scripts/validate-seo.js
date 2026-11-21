const fs = require('fs');
const path = require('path');
const assert = require('assert');

const root = process.cwd();
const homepage = fs.readFileSync(path.join(root, 'public/index.html'), 'utf8');
const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
const robots = fs.readFileSync(path.join(root, 'public/robots.txt'), 'utf8');
const sitemap = fs.readFileSync(path.join(root, 'public/sitemap.xml'), 'utf8');

assert.match(homepage, /<title>Descargar videos online, imágenes y GIFs \| MediaGrab Pro<\/title>/);
assert.match(homepage, /<meta name="description"/);
assert.match(homepage, /rel="canonical"/);
assert.match(homepage, /property="og:title"/);
assert.match(homepage, /name="twitter:card"/);
assert.match(homepage, /application\/ld\+json/);
assert.match(homepage, /<h1[ >]/);
assert.match(homepage, /href="\/descargar-videos-tiktok\//);
assert.match(robots, /User-agent: \*/);
assert.match(robots, /Disallow: \/api\//);
assert.match(robots, /Sitemap: https:\/\/descargar-online\.onrender\.com\/sitemap\.xml/);
assert.match(sitemap, /<urlset[ >]/);

const urls = [...sitemap.matchAll(/<loc>(https:\/\/descargar-online\.onrender\.com([^<]*))<\/loc>/g)];
assert.ok(urls.length >= 8, 'El sitemap debe incluir la portada y las guías públicas.');
const titles = new Set();
for (const [, absolute, route] of urls) {
  const file = route === '/' ? 'public/index.html' : path.join('public', route.replace(/^\//, ''), 'index.html');
  assert.ok(fs.existsSync(path.join(root, file)), 'Falta el archivo publicado para ' + absolute);
  if (route === '/') continue;
  const page = fs.readFileSync(path.join(root, file), 'utf8');
  const title = (page.match(/<title>([^<]+)<\/title>/) || [])[1];
  assert.ok(title, 'Falta title en ' + route);
  assert.ok(!titles.has(title), 'Title duplicado: ' + title);
  titles.add(title);
  assert.match(page, /<meta name="description"/);
  assert.match(page, /rel="canonical"/);
  assert.match(page, /<h1[ >]/);
  assert.match(page, /<script type="application\/ld\+json">/);
  assert.match(page, /href="\/"|href="\/"/);
}
assert.match(server, /windowMs: 24 \* 60 \* 60 \* 1000/);
assert.match(server, /max: 3/);
assert.match(server, /return res\.status\(404\)/);
console.log('SEO validation passed: homepage, 7 landing pages, robots, sitemap, daily limit and 404 handling.');
