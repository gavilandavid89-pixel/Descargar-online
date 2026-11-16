const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const source = fs.readFileSync('server.js', 'utf8');
const match = source.match(/function parseInstagramEfgMetadata\(mediaUrl\) \{[\s\S]*?\n\}/);
assert.ok(match, 'parseInstagramEfgMetadata must exist in server.js');

const parse = vm.runInNewContext('(' + match[0] + ')', { URL, Buffer });
const payload = {
  duration_s: 15,
  video_codec_tag: 'xpv_progressive.INSTAGRAM.CLIPS.C3.720.dash_baseline_1_v1',
  asset_id: 123456789
};
const encoded = Buffer.from(JSON.stringify(payload)).toString('base64url');
const result = parse('https://cdn.example/video.mp4?efg=' + encoded + '&token=must-not-be-returned');

assert.equal(result.durationSeconds, 15);
assert.equal(result.videoCodecTag, payload.video_codec_tag);
assert.deepEqual(JSON.parse(JSON.stringify(parse('https://cdn.example/video.mp4'))), {});
assert.deepEqual(JSON.parse(JSON.stringify(parse('not-a-url'))), {});
assert.equal(Object.hasOwn(result, 'asset_id'), false);
console.log('Instagram EFG metadata tests passed.');
