import assert = require('assert');

// Initial state
let low = 1;
let high = 0xffffffff;

function putc(c: number) {
  console.log(c);
}

function and(a: number, b: number) {
  let c = a & b;
  c += c < 0 ? 0x100000000 : 0;
  return c;
}

function xor(a: number, b: number) {
  let c = a ^ b;
  c += c < 0 ? 0x100000000 : 0;
  return c;
}

// Encode bit y with probability p/65536
function encode(y: number, p: number) {
  assert(p >= 0 && p < 65536);
  assert(y === 0 || y === 1);
  assert(high > low && low > 0);
  const mid = and(
    (low +
      ((high - low) >>> 16) * p
      + ((((high - low) & 0xffff) * p) >>> 16)
    ),
    0xffffffff); // split range
  assert(high > mid && mid >= low);
  if (y) high = mid; else low = mid + 1; // pick half
  let out = 0;
  while (xor(high, low) < 0x1000000) { // write identical leading bytes
    out++;
    putc(high >>> 24);  // same as low>>24
    high = and((high << 8) | 255, 0xffffffff);
    low = and(low << 8, 0xffffffff);
    low += (low === 0) ? 1 : 0; // so we don't code 4 0 bytes in a row
  }
  return out;
}

function hh() {
  const data = Buffer.alloc(90024);
  const blen = data.length * 8;
  let ones = 1;
  let zeros = 1;
  let out = 0;
  for (let i = 0; i < blen; i++) {
    const bit = (data[i >> 3] & (1 << (8 - i % 8))) ? 1 : 0;
    const p = 1 ? Math.floor(ones * 65536 / (ones + zeros)) : Math.floor(zeros * 65536 / (ones + zeros));
    // console.log(bit);
    out += encode(bit, p);
    ones += bit ? 1 : 0;
    zeros += bit ? 0 : 1;
  }
  console.log(data.length, out);
}

hh();
