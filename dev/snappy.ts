import snappy = require('../src/snappy');
const snappyjs = require('snappy');
import assert = require('assert');
import fs = require('fs');

let qq =
  'Test string est data Test string data '
  + '.XXXXX....................................................................  '
  + 'XXXXX....................................................................';

qq = fs.readFileSync('./data/alice29.txt').toString();

const buf = Buffer.from(qq);
const zz = snappy.compress(buf);
const yy = snappyjs.compressSync(buf);

const big = Buffer.concat([buf, buf, buf, buf, buf]);

let now = Date.now();
for (let i = 0; i < 1000; i++) {
  snappy.compress(big);
}
console.log(Date.now() - now);

now = Date.now();
for (let i = 0; i < 1000; i++) {
  snappyjs.compressSync(big);
}
console.log(Date.now() - now);

const vv = snappyjs.uncompressSync(zz);
assert.deepStrictEqual(vv, Buffer.from(qq));
assert.deepStrictEqual(zz, yy);

// export function compressFragment2(input: Buffer, baseIp: number, inputSize: number, output: Buffer, baseOp: number) {
//   if (inputSize < INPUT_MARGIN) {
//     return emitLiteral(input, baseIp, inputSize, output, baseOp);
//   }

//   const hashTableBits = hashBits(inputSize);
//   const shift = 32 - hashTableBits;
//   if (typeof globalHashTables[hashTableBits] === 'undefined') {
//     globalHashTables[hashTableBits] = new Uint16Array(1 << hashTableBits);
//   }
//   const table = globalHashTables[hashTableBits];
//   for (let i = 0; i < table.length; i++) {
//     table[i] = 0;
//   }

//   const ipEnd = baseIp + inputSize;
//   const ipLimit = ipEnd - INPUT_MARGIN;

//   let ip = baseIp + 1;
//   let op = baseOp;

//   let hash = hash32(input, baseIp, shift);
//   table[hash] = 1;

//   let nextEmit = baseIp;
//   loop: while (ip < ipLimit) {
//     let nextIp = ip;
//     let candidate = 0;
//     let matched = 0;
//     do {
//       ip = nextIp;
//       hash = hash32(input, ip, shift);
//       const pos = table[hash] - 1;
//       if (pos < 0) table[hash] = ip - baseIp + 1;
//       nextIp = ip + 1;
//       if (nextIp > ipLimit) break loop;
//       for (let c = pos; pos >= 0 && c < ip - baseIp - 4; c++) {
//         let x = 0;
//         while (ip + x < ipEnd && input[baseIp + c + x] === input[ip + x]) x++;
//         if (x > matched) {
//           matched = x;
//           candidate = c;
//         }
//       }
//     } while (matched < 4);
//     if (nextEmit < ip) {
//       op = emitLiteral(input, nextEmit, ip - nextEmit, output, op);
//     }
//     op = emitCopy(output, op, ip - candidate, matched);
//     ip += matched;
//     nextEmit = ip;
//   }

//   if (nextEmit < ipEnd) {
//     op = emitLiteral(input, nextEmit, ipEnd - nextEmit, output, op);
//   }
//   return op;
// }
