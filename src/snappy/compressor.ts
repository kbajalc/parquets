// tslint:disable: prefer-array-literal
// tslint:disable: no-parameter-reassignment

const BLOCK_LOG = 16;
const BLOCK_SIZE = 1 << BLOCK_LOG;
const INPUT_MARGIN = 15;

const MAX_HASH_TABLE_BITS = 14;
const globalHashTables: Uint16Array[] = new Array(MAX_HASH_TABLE_BITS + 1);

function hashFunc(key: number, hashFuncShift: number) {
  return (key * 0x1e35a7bd) >>> hashFuncShift;
}

function load32(array: Buffer, pos: number) {
  return array[pos] + (array[pos + 1] << 8) + (array[pos + 2] << 16) + (array[pos + 3] << 24);
}

function equals32(array: Buffer, pos1: number, pos2: number) {
  return array[pos1] === array[pos2] &&
    array[pos1 + 1] === array[pos2 + 1] &&
    array[pos1 + 2] === array[pos2 + 2] &&
    array[pos1 + 3] === array[pos2 + 3];
}

function copyBytes(fromArray: Buffer, fromPos: number, toArray: Buffer, toPos: number, length: number) {
  for (let i = 0; i < length; i++) {
    toArray[toPos + i] = fromArray[fromPos + i];
  }
}

function emitLiteral(input: Buffer, ip: number, len: number, output: Buffer, op: number) {
  // console.log(`[${len}]`, input.toString('utf-8', ip, ip + len));
  if (len <= 60) {
    output[op] = (len - 1) << 2;
    op += 1;
  } else if (len < 256) {
    output[op] = 60 << 2;
    output[op + 1] = len - 1;
    op += 2;
  } else {
    output[op] = 61 << 2;
    output[op + 1] = (len - 1) & 0xff;
    output[op + 2] = (len - 1) >>> 8;
    op += 3;
  }
  copyBytes(input, ip, output, op, len);
  return op + len;
}

function emitCopyLessThan64(output: Buffer, op: number, offset: number, len: number) {
  if (len < 12 && offset < 2048) {
    output[op] = 1 + ((len - 4) << 2) + ((offset >>> 8) << 5);
    output[op + 1] = offset & 0xff;
    return op + 2;
  } else {
    output[op] = 2 + ((len - 1) << 2);
    output[op + 1] = offset & 0xff;
    output[op + 2] = offset >>> 8;
    return op + 3;
  }
}

function emitCopy(output: Buffer, op: number, offset: number, len: number) {
  // console.log(`[-${offset}, ${len}]`);
  while (len >= 68) {
    op = emitCopyLessThan64(output, op, offset, 64);
    len -= 64;
  }
  if (len > 64) {
    op = emitCopyLessThan64(output, op, offset, 60);
    len -= 60;
  }
  return emitCopyLessThan64(output, op, offset, len);
}

function putVarint(value: number, output: Buffer, op: number) {
  do {
    output[op] = value & 0x7f;
    value = value >>> 7;
    if (value > 0) {
      output[op] += 0x80;
    }
    op += 1;
  } while (value > 0);
  return op;
}

function compressFragment(input: Buffer, baseIp: number, inputSize: number, output: Buffer, baseOp: number) {
  if (inputSize < INPUT_MARGIN) {
    return emitLiteral(input, baseIp, inputSize, output, baseOp);
  }

  let hashTableBits = 1;
  while ((1 << hashTableBits) <= inputSize && hashTableBits <= MAX_HASH_TABLE_BITS) {
    hashTableBits += 1;
  }
  hashTableBits -= 1;
  const hashFuncShift = 32 - hashTableBits;

  if (typeof globalHashTables[hashTableBits] === 'undefined') {
    globalHashTables[hashTableBits] = new Uint16Array(1 << hashTableBits);
  }
  const hashTable = globalHashTables[hashTableBits];
  for (let i = 0; i < hashTable.length; i++) {
    hashTable[i] = 0;
  }

  const ipEnd = baseIp + inputSize;
  const ipLimit = ipEnd - INPUT_MARGIN;

  let ip = baseIp + 1;
  let op = baseOp;
  let candidate = 0;
  let nextEmit = baseIp;
  let nextHash = hashFunc(load32(input, ip), hashFuncShift);
  let flag = true;
  while (flag) {
    let skip = 32;
    let nextIp = ip;
    do {
      ip = nextIp;
      const hash = nextHash;
      const bytesBetweenHashLookups = skip >>> 5;
      skip += 1;
      nextIp = ip + bytesBetweenHashLookups;
      if (ip > ipLimit) {
        flag = false;
        break;
      }
      nextHash = hashFunc(load32(input, nextIp), hashFuncShift);
      candidate = baseIp + hashTable[hash];
      hashTable[hash] = ip - baseIp;
    } while (!equals32(input, ip, candidate));

    if (!flag) {
      break;
    }

    op = emitLiteral(input, nextEmit, ip - nextEmit, output, op);

    do {
      const base = ip;
      let matched = 4;
      while (ip + matched < ipEnd && input[ip + matched] === input[candidate + matched]) {
        matched += 1;
      }
      ip += matched;
      const offset = base - candidate;
      op = emitCopy(output, op, offset, matched);
      nextEmit = ip;

      if (ip >= ipLimit) {
        flag = false;
        break;
      }

      const prevHash = hashFunc(load32(input, ip - 1), hashFuncShift);
      hashTable[prevHash] = ip - 1 - baseIp;
      const curHash = hashFunc(load32(input, ip), hashFuncShift);
      candidate = baseIp + hashTable[curHash];
      hashTable[curHash] = ip - baseIp;
    } while (equals32(input, ip, candidate));

    if (!flag) break;

    ip += 1;
    nextHash = hashFunc(load32(input, ip), hashFuncShift);
  }

  if (nextEmit < ipEnd) {
    op = emitLiteral(input, nextEmit, ipEnd - nextEmit, output, op);
  }
  return op;
}

export function maxCompressedLength(input: Buffer) {
  const sourceLen = input.length;
  return 32 + sourceLen + Math.floor(sourceLen / 6);
}

export function compressToBuffer(input: Buffer, output: Buffer) {
  const length = input.length;

  let pos = 0;
  let fragmentSize: number;
  let outPos = putVarint(length, output, 0);
  while (pos < length) {
    fragmentSize = Math.min(length - pos, BLOCK_SIZE);
    outPos = compressFragment(input, pos, fragmentSize, output, outPos);
    pos += fragmentSize;
  }

  return outPos;
}
