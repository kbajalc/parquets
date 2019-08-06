// tslint:disable: prefer-array-literal
// tslint:disable: no-parameter-reassignment

export interface ByteArray {
  readonly length: number;
  [number: number]: number;
}

const BLOCK_LOG = 16;
const BLOCK_SIZE = 1 << BLOCK_LOG;

const MAX_HASH_TABLE_BITS = 14;
const globalHashTables: Uint16Array[] = new Array(MAX_HASH_TABLE_BITS + 1);

function hashFunc(key: number, hashFuncShift: number) {
  return (key * 0x1e35a7bd) >>> hashFuncShift;
}

function load32(array: ByteArray, pos: number) {
  return array[pos] + (array[pos + 1] << 8) + (array[pos + 2] << 16) + (array[pos + 3] << 24);
}

function equals32(array: ByteArray, pos1: number, pos2: number) {
  return array[pos1] === array[pos2] &&
    array[pos1 + 1] === array[pos2 + 1] &&
    array[pos1 + 2] === array[pos2 + 2] &&
    array[pos1 + 3] === array[pos2 + 3];
}

function copyBytes(fromArray: ByteArray, fromPos: number, toArray: ByteArray, toPos: number, length: number) {
  for (let i = 0; i < length; i++) {
    toArray[toPos + i] = fromArray[fromPos + i];
  }
}

function emitLiteral(input: ByteArray, ip: number, len: number, output: ByteArray, op: number) {
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

function emitCopyLessThan64(output: ByteArray, op: number, offset: number, len: number) {
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

function emitCopy(output: ByteArray, op: number, offset: number, len: number) {
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

function compressFragment(input: ByteArray, ip: number, inputSize: number, output: ByteArray, op: number) {
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

  const ipEnd = ip + inputSize;
  const baseIp = ip;
  let ipLimit;
  let nextEmit = ip;

  let hash: number;
  let nextHash: number;
  let nextIp: number;
  let candidate: number;
  let skip: number;
  let bytesBetweenHashLookups;
  let base: number;
  let matched: number;
  let offset: number;
  let prevHash: number;
  let curHash: number;
  let flag = true;

  const INPUT_MARGIN = 15;
  if (inputSize >= INPUT_MARGIN) {
    ipLimit = ipEnd - INPUT_MARGIN;

    ip += 1;
    nextHash = hashFunc(load32(input, ip), hashFuncShift);

    while (flag) {
      skip = 32;
      nextIp = ip;
      do {
        ip = nextIp;
        hash = nextHash;
        bytesBetweenHashLookups = skip >>> 5;
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
        base = ip;
        matched = 4;
        while (ip + matched < ipEnd && input[ip + matched] === input[candidate + matched]) {
          matched += 1;
        }
        ip += matched;
        offset = base - candidate;
        op = emitCopy(output, op, offset, matched);

        nextEmit = ip;
        if (ip >= ipLimit) {
          flag = false;
          break;
        }
        prevHash = hashFunc(load32(input, ip - 1), hashFuncShift);
        hashTable[prevHash] = ip - 1 - baseIp;
        curHash = hashFunc(load32(input, ip), hashFuncShift);
        candidate = baseIp + hashTable[curHash];
        hashTable[curHash] = ip - baseIp;
      } while (equals32(input, ip, candidate));

      if (!flag) {
        break;
      }

      ip += 1;
      nextHash = hashFunc(load32(input, ip), hashFuncShift);
    }
  }

  if (nextEmit < ipEnd) {
    op = emitLiteral(input, nextEmit, ipEnd - nextEmit, output, op);
  }

  return op;
}

function putVarint(value: number, output: ByteArray, op: number) {
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

export class SnappyCompressor {
  private array: ByteArray;

  constructor(uncompressed: ByteArray) {
    this.array = uncompressed;
  }

  maxCompressedLength() {
    const sourceLen = this.array.length;
    return 32 + sourceLen + Math.floor(sourceLen / 6);
  }

  compressToBuffer(outBuffer: ByteArray) {
    const array = this.array;
    const length = array.length;

    let pos = 0;
    let fragmentSize: number;
    let outPos = putVarint(length, outBuffer, 0);
    while (pos < length) {
      fragmentSize = Math.min(length - pos, BLOCK_SIZE);
      outPos = compressFragment(array, pos, fragmentSize, outBuffer, outPos);
      pos += fragmentSize;
    }

    return outPos;
  }
}
