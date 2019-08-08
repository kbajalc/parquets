// tslint:disable: prefer-array-literal
// tslint:disable: no-parameter-reassignment

const BLOCK_LOG = 16;
const BLOCK_SIZE = 1 << BLOCK_LOG;
const INPUT_MARGIN = 15;

const MAX_HASH_TABLE_BITS = 14;
const globalHashTables: Uint16Array[] = new Array(MAX_HASH_TABLE_BITS + 1);

function hash32(array: Buffer, pos: number, shift: number) {
  return (((
    array[pos]
    + (array[pos + 1] << 8)
    + (array[pos + 2] << 16)
    + (array[pos + 3] << 24)
  ) * 0x1e35a7bd) & 0xffffffff) >>> shift;
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
  // The vast majority of copies are below 16 bytes, for which a
  // call to memcpy is overkill. This fast path can sometimes
  // copy up to 15 bytes too much, but that is okay in the
  // main loop, since we have a bit to go on for both sides:
  //
  //   - The input will always have kInputMarginBytes = 15 extra
  //     available bytes, as long as we're in the main loop, and
  //     if not, allow_fast_path = false.
  //   - The output will always have 32 spare bytes (see
  //     MaxCompressedLength).
  if (len <= 60) {
    // Fits in tag byte
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

function emitCopyAtMost64(output: Buffer, op: number, offset: number, len: number) {
  if (len < 12 && offset < 2048) {
    // offset fits in 11 bits.  The 3 highest go in the top of the first byte,
    // and the rest go in the second byte.
    output[op] = 1 + ((len - 4) << 2) + ((offset >>> 8) << 5);
    output[op + 1] = offset & 0xff;
    return op + 2;
  } else {
    // Write 4 bytes, though we only care about 3 of them.  The output buffer
    // is required to have some slack, so the extra byte won't overrun it.
    output[op] = 2 + ((len - 1) << 2);
    output[op + 1] = offset & 0xff;
    output[op + 2] = offset >>> 8;
    return op + 3;
  }
}

function emitCopy(output: Buffer, op: number, offset: number, len: number) {
  // Emit 64 byte copies but make sure to keep at least four bytes reserved.
  while (len >= 68) {
    op = emitCopyAtMost64(output, op, offset, 64);
    len -= 64;
  }
  // One or two copies will now finish the job.
  if (len > 64) {
    op = emitCopyAtMost64(output, op, offset, 60);
    len -= 60;
  }
  // Emit remainder.
  op = emitCopyAtMost64(output, op, offset, len);
  return op;
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

function hashBits(inputSize: number) {
  let bits = 9;
  while ((1 << bits) <= inputSize && bits <= MAX_HASH_TABLE_BITS) {
    bits += 1;
  }
  bits--;
  return bits;
}

function compressFragment(input: Buffer, baseIp: number, inputSize: number, output: Buffer, baseOp: number) {
  if (inputSize < INPUT_MARGIN) {
    return emitLiteral(input, baseIp, inputSize, output, baseOp);
  }

  const hashTableBits = hashBits(inputSize);
  const shift = 32 - hashTableBits;
  if (typeof globalHashTables[hashTableBits] === 'undefined') {
    globalHashTables[hashTableBits] = new Uint16Array(1 << hashTableBits);
  }
  const table = globalHashTables[hashTableBits];
  for (let i = 0; i < table.length; i++) {
    table[i] = 0;
  }

  const ipEnd = baseIp + inputSize;
  const ipLimit = ipEnd - INPUT_MARGIN;

  // "ip" is the input pointer, and "op" is the output pointer.
  let ip = baseIp + 1;
  let op = baseOp;

  // Bytes in [next_emit, ip) will be emitted as literal bytes.  Or
  // [next_emit, ip_end) after the main loop.
  let nextEmit = baseIp;
  let nextHash = hash32(input, ip, shift);

  // The body of this loop calls EmitLiteral once and then EmitCopy one or
  // more times.  (The exception is that when we're close to exhausting
  // the input we goto emit_remainder.)
  //
  // In the first iteration of this loop we're just starting, so
  // there's nothing to copy, so calling EmitLiteral once is
  // necessary.  And we only start a new iteration when the
  // current iteration has determined that a call to EmitLiteral will
  // precede the next call to EmitCopy (if any).
  loop: while (ip < ipEnd) {

    // The body of this loop calls EmitLiteral once and then EmitCopy one or
    // more times.  (The exception is that when we're close to exhausting
    // the input we goto emit_remainder.)
    //
    // In the first iteration of this loop we're just starting, so
    // there's nothing to copy, so calling EmitLiteral once is
    // necessary.  And we only start a new iteration when the
    // current iteration has determined that a call to EmitLiteral will
    // precede the next call to EmitCopy (if any).

    // Step 1: Scan forward in the input looking for a 4-byte-long match.
    // If we get close to exhausting the input then goto emit_remainder.
    //
    // Heuristic match skipping: If 32 bytes are scanned with no matches
    // found, start looking only at every other byte. If 32 more bytes are
    // scanned (or skipped), look at every third byte, etc.. When a match is
    // found, immediately go back to looking at every byte. This is a small
    // loss (~5% performance, ~0.1% density) for compressible data due to more
    // bookkeeping, but for non-compressible data (such as JPEG) it's a huge
    // win since the compressor quickly "realizes" the data is incompressible
    // and doesn't bother looking for matches everywhere.
    //
    // The "skip" variable keeps track of how many bytes there are since the
    // last match; dividing it by 32 (ie. right-shifting by five) gives the
    // number of bytes to move ahead for each iteration.
    let skip = 32;
    let nextIp = ip;
    let candidate = 0;
    do {
      ip = nextIp;
      const inc = skip >>> 5;
      skip += inc;
      nextIp = ip + inc;
      if (nextIp > ipLimit) break loop;

      const hash = nextHash;
      nextHash = hash32(input, nextIp, shift);
      candidate = baseIp + table[hash];
      table[hash] = ip - baseIp;
    } while (!equals32(input, ip, candidate));

    // Step 2: A 4-byte match has been found.  We'll later see if more
    // than 4 bytes match.  But, prior to the match, input
    // bytes [next_emit, ip) are unmatched.  Emit them as "literal bytes."
    op = emitLiteral(input, nextEmit, ip - nextEmit, output, op);

    // Step 3: Call EmitCopy, and then see if another EmitCopy could
    // be our next move.  Repeat until we find no match for the
    // input immediately after what was consumed by the last EmitCopy call.
    //
    // If we exit this loop normally then we need to call EmitLiteral next,
    // though we don't yet know how big the literal will be.  We handle that
    // by proceeding to the next iteration of the main loop.  We also can exit
    // this loop via goto if we get close to exhausting the input.
    do {
      // We have a 4-byte match at ip, and no need to emit any
      // "literal bytes" prior to ip.
      let matched = 4;
      while (ip + matched < ipEnd && input[ip + matched] === input[candidate + matched]) {
        matched += 1;
      }
      op = emitCopy(output, op, ip - candidate, matched);
      ip += matched;
      nextEmit = ip;

      if (ip >= ipLimit) break loop;

      // We are now looking for a 4-byte match again.  We read
      // table[Hash(ip, shift)] for that.  To improve compression,
      // we also update table[Hash(ip - 1, shift)] and table[Hash(ip, shift)].
      const prevHash = hash32(input, ip - 1, shift);
      table[prevHash] = ip - 1 - baseIp;
      const curHash = hash32(input, ip, shift);
      candidate = baseIp + table[curHash];
      table[curHash] = ip - baseIp;
    } while (equals32(input, ip, candidate));

    ip += 1;
    nextHash = hash32(input, ip, shift);
  }

  // Emit the remaining bytes as a literal
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
