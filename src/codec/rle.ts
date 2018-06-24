import varint = require('varint');
import { ParquetType, TODO } from '../declare';

function encodeRunBitpacked(values: number[], opts: TODO): Buffer {
  if (values.length % 8 !== 0) {
    throw 'must be a multiple of 8';
  }

  const buf = Buffer.alloc(Math.ceil(opts.bitWidth * (values.length / 8)));
  for (let b = 0; b < opts.bitWidth * values.length; ++b) {
    if ((values[Math.floor(b / opts.bitWidth)] & (1 << b % opts.bitWidth)) > 0) {
      buf[Math.floor(b / 8)] |= (1 << (b % 8));
    }
  }

  return Buffer.concat([
    Buffer.from(varint.encode(((values.length / 8) << 1) | 1)),
    buf
  ]);
}

function encodeRunRepeated(value: number, count: number, opts: TODO) {
  const buf = Buffer.alloc(Math.ceil(opts.bitWidth / 8));

  for (let i = 0; i < buf.length; ++i) {
    buf.writeUInt8(value & 0xff, i);
    value >> 8;
  }

  return Buffer.concat([
    Buffer.from(varint.encode(count << 1)),
    buf
  ]);
}

export function encodeValues(type: ParquetType, values: any[], opts: TODO): Buffer {
  if (!('bitWidth' in opts)) {
    throw 'bitWidth is required';
  }

  switch (type) {

    case 'BOOLEAN':
    case 'INT32':
    case 'INT64':
      // tslint:disable-next-line:no-parameter-reassignment
      values = values.map(x => parseInt(x, 10));
      break;

    default:
      throw 'unsupported type: ' + type;
  }

  let buf = Buffer.alloc(0);
  const runs = [];
  for (let cur = 0; cur < values.length; cur += 8) {
    let repeating = true;
    for (let i = 1; i < 8; ++i) {
      if (values[cur + i] !== values[cur]) {
        repeating = false;
      }
    }

    const append =
      runs.length > 0 &&
      (runs[runs.length - 1][1] !== null) === repeating &&
      (!repeating || runs[runs.length - 1][1] === values[cur]);

    if (!append) {
      runs.push([cur, repeating ? values[cur] : null]);
    }
  }

  for (let i = values.length - (values.length % 8); i < values.length; ++i) {
    runs.push([i, values[i]]);
  }

  for (let i = 0; i < runs.length; ++i) {
    const begin = runs[i][0];
    const end = i < runs.length - 1 ? runs[i + 1][0] : values.length;
    const rep = runs[i][1];

    if (rep === null) {
      buf = Buffer.concat([buf, encodeRunBitpacked(values.slice(begin, end), opts)]);
    } else {
      buf = Buffer.concat([buf, encodeRunRepeated(rep, end - begin, opts)]);
    }
  }

  if (opts.disableEnvelope) {
    return buf;
  }

  const envelope = Buffer.alloc(buf.length + 4);
  envelope.writeUInt32LE(buf.length, undefined);
  buf.copy(envelope, 4);

  return envelope;
}

function decodeRunBitpacked(cursor: TODO, count: number, opts: TODO): number[] {
  if (count % 8 !== 0) {
    throw 'must be a multiple of 8';
  }

  // tslint:disable-next-line:prefer-array-literal
  const values = new Array(count).fill(0);
  for (let b = 0; b < opts.bitWidth * count; ++b) {
    if (cursor.buffer[cursor.offset + Math.floor(b / 8)] & (1 << (b % 8))) {
      values[Math.floor(b / opts.bitWidth)] |= (1 << b % opts.bitWidth);
    }
  }

  cursor.offset += opts.bitWidth * (count / 8);
  return values;
}

function decodeRunRepeated(cursor: TODO, count: number, opts: TODO): number[] {
  let value = 0;
  for (let i = 0; i < Math.ceil(opts.bitWidth / 8); ++i) {
    value << 8;
    value += cursor.buffer[cursor.offset];
    cursor.offset += 1;
  }

  // tslint:disable-next-line:prefer-array-literal
  return new Array(count).fill(value);
}

export function decodeValues(type: ParquetType, cursor: TODO, count: number, opts: TODO): number[] {
  if (!('bitWidth' in opts)) {
    throw 'bitWidth is required';
  }

  if (!opts.disableEnvelope) {
    cursor.offset += 4;
  }

  const values = [];
  while (values.length < count) {
    const header = varint.decode(cursor.buffer, cursor.offset);
    cursor.offset += varint.encodingLength(header);
    if (header & 1) {
      const count = (header >> 1) * 8;
      values.push(...decodeRunBitpacked(cursor, count, opts));
    } else {
      const count = header >> 1;
      values.push(...decodeRunRepeated(cursor, count, opts));
    }
  }

  if (values.length !== count) {
    throw 'invalid RLE encoding';
  }

  return values;
}
