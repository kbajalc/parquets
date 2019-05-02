import zlib = require('zlib');
import snappyjs = require('snappyjs');
import { ParquetCompression } from './declare';

let brotli: any;
let lzo: any;
let lz4js: any;

export const PARQUET_COMPRESSION_METHODS: Record<ParquetCompression, {
  deflate: (value: Buffer) => Buffer,
  inflate: (value: Buffer, size: number) => Buffer
}> = {
  UNCOMPRESSED: {
    deflate: deflate_identity,
    inflate: inflate_identity
  },
  GZIP: {
    deflate: deflate_gzip,
    inflate: inflate_gzip
  },
  SNAPPY: {
    deflate: deflate_snappy,
    inflate: inflate_snappy
  },
  LZO: {
    deflate: deflate_lzo,
    inflate: inflate_lzo
  },
  BROTLI: {
    deflate: deflate_brotli,
    inflate: inflate_brotli
  },
  LZ4: {
    deflate: deflate_lz4,
    inflate: inflate_lz4
  }
};

/**
 * Deflate a value using compression method `method`
 */
export function deflate(method: ParquetCompression, value: Buffer): Buffer {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw new Error('invalid compression method: ' + method);
  }

  return PARQUET_COMPRESSION_METHODS[method].deflate(value);
}

function deflate_identity(value: Buffer): Buffer {
  return value;
}

function deflate_gzip(value: Buffer): Buffer {
  return zlib.gzipSync(value);
}

function deflate_snappy(value: Buffer): Buffer {
  return snappyjs.compress(value);
}

function deflate_lzo(value: Buffer): Buffer {
  lzo = lzo || (module || global)['require']('lzo');
  return lzo.compress(value);
}

function deflate_brotli(value: Buffer): Buffer {
  brotli = brotli || (module || global)['require']('brotli');
  const result = brotli.compress(value, {
    mode: 0,
    quality: 8,
    lgwin: 22
  });
  return result ? Buffer.from(result) : Buffer.alloc(0);
}

function deflate_lz4(value: Buffer): Buffer {
  lz4js = lz4js || (module || global)['require']('lz4js');
  try {
    // let result = Buffer.alloc(lz4js.encodeBound(value.length));
    // const compressedSize = lz4.encodeBlock(value, result);
    // // remove unnecessary bytes
    // result = result.slice(0, compressedSize);
    // return result;
    return Buffer.from(lz4js.compress(value));
  } catch (err) {
    throw err;
  }
}

/**
 * Inflate a value using compression method `method`
 */
export function inflate(method: ParquetCompression, value: Buffer, size: number): Buffer {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw new Error('invalid compression method: ' + method);
  }

  return PARQUET_COMPRESSION_METHODS[method].inflate(value, size);
}

function inflate_identity(value: Buffer): Buffer {
  return value;
}

function inflate_gzip(value: Buffer): Buffer {
  return zlib.gunzipSync(value);
}

function inflate_snappy(value: Buffer): Buffer {
  return snappyjs.uncompress(value);
}

function inflate_lzo(value: Buffer, size: number): Buffer {
  lzo = lzo || (module || global)['require']('lzo');
  return lzo.decompress(value, size);
}

function inflate_lz4(value: Buffer, size: number): Buffer {
  lz4js = lz4js || (module || global)['require']('lz4js');
  try {
    // let result = Buffer.alloc(size);
    // const uncompressedSize = lz4js.decodeBlock(value, result);
    // // remove unnecessary bytes
    // result = result.slice(0, uncompressedSize);
    // return result;
    return Buffer.from(lz4js.decompress(value, size));
  } catch (err) {
    throw err;
  }
}

function inflate_brotli(value: Buffer): Buffer {
  brotli = brotli || (module || global)['require']('brotli');
  if (!value.length) {
    return Buffer.alloc(0);
  }
  return Buffer.from(brotli.decompress(value));
}
