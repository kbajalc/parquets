import zlib = require('zlib');
import snappy = require('snappyjs');
import lzo = require('lzo');
import brotli = require('brotli');
import { ParquetCompression } from './declare';

export const PARQUET_COMPRESSION_METHODS: Record<ParquetCompression, { deflate: (value: Buffer) => Buffer, inflate: (Buffer) => Buffer }> = {
  'UNCOMPRESSED': {
    deflate: deflate_identity,
    inflate: inflate_identity
  },
  'GZIP': {
    deflate: deflate_gzip,
    inflate: inflate_gzip
  },
  'SNAPPY': {
    deflate: deflate_snappy,
    inflate: inflate_snappy
  },
  'LZO': {
    deflate: deflate_lzo,
    inflate: inflate_lzo
  },
  'BROTLI': {
    deflate: deflate_brotli,
    inflate: inflate_brotli
  }
};

/**
 * Deflate a value using compression method `method`
 */
export function deflate(method: ParquetCompression, value: Buffer): Buffer {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw 'invalid compression method: ' + method;
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
  return snappy.compress(value);
}

function deflate_lzo(value: Buffer): Buffer {
  return lzo.compress(value);
}

function deflate_brotli(value: Buffer): Buffer {
  return new Buffer(brotli.compress(value, {
    mode: 0,
    quality: 8,
    lgwin: 22
  }));
}

/**
 * Inflate a value using compression method `method`
 */
export function inflate(method: ParquetCompression, value: Buffer): Buffer {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw 'invalid compression method: ' + method;
  }

  return PARQUET_COMPRESSION_METHODS[method].inflate(value);
}

function inflate_identity(value: Buffer): Buffer {
  return value;
}

function inflate_gzip(value: Buffer): Buffer {
  return zlib.gunzipSync(value);
}

function inflate_snappy(value: Buffer): Buffer {
  return snappy.uncompress(value);
}

function inflate_lzo(value: Buffer): Buffer {
  return lzo.decompress(value);
}

function inflate_brotli(value: Buffer): Buffer {
  return new Buffer(brotli.decompress(value));
}
