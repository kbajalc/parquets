export const PLAIN = require('./plain');
export const RLE = require('./rle');
export const PARQUET_CODEC = { RLE, PLAIN };
export type ParquetCodec = 'PLAIN' | 'RLE';
