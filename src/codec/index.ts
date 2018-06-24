import { ParquetType, TODO } from '../declare';

export interface ParquetCodec {
  encodeValues(type: ParquetType, values, opts?: TODO): Buffer;
  decodeValues(type: ParquetType, cursor: TODO, count: number, opts: TODO): any[];
}

export const PLAIN = require('./plain');
export const RLE = require('./rle');
export const PARQUET_CODEC: Record<string, ParquetCodec> = { RLE, PLAIN };
