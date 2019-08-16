import { PrimitiveType } from '../types';
import { CursorBuffer, ParquetCodecKit, ParquetCodecOptions } from './types';
import RLE = require('./rle');
import PLAIN = require('./plain');

export * from './types';

export type ParquetCodec = 'PLAIN' | 'RLE';

export namespace ParquetCodec {
  export function is(name: string) {
    return name in PARQUET_CODEC;
  }

  /**
   * Encode a consecutive array of data using one of the parquet encodings
   */
  export function encodeValues(type: PrimitiveType, encoding: ParquetCodec, values: any[], opts: ParquetCodecOptions) {
    if (!(encoding in PARQUET_CODEC)) {
      throw new Error(`invalid encoding: ${encoding}`);
    }
    return PARQUET_CODEC[encoding].encodeValues(type, values, opts);
  }

  /**
   * Decode a consecutive array of data using one of the parquet encodings
   */
  export function decodeValues(type: PrimitiveType, encoding: ParquetCodec, cursor: CursorBuffer, count: number, opts: ParquetCodecOptions): any[] {
    if (!(encoding in PARQUET_CODEC)) {
      throw new Error(`invalid encoding: ${encoding}`);
    }
    return PARQUET_CODEC[encoding].decodeValues(type, cursor, count, opts);
  }
}

const PARQUET_CODEC: Record<ParquetCodec, ParquetCodecKit> = {
  PLAIN: {
    encodeValues: PLAIN.encodeValues,
    decodeValues: PLAIN.decodeValues
  },
  RLE: {
    encodeValues: RLE.encodeValues,
    decodeValues: RLE.decodeValues
  }
};
