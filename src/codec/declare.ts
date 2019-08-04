import { PrimitiveType } from '../declare';

export interface CursorBuffer {
  buffer: Buffer;
  offset: number;
  size?: number;
}

export interface ParquetCodecOptions {
  bitWidth?: number;
  disableEnvelope?: boolean;
  typeLength?: number;
}

export interface ParquetCodecKit {
  encodeValues(type: PrimitiveType, values: any[], opts?: ParquetCodecOptions): Buffer;
  decodeValues(type: PrimitiveType, cursor: CursorBuffer, count: number, opts: ParquetCodecOptions): any[];
}
