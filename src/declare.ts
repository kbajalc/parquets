import { RowGroup } from './gen/parquet_types';

export type TODO = any;

export type ParquetCodec = 'PLAIN' | 'RLE';
export type ParquetCompression = 'UNCOMPRESSED' | 'GZIP' | 'SNAPPY' | 'LZO' | 'BROTLI' | 'LZ4';
export type RepetitionType = 'REQUIRED' | 'OPTIONAL' | 'REPEATED';
export type ParquetType =
  'BOOLEAN'
  | 'INT32'
  | 'INT64'
  | 'INT96'
  | 'FLOAT'
  | 'DOUBLE'
  | 'BYTE_ARRAY'
  | 'FIXED_LEN_BYTE_ARRAY'
  | 'UTF8'
  | 'TIME_MILLIS'
  | 'TIME_MICROS'
  | 'DATE'
  | 'TIMESTAMP_MILLIS'
  | 'TIMESTAMP_MICROS'
  | 'UINT_8'
  | 'UINT_16'
  | 'UINT_32'
  | 'UINT_64'
  | 'INT_8'
  | 'INT_16'
  | 'INT_32'
  | 'INT_64'
  | 'JSON'
  | 'BSON'
  | 'INTERVAL';

export interface TypeDef {
  primitiveType: ParquetType;
  originalType?: ParquetType;
  typeLength?: number;
  toPrimitive: Function;
  fromPrimitive?: Function;
}

export interface SchemaDefinition {
  [string: string]: ElementDefinition;
}

export interface ElementDefinition {
  type?: ParquetType;
  typeLength?: number;
  encoding?: ParquetCodec;
  compression?: ParquetCompression;
  optional?: boolean;
  repeated?: boolean;
  fields?: SchemaDefinition;
}

export interface FieldDefinition {
  name: string;
  path: string[];
  primitiveType?: ParquetType;
  originalType?: ParquetType;
  repetitionType: RepetitionType;
  typeLength?: number;
  encoding?: ParquetCodec;
  compression?: ParquetCompression;
  rLevelMax: number;
  dLevelMax: number;
  isNested?: boolean;
  fieldCount?: number;
  fields?: Record<string, FieldDefinition>;
}

export interface RecordBuffer {
  rowCount?: number;
  columnData?: Record<string, ColumnData>;
  [path: string]: {
    dlevels: any[],
    rlevels: any[],
    values: any[],
    count: number
  } | any;
}

export interface ColumnData {
  dlevels: number[];
  rlevels: number[];
  values: any[];
  count: number;
}

export interface CursorBuffer {
  buffer: Buffer;
  offset: number;
  size: number;
}

export interface ParquetRow {
  [key: string]: any;
}

export interface ParquetRowGroup {
  body: Buffer;
  metadata: RowGroup;
}
