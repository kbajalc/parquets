import BSON = require('bson');
import int53 = require('int53');

export type ParquetType = PrimitiveType | OriginalType
  | 'DECIMAL_32' // 5
  | 'DECIMAL_64' // 5
  | 'DECIMAL_FIXED' // 5
  | 'DECIMAL_BINARY'; // 5

export type PrimitiveType =
  // Base Types
  'BOOLEAN' // 0
  | 'INT32' // 1
  | 'INT64' // 2
  | 'INT96' // 3
  | 'FLOAT' // 4
  | 'DOUBLE' // 5
  | 'BYTE_ARRAY' // 6,
  | 'FIXED_LEN_BYTE_ARRAY'; // 7

export type OriginalType =
  // Converted Types
  | 'UTF8' // 0
  | 'MAP' // 1
  | 'MAP_KEY_VALUE' // 2
  | 'LIST' // 3
  | 'ENUM' // 4
  | 'DECIMAL' // 5
  | 'DATE' // 6
  | 'TIME_MILLIS' // 7
  | 'TIME_MICROS' // 8
  | 'TIMESTAMP_MILLIS' // 9
  | 'TIMESTAMP_MICROS' // 10
  | 'UINT_8' // 11
  | 'UINT_16' // 12
  | 'UINT_32' // 13
  | 'UINT_64' // 14
  | 'INT_8' // 15
  | 'INT_16' // 16
  | 'INT_32' // 17
  | 'INT_64' // 18
  | 'JSON' // 19
  | 'BSON' // 20
  | 'INTERVAL'; // 21

export interface ParquetTypeKit {
  primitiveType: PrimitiveType;
  originalType?: OriginalType;
  typeLength?: number;
  toPrimitive: Function;
  fromPrimitive?: Function;
}

export namespace ParquetType {
  export const MAX_PRECISION_INT32 = 9;
  export const MAX_PRECISION_INT64 = 15; // 18 for 64bit

  export function is(name: string) {
    return name in PARQUET_LOGICAL_TYPES;
  }

  export function get(name: string): ParquetTypeKit {
    return name ? PARQUET_LOGICAL_TYPES[name as ParquetType] : undefined;
  }

  export function resolve(original: OriginalType, primitive: PrimitiveType): ParquetType {
    if (original !== 'DECIMAL') return original || primitive;
    switch (primitive) {
      case 'INT32':
        return 'DECIMAL_32';
      case 'INT64':
        return 'DECIMAL';
      case 'BYTE_ARRAY':
        return 'DECIMAL_BINARY';
      case 'FIXED_LEN_BYTE_ARRAY':
        return 'DECIMAL_FIXED';
      default:
        throw TypeError('unexpected primitive type: ' + primitive);
    }
  }

  export function maxPrecision(numBytes: number) {
    return Math.round(                      // convert double to long
      Math.floor(Math.log10(              // number of base-10 digits
        Math.pow(2, 8 * numBytes - 1) - 1)  // max value stored in numBytes
      )
    );
  }

  export function precisionBytes(precision: number) {
    return Math.ceil((Math.log2(Math.pow(10, precision)) + 1) / 8);
  }

  /**
   * Convert a value from it's native representation to the internal/underlying
   * primitive type
   */
  export function toPrimitive(type: ParquetType, value: any, scale?: number, length?: number) {
    if (!(type in PARQUET_LOGICAL_TYPES)) {
      throw new Error('invalid type: ' + type);
    }
    return PARQUET_LOGICAL_TYPES[type].toPrimitive(value, scale, length);
  }

  /**
   * Convert a value from it's internal/underlying primitive representation to
   * the native representation
   */
  export function fromPrimitive(type: ParquetType, value: any, scale?: number, length?: number) {
    if (!(type in PARQUET_LOGICAL_TYPES)) {
      throw new Error('invalid type: ' + type);
    }
    if ('fromPrimitive' in PARQUET_LOGICAL_TYPES[type]) {
      return PARQUET_LOGICAL_TYPES[type].fromPrimitive(value, scale, length);
      // tslint:disable-next-line:no-else-after-return
    } else {
      return value;
    }
  }
}

const PARQUET_LOGICAL_TYPES: Record<ParquetType, ParquetTypeKit> = {
  BOOLEAN: {
    primitiveType: 'BOOLEAN',
    toPrimitive: toPrimitive_BOOLEAN,
    fromPrimitive: fromPrimitive_BOOLEAN
  },
  INT32: {
    primitiveType: 'INT32',
    toPrimitive: toPrimitive_INT32
  },
  INT64: {
    primitiveType: 'INT64',
    toPrimitive: toPrimitive_INT64
  },
  INT96: {
    primitiveType: 'INT96',
    toPrimitive: toPrimitive_INT96
  },
  FLOAT: {
    primitiveType: 'FLOAT',
    toPrimitive: toPrimitive_FLOAT
  },
  DOUBLE: {
    primitiveType: 'DOUBLE',
    toPrimitive: toPrimitive_DOUBLE
  },
  BYTE_ARRAY: {
    primitiveType: 'BYTE_ARRAY',
    toPrimitive: toPrimitive_BYTE_ARRAY
  },
  FIXED_LEN_BYTE_ARRAY: {
    primitiveType: 'FIXED_LEN_BYTE_ARRAY',
    toPrimitive: toPrimitive_BYTE_ARRAY
  },
  UTF8: {
    primitiveType: 'BYTE_ARRAY',
    originalType: 'UTF8',
    toPrimitive: toPrimitive_UTF8,
    fromPrimitive: fromPrimitive_UTF8
  },
  ENUM: {
    primitiveType: 'BYTE_ARRAY',
    originalType: 'ENUM',
    toPrimitive: toPrimitive_ENUM,
    fromPrimitive: fromPrimitive_ENUM
  },
  TIME_MILLIS: {
    primitiveType: 'INT32',
    originalType: 'TIME_MILLIS',
    toPrimitive: toPrimitive_TIME_MILLIS
  },
  TIME_MICROS: {
    primitiveType: 'INT64',
    originalType: 'TIME_MICROS',
    toPrimitive: toPrimitive_TIME_MICROS
  },
  DATE: {
    primitiveType: 'INT32',
    originalType: 'DATE',
    toPrimitive: toPrimitive_DATE,
    fromPrimitive: fromPrimitive_DATE
  },
  TIMESTAMP_MILLIS: {
    primitiveType: 'INT64',
    originalType: 'TIMESTAMP_MILLIS',
    toPrimitive: toPrimitive_TIMESTAMP_MILLIS,
    fromPrimitive: fromPrimitive_TIMESTAMP_MILLIS
  },
  TIMESTAMP_MICROS: {
    primitiveType: 'INT64',
    originalType: 'TIMESTAMP_MICROS',
    toPrimitive: toPrimitive_TIMESTAMP_MICROS,
    fromPrimitive: fromPrimitive_TIMESTAMP_MICROS
  },
  UINT_8: {
    primitiveType: 'INT32',
    originalType: 'UINT_8',
    toPrimitive: toPrimitive_UINT8
  },
  UINT_16: {
    primitiveType: 'INT32',
    originalType: 'UINT_16',
    toPrimitive: toPrimitive_UINT16
  },
  UINT_32: {
    primitiveType: 'INT32',
    originalType: 'UINT_32',
    toPrimitive: toPrimitive_UINT32
  },
  UINT_64: {
    primitiveType: 'INT64',
    originalType: 'UINT_64',
    toPrimitive: toPrimitive_UINT64
  },
  INT_8: {
    primitiveType: 'INT32',
    originalType: 'INT_8',
    toPrimitive: toPrimitive_INT8
  },
  INT_16: {
    primitiveType: 'INT32',
    originalType: 'INT_16',
    toPrimitive: toPrimitive_INT16
  },
  INT_32: {
    primitiveType: 'INT32',
    originalType: 'INT_32',
    toPrimitive: toPrimitive_INT32
  },
  INT_64: {
    primitiveType: 'INT64',
    originalType: 'INT_64',
    toPrimitive: toPrimitive_INT64
  },
  JSON: {
    primitiveType: 'BYTE_ARRAY',
    originalType: 'JSON',
    toPrimitive: toPrimitive_JSON,
    fromPrimitive: fromPrimitive_JSON
  },
  BSON: {
    primitiveType: 'BYTE_ARRAY',
    originalType: 'BSON',
    toPrimitive: toPrimitive_BSON,
    fromPrimitive: fromPrimitive_BSON
  },
  INTERVAL: {
    primitiveType: 'FIXED_LEN_BYTE_ARRAY',
    originalType: 'INTERVAL',
    typeLength: 12,
    toPrimitive: toPrimitive_INTERVAL,
    fromPrimitive: fromPrimitive_INTERVAL
  },
  LIST: {
    primitiveType: null,
    originalType: 'LIST',
    toPrimitive: null
  },
  MAP: {
    primitiveType: null,
    originalType: 'MAP',
    toPrimitive: null
  },
  MAP_KEY_VALUE: {
    primitiveType: null,
    originalType: 'MAP_KEY_VALUE',
    toPrimitive: null
  },
  DECIMAL: {
    primitiveType: 'INT64',
    originalType: 'DECIMAL',
    toPrimitive: toPrimitive_DECIMAL_64,
    fromPrimitive: fromPrimitive_DECIMAL_INT
  },
  DECIMAL_64: {
    primitiveType: 'INT64',
    originalType: 'DECIMAL',
    toPrimitive: toPrimitive_DECIMAL_64,
    fromPrimitive: fromPrimitive_DECIMAL_INT
  },
  DECIMAL_32: {
    primitiveType: 'INT32',
    originalType: 'DECIMAL',
    toPrimitive: toPrimitive_DECIMAL_32,
    fromPrimitive: fromPrimitive_DECIMAL_INT
  },
  DECIMAL_FIXED: {
    primitiveType: 'FIXED_LEN_BYTE_ARRAY',
    originalType: 'DECIMAL',
    toPrimitive: toPrimitive_DECIMAL_FIXED,
    fromPrimitive: fromPrimitive_DECIMAL_FIXED
  },
  DECIMAL_BINARY: {
    primitiveType: 'BYTE_ARRAY',
    originalType: 'DECIMAL',
    toPrimitive: toPrimitive_DECIMAL_BIN,
    fromPrimitive: fromPrimitive_DECIMAL_BIN
  }
};

function toPrimitive_BOOLEAN(value: any) {
  return !!value;
}

function fromPrimitive_BOOLEAN(value: any) {
  return !!value;
}

function toPrimitive_FLOAT(value: any) {
  const v = parseFloat(value);
  if (isNaN(v)) {
    throw new Error('invalid value for FLOAT: ' + value);
  }
  return v;
}

function toPrimitive_DOUBLE(value: any) {
  const v = parseFloat(value);
  if (isNaN(v)) {
    throw new Error('invalid value for DOUBLE: ' + value);
  }
  return v;
}

function toPrimitive_INT8(value: any) {
  const v = parseInt(value, 10);
  if (v < -0x80 || v > 0x7f || isNaN(v)) {
    throw new Error('invalid value for INT8: ' + value);
  }
  return v;
}

function toPrimitive_UINT8(value: any) {
  const v = parseInt(value, 10);
  if (v < 0 || v > 0xff || isNaN(v)) {
    throw new Error('invalid value for UINT8: ' + value);
  }
  return v;
}

function toPrimitive_INT16(value: any) {
  const v = parseInt(value, 10);
  if (v < -0x8000 || v > 0x7fff || isNaN(v)) {
    throw new Error('invalid value for INT16: ' + value);
  }
  return v;
}

function toPrimitive_UINT16(value: any) {
  const v = parseInt(value, 10);
  if (v < 0 || v > 0xffff || isNaN(v)) {
    throw new Error('invalid value for UINT16: ' + value);
  }
  return v;
}

function toPrimitive_INT32(value: any) {
  const v = parseInt(value, 10);
  if (v < -0x80000000 || v > 0x7fffffff || isNaN(v)) {
    throw new Error('invalid value for INT32: ' + value);
  }
  return v;
}

function toPrimitive_UINT32(value: any) {
  const v = parseInt(value, 10);
  if (v < 0 || v > 0xffffffffffff || isNaN(v)) {
    throw new Error('invalid value for UINT32: ' + value);
  }
  return v;
}

function toPrimitive_INT64(value: any) {
  const v = parseInt(value, 10);
  if (isNaN(v)) {
    throw new Error('invalid value for INT64: ' + value);
  }
  return v;
}

function toPrimitive_UINT64(value: any) {
  const v = parseInt(value, 10);
  if (v < 0 || isNaN(v)) {
    throw new Error('invalid value for UINT64: ' + value);
  }
  return v;
}

function toPrimitive_INT96(value: any) {
  const v = parseInt(value, 10);
  if (isNaN(v)) {
    throw new Error('invalid value for INT96: ' + value);
  }
  return v;
}

function toPrimitive_BYTE_ARRAY(value: any) {
  return Buffer.from(value);
}

function toPrimitive_UTF8(value: any) {
  return Buffer.from(value, 'utf8');
}

function fromPrimitive_UTF8(value: any) {
  return value.toString();
}

function toPrimitive_ENUM(value: any) {
  return Buffer.from(value && String(value), 'utf8');
}

function fromPrimitive_ENUM(value: any) {
  return value.toString();
}

function toPrimitive_JSON(value: any) {
  return Buffer.from(JSON.stringify(value));
}

function fromPrimitive_JSON(value: any) {
  return JSON.parse(value);
}

function toPrimitive_BSON(value: any) {
  return Buffer.from(BSON.serialize(value));
}

function fromPrimitive_BSON(value: any) {
  return BSON.deserialize(value);
}

function toPrimitive_TIME_MILLIS(value: any) {
  const v = parseInt(value, 10);
  if (v < 0 || v > 0xffffffffffffffff || isNaN(v)) {
    throw new Error('invalid value for TIME_MILLIS: ' + value);
  }
  return v;
}

function toPrimitive_TIME_MICROS(value: any) {
  const v = parseInt(value, 10);
  if (v < 0 || isNaN(v)) {
    throw new Error('invalid value for TIME_MICROS: ' + value);
  }
  return v;
}

const kMillisPerDay = 86400000;

function toPrimitive_DATE(value: any) {
  /* convert from date */
  if (value instanceof Date) {
    return value.getTime() / kMillisPerDay;
  }
  /* convert from integer */
  {
    const v = parseInt(value, 10);
    if (v < 0 || isNaN(v)) {
      throw new Error('invalid value for DATE: ' + value);
    }
    return v;
  }
}

function fromPrimitive_DATE(value: any) {
  return new Date(value * kMillisPerDay);
}

function toPrimitive_TIMESTAMP_MILLIS(value: any) {
  /* convert from date */
  if (value instanceof Date) {
    return value.getTime();
  }
  /* convert from integer */
  {
    const v = parseInt(value, 10);
    if (v < 0 || isNaN(v)) {
      throw new Error('invalid value for TIMESTAMP_MILLIS: ' + value);
    }
    return v;
  }
}

function fromPrimitive_TIMESTAMP_MILLIS(value: any) {
  return new Date(value);
}

function toPrimitive_TIMESTAMP_MICROS(value: any) {
  /* convert from date */
  if (value instanceof Date) {
    return value.getTime() * 1000;
  }
  /* convert from integer */
  {
    const v = parseInt(value, 10);
    if (v < 0 || isNaN(v)) {
      throw new Error('invalid value for TIMESTAMP_MICROS: ' + value);
    }
    return v;
  }
}

function fromPrimitive_TIMESTAMP_MICROS(value: any) {
  return new Date(value / 1000);
}

function toPrimitive_INTERVAL(value: any) {
  if (!value.months || !value.days || !value.milliseconds) {
    throw new Error('value for INTERVAL must be object { months: ..., days: ..., milliseconds: ... }');
  }

  const buf = Buffer.alloc(12);

  buf.writeUInt32LE(value.months, 0);
  buf.writeUInt32LE(value.days, 4);
  buf.writeUInt32LE(value.milliseconds, 8);
  return buf;
}

function fromPrimitive_INTERVAL(value: any) {
  const buf = Buffer.from(value);
  const months = buf.readUInt32LE(0);
  const days = buf.readUInt32LE(4);
  const millis = buf.readUInt32LE(8);

  return { months, days, milliseconds: millis };
}

function fromPrimitive_DECIMAL_INT(value: any, scale: number) {
  if (!value) return value;
  return value / Math.pow(10, scale);
}

function toPrimitive_DECIMAL_32(value: any, scale: number) {
  const unscaled = value ? Math.floor(value * Math.pow(10, scale)) : value;
  return toPrimitive_INT32(unscaled);
}

function toPrimitive_DECIMAL_64(value: any, scale: number) {
  const unscaled = value ? Math.floor(value * Math.pow(10, scale)) : value;
  return toPrimitive_INT64(unscaled);
}

function fromPrimitive_DECIMAL_FIXED(value: Buffer, scale: number, length: number) {
  if (length === 8) {
    return int53.readInt64BE(value) / Math.pow(10, scale);
  } else if (length < 8) {
    const buf = Buffer.alloc(8);
    buf.fill(value[0] >= 128 ? 255 : 0);
    for (let i = 0; i < length; i++) {
      buf[8 - length + i] = value[i];
    }
    return int53.readInt64BE(buf) / Math.pow(10, scale);
  } else {
    return value;
  }
}

function toPrimitive_DECIMAL_FIXED(value: any, scale: number, length: number) {
  if (value === null || value === undefined) return value;
  if (Buffer.isBuffer(value)) return value;
  const unscaled = Math.floor(value * Math.pow(10, scale));
  if (length <= 8) {
    const buf = Buffer.alloc(8);
    int53.writeInt64BE(unscaled, buf);
    return buf.slice(8 - length);
  } else {
    const buf = Buffer.alloc(length);
    buf.fill(unscaled < 0 ? 255 : 0);
    int53.writeInt64BE(unscaled, buf, buf.length - 8);
    return buf;
  }
}

function fromPrimitive_DECIMAL_BIN(value: Buffer, scale: number, lll: number) {
  const length = value.length;
  if (length === 8) {
    return int53.readInt64BE(value) / Math.pow(10, scale);
  } else if (length < 8) {
    const buf = Buffer.alloc(8);
    buf.fill(value[0] >= 128 ? 255 : 0);
    for (let i = 0; i < length; i++) {
      buf[8 - length + i] = value[i];
    }
    return int53.readInt64BE(buf) / Math.pow(10, scale);
  } else {
    return value;
  }
}

function toPrimitive_DECIMAL_BIN(value: any, scale: number, length: number) {
  if (value === null || value === undefined) return value;
  if (Buffer.isBuffer(value)) return value;
  const unscaled = Math.floor(value * Math.pow(10, scale));
  const buf = Buffer.alloc(8);
  int53.writeInt64BE(unscaled, buf);
  const len = Math.ceil((Math.log2(Math.abs(unscaled)) + 1) / 8);
  return buf.slice(8 - len);
}
