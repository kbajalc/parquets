import { ParquetCodec } from './codec';
import { ParquetCompression } from './compression';
import { ParquetBuffer, ParquetRecord } from './shred';
import { OriginalType, ParquetType, PrimitiveType } from './types';

export type RepetitionType = 'REQUIRED' | 'OPTIONAL' | 'REPEATED';

export interface SchemaDefinition {
  [string: string]: FieldDefinition;
}

export interface FieldDefinition {
  type?: ParquetType;
  typeLength?: number;
  encoding?: ParquetCodec;
  compression?: ParquetCompression;
  optional?: boolean;
  repeated?: boolean;
  scale?: number;
  precision?: number;
  fieldId?: number;
  fields?: SchemaDefinition;
  list?: {
    elementName?: string;
    element: FieldDefinition;
  };
  map?: {
    key: FieldDefinition;
    value: FieldDefinition;
  };
}

export interface ParquetField {
  name: string;
  path: string[];
  key: string;
  primitiveType?: PrimitiveType;
  originalType?: OriginalType;
  repetitionType: RepetitionType;
  typeLength?: number;
  scale?: number;
  precision?: number;
  fieldId?: number;
  encoding?: ParquetCodec;
  compression?: ParquetCompression;
  rLevelMax: number;
  dLevelMax: number;
  isNested?: boolean;
  fieldCount?: number;
  fields?: Record<string, ParquetField>;
}

/**
 * A parquet file schema
 */
export class ParquetSchema {
  public schema: Record<string, FieldDefinition>;
  public fields: Record<string, ParquetField>;
  public fieldList: ParquetField[];

  /**
   * Create a new schema from a JSON schema definition
   */
  constructor(schema: SchemaDefinition) {
    this.schema = schema;
    this.fields = buildFields(schema, 0, 0, []);
    this.fieldList = listFields(this.fields);
  }

  /**
   * Retrieve a field definition
   */
  findField(path: string): ParquetField;
  findField(path: string[]): ParquetField;
  findField(path: any): ParquetField {
    if (path.constructor !== Array) {
      // tslint:disable-next-line:no-parameter-reassignment
      path = path.split(',');
    } else {
      // tslint:disable-next-line:no-parameter-reassignment
      path = path.slice(0); // clone array
    }

    let n = this.fields;
    for (; path.length > 1; path.shift()) {
      n = n[path[0]].fields;
    }

    return n[path[0]];
  }

  /**
   * Retrieve a field definition and all the field's ancestors
   */
  findFieldBranch(path: string): ParquetField[];
  findFieldBranch(path: string[]): ParquetField[];
  findFieldBranch(path: any): any[] {
    if (path.constructor !== Array) {
      // tslint:disable-next-line:no-parameter-reassignment
      path = path.split(',');
    }
    const branch = [];
    let n = this.fields;
    for (; path.length > 0; path.shift()) {
      branch.push(n[path[0]]);
      if (path.length > 1) {
        n = n[path[0]].fields;
      }
    }
    return branch;
  }

  shredRecord(record: any, buffer: ParquetBuffer): void {
    ParquetRecord.shred(this, record, buffer);
  }

  materializeRecords(buffer: ParquetBuffer): any[] {
    return ParquetRecord.materialize(this, buffer);
  }

  compress(type: ParquetCompression): this {
    setCompress(this.schema, type);
    setCompress(this.fields, type);
    return this;
  }

  buffer(): ParquetBuffer {
    return ParquetBuffer.create(this);
  }
}

function setCompress(schema: any, type: ParquetCompression) {
  for (const name in schema) {
    const node = schema[name];
    if (node.fields) {
      setCompress(node.fields, type);
    } else {
      node.compression = type;
    }
  }
}

function buildFields(
  schema: SchemaDefinition,
  rLevelParentMax: number,
  dLevelParentMax: number,
  path: string[]
): Record<string, ParquetField> {
  const fieldList: Record<string, ParquetField> = {};

  for (const name in schema) {
    const opts = schema[name];

    if (opts.list) {
      opts.type = 'LIST';
      opts.fields = {
        list: {
          repeated: true,
          fields: {
            [opts.list.elementName || 'element']: opts.list.element
          }
        }
      };
    }

    if (opts.map) {
      opts.type = 'MAP';
      delete opts.map.key.optional;
      opts.fields = {
        map: {
          type: 'MAP_KEY_VALUE',
          repeated: true,
          fields: {
            key: opts.map.key,
            value: opts.map.value
          }
        }
      };
    }

    /* field repetition type */
    const required = !opts.optional;
    const repeated = !!opts.repeated;
    let rLevelMax = rLevelParentMax;
    let dLevelMax = dLevelParentMax;

    let repetitionType: RepetitionType = 'REQUIRED';
    if (!required) {
      repetitionType = 'OPTIONAL';
      dLevelMax++;
    }
    if (repeated) {
      repetitionType = 'REPEATED';
      rLevelMax++;
      if (required) dLevelMax++;
    }

    const typeDef = ParquetType.get(opts.type);

    /* nested field */
    if (opts.fields) {
      const cpath = path.concat([name]);
      fieldList[name] = {
        name,
        originalType: typeDef ? typeDef.originalType : null,
        path: cpath,
        key: cpath.join(),
        repetitionType,
        rLevelMax,
        dLevelMax,
        isNested: true,
        fieldCount: Object.keys(opts.fields).length,
        fields: buildFields(
          opts.fields,
          rLevelMax,
          dLevelMax,
          cpath
        )
      };
      continue;
    }

    if (!typeDef) {
      throw new Error(`invalid parquet type: ${opts.type}`);
    }

    opts.encoding = opts.encoding || 'PLAIN';
    if (!ParquetCodec.is(opts.encoding)) {
      throw new Error(`unsupported parquet encoding: ${opts.encoding}`);
    }

    opts.compression = opts.compression || 'UNCOMPRESSED';
    if (!ParquetCompression.is(opts.compression)) {
      throw new Error(`unsupported compression method: ${opts.compression}`);
    }

    let precision = opts.precision;
    let scale = opts.scale;
    let typeLength = opts.typeLength;
    if (typeDef.originalType === 'DECIMAL') {
      scale = scale || 0;
      switch (typeDef.primitiveType) {
        case 'INT32':
          precision = opts.precision === undefined ? ParquetType.MAX_PRECISION_INT32 : opts.precision;
          if (precision > ParquetType.MAX_PRECISION_INT32) {
            throw new TypeError('invalid precision digits for INT32: ' + precision);
          }
          break;
        case 'INT64':
          precision = opts.precision === undefined ? ParquetType.MAX_PRECISION_INT64 : opts.precision;
          if (precision > ParquetType.MAX_PRECISION_INT64) {
            throw new TypeError('invalid precision digits for INT64: ' + precision);
          }
          break;
        case 'BYTE_ARRAY':
          precision = opts.precision === undefined ? ParquetType.MAX_PRECISION_INT64 : opts.precision;
          break;
        case 'FIXED_LEN_BYTE_ARRAY':
          if (!typeLength && !precision) {
            precision = ParquetType.MAX_PRECISION_INT64;
            typeLength = 8;
          } else if (!typeLength) {
            typeLength = ParquetType.precisionBytes(precision);
          } else if (!precision) {
            precision = ParquetType.maxPrecision(typeLength);
          } else if (precision > ParquetType.maxPrecision(typeLength)) {
            throw new TypeError('invalid precision digits for BINARY: ' + precision);
          }
          break;
        default: throw new TypeError('unsupport DECIMAL primitive type: ' + typeDef.primitiveType);
      }
      if (scale > precision) {
        throw new TypeError('invalid scale: ' + opts.scale);
      }
    }

    /* add to schema */
    const cpath = path.concat([name]);
    fieldList[name] = {
      name,
      primitiveType: typeDef.primitiveType,
      originalType: typeDef.originalType,
      path: cpath,
      key: cpath.join(),
      repetitionType,
      scale,
      precision,
      fieldId: opts.fieldId,
      encoding: opts.encoding,
      compression: opts.compression,
      typeLength: typeLength || typeDef.typeLength,
      rLevelMax,
      dLevelMax
    };
  }
  return fieldList;
}

function listFields(fields: Record<string, ParquetField>): ParquetField[] {
  let list: ParquetField[] = [];
  for (const k in fields) {
    list.push(fields[k]);
    if (fields[k].isNested) {
      list = list.concat(listFields(fields[k].fields));
    }
  }
  return list;
}
