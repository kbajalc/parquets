import { PARQUET_CODEC } from './codec';
import { PARQUET_COMPRESSION_METHODS } from './compression';
import {
  FieldDefinition,
  ParquetBuffer,
  ParquetCompression,
  ParquetField,
  ParquetRecord,
  RepetitionType,
  SchemaDefinition,
} from './declare';
import { materializeRecords, shredBuffer, shredRecord } from './shred';
import { PARQUET_LOGICAL_TYPES } from './types';

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
   *
   * If a string is provided it will be split using comma as a separator to
   * give the field path to look for.
   *
   * The code assumes the given field path is valid and a TypeError will
   * be thrown as a side effect if the field path isn't found in the schema.
   */
  findField(path: string): ParquetField;
  findField(path: string[]): ParquetField;
  findField(path: any): ParquetField {
    return (Array.isArray(path) ? path : path.split(',')).reduce(
      (field: ParquetField | ParquetSchema, segment: string) =>
        field.fields[segment],
      this
    ) as ParquetField;
  }

  /**
   * Retrieve a field definition and all the field's ancestors
   *
   * If a string is provided it will be split using comma as a separator to
   * give the field path to look for.
   *
   * The resulting array will have one ParquetField per segment of the
   * provided path.
   *
   * The code assumes the given field path is valid and a TypeError will
   * be thrown as a side effect if the field path isn't found in the schema.
   */
  findFieldBranch(path: string): ParquetField[];
  findFieldBranch(path: string[]): ParquetField[];
  findFieldBranch(path: any): any[] {
    const branch = [];
    // tslint:disable-next-line:no-this-assignment
    let field: ParquetField | ParquetSchema = this;
    for (const segment of Array.isArray(path) ? path : path.split(',')) {
      field = field.fields[segment];
      branch.push(field);
    }
    return branch;
  }

  shredRecord(record: ParquetRecord, buffer: ParquetBuffer): void {
    shredRecord(this, record, buffer);
  }

  materializeRecords(buffer: ParquetBuffer): ParquetRecord[] {
    return materializeRecords(this, buffer);
  }

  compress(type: ParquetCompression): this {
    setCompress(this.schema, type);
    setCompress(this.fields, type);
    return this;
  }

  buffer(): ParquetBuffer {
    return shredBuffer(this);
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

    /* nested field */
    if (opts.fields) {
      const cpath = path.concat([name]);
      fieldList[name] = {
        name,
        path: cpath,
        key: cpath.join(),
        repetitionType,
        rLevelMax,
        dLevelMax,
        isNested: true,
        fieldCount: Object.keys(opts.fields).length,
        fields: buildFields(opts.fields, rLevelMax, dLevelMax, cpath),
      };
      continue;
    }

    const typeDef: any = PARQUET_LOGICAL_TYPES[opts.type];
    if (!typeDef) {
      throw new Error(`invalid parquet type: ${opts.type}`);
    }

    opts.encoding = opts.encoding || 'PLAIN';
    if (!(opts.encoding in PARQUET_CODEC)) {
      throw new Error(`unsupported parquet encoding: ${opts.encoding}`);
    }

    opts.compression = opts.compression || 'UNCOMPRESSED';
    if (!(opts.compression in PARQUET_COMPRESSION_METHODS)) {
      throw new Error(`unsupported compression method: ${opts.compression}`);
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
      encoding: opts.encoding,
      compression: opts.compression,
      typeLength: opts.typeLength || typeDef.typeLength,
      rLevelMax,
      dLevelMax,
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
