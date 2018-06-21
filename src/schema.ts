'use strict';
import { ParquetCodec, PARQUET_CODEC } from './codec';
import { ParquetCompression, PARQUET_COMPRESSION_METHODS } from './compression';
import { ParquetType, PARQUET_LOGICAL_TYPES } from './types';

export type RepetitionType = 'REQUIRED' | 'OPTIONAL' | 'REPEATED';

export interface SchemaDefinition {
  [string: string]: {
    type: ParquetType,
    typeLength?: number,
    encoding?: ParquetCodec,
    compression?: ParquetCompression,
    optional?: boolean,
    repeated?: boolean,
    fields?: SchemaDefinition
  }
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

/**
 * A parquet file schema
 */
export class ParquetSchema {
  public schema: SchemaDefinition;
  public fields: Record<string, FieldDefinition>;
  public fieldList: FieldDefinition[];

  /**
   * Create a new schema from a JSON schema definition
   */
  constructor(schema: SchemaDefinition) {
    this.schema = schema;
    this.fields = buildFields(schema);
    this.fieldList = listFields(this.fields);
  }

  /**
   * Retrieve a field definition
   */
  findField(path: string): FieldDefinition;
  findField(path: string[]): FieldDefinition;
  findField(path: any): FieldDefinition {
    if (path.constructor !== Array) {
      path = path.split(",");
    } else {
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
  findFieldBranch(path: string): FieldDefinition[];
  findFieldBranch(path: string[]): FieldDefinition[];
  findFieldBranch(path: any): any[] {
    if (path.constructor !== Array) {
      path = path.split(",");
    }

    let branch = [];
    let n = this.fields;
    for (; path.length > 0; path.shift()) {
      branch.push(n[path[0]]);

      if (path.length > 1) {
        n = n[path[0]].fields;
      }
    }

    return branch;
  }

};

function buildFields(schema: SchemaDefinition, rLevelParentMax?: number, dLevelParentMax?: number, path?: string[]): Record<string, FieldDefinition> {
  if (!rLevelParentMax) {
    rLevelParentMax = 0;
  }

  if (!dLevelParentMax) {
    dLevelParentMax = 0;
  }

  if (!path) {
    path = [];
  }

  let fieldList: Record<string, FieldDefinition> = {};
  for (let name in schema) {
    const opts = schema[name];

    /* field repetition type */
    const required = !opts.optional;
    const repeated = !!opts.repeated;
    let rLevelMax = rLevelParentMax;
    let dLevelMax = dLevelParentMax;

    let repetitionType: RepetitionType = 'REQUIRED';
    if (!required) {
      repetitionType = 'OPTIONAL';
      ++dLevelMax;
    }
    if (repeated) {
      repetitionType = 'REPEATED';
      ++rLevelMax;

      if (required) {
        ++dLevelMax;
      }
    }

    /* nested field */
    if (opts.fields) {
      fieldList[name] = {
        name: name,
        path: path.concat([name]),
        repetitionType: repetitionType,
        rLevelMax: rLevelMax,
        dLevelMax: dLevelMax,
        isNested: true,
        fieldCount: Object.keys(opts.fields).length,
        fields: buildFields(
          opts.fields,
          rLevelMax,
          dLevelMax,
          path.concat([name]))
      };

      continue;
    }

    /* field type */
    const typeDef: any = PARQUET_LOGICAL_TYPES[opts.type];
    if (!typeDef) {
      throw 'invalid parquet type: ' + opts.type;
    }

    /* field encoding */
    if (!opts.encoding) {
      opts.encoding = 'PLAIN';
    }

    if (!(opts.encoding in PARQUET_CODEC)) {
      throw 'unsupported parquet encoding: ' + opts.encoding;
    }

    if (!opts.compression) {
      opts.compression = 'UNCOMPRESSED';
    }

    if (!(opts.compression in PARQUET_COMPRESSION_METHODS)) {
      throw 'unsupported compression method: ' + opts.compression;
    }

    /* add to schema */
    fieldList[name] = {
      name: name,
      primitiveType: typeDef.primitiveType,
      originalType: typeDef.originalType,
      path: path.concat([name]),
      repetitionType: repetitionType,
      encoding: opts.encoding,
      compression: opts.compression,
      typeLength: opts.typeLength || typeDef.typeLength,
      rLevelMax: rLevelMax,
      dLevelMax: dLevelMax
    };
  }

  return fieldList;
}

function listFields(fields: Record<string, FieldDefinition>): FieldDefinition[] {
  let list: FieldDefinition[] = [];

  for (let k in fields) {
    list.push(fields[k]);

    if (fields[k].isNested) {
      list = list.concat(listFields(fields[k].fields));
    }
  }

  return list;
}
