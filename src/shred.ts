import { ParquetField, ParquetSchema } from './schema';
import { ParquetType } from './types';
import * as Util from './util';

export interface ParquetRecord {
  [key: string]: any;
}

export namespace ParquetRecord {
  export const shred = shredRecord;
  export const materialize = materializeRecords;
}

export interface ParquetData {
  dlevels: number[];
  rlevels: number[];
  values: any[];
  count: number;
}

export interface ParquetBuffer {
  rowCount?: number;
  columnData?: Record<string, ParquetData>;
}

export namespace ParquetBuffer {
  export function create(schema: ParquetSchema): ParquetBuffer {
    const columnData: Record<string, ParquetData> = {};
    for (const field of schema.fieldList) {
      columnData[field.key] = {
        dlevels: [],
        rlevels: [],
        values: [],
        count: 0
      };
    }
    return { rowCount: 0, columnData };
  }
}

const OBJ_TAG = Symbol('object');
const MAP_TAG = Symbol('map');
const LIST_TAG = Symbol('list');

/**
 * 'Shred' a record into a list of <value, repetition_level, definition_level>
 * tuples per column using the Google Dremel Algorithm..
 *
 * The buffer argument must point to an object into which the shredded record
 * will be returned. You may re-use the buffer for repeated calls to this function
 * to append to an existing buffer, as long as the schema is unchanged.
 *
 * The format in which the shredded records will be stored in the buffer is as
 * follows:
 *
 *   buffer = {
 *     columnData: [
 *       'my_col': {
 *          dlevels: [d1, d2, .. dN],
 *          rlevels: [r1, r2, .. rN],
 *          values: [v1, v2, .. vN],
 *        }, ...
 *      ],
 *      rowCount: X,
 *   }
 */
function shredRecord(schema: ParquetSchema, record: any, buffer: ParquetBuffer): void {
  /* shred the record, this may raise an exception */
  const data = ParquetBuffer.create(schema).columnData;

  shredRecordFields(schema.fields, record, data, 0, 0);

  /* if no error during shredding, add the shredded record to the buffer */
  if (!('columnData' in buffer) || !('rowCount' in buffer)) {
    buffer.rowCount = 1;
    buffer.columnData = data;
    return;
  }
  buffer.rowCount += 1;
  for (const field of schema.fieldList) {
    Util.push(buffer.columnData[field.key].rlevels, data[field.key].rlevels);
    Util.push(buffer.columnData[field.key].dlevels, data[field.key].dlevels);
    Util.push(buffer.columnData[field.key].values, data[field.key].values);
    buffer.columnData[field.key].count += data[field.key].count;
  }
}

function shredRecordFields(
  fields: Record<string, ParquetField>,
  record: any,
  data: Record<string, ParquetData>,
  rLevel: number,
  dLevel: number
) {
  for (const name in fields) {
    const field = fields[name];
    const val = record && record[name];
    if (val && field.originalType === 'MAP') {
      if (val instanceof Map) {
        const map: any[] = [];
        val.forEach((value, key) => map.push({ key, value }));
        // tslint:disable-next-line: no-parameter-reassignment
        record = { ...record, [name]: { map } };
      } else {
        const keys = Object.keys(val);
        if (keys.length > 1 || keys[0] !== 'map') {
          const map: any[] = [];
          keys.forEach(key => map.push({ key, value: val[key] }));
          // tslint:disable-next-line: no-parameter-reassignment
          record = { ...record, [name]: { map } };
        }
      }
    }

    if (field.originalType === 'LIST' && val instanceof Array) {
      const element = Object.keys(field.fields.list.fields)[0];
      const list = val.map(v => ({ [element]: v }));
      // tslint:disable-next-line: no-parameter-reassignment
      record = { ...record, [name]: { list } };
    }

    // fetch values
    let values = [];
    if (record && (field.name in record) && record[field.name] !== undefined && record[field.name] !== null) {
      if (record[field.name].constructor === Array) {
        values = record[field.name];
      } else {
        values.push(record[field.name]);
      }
    }
    // check values
    if (values.length === 0 && !!record && field.repetitionType === 'REQUIRED') {
      throw new Error(`missing required field: ${field.name}`);
    }
    if (values.length > 1 && field.repetitionType !== 'REPEATED') {
      throw new Error(`too many values for field: ${field.name}`);
    }

    // push null
    if (values.length === 0) {
      if (field.isNested) {
        shredRecordFields(
          field.fields,
          null,
          data,
          rLevel,
          dLevel);
      } else {
        data[field.key].count += 1;
        data[field.key].rlevels.push(rLevel);
        data[field.key].dlevels.push(dLevel);
      }
      continue;
    }

    // push values
    for (let i = 0; i < values.length; i++) {
      const rlvl = i === 0 ? rLevel : field.rLevelMax;
      if (field.isNested) {
        shredRecordFields(
          field.fields,
          values[i],
          data,
          rlvl,
          field.dLevelMax);
      } else {
        const value = ParquetType.toPrimitive(
          ParquetType.resolve(field.originalType, field.primitiveType),
          values[i],
          field.scale,
          field.typeLength
        );
        data[field.key].count += 1;
        data[field.key].rlevels.push(rlvl);
        data[field.key].dlevels.push(field.dLevelMax);
        data[field.key].values.push(value);
      }
    }
  }
}

/**
 * 'Materialize' a list of <value, repetition_level, definition_level>
 * tuples back to nested records (objects/arrays) using the Google Dremel
 * Algorithm..
 *
 * The buffer argument must point to an object with the following structure (i.e.
 * the same structure that is returned by shredRecords):
 *
 *   buffer = {
 *     columnData: [
 *       'my_col': {
 *          dlevels: [d1, d2, .. dN],
 *          rlevels: [r1, r2, .. rN],
 *          values: [v1, v2, .. vN],
 *        }, ...
 *      ],
 *      rowCount: X,
 *   }
 */
function materializeRecords(schema: ParquetSchema, buffer: ParquetBuffer, packed?: boolean): ParquetRecord[] {
  const need = packed && !!schema.fieldList.find(f => f.originalType === 'MAP' || f.originalType === 'LIST');
  const records: ParquetRecord[] = [];
  for (let i = 0; i < buffer.rowCount; i++) records.push(need ? { [OBJ_TAG]: true } : {});
  for (const key in buffer.columnData) {
    materializeColumn(schema, buffer, key, records, need);
  }
  return need ? convertTypes(records) : records;
}

function convertTypes(val: any) {
  if (!val) return val;
  if (val instanceof Array || val.constructor === Array) {
    return val.map((v: any) => convertTypes(v));
  } else if (typeof val !== 'object') {
    return val;
  }
  if (val[LIST_TAG] && val.list instanceof Array) {
    const element = val[LIST_TAG];
    delete val[LIST_TAG];
    return val.list.map((e: any) => convertTypes(e[element]));
  } else if (val[MAP_TAG] && val.map instanceof Array) {
    delete val[MAP_TAG];
    const map = new Map();
    val.map.forEach((e: any) => map.set(convertTypes(e.key), convertTypes(e.value)));
    return map;
  } else if (val[OBJ_TAG]) {
    delete val[OBJ_TAG];
    for (const name in val) {
      val[name] = convertTypes(val[name]);
    }
  }
  return val;
}

function materializeColumn(schema: ParquetSchema, buffer: ParquetBuffer, key: string, records: ParquetRecord[], packed?: boolean) {
  const data = buffer.columnData[key];
  if (!data.count) return;

  const field = schema.findField(key);
  const branch = schema.findFieldBranch(key);

  // tslint:disable-next-line:prefer-array-literal
  const rLevels: number[] = new Array(field.rLevelMax + 1).fill(0);
  let vIndex = 0;
  for (let i = 0; i < data.count; i++) {
    const dLevel = data.dlevels[i];
    const rLevel = data.rlevels[i];
    rLevels[rLevel]++;
    rLevels.fill(0, rLevel + 1);

    let rIndex = 0;
    let record = records[rLevels[rIndex++] - 1];

    // Internal nodes
    for (let i = 0; i < branch.length - 1; i++) {
      const step = branch[i];
      if (dLevel < step.dLevelMax) break;
      if (step.repetitionType === 'REPEATED') {
        if (!(step.name in record)) record[step.name] = [];
        const ix = rLevels[rIndex++];
        while (record[step.name].length <= ix) record[step.name].push({});
        record = record[step.name][ix];
      } else {
        record[step.name] = record[step.name] || {};
        record = record[step.name];
        if (!packed) continue;
        if (step.originalType === 'LIST') {
          (record as any)[LIST_TAG] = Object.keys(step.fields.list.fields)[0];
        } else if (step.originalType === 'MAP') {
          (record as any)[MAP_TAG] = true;
        } else if (step.primitiveType === undefined) {
          (record as any)[OBJ_TAG] = true;
        }
      }
    }

    // Leaf node
    if (dLevel === field.dLevelMax) {
      const value = ParquetType.fromPrimitive(
        ParquetType.resolve(field.originalType, field.primitiveType),
        data.values[vIndex],
        field.scale,
        field.typeLength
      );
      vIndex++;
      if (field.repetitionType === 'REPEATED') {
        if (!(field.name in record)) record[field.name] = [];
        const ix = rLevels[rIndex];
        while (record[field.name].length <= ix) record[field.name].push(null);
        record[field.name][ix] = value;
      } else {
        record[field.name] = value;
      }
    }
  }
}
