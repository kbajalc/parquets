import { ParquetBuffer, ParquetData, ParquetField, ParquetRecord } from '../src/declare';
import { ParquetSchema } from '../src/schema';
import * as Types from '../src/types';

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
 *
 */
export function shredRecord(schema: ParquetSchema, record: any, buffer: ParquetBuffer): void {
  /* shred the record, this may raise an exception */
  const recordShredded: Record<string, ParquetData> = {};
  for (const field of schema.fieldList) {
    recordShredded[field.path.join()] = {
      dlevels: [],
      rlevels: [],
      values: [],
      count: 0
    };
  }

  shredRecordInternal(schema.fields, record, recordShredded, 0, 0);

  /* if no error during shredding, add the shredded record to the buffer */
  if (!('columnData' in buffer) || !('rowCount' in buffer)) {
    buffer.rowCount = 0;
    buffer.columnData = {};

    for (const field of schema.fieldList) {
      const cd: ParquetData = {
        dlevels: [],
        rlevels: [],
        values: [],
        count: 0
      };
      buffer.columnData[field.path.join()] = cd;
    }
  }

  buffer.rowCount += 1;
  for (const field of schema.fieldList) {
    Array.prototype.push.apply(
      buffer.columnData[field.path.join()].rlevels,
      recordShredded[field.path.join()].rlevels);

    Array.prototype.push.apply(
      buffer.columnData[field.path.join()].dlevels,
      recordShredded[field.path.join()].dlevels);

    Array.prototype.push.apply(
      buffer.columnData[field.path.join()].values,
      recordShredded[field.path.join()].values);

    buffer.columnData[field.path.join()].count += recordShredded[field.path.join()].count;
  }
}

function shredRecordInternal(
  fields: Record<string, ParquetField>,
  record: any,
  data: Record<string, ParquetData>,
  rlvl: number,
  dlvl: number
) {
  for (const fieldName in fields) {
    const field = fields[fieldName];
    const fieldType = field.originalType || field.primitiveType;

    // fetch values
    let values = [];
    if (record && (fieldName in record) && record[fieldName] !== undefined && record[fieldName] !== null) {
      if (record[fieldName].constructor === Array) {
        values = record[fieldName];
      } else {
        values.push(record[fieldName]);
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
        shredRecordInternal(
          field.fields,
          null,
          data,
          rlvl,
          dlvl);
      } else {
        data[field.path.join()].rlevels.push(rlvl);
        data[field.path.join()].dlevels.push(dlvl);
        data[field.path.join()].count += 1;
      }
      continue;
    }

    // push values
    for (let i = 0; i < values.length; ++i) {
      // tslint:disable-next-line:variable-name
      const rlvl_i = i === 0 ? rlvl : field.rLevelMax;

      if (field.isNested) {
        shredRecordInternal(
          field.fields,
          values[i],
          data,
          rlvl_i,
          field.dLevelMax);
      } else {
        data[field.path.join()].values.push(Types.toPrimitive(fieldType, values[i]));
        data[field.path.join()].rlevels.push(rlvl_i);
        data[field.path.join()].dlevels.push(field.dLevelMax);
        data[field.path.join()].count += 1;
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
 *
 */
export function materializeRecords(schema: ParquetSchema, buffer: ParquetBuffer): ParquetRecord[] {
  const records: ParquetRecord[] = [];
  for (let i = 0; i < buffer.rowCount; ++i) {
    records.push({});
  }

  for (const k in buffer.columnData) {
    const field = schema.findField(k);
    const fieldBranch = schema.findFieldBranch(k);
    const values = buffer.columnData[k].values[Symbol.iterator]();

    // tslint:disable-next-line:prefer-array-literal
    const rLevels = new Array(field.rLevelMax + 1);
    rLevels.fill(0);

    for (let i = 0; i < buffer.columnData[k].count; ++i) {
      const dLevel = buffer.columnData[k].dlevels[i];
      const rLevel = buffer.columnData[k].rlevels[i];

      rLevels[rLevel]++;
      rLevels.fill(0, rLevel + 1);

      let value = null;
      if (dLevel === field.dLevelMax) {
        value = Types.fromPrimitive(
          field.originalType || field.primitiveType,
          values.next().value);
      }

      materializeRecordField(
        records[rLevels[0] - 1],
        fieldBranch,
        rLevels.slice(1),
        dLevel,
        value);
    }
  }

  return records;
}

function materializeRecordField(record: any, branch: ParquetField[], rLevels: number[], dLevel: number, value: any): void {
  const node = branch[0];

  if (dLevel < node.dLevelMax) {
    return;
  }

  if (branch.length > 1) {
    if (node.repetitionType === 'REPEATED') {
      if (!(node.name in record)) {
        record[node.name] = [];
      }

      while (record[node.name].length < rLevels[0] + 1) {
        record[node.name].push({});
      }

      materializeRecordField(
        record[node.name][rLevels[0]],
        branch.slice(1),
        rLevels.slice(1),
        dLevel,
        value);
    } else {
      record[node.name] = record[node.name] || {};

      materializeRecordField(
        record[node.name],
        branch.slice(1),
        rLevels,
        dLevel,
        value);
    }
  } else {
    if (node.repetitionType === 'REPEATED') {
      if (!(node.name in record)) {
        record[node.name] = [];
      }

      while (record[node.name].length < rLevels[0] + 1) {
        record[node.name].push(null);
      }

      record[node.name][rLevels[0]] = value;
    } else {
      record[node.name] = value;
    }
  }
}
