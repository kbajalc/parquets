import chai = require('chai');
const assert = chai.assert;
import parquet = require('../src');
import { ParquetBuffer } from '../src/shred';

// tslint:disable:ter-prefer-arrow-callback
describe('ParquetShredder', function () {
  it('should shred Dremel example', function () {
    const schema = new parquet.ParquetSchema({
      DocId: { type: 'INT64' },
      Links: {
        optional: true,
        fields: {
          Backward: {
            repeated: true,
            type: 'INT64'
          },
          Forward: {
            repeated: true,
            type: 'INT64'
          }
        }
      },
      Name: {
        repeated: true,
        fields: {
          Language: {
            repeated: true,
            fields: {
              Code: { type: 'UTF8' },
              Country: { type: 'UTF8', optional: true }
            }
          },
          Url: { type: 'UTF8', optional: true }
        }
      }
    });

    const r1 = {
      DocId: 10,
      Links: {
        Forward: [20, 40, 60]
      },
      Name: [
        {
          Language: [
            { Code: 'en-us', Country: 'us' },
            { Code: 'en' }
          ],
          Url: 'http://A'
        },
        {
          Url: 'http://B'
        },
        {
          Language: [
            { Code: 'en-gb', Country: 'gb' }
          ]
        }
      ]
    };

    const r2 = {
      DocId: 20,
      Links: {
        Backward: [10, 30],
        Forward: [80]
      },
      Name: [
        {
          Url: 'http://C'
        }
      ]
    };

    const buffer: ParquetBuffer = {};
    schema.shredRecord(r1, buffer);
    schema.shredRecord(r2, buffer);

    assert.equal(buffer.rowCount, 2);
    {
      const c = buffer.columnData[['DocId'].join()];
      assert.deepEqual(c.rlevels, [0, 0]);
      assert.deepEqual(c.dlevels, [0, 0]);
      assert.deepEqual(c.values, [10, 20]);
    }
    {
      const c = buffer.columnData[['Links', 'Forward'].join()];
      assert.deepEqual(c.rlevels, [0, 1, 1, 0]);
      assert.deepEqual(c.dlevels, [2, 2, 2, 2]);
      assert.deepEqual(c.values, [20, 40, 60, 80]);
    }
    {
      const c = buffer.columnData[['Links', 'Backward'].join()];
      assert.deepEqual(c.rlevels, [0, 0, 1]);
      assert.deepEqual(c.dlevels, [1, 2, 2]);
      assert.deepEqual(c.values, [10, 30]);
    }
    {
      const c = buffer.columnData[['Name', 'Url'].join()];
      assert.deepEqual(c.rlevels, [0, 1, 1, 0]);
      assert.deepEqual(c.dlevels, [2, 2, 1, 2]);
      assert.deepEqual(c.values.map(v => v.toString()), ['http://A', 'http://B', 'http://C']);
    }
    {
      const c = buffer.columnData[['Name', 'Language', 'Code'].join()];
      assert.deepEqual(c.rlevels, [0, 2, 1, 1, 0]);
      assert.deepEqual(c.dlevels, [2, 2, 1, 2, 1]);
      assert.deepEqual(c.values.map(v => v.toString()), ['en-us', 'en', 'en-gb']);
    }
    {
      const c = buffer.columnData[['Name', 'Language', 'Country'].join()];
      assert.deepEqual(c.rlevels, [0, 2, 1, 1, 0]);
      assert.deepEqual(c.dlevels, [3, 2, 1, 3, 1]);
      assert.deepEqual(c.values.map(v => v.toString()), ['us', 'gb']);
    }

    const records = schema.materializeRecords(buffer);
    assert.deepEqual(records[0], r1);
    assert.deepEqual(records[1], r2);
  });

  it('should shred a optional nested record with blank optional value', function () {
    const schema = new parquet.ParquetSchema({
      fruit: {
        optional: true,
        fields: {
          color: { type: 'UTF8', repeated: true },
          type: { type: 'UTF8', optional: true }
        }
      }
    });

    const buffer: ParquetBuffer = {};
    schema.shredRecord({}, buffer);
    schema.shredRecord({ fruit: {} }, buffer);
    schema.shredRecord({ fruit: { color: [] } }, buffer);
    schema.shredRecord({ fruit: { color: ['red', 'blue'], type: 'x' } }, buffer);

    const records = schema.materializeRecords(buffer);
    assert.deepEqual(records[0], {});
    assert.deepEqual(records[1], { fruit: {} });
    assert.deepEqual(records[2], { fruit: {} });
    assert.deepEqual(records[3], { fruit: { color: ['red', 'blue'], type: 'x' } });
  });

  it('should shred ENUM', function () {
    const schema = new parquet.ParquetSchema({
      enum: {
        type: 'ENUM',
        optional: true
      }
    });

    const buffer: ParquetBuffer = {};
    schema.shredRecord({}, buffer);
    schema.shredRecord({ enum: null }, buffer);
    schema.shredRecord({ enum: '' }, buffer);
    schema.shredRecord({ enum: 'TEST' }, buffer);

    const records = schema.materializeRecords(buffer);
    assert.deepEqual(records[0], {});
    assert.deepEqual(records[1], {});
    assert.deepEqual(records[2], { enum: '' });
    assert.deepEqual(records[3], { enum: 'TEST' });
  });

  it('should shred LIST', function () {
    const schema = new parquet.ParquetSchema({
      str: {
        optional: true,
        list: { type: 'UTF8' }
      },
      num: {
        optional: true,
        list: { type: 'FLOAT', optional: true }
      }
    });

    const buffer: ParquetBuffer = {};
    schema.shredRecord({}, buffer);
    schema.shredRecord({ str: [], num: null }, buffer);
    schema.shredRecord({ str: ['a'], num: [1] }, buffer);
    schema.shredRecord({ str: ['a', 'b', 'c'], num: [1, null, 2] }, buffer);

    {
      const records = schema.materializeRecords(buffer);
      assert.deepEqual(records[0], {});
      assert.deepEqual(records[1], { str: {} });
      assert.deepEqual(records[2], {
        str: { list: [{ element: 'a' }] },
        num: { list: [{ element: 1 }] }
      });
      assert.deepEqual(records[3], {
        str: { list: [{ element: 'a' }, { element: 'b' }, { element: 'c' }] },
        num: { list: [{ element: 1 }, {}, { element: 2 }] }
      });
    }

    {
      const records = schema.materializeRecords(buffer, true);
      assert.deepEqual(records[0], {});
      assert.deepEqual(records[1], { str: [] });
      assert.deepEqual(records[2], { str: ['a'], num: [1] });
      assert.deepEqual(records[3], { str: ['a', 'b', 'c'], num: [1, undefined, 2] });
    }
  });

  it('should shred LIST of LIST', function () {
    const schema = new parquet.ParquetSchema({
      matrix: {
        optional: true,
        list: {
          optional: true,
          list: { type: 'INT32', optional: true }
        }
      }
    });

    const buffer: ParquetBuffer = {};
    schema.shredRecord({}, buffer);
    schema.shredRecord({ matrix: [] }, buffer);
    schema.shredRecord({ matrix: [[1, 2], [3, 4]] }, buffer);
    schema.shredRecord({ matrix: [[1, 2], null, [3, 4, null, 5]] }, buffer);

    const records = schema.materializeRecords(buffer, true);
    assert.deepEqual(records[0], {});
    assert.deepEqual(records[1], { matrix: [] });
    assert.deepEqual(records[2], { matrix: [[1, 2], [3, 4]] });
    assert.deepEqual(records[3], { matrix: [[1, 2], undefined, [3, 4, undefined, 5]] });
  });

  it('should shred MAP', function () {
    const schema = new parquet.ParquetSchema({
      test: {
        optional: true,
        map: {
          key: { type: 'UTF8' },
          value: {
            optional: true,
            list: {
              type: 'INT32',
              optional: true
            }
          }
        }
      }
    });

    const buffer: ParquetBuffer = {};
    schema.shredRecord({}, buffer);
    schema.shredRecord({ test: { a: null, b: [] } }, buffer);
    schema.shredRecord({ test: { a: [1], b: [1, null, 3] } }, buffer);

    {
      const records = schema.materializeRecords(buffer);
      assert.deepEqual(records[0], {});
      assert.deepEqual(records[1], {
        test: {
          key_value: [
            { key: 'a' },
            { key: 'b', value: {} }
          ]
        }
      });
      assert.deepEqual(records[2], {
        test: {
          key_value: [
            {
              key: 'a', value: {
                list: [
                  { element: 1 }
                ]
              }
            },
            {
              key: 'b', value: {
                list: [
                  { element: 1 },
                  {},
                  { element: 3 }
                ]
              }
            }
          ]
        }
      });
    }

    {
      const records = schema.materializeRecords(buffer, true);
      assert.deepEqual(records[0], {});
      assert.deepEqual(records[1], {
        test: new Map([
          ['a', undefined],
          ['b', []]
        ])
      });
      assert.deepEqual(records[2], {
        test: new Map([
          ['a', [1]],
          ['b', [1, undefined, 3]]
        ])
      });
    }
  });
});
