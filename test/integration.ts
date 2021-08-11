import 'jest';
import { promisify } from 'util';
import { ParquetCompression } from '../src';
import chai = require('chai');
import fs = require('fs');
import parquet = require('../src');
const assert = chai.assert;
const objectStream = require('object-stream');

const TEST_NUM_ROWS = 1000;
const TEST_VTIME = Date.now();

interface TestOptions {
  useDataPageV2: boolean;
  compression: ParquetCompression;
}

function mkTestSchema(opts: TestOptions) {
  return new parquet.ParquetSchema({
    name: { type: 'UTF8', compression: opts.compression },
    // quantity:   { type: 'INT64', encoding: 'RLE', typeLength: 6, optional: true, compression: opts.compression },
    // parquet-mr actually doesnt support this
    quantity: { type: 'INT64', optional: true, compression: opts.compression },
    price: { type: 'DOUBLE', compression: opts.compression },
    date: { type: 'TIMESTAMP_MICROS', compression: opts.compression },
    day: { type: 'DATE', compression: opts.compression },
    finger: {
      type: 'FIXED_LEN_BYTE_ARRAY',
      compression: opts.compression,
      typeLength: 5,
    },
    inter: { type: 'INTERVAL', compression: opts.compression },
    stock: {
      repeated: true,
      fields: {
        quantity: {
          type: 'INT64',
          repeated: true,
          compression: opts.compression,
        },
        warehouse: { type: 'UTF8', compression: opts.compression },
        opts: {
          optional: true,
          fields: {
            a: { type: 'INT32', compression: opts.compression },
            b: { type: 'INT32', optional: true, compression: opts.compression },
          },
        },
        tags: {
          optional: true,
          repeated: true,
          fields: {
            name: { type: 'UTF8', compression: opts.compression },
            val: { type: 'UTF8', compression: opts.compression },
          },
        },
      },
    },
    colour: { type: 'UTF8', repeated: true, compression: opts.compression },
    meta_json: { type: 'BSON', optional: true, compression: opts.compression },
  });
}

function mkTestRows() {
  const rows: any[] = [];

  for (let i = 0; i < TEST_NUM_ROWS; i++) {
    rows.push(
      {
        name: 'apples',
        quantity: 10,
        price: 2.6,
        day: new Date('2017-11-26'),
        date: new Date(TEST_VTIME + 1000 * i),
        finger: Buffer.from('FNORD'),
        inter: { months: 42, days: 23, milliseconds: 777 },
        stock: [
          { quantity: [10], warehouse: 'A' },
          {
            quantity: [20],
            warehouse: 'B',
            opts: { a: 1 },
            tags: [{ name: 't', val: 'v' }],
          },
        ],
        colour: ['green', 'red'],
      },
      {
        name: 'oranges',
        quantity: 20,
        price: 2.7,
        day: new Date('2017-11-26'),
        date: new Date(TEST_VTIME + 2000 * i),
        finger: Buffer.from('FNORD'),
        inter: { months: 42, days: 23, milliseconds: 777 },
        stock: [{ quantity: [50, 33], warehouse: 'X' }],
        colour: ['orange'],
      },
      {
        name: 'kiwi',
        price: 4.2,
        day: new Date('2017-11-26'),
        date: new Date(TEST_VTIME + 8000 * i),
        finger: Buffer.from('FNORD'),
        inter: { months: 42, days: 23, milliseconds: 777 },
        stock: [
          { quantity: [42], warehouse: 'f' },
          { quantity: [20], warehouse: 'x' },
        ],
        colour: ['green', 'brown'],
        meta_json: { expected_ship_date: new Date(TEST_VTIME) },
      },
      {
        name: 'banana',
        price: 3.2,
        day: new Date('2017-11-26'),
        date: new Date(TEST_VTIME + 6000 * i),
        finger: Buffer.from('FNORD'),
        inter: { months: 42, days: 23, milliseconds: 777 },
        colour: ['yellow'],
        meta_json: { shape: 'curved' },
      }
    );
  }

  return rows;
}

async function writeTestData(
  writer: parquet.ParquetWriter<unknown>,
) {
  writer.setMetadata('myuid', '420');
  writer.setMetadata('fnord', 'dronf');
  const rows = mkTestRows();
  for (const row of rows) {
    await writer.appendRow(row);
  }
  await writer.close();
}

async function writeTestFile(opts: TestOptions) {
  const schema = mkTestSchema(opts);
  const writer = await parquet.ParquetWriter.openFile(
    schema,
    'fruits.parquet',
    opts
  );
  await writeTestData(writer);
}

async function readTestFile() {
  const reader = await parquet.ParquetReader.openFile('fruits.parquet');
  await checkTestData(reader);
}

function checkTestDataMetadata(
  reader: parquet.ParquetReader<unknown> | parquet.ParquetBufferReader<unknown>
) {
  assert.equal(reader.getRowCount(), TEST_NUM_ROWS * 4);
  assert.deepEqual(reader.getMetadata(), { myuid: '420', fnord: 'dronf' });

  const schema = reader.getSchema();
  assert.equal(schema.fieldList.length, 18);
  assert(schema.fields.name);
  assert(schema.fields.stock);
  assert(schema.fields.stock.fields.quantity);
  assert(schema.fields.stock.fields.warehouse);
  assert(schema.fields.stock.fields.opts);
  assert(schema.fields.stock.fields.opts.fields.a);
  assert(schema.fields.stock.fields.opts.fields.b);
  assert(schema.fields.stock.fields.tags);
  assert(schema.fields.stock.fields.tags.fields.name);
  assert(schema.fields.stock.fields.tags.fields.val);
  assert(schema.fields.price);

  {
    const c = schema.fields.name;
    assert.equal(c.name, 'name');
    assert.equal(c.primitiveType, 'BYTE_ARRAY');
    assert.equal(c.originalType, 'UTF8');
    assert.deepEqual(c.path, ['name']);
    assert.equal(c.repetitionType, 'REQUIRED');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 0);
    assert.equal(c.dLevelMax, 0);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  }

  {
    const c = schema.fields.stock;
    assert.equal(c.name, 'stock');
    assert.equal(c.primitiveType, undefined);
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['stock']);
    assert.equal(c.repetitionType, 'REPEATED');
    assert.equal(c.encoding, undefined);
    assert.equal(c.compression, undefined);
    assert.equal(c.rLevelMax, 1);
    assert.equal(c.dLevelMax, 1);
    assert.equal(!!c.isNested, true);
    assert.equal(c.fieldCount, 4);
  }

  {
    const c = schema.fields.stock.fields.quantity;
    assert.equal(c.name, 'quantity');
    assert.equal(c.primitiveType, 'INT64');
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['stock', 'quantity']);
    assert.equal(c.repetitionType, 'REPEATED');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 2);
    assert.equal(c.dLevelMax, 2);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  }

  {
    const c = schema.fields.stock.fields.warehouse;
    assert.equal(c.name, 'warehouse');
    assert.equal(c.primitiveType, 'BYTE_ARRAY');
    assert.equal(c.originalType, 'UTF8');
    assert.deepEqual(c.path, ['stock', 'warehouse']);
    assert.equal(c.repetitionType, 'REQUIRED');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 1);
    assert.equal(c.dLevelMax, 1);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  }

  {
    const c = schema.fields.stock.fields.opts;
    assert.equal(c.name, 'opts');
    assert.equal(c.primitiveType, undefined);
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['stock', 'opts']);
    assert.equal(c.repetitionType, 'OPTIONAL');
    assert.equal(c.encoding, undefined);
    assert.equal(c.compression, undefined);
    assert.equal(c.rLevelMax, 1);
    assert.equal(c.dLevelMax, 2);
    assert.equal(!!c.isNested, true);
    assert.equal(c.fieldCount, 2);
  }

  {
    const c = schema.fields.stock.fields.opts.fields.a;
    assert.equal(c.name, 'a');
    assert.equal(c.primitiveType, 'INT32');
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['stock', 'opts', 'a']);
    assert.equal(c.repetitionType, 'REQUIRED');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 1);
    assert.equal(c.dLevelMax, 2);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  }

  {
    const c = schema.fields.stock.fields.opts.fields.b;
    assert.equal(c.name, 'b');
    assert.equal(c.primitiveType, 'INT32');
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['stock', 'opts', 'b']);
    assert.equal(c.repetitionType, 'OPTIONAL');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 1);
    assert.equal(c.dLevelMax, 3);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  }

  {
    const c = schema.fields.stock.fields.tags;
    assert.equal(c.name, 'tags');
    assert.equal(c.primitiveType, undefined);
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['stock', 'tags']);
    assert.equal(c.repetitionType, 'REPEATED');
    assert.equal(c.encoding, undefined);
    assert.equal(c.compression, undefined);
    assert.equal(c.rLevelMax, 2);
    assert.equal(c.dLevelMax, 2);
    assert.equal(!!c.isNested, true);
    assert.equal(c.fieldCount, 2);
  }

  {
    const c = schema.fields.stock.fields.tags.fields.name;
    assert.equal(c.name, 'name');
    assert.equal(c.primitiveType, 'BYTE_ARRAY');
    assert.equal(c.originalType, 'UTF8');
    assert.deepEqual(c.path, ['stock', 'tags', 'name']);
    assert.equal(c.repetitionType, 'REQUIRED');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 2);
    assert.equal(c.dLevelMax, 2);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  }

  {
    const c = schema.fields.stock.fields.tags.fields.val;
    assert.equal(c.name, 'val');
    assert.equal(c.primitiveType, 'BYTE_ARRAY');
    assert.equal(c.originalType, 'UTF8');
    assert.deepEqual(c.path, ['stock', 'tags', 'val']);
    assert.equal(c.repetitionType, 'REQUIRED');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 2);
    assert.equal(c.dLevelMax, 2);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  }

  {
    const c = schema.fields.price;
    assert.equal(c.name, 'price');
    assert.equal(c.primitiveType, 'DOUBLE');
    assert.equal(c.originalType, undefined);
    assert.deepEqual(c.path, ['price']);
    assert.equal(c.repetitionType, 'REQUIRED');
    assert.equal(c.encoding, 'PLAIN');
    assert.equal(c.compression, 'UNCOMPRESSED');
    assert.equal(c.rLevelMax, 0);
    assert.equal(c.dLevelMax, 0);
    assert.equal(!!c.isNested, false);
    assert.equal(c.fieldCount, undefined);
  }
}

async function checkTestDataAllFields(reader: parquet.ParquetReader<unknown>) {
  const cursor = reader.getCursor();
  for (const expected of mkTestRows()) {
    assert.deepEqual(await cursor.next(), expected);
  }
  assert.equal(await cursor.next(), null);
}

async function checkTestDataNameOnly(reader: parquet.ParquetReader<unknown>) {
  const cursor = reader.getCursor(['name']);
  for (const { name } of mkTestRows()) {
    assert.deepEqual(await cursor.next(), { name });
  }
  assert.equal(await cursor.next(), null);
}

async function checkTestDataNameAndQuantityOnly(
  reader: parquet.ParquetReader<unknown>
) {
  const cursor = reader.getCursor(['name', 'quantity']);
  for (const { name, quantity } of mkTestRows()) {
    assert.deepEqual(
      await cursor.next(),
      typeof quantity !== 'undefined' ? { name, quantity } : { name }
    );
  }
  assert.equal(await cursor.next(), null);
}

async function checkTestData(reader: parquet.ParquetReader<unknown>) {
  checkTestDataMetadata(reader);
  await checkTestDataAllFields(reader);
  await checkTestDataNameOnly(reader);
  await checkTestDataNameAndQuantityOnly(reader);
  await reader.close();
}

function checkTestDataFromBufferAllFields(
  reader: parquet.ParquetBufferReader<unknown>
) {
  const cursor = reader.getCursor();
  for (const expected of mkTestRows()) {
    assert.deepEqual(cursor.next(), expected);
  }
  assert.equal(cursor.next(), null);
}

function checkTestDataFromBufferNameOnly(
  reader: parquet.ParquetBufferReader<unknown>
) {
  const cursor = reader.getCursor(['name']);
  for (const { name } of mkTestRows()) {
    assert.deepEqual(cursor.next(), { name });
  }
  assert.equal(cursor.next(), null);
}

function checkTestDataFromBufferNameAndQuantityOnly(
  reader: parquet.ParquetBufferReader<unknown>
) {
  const cursor = reader.getCursor(['name', 'quantity']);
  for (const { name, quantity } of mkTestRows()) {
    assert.deepEqual(
      cursor.next(),
      typeof quantity !== 'undefined' ? { name, quantity } : { name }
    );
  }
  assert.equal(cursor.next(), null);
}

function checkTestDataFromBuffer(reader: parquet.ParquetBufferReader<unknown>) {
  checkTestDataMetadata(reader);
  checkTestDataFromBufferAllFields(reader);
  checkTestDataFromBufferNameOnly(reader);
  checkTestDataFromBufferNameAndQuantityOnly(reader);
}

// tslint:disable:ter-prefer-arrow-callback
describe('Parquet', function () {
  jest.setTimeout(90000);

  describe('with DataPageHeaderV1', function () {
    it('write a test file', function () {
      const opts: TestOptions = {
        useDataPageV2: false,
        compression: 'UNCOMPRESSED',
      };
      return writeTestFile(opts);
    });

    it('write a test file and then read it back', function () {
      const opts: TestOptions = {
        useDataPageV2: false,
        compression: 'UNCOMPRESSED',
      };
      return writeTestFile(opts).then(readTestFile);
    });

    it('write an empty test file and then read it back', async function () {
      const opts: TestOptions = {
        useDataPageV2: false,
        compression: 'UNCOMPRESSED',
      };
      const schema = mkTestSchema(opts);
      const writer = await parquet.ParquetWriter.openFile(
        schema,
        'empty.parquet',
        opts
      );
      await writer.close();
      const reader = await parquet.ParquetReader.openFile('empty.parquet');
      expect(reader.getRowCount()).toBe(0);
    });

    it('write an empty test file with empty schema and then read it back', async function () {
      const opts: TestOptions = {
        useDataPageV2: false,
        compression: 'UNCOMPRESSED',
      };
      const schema = new parquet.ParquetSchema({});
      const writer = await parquet.ParquetWriter.openFile(
        schema,
        'empty.parquet',
        opts
      );
      await writer.close();
      const reader = await parquet.ParquetReader.openFile('empty.parquet');
      expect(reader.getRowCount()).toBe(0);
    });

    it('supports reading from a buffer asynchronously', function () {
      const opts: TestOptions = {
        useDataPageV2: false,
        compression: 'UNCOMPRESSED',
      };
      return writeTestFile(opts).then(async function () {
        const data = await promisify(fs.readFile)('fruits.parquet');
        const reader = await parquet.ParquetReader.openBuffer(data);
        await checkTestData(reader);
      });
    });

    it('supports reading from a buffer synchronously', function () {
      const opts: TestOptions = {
        useDataPageV2: false,
        compression: 'UNCOMPRESSED',
      };
      return writeTestFile(opts).then(async function () {
        const data = await promisify(fs.readFile)('fruits.parquet');
        const reader = await parquet.ParquetBufferReader.openBuffer(data);
        await checkTestDataFromBuffer(reader);
      });
    });

    it('write a test file with GZIP compression and then read it back', function () {
      const opts: TestOptions = { useDataPageV2: false, compression: 'GZIP' };
      return writeTestFile(opts).then(readTestFile);
    });

    it('write a test file with SNAPPY compression and then read it back', function () {
      const opts: TestOptions = { useDataPageV2: false, compression: 'SNAPPY' };
      return writeTestFile(opts).then(readTestFile);
    });

    it('write a test file with LZO compression and then read it back', function () {
      const opts: TestOptions = { useDataPageV2: false, compression: 'LZO' };
      return writeTestFile(opts).then(readTestFile);
    });

    it('write a test file with BROTLI compression and then read it back', function () {
      const opts: TestOptions = { useDataPageV2: false, compression: 'BROTLI' };
      return writeTestFile(opts).then(readTestFile);
    });

    it('write a test file with LZ4 compression and then read it back', function () {
      const opts: TestOptions = { useDataPageV2: false, compression: 'LZ4' };
      return writeTestFile(opts).then(readTestFile);
    });
  });

  describe('with DataPageHeaderV2', function () {
    it('write a test file and then read it back', function () {
      const opts: TestOptions = {
        useDataPageV2: true,
        compression: 'UNCOMPRESSED',
      };
      return writeTestFile(opts).then(readTestFile);
    });

    it('write an empty test file and then read it back', async function () {
      const opts: TestOptions = {
        useDataPageV2: true,
        compression: 'UNCOMPRESSED',
      };
      const schema = mkTestSchema(opts);
      const writer = await parquet.ParquetWriter.openFile(
        schema,
        'empty.parquet',
        opts
      );
      await writer.close();
      const reader = await parquet.ParquetReader.openFile('empty.parquet');
      expect(reader.getRowCount()).toBe(0);
    });

    it('write an empty test file with empty schema and then read it back', async function () {
      const opts: TestOptions = {
        useDataPageV2: true,
        compression: 'UNCOMPRESSED',
      };
      const schema = new parquet.ParquetSchema({});
      const writer = await parquet.ParquetWriter.openFile(
        schema,
        'empty.parquet',
        opts
      );
      await writer.close();
      const reader = await parquet.ParquetReader.openFile('empty.parquet');
      expect(reader.getRowCount()).toBe(0);
    });
    it('write a test file with GZIP compression and then read it back', function () {
      const opts: TestOptions = { useDataPageV2: true, compression: 'GZIP' };
      return writeTestFile(opts).then(readTestFile);
    });

    it('write a test file with SNAPPY compression and then read it back', function () {
      const opts: TestOptions = { useDataPageV2: true, compression: 'SNAPPY' };
      return writeTestFile(opts).then(readTestFile);
    });

    it('write a test file with LZO compression and then read it back', function () {
      const opts: TestOptions = { useDataPageV2: true, compression: 'LZO' };
      return writeTestFile(opts).then(readTestFile);
    });

    it('write a test file with BROTLI compression and then read it back', function () {
      const opts: TestOptions = { useDataPageV2: true, compression: 'BROTLI' };
      return writeTestFile(opts).then(readTestFile);
    });

    it('write a test file with LZ4 compression and then read it back', function () {
      const opts: TestOptions = { useDataPageV2: true, compression: 'LZ4' };
      return writeTestFile(opts).then(readTestFile);
    });
  });

  describe('using the Stream/Transform API', function () {
    it('write a test file', async function () {
      const opts: any = { useDataPageV2: true, compression: 'GZIP' };
      const schema = mkTestSchema(opts);
      const transform = new parquet.ParquetTransformer(schema, opts);
      transform.writer.setMetadata('myuid', '420');
      transform.writer.setMetadata('fnord', 'dronf');
      const ostream = fs.createWriteStream('fruits_stream.parquet');
      const istream = objectStream.fromArray(mkTestRows());
      istream.pipe(transform).pipe(ostream);
      await promisify(ostream.on.bind(ostream, 'finish'))();
      await readTestFile();
    });
  });

  if ('asyncIterator' in Symbol) {
    describe('using the AsyncIterable API', function () {
      it('allows iteration on a cursor using for-await-of', async function () {
        await writeTestFile({ useDataPageV2: true, compression: 'GZIP' });
        const reader = await parquet.ParquetReader.openFile<{ name: string }>(
          'fruits.parquet'
        );

        async function checkTestDataUsingForAwaitOf(
          cursor: AsyncIterable<{ name: string }>
        ) {
          const names: Set<string> = new Set();
          let rowCount = 0;
          for await (const row of cursor) {
            names.add(row.name);
            rowCount++;
          }
          assert.equal(rowCount, TEST_NUM_ROWS * names.size);
          assert.deepEqual(
            names,
            new Set(['apples', 'oranges', 'kiwi', 'banana'])
          );
        }

        // Works with reader (will return all columns)
        await checkTestDataUsingForAwaitOf(reader);

        // Works with a cursor
        const cursor = reader.getCursor(['name']);
        await checkTestDataUsingForAwaitOf(cursor);
      });
    });
  }
});
