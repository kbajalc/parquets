import { ParquetEnvelopeReader, ParquetReader, ParquetSchema, ParquetWriter, ParquetWriterOptions } from '../src';

const TEST_VTIME = Date.now();
const TEST_NUM_ROWS = 10;

example();
false && example2();

async function example() {
  const schema = new ParquetSchema({
    decimal: { type: 'DECIMAL', scale: 5 },
    decimal32: { type: 'DECIMAL_32', optional: true, scale: 4 },
    decimalFix: { type: 'DECIMAL_FIXED', precision: 12, scale: 9 },
    decimalVar: { type: 'DECIMAL_BINARY', precision: 15, scale: 13 }
  });

  console.log(schema);

  const writer = await ParquetWriter.openFile(schema, './decimal.parquet');
  for (let i = 0; i < 100; i++) {
    await writer.appendRow({
      decimal: -Math.PI,
      decimal32: -Math.PI,
      decimalFix: -Math.PI,
      decimalVar: -Math.PI + 0.00000000000001
    });
  }
  await writer.close();

  const env = await ParquetEnvelopeReader.openFile('./decimal.parquet');
  await env.readHeader();
  const meta = await env.readFooter();
  console.log(meta);

  const reader = await ParquetReader.openFile('./decimal.parquet');
  console.log(reader.getSchema());
  const cursor = reader.getCursor();
  let record = null;
  while (record = await cursor.next()) {
    console.log(record);
  }
  reader.close();
  console.log('Read: OK');
}

async function example2() {

  const opts: ParquetWriterOptions = {
    useDataPageV2: false,
    // pageSize: 12
  };

  const schema = new ParquetSchema({
    name: { type: 'UTF8' },
    type: { type: 'ENUM', optional: true },
    // dec: { type: 'DECIMAL', scale: 10, precision: 2, optional: true },
    aList: {
      type: 'LIST',
      optional: true,
      fields: {
        list: {
          repeated: true,
          fields: {
            item: {
              type: 'UTF8'
            }
          }
        }
      }
    },
    bList: {
      optional: true,
      list: {
        element: { type: 'UTF8' }
      }
    },
    aMap: {
      optional: true,
      map: {
        key: { type: 'UTF8' },
        value: { type: 'UTF8' }
      }
    },
    bMap: {
      optional: true,
      map: {
        key: { type: 'UTF8' },
        value: { type: 'UTF8' }
      }
    },
    // parquet-mr actually doesnt support this
    // quantity: { type: 'INT64', encoding: 'RLE', typeLength: 6, optional: true, compression: opts.compression },
    quantity: { type: 'INT64', optional: true },
    price: { type: 'DOUBLE' },
    date: { type: 'TIMESTAMP_MICROS' },
    day: { type: 'DATE' },
    finger: { type: 'FIXED_LEN_BYTE_ARRAY', typeLength: 5 },
    inter: { type: 'INTERVAL' },
    // // TODO: Drill compatible
    stock: {
      repeated: true,
      fields: {
        quantity: { type: 'INT64', repeated: true },
        warehouse: { type: 'UTF8' },
        loc: {
          optional: true,
          fields: {
            tags: {
              optional: true,
              fields: {
                val: { type: 'UTF8' },
                xyz: { type: 'INT32' }
              }
            },
            lon: { type: 'FLOAT' },
            lat: { type: 'FLOAT' },
            zags: {
              optional: true,
              fields: {
                zal: { type: 'UTF8' },
                zyx: { type: 'INT32' }
              }
            },
          }
        }
      }
    },
    // colour: { type: 'UTF8', repeated: true, compression: opts.compression },
    // meta_json: { type: 'BSON', optional: true, compression: opts.compression },
    // compression: { type: 'UTF8', optional: true, compression: opts.compression }
  }).compress('SNAPPY');

  console.log(schema);

  const writer = await ParquetWriter.openFile(schema, 'fruits.parquet', opts);
  // const writer = ParquetWriter.openStream(schema, process.stdout, opts);
  const rows = mkTestRows(opts);
  for (const row of rows) {
    writer.appendRow(row);
  }
  await writer.close();
  console.log('Write: OK');

  const reader = await ParquetReader.openFile('fruits.parquet');
  console.log(reader.getSchema());
  const cursor = reader.getCursor(true); // ['name', 'type', ['stock', 'loc', '#']]);
  let record = null;
  while (record = await cursor.next()) {
    console.log(record);
  }
  reader.close();
  console.log('Read: OK');
}

function mkTestRows(opts?: any) {
  const rows: any[] = [];

  const map = new Map([['a', 'b'], ['c', 'd']]);

  for (let i = 0; i < TEST_NUM_ROWS; i++) {
    rows.push({
      name: 'apples',
      type: '1',
      dec: 10 / 7,
      aList: [
        'test',
        'b'
      ],
      bList: {
        list: [
          { element: 'test' },
          { element: 'b' }
        ]
      },
      aMap: map,
      bMap: {
        a: '1',
        b: '2'
      },
      quantity: 10,
      price: 2.6,
      day: new Date('2017-11-26'),
      date: new Date(TEST_VTIME + 1000 * i),
      finger: 'FNORD',
      inter: { months: 42, days: 23, milliseconds: 777 },
      stock: [
        { quantity: 10, warehouse: 'A' },
        { quantity: 20, warehouse: 'B', loc: { lon: 6, lat: 9, tags: { val: 'abc', xyz: 77 } } }
      ],
      colour: ['green', 'red', 'blue'],
      compression: opts && opts.compression
    });

    rows.push({
      name: 'oranges',
      type: 2,
      quantity: 20,
      price: 2.7,
      day: new Date('2017-11-26'),
      date: new Date(TEST_VTIME + 2000 * i),
      finger: 'FNORD',
      inter: { months: 42, days: 23, milliseconds: 777 },
      stock: {
        quantity: [50, 33],
        warehouse: 'X'
      },
      colour: ['orange']
    });

    rows.push({
      name: 'kiwi',
      price: 4.2,
      quantity: undefined,
      day: new Date('2017-11-26'),
      date: new Date(TEST_VTIME + 8000 * i),
      finger: 'FNORD',
      inter: { months: 42, days: 23, milliseconds: 777 },
      stock: [
        { quantity: 42, warehouse: 'f' },
        { quantity: 20, warehouse: 'x' }
      ],
      colour: ['green', 'brown'],
      meta_json: { expected_ship_date: new Date(TEST_VTIME) }
    });

    rows.push({
      name: 'banana',
      price: 3.2,
      day: new Date('2017-11-26'),
      date: new Date(TEST_VTIME + 6000 * i),
      finger: 'FNORD',
      inter: { months: 42, days: 23, milliseconds: 777 },
      colour: ['yellow'],
      meta_json: { shape: 'curved' }
    });
  }

  return rows;
}
