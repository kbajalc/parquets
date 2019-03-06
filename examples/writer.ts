import { ParquetSchema, ParquetWriter } from '../src';

// write a new file 'fruits.parquet'
async function example() {
  const schema = new ParquetSchema({
    name: { type: 'UTF8' },
    quantity: { type: 'INT64', optional: true },
    price: { type: 'DOUBLE', compression: 'GZIP' },
    // date: { type: 'TIMESTAMP_MICROS' },
    in_stock: { type: 'BOOLEAN', compression: 'GZIP' },
    // colour: { type: 'UTF8', repeated: true },
    // meta_json: { type: 'BSON', optional: true, compression: 'GZIP' },
  });

  const schema2 = {
    name: { type: 'string', compression: 'gzip' },
    quantity: { type: 'int64', optional: true, compression: 'gzip' },
    price: { type: 'double', compression: 'gzip' },
    // date: { type: 'TIMESTAMP_MICROS' },
    in_stock: { type: 'bool', compression: 'gzip' },
    // colour: { type: 'UTF8', repeated: true },
    // meta_json: { type: 'BSON', optional: true, compression: 'GZIP' },
  };

  console.log(schema2);

  const writer = await ParquetWriter.openFile(schema, 'fruits2.parquet', { useDataPageV2: false });

  await writer.appendRow({
    name: 'apples',
    quantity: 10,
    price: 2.6,
    date: new Date(),
    in_stock: true,
    // colour: ['green', 'red']
  });

  await writer.appendRow({
    name: 'oranges',
    quantity: 20,
    price: 2.7,
    date: new Date(),
    in_stock: true,
    // colour: ['orange']
  });

  await writer.appendRow({
    name: 'kiwi',
    price: 4.2,
    date: new Date(),
    in_stock: false,
    // colour: ['green', 'brown'],
    meta_json: { expected_ship_date: new Date() }
  });

  await writer.close();
}

example();
