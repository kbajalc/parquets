import { ParquetReader } from '../src';

async function example() {
  const reader = await ParquetReader.openFile('fruits.parquet');

  const cursor = reader.getCursor();
  let record = null;
  while (record = await cursor.next()) {
    console.log(record);
  }

  reader.close();
}

example();
