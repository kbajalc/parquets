import { ParquetReader } from '../lib';

async function example() {
  let reader = await ParquetReader.openFile('fruits.parquet');

  let cursor = reader.getCursor();
  let record = null;
  while (record = await cursor.next()) {
    console.log(record);
  }

  reader.close();
}

example();

