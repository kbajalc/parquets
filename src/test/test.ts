import { ParquetReader } from '../reader';
import { ParquetWriter } from '../writer';
main();

async function main() {
  const reader = await ParquetReader.openFile<any>('nation.parquet');
  const schema = reader.getSchema();
  console.dir(schema);
  const writer = await ParquetWriter.openFile<any>(schema, 'nation2.parquet');
  const cursor = reader.getCursor();
  let rec: any;
  while (rec = await cursor.next()) {
    console.dir(rec);
    writer.appendRow(rec);
  }
  await writer.close();
  const reader2 = await ParquetReader.openFile<any>('nation2.parquet');
  const cursor2 = reader2.getCursor();
  while (rec = await cursor2.next()) {
    console.dir(rec);
  }
}
