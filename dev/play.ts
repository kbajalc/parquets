import { SchemaDefinition } from '../src/declare';
import { ParquetReader } from '../src/reader';
import { ParquetSchema } from '../src/schema';
import { ParquetWriter } from '../src/writer';
main();

async function main() {
  const reader = await ParquetReader.openFile<any>('nation.parquet');
  const schema = reader.getSchema();
  console.dir(schema);
  const ns: SchemaDefinition = JSON.parse(JSON.stringify(schema.schema));
  ns.N_COMMENT.compression = 'SNAPPY';
  const writer = await ParquetWriter.openFile<any>(new ParquetSchema(ns), 'nation2.parquet');
  const cursor = reader.getCursor();
  let rec: any;
  while (rec = await cursor.next()) {
    console.dir(rec);
    rec.N_REGIONKEY += 100;
    writer.appendRow(rec);
  }
  await writer.close();
  const reader2 = await ParquetReader.openFile<any>('nation2.parquet');
  const cursor2 = reader2.getCursor();
  while (rec = await cursor2.next()) {
    console.dir(rec);
  }
}
