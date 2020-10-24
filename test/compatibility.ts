import { readdirSync, readFileSync } from 'fs';
import * as path from 'path';
import parquet = require('../src');

// Convert objects to match the base64 encoding of binary fields
// used by parquet-tools
const base64Buffers = (obj: any) => {
  const res = { ...obj };
  for (const [k, v] of Object.entries(obj)) {
    if (Buffer.isBuffer(v)) {
      res[k] = v.toString('base64');
    }
  }
  return res;
};

describe('compatibility', () => {
  for (const f of readdirSync(path.resolve(__dirname, 'files')).filter(f => /\.parquet$/.test(f))) {
    const parquetPath = path.resolve(__dirname, 'files', f);
    const jsonPath = path.resolve(__dirname, 'files', f + '.json');
    test(f, async () => {
      const rows = [];
      const reader = await parquet.ParquetReader.openFile(parquetPath);
      const cursor = await reader.getCursor();
      let row;
      while ((row = await cursor.next()) !== null) {
        rows.push(row);
      }
      const rowsJson = rows.map(r => JSON.stringify(base64Buffers(r)) + '\n').join('');
      const expectedJson = readFileSync(jsonPath).toString();
      expect(rowsJson).toBe(expectedJson);
    });
  }
});

