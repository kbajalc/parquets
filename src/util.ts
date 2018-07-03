import fs = require('fs');
import { TBufferedTransport, TCompactProtocol, TFramedTransport } from 'thrift';
import { TODO } from './declare';
import { FileMetaData, PageHeader } from './gen/parquet_types';

export interface WriteStreamOptions {
  flags?: string;
  encoding?: string;
  fd?: number;
  mode?: number;
  autoClose?: boolean;
  start?: number;
}

/**
 * Helper function that serializes a thrift object into a buffer
 */
export function serializeThrift(obj: any): Buffer {
  const output: Buffer[] = [];

  const transport = new TBufferedTransport(null, (buf) => {
    output.push(buf);
  });

  const protocol = new TCompactProtocol(transport);
  obj.write(protocol);
  transport.flush();

  return Buffer.concat(output);
}

export function decodeThrift(obj: any, buf: Buffer, offset?: number) {
  if (!offset) {
    // tslint:disable-next-line:no-parameter-reassignment
    offset = 0;
  }

  const transport = new TFramedTransport(buf);
  transport.readPos = offset;
  const protocol = new TCompactProtocol(transport);
  obj.read(protocol);
  return transport.readPos - offset;
}

export function decodeFileMetadata(buf: Buffer, offset?: number) {
  if (!offset) {
    // tslint:disable-next-line:no-parameter-reassignment
    offset = 0;
  }

  const transport = new TFramedTransport(buf);
  transport.readPos = offset;
  const protocol = new TCompactProtocol(transport);
  const metadata = FileMetaData.read(protocol);
  return { length: transport.readPos - offset, metadata };
}

export function decodePageHeader(buf: Buffer, offset?: number) {
  if (!offset) {
    // tslint:disable-next-line:no-parameter-reassignment
    offset = 0;
  }

  const transport = new TFramedTransport(buf);
  transport.readPos = offset;
  const protocol = new TCompactProtocol(transport);
  const pageHeader = PageHeader.read(protocol);
  return { length: transport.readPos - offset, pageHeader };
}

/**
 * Get the number of bits required to store a given value
 */
export function getBitWidth(val: number): number {
  if (val === 0) {
    return 0;
    // tslint:disable-next-line:no-else-after-return
  } else {
    return Math.ceil(Math.log2(val + 1));
  }
}

/**
 * FIXME not ideal that this is linear
 */
export function getThriftEnum(klass: Object, value: number | string): string {
  for (const k in klass) {
    if (klass[k] === value) {
      return k;
    }
  }
  throw new Error('Invalid ENUM value');
}

export function fopen(filePath: string): Promise<number> {
  return new Promise((resolve, reject) => {
    fs.open(filePath, 'r', (err, fd) => {
      if (err) {
        reject(err);
      } else {
        resolve(fd);
      }
    });
  });
}

export function fstat(filePath: string): Promise<fs.Stats> {
  return new Promise((resolve, reject) => {
    fs.stat(filePath, (err, stat) => {
      if (err) {
        reject(err);
      } else {
        resolve(stat);
      }
    });
  });
}

export function fread(fd: number, position: number, length: number): Promise<Buffer> {
  const buffer = Buffer.alloc(length);

  return new Promise((resolve, reject) => {
    fs.read(fd, buffer, 0, length, position, (err, bytesRead, buf) => {
      if (err || bytesRead !== length) {
        reject(err || Error('read failed'));
      } else {
        resolve(buf);
      }
    });
  });
}

export function fclose(fd: number): Promise<void> {
  return new Promise((resolve, reject) => {
    fs.close(fd, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

export function oswrite(os: fs.WriteStream, buf: Buffer): Promise<void> {
  return new Promise((resolve, reject) => {
    os.write(buf, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

export function osclose(os: fs.WriteStream): Promise<void> {
  return new Promise((resolve, reject) => {
    (os as TODO).close((err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

export function osopen(path: string, opts: WriteStreamOptions): Promise<fs.WriteStream> {
  return new Promise((resolve, reject) => {
    const outputStream = fs.createWriteStream(path, opts);

    // tslint:disable-next-line:ter-prefer-arrow-callback
    outputStream.on('open', function (fd) {
      resolve(outputStream);
    });

    // tslint:disable-next-line:ter-prefer-arrow-callback
    outputStream.on('error', function (err) {
      reject(err);
    });
  });
}

export function fieldIndexOf(arr: any[][], elem: any[]): number {
  for (let j = 0; j < arr.length; ++j) {
    if (arr[j].length !== elem.length) {
      continue;
    }

    let m = true;
    for (let i = 0; i < elem.length; ++i) {
      if (arr[j][i] !== elem[i]) {
        m = false;
        break;
      }
    }

    if (m) {
      return j;
    }
  }

  return -1;
}
