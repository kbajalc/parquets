'use strict';
import fs = require('fs');
import { TBufferedTransport, TCompactProtocol, TFramedTransport } from 'thrift';

/**
 * Helper function that serializes a thrift object into a buffer
 */
export function serializeThrift(obj: Buffer) {
  let output = []

  let transport = new TBufferedTransport(null, (buf) => {
    output.push(buf)
  })

  let protocol = new TCompactProtocol(transport)
  obj.write(protocol);
  transport.flush();

  return Buffer.concat(output)
}

export function decodeThrift(obj: any, buf: Buffer, offset?: number) {
  if (!offset) {
    offset = 0;
  }

  var transport = new TFramedTransport(buf);
  transport.readPos = offset;
  var protocol = new TCompactProtocol(transport);
  obj.read(protocol);
  return transport.readPos - offset;
}

/**
 * Get the number of bits required to store a given value
 */
export function getBitWidth(val: number) {
  if (val === 0) {
    return 0;
  } else {
    return Math.ceil(Math.log2(val + 1));
  }
}

/**
 * FIXME not ideal that this is linear
 */
export function getThriftEnum(klass: Object, value: number | string): any {
  for (let k in klass) {
    if (klass[k] === value) {
      return k;
    }
  }

  throw 'Invalid ENUM value';
}

export function fopen(filePath: string) {
  return new Promise((resolve, reject) => {
    fs.open(filePath, 'r', (err, fd) => {
      if (err) {
        reject(err);
      } else {
        resolve(fd);
      }
    })
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
    })
  });
}

export function fread(fd: number, position, length) {
  let buffer = Buffer.alloc(length);

  return new Promise((resolve, reject) => {
    fs.read(fd, buffer, 0, length, position, (err, bytesRead, buf) => {
      if (err || bytesRead != length) {
        reject(err || Error('read failed'));
      } else {
        resolve(buf);
      }
    });
  });
}

export function fclose(fd: number) {
  return new Promise((resolve, reject) => {
    fs.close(fd, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve(err);
      }
    });
  });
}

export function oswrite(os, buf: Buffer) {
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

export function osclose(os) {
  return new Promise((resolve, reject) => {
    os.close((err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

export function osopen(path, opts) {
  return new Promise((resolve, reject) => {
    let outputStream = fs.createWriteStream(path, opts);

    outputStream.on('open', function (fd) {
      resolve(outputStream);
    });

    outputStream.on('error', function (err) {
      reject(err);
    });
  });
}

export function fieldIndexOf(arr, elem) {
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

