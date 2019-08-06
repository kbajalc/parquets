import { SnappyCompressor } from './compressor';
import { SnappyDecompressor } from './decompressor';

export interface ByteArray {
  readonly length: number;
  [number: number]: number;
}

function isNode() {
  if (typeof process === 'object') {
    if (typeof process.versions === 'object') {
      if (typeof process.versions.node !== 'undefined') {
        return true;
      }
    }
  }
  return false;
}

function isUint8Array(object: any) {
  return object instanceof Uint8Array && (!isNode() || !Buffer.isBuffer(object));
}

function isArrayBuffer(object: any) {
  return object instanceof ArrayBuffer;
}

function isBuffer(object: any) {
  if (!isNode()) {
    return false;
  }
  return Buffer.isBuffer(object);
}

const TYPE_ERROR_MSG = 'Argument compressed must be type of ArrayBuffer, Buffer, or Uint8Array';

export function uncompress(compressed: Buffer): Buffer;
export function uncompress(compressed: ArrayBuffer): ArrayBuffer;
export function uncompress(compressed: Uint8Array): Uint8Array;
export function uncompress(compressed: any) {
  if (!isUint8Array(compressed) && !isArrayBuffer(compressed) && !isBuffer(compressed)) {
    throw new TypeError(TYPE_ERROR_MSG);
  }
  let uint8Mode = false;
  let arrayBufferMode = false;
  let array: ByteArray;
  if (isUint8Array(compressed)) {
    uint8Mode = true;
    array = compressed;
  } else if (isArrayBuffer(array)) {
    arrayBufferMode = true;
    array = new Uint8Array(array);
  } else {
    array = compressed;
  }
  const decompressor = new SnappyDecompressor(array);
  const length = decompressor.readUncompressedLength();
  if (length === -1) {
    throw new Error('Invalid Snappy bitstream');
  }
  let uncompressed: any;
  let view: ByteArray;
  if (uint8Mode) {
    uncompressed = view = new Uint8Array(length);
  } else if (arrayBufferMode) {
    uncompressed = new ArrayBuffer(length);
    view = new Uint8Array(uncompressed);
  } else {
    uncompressed = view = Buffer.alloc(length);
  }
  if (!decompressor.uncompressToBuffer(view)) {
    throw new Error('Invalid Snappy bitstream');
  }
  return uncompressed;
}

export function compress(uncompressed: Buffer): Buffer;
export function compress(uncompressed: ArrayBuffer): ArrayBuffer;
export function compress(uncompressed: Uint8Array): Uint8Array;
export function compress(uncompressed: any) {
  if (!isUint8Array(uncompressed) && !isArrayBuffer(uncompressed) && !isBuffer(uncompressed)) {
    throw new TypeError(TYPE_ERROR_MSG);
  }
  let uint8Mode = false;
  let arrayBufferMode = false;
  let array: ByteArray;
  if (isUint8Array(uncompressed)) {
    uint8Mode = true;
    array = uncompressed;
  } else if (isArrayBuffer(array)) {
    arrayBufferMode = true;
    array = new Uint8Array(array);
  } else {
    array = uncompressed;
  }
  const compressor = new SnappyCompressor(array);
  const maxLength = compressor.maxCompressedLength();
  let compressed: any;
  let view: ByteArray;
  if (uint8Mode) {
    compressed = view = new Uint8Array(maxLength);
  } else if (arrayBufferMode) {
    compressed = new ArrayBuffer(maxLength);
    view = new Uint8Array(compressed);
  } else {
    compressed = view = Buffer.alloc(maxLength);
  }
  const length = compressor.compressToBuffer(view);
  return compressed.slice(0, length);
}
