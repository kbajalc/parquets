const WORD_MASK = [0, 0xff, 0xffff, 0xffffff, 0xffffffff];

function copyBytes(fromArray: Buffer, fromPos: number, toArray: Buffer, toPos: number, length: number) {
  for (let i = 0; i < length; i++) {
    toArray[toPos + i] = fromArray[fromPos + i];
  }
}

function selfCopyBytes(array: Buffer, pos: number, offset: number, length: number) {
  for (let i = 0; i < length; i++) {
    array[pos + i] = array[pos - offset + i];
  }
}

export function readUncompressedLength(input: Buffer, varLen?: boolean) {
  let pos = 0;
  let result = 0;
  let shift = 0;
  while (shift < 32 && pos < input.length) {
    const c = input[pos];
    pos += 1;
    const val = c & 0x7f;
    if (((val << shift) >>> shift) !== val) {
      return -1;
    }
    result |= val << shift;
    if (c < 128) {
      return varLen ? pos : result;
    }
    shift += 7;
  }
  return -1;
}

export function uncompressToBuffer(input: Buffer, output: Buffer) {
  let pos = readUncompressedLength(input, true);

  const arrayLength = input.length;
  let outPos = 0;
  let c: number;
  let len: number;
  let smallLen: number;
  let offset: number;
  while (pos < arrayLength) {
    c = input[pos];
    pos += 1;
    if ((c & 0x3) === 0) {
      // Literal
      len = (c >>> 2) + 1;
      if (len > 60) {
        if (pos + 3 >= arrayLength) {
          return false;
        }
        smallLen = len - 60;
        len = input[pos] + (input[pos + 1] << 8) + (input[pos + 2] << 16) + (input[pos + 3] << 24);
        len = (len & WORD_MASK[smallLen]) + 1;
        pos += smallLen;
      }
      if (pos + len > arrayLength) {
        return false;
      }
      copyBytes(input, pos, output, outPos, len);
      pos += len;
      outPos += len;
    } else {
      switch (c & 0x3) {
        case 1:
          len = ((c >>> 2) & 0x7) + 4;
          offset = input[pos] + ((c >>> 5) << 8);
          pos += 1;
          break;
        case 2:
          if (pos + 1 >= arrayLength) {
            return false;
          }
          len = (c >>> 2) + 1;
          offset = input[pos] + (input[pos + 1] << 8);
          pos += 2;
          break;
        case 3:
          if (pos + 3 >= arrayLength) {
            return false;
          }
          len = (c >>> 2) + 1;
          offset = input[pos] + (input[pos + 1] << 8) + (input[pos + 2] << 16) + (input[pos + 3] << 24);
          pos += 4;
          break;
        default:
          break;
      }
      if (offset === 0 || offset > outPos) {
        return false;
      }
      selfCopyBytes(output, outPos, offset, len);
      outPos += len;
    }
  }
  return true;
}
