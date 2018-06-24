import chai = require('chai');
const assert = chai.assert;

const EPSILON_DEFAULT = 0.01;

export function assertArrayEqualEpsilon(a: number[], b: number[], e?: number): void {
  assert.equal(a.length, b.length);
  for (let i = 0; i < a.length; ++i) {
    assert(Math.abs(a[i] - b[i]) < (e || EPSILON_DEFAULT));
  }
}

test('Ok', () => void 0);
