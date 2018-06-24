// @flow

import chai = require('chai');
const assert = chai.assert;
import parquet_thrift = require('../src/gen/parquet_types');
import parquet_util = require('../src/util');

// tslint:disable:ter-prefer-arrow-callback
describe('Thrift', function () {

  it('should correctly en/decode literal zeroes with the CompactProtocol', function () {
    const obj = new parquet_thrift.ColumnMetaData();
    obj.num_values = 0;

    // tslint:disable-next-line:variable-name
    const obj_bin = parquet_util.serializeThrift(obj);
    assert.equal(obj_bin.length, 3);
  });

});
