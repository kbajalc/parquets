// @flow

import chai = require('chai');
const assert = chai.assert;
import parquet_thrift = require('../src/gen');
import parquet_util = require('../src/util');

// tslint:disable:ter-prefer-arrow-callback
describe('Thrift', function () {

  it('should correctly en/decode literal zeroes with the CompactProtocol', function () {
    const obj = new parquet_thrift.ColumnMetaData({
      type: parquet_thrift.Type.BOOLEAN,
      path_in_schema: ['test'],
      codec: parquet_thrift.CompressionCodec.UNCOMPRESSED,
      encodings: [parquet_thrift.Encoding.PLAIN],
      num_values: 0,
      total_uncompressed_size: 100,
      total_compressed_size: 100,
      data_page_offset: 0
    });

    // tslint:disable-next-line:variable-name
    const obj_bin = parquet_util.serializeThrift(obj);
    assert.equal(obj_bin.length, 25);
  });

});
