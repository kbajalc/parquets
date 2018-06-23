import { WriteStream } from 'fs';
import { Stream, Transform } from 'stream';
import { TODO, WriteStreamOptions } from './declare';
import { ParquetSchema } from './schema';
import parquet_shredder = require('./shred')
import parquet_util = require('./util')
import parquet_codec = require('./codec')
import parquet_compression = require('./compression')
import parquet_thrift = require('./gen/parquet_types')

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = 'PAR1';

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 1;

/**
 * Default Page and Row Group sizes
 */
const PARQUET_DEFAULT_PAGE_SIZE = 8192;
const PARQUET_DEFAULT_ROW_GROUP_SIZE = 4096;

/**
 * Repetition and Definition Level Encoding
 */
const PARQUET_RDLVL_TYPE = 'INT32';
const PARQUET_RDLVL_ENCODING = 'RLE';

/**
 * Write a parquet file to an output stream. The ParquetWriter will perform
 * buffering/batching for performance, so close() must be called after all rows
 * are written.
 */
export class ParquetWriter {

  /**
   * Convenience method to create a new buffered parquet writer that writes to
   * the specified file
   */
  static async openFile(schema: ParquetSchema, path: string, opts?: WriteStreamOptions): Promise<ParquetWriter> {
    let outputStream = await parquet_util.osopen(path, opts);
    return ParquetWriter.openStream(schema, outputStream, opts);
  }

  /**
   * Convenience method to create a new buffered parquet writer that writes to
   * the specified stream
   */
  static async openStream(schema: ParquetSchema, outputStream: WriteStream, opts?: WriteStreamOptions): Promise<ParquetWriter> {
    if (!opts) {
      opts = {};
    }

    let envelopeWriter = await ParquetEnvelopeWriter.openStream(
      schema,
      outputStream,
      opts
    );

    return new ParquetWriter(schema, envelopeWriter, opts);
  }

  public schema: ParquetSchema;
  public envelopeWriter: ParquetEnvelopeWriter;
  public rowBuffer: Record<string, TODO>;
  public rowGroupSize: number;
  public closed: boolean;
  public userMetadata: Record<string, string>;
  public writer: any;

  /**
   * Create a new buffered parquet writer for a given envelope writer
   */
  constructor(schema: ParquetSchema, envelopeWriter: ParquetEnvelopeWriter, opts: TODO) {
    this.schema = schema;
    this.envelopeWriter = envelopeWriter;
    this.rowBuffer = {};
    this.rowGroupSize = opts.rowGroupSize || PARQUET_DEFAULT_ROW_GROUP_SIZE;
    this.closed = false;
    this.userMetadata = {};

    try {
      envelopeWriter.writeHeader();
    } catch (err) {
      envelopeWriter.close();
      throw err;
    }
  }

  /**
   * Append a single row to the parquet file. Rows are buffered in memory until
   * rowGroupSize rows are in the buffer or close() is called
   */
  async appendRow(row: Record<string, any>): Promise<void> {
    if (this.closed) {
      throw 'writer was closed';
    }

    parquet_shredder.shredRecord(this.schema, row, this.rowBuffer);

    if (this.rowBuffer.rowCount >= this.rowGroupSize) {
      await this.envelopeWriter.writeRowGroup(this.rowBuffer);
      this.rowBuffer = {};
    }
  }

  /**
   * Finish writing the parquet file and commit the footer to disk. This method
   * MUST be called after you are finished adding rows. You must not call this
   * method twice on the same object or add any rows after the close() method has
   * been called
   */
  async close(callback?: () => void): Promise<void> {
    if (this.closed) {
      throw 'writer was closed';
    }

    this.closed = true;

    if (this.rowBuffer.rowCount > 0 || this.rowBuffer.rowCount >= this.rowGroupSize) {
      await this.envelopeWriter.writeRowGroup(this.rowBuffer);
      this.rowBuffer = {};
    }

    await this.envelopeWriter.writeFooter(this.userMetadata);
    await this.envelopeWriter.close();
    this.envelopeWriter = null;

    if (callback) {
      callback();
    }
  }

  /**
   * Add key<>value metadata to the file
   */
  setMetadata(key: any, value: any): void {
    this.userMetadata[key.toString()] = value.toString();
  }

  /**
   * Set the parquet row group size. This values controls the maximum number
   * of rows that are buffered in memory at any given time as well as the number
   * of rows that are co-located on disk. A higher value is generally better for
   * read-time I/O performance at the tradeoff of write-time memory usage.
   */
  setRowGroupSize(cnt: number): void {
    this.rowGroupSize = cnt;
  }

  /**
   * Set the parquet data page size. The data page size controls the maximum 
   * number of column values that are written to disk as a consecutive array
   */
  setPageSize(cnt: number): void {
    this.writer.setPageSize(cnt);
  }

}

/**
 * Create a parquet file from a schema and a number of row groups. This class
 * performs direct, unbuffered writes to the underlying output stream and is
 * intendend for advanced and internal users; the writeXXX methods must be
 * called in the correct order to produce a valid file.
 */
export class ParquetEnvelopeWriter {

  /**
   * Create a new parquet envelope writer that writes to the specified stream
   */
  static async openStream(schema: ParquetSchema, outputStream: Stream, opts): Promise<ParquetEnvelopeWriter> {
    let writeFn = parquet_util.oswrite.bind(undefined, outputStream);
    let closeFn = parquet_util.osclose.bind(undefined, outputStream);
    return new ParquetEnvelopeWriter(schema, writeFn, closeFn, 0, opts);
  }

  public schema: ParquetSchema;
  public write: (buf: Buffer) => void;
  public close: () => void;
  public offset: number;
  public rowCount: number;
  public rowGroups: any[];
  public pageSize: number;
  public useDataPageV2: boolean;

  constructor(schema: ParquetSchema, writeFn: (buf: Buffer) => void, closeFn: () => void, fileOffset: number, opts) {
    this.schema = schema;
    this.write = writeFn;
    this.close = closeFn;
    this.offset = fileOffset;
    this.rowCount = 0;
    this.rowGroups = [];
    this.pageSize = PARQUET_DEFAULT_PAGE_SIZE;
    this.useDataPageV2 = ("useDataPageV2" in opts) ? opts.useDataPageV2 : true;
  }

  writeSection(buf): void {
    this.offset += buf.length;
    return this.write(buf);
  }

  /**
   * Encode the parquet file header
   */
  writeHeader(): void {
    return this.writeSection(Buffer.from(PARQUET_MAGIC));
  }

  /**
   * Encode a parquet row group. The records object should be created using the
   * shredRecord method
   */
  writeRowGroup(records): void {
    let rgroup = encodeRowGroup(
      this.schema,
      records,
      {
        baseOffset: this.offset,
        pageSize: this.pageSize,
        useDataPageV2: this.useDataPageV2
      }
    );

    this.rowCount += records.rowCount;
    this.rowGroups.push(rgroup.metadata);
    return this.writeSection(rgroup.body);
  }

  /**
   * Write the parquet file footer
   */
  writeFooter(userMetadata) {
    if (!userMetadata) {
      userMetadata = {};
    }

    if (this.rowCount === 0) {
      throw 'cannot write parquet file with zero rows';
    }

    if (this.schema.fieldList.length === 0) {
      throw 'cannot write parquet file with zero fieldList';
    }

    return this.writeSection(
      encodeFooter(this.schema, this.rowCount, this.rowGroups, userMetadata));
  };

  /**
   * Set the parquet data page size. The data page size controls the maximum 
   * number of column values that are written to disk as a consecutive array
   */
  setPageSize(cnt) {
    this.pageSize = cnt;
  }

}

/**
 * Create a parquet transform stream
 */
export class ParquetTransformer extends Transform {

  public writer: ParquetWriter;

  constructor(schema, opts = {}) {
    super({ objectMode: true });

    let writeProxy = (function (t) {
      return function (b) {
        t.push(b);
      }
    })(this);

    this.writer = new ParquetWriter(
      schema,
      new ParquetEnvelopeWriter(schema, writeProxy, function () { }, 0, opts),
      opts);
  }

  _transform(row, encoding, callback) {
    if (row) {
      this.writer.appendRow(row).then(callback);
    } else {
      callback();
    }
  }

  _flush(callback) {
    this.writer.close(callback);
  }

}

/**
 * Encode a consecutive array of data using one of the parquet encodings
 */
function encodeValues(type, encoding, values, opts) {
  if (!(encoding in parquet_codec)) {
    throw 'invalid encoding: ' + encoding;
  }

  return parquet_codec[encoding].encodeValues(type, values, opts);
}

/**
 * Encode a parquet data page
 */
function encodeDataPage(column, valueCount, values, rlevels, dlevels) {
  /* encode values */
  let valuesBuf = encodeValues(
    column.primitiveType,
    column.encoding,
    values, {
      typeLength: column.typeLength,
      bitWidth: column.typeLength
    });

  /* encode repetition and definition levels */
  let rLevelsBuf = Buffer.alloc(0);
  if (column.rLevelMax > 0) {
    rLevelsBuf = encodeValues(
      PARQUET_RDLVL_TYPE,
      PARQUET_RDLVL_ENCODING,
      rlevels,
      { bitWidth: parquet_util.getBitWidth(column.rLevelMax) });
  }

  let dLevelsBuf = Buffer.alloc(0);
  if (column.dLevelMax > 0) {
    dLevelsBuf = encodeValues(
      PARQUET_RDLVL_TYPE,
      PARQUET_RDLVL_ENCODING,
      dlevels,
      { bitWidth: parquet_util.getBitWidth(column.dLevelMax) });
  }

  /* build page header */
  let pageBody = Buffer.concat([rLevelsBuf, dLevelsBuf, valuesBuf]);
  let pageHeader = new parquet_thrift.PageHeader({
    type: parquet_thrift.PageType.DATA_PAGE,
    uncompressed_page_size: pageBody.length,
    compressed_page_size: pageBody.length,
    data_page_header: new parquet_thrift.DataPageHeader({
      num_values: valueCount,
      encoding: parquet_thrift.Encoding[column.encoding] as any,
      definition_level_encoding:
        parquet_thrift.Encoding[PARQUET_RDLVL_ENCODING], // [PARQUET_RDLVL_ENCODING],
      repetition_level_encoding:
        parquet_thrift.Encoding[PARQUET_RDLVL_ENCODING], // [PARQUET_RDLVL_ENCODING]
    })
  });

  /* concat page header, repetition and definition levels and values */
  return Buffer.concat([parquet_util.serializeThrift(pageHeader), pageBody]);
}

/**
 * Encode a parquet data page (v2)
 */
function encodeDataPageV2(column, valueCount, rowCount, values, rlevels, dlevels) {
  /* encode values */
  let valuesBuf = encodeValues(
    column.primitiveType,
    column.encoding,
    values, {
      typeLength: column.typeLength,
      bitWidth: column.typeLength
    });

  let valuesBufCompressed = parquet_compression.deflate(
    column.compression,
    valuesBuf);

  /* encode repetition and definition levels */
  let rLevelsBuf = Buffer.alloc(0);
  if (column.rLevelMax > 0) {
    rLevelsBuf = encodeValues(
      PARQUET_RDLVL_TYPE,
      PARQUET_RDLVL_ENCODING,
      rlevels, {
        bitWidth: parquet_util.getBitWidth(column.rLevelMax),
        disableEnvelope: true
      });
  }

  let dLevelsBuf = Buffer.alloc(0);
  if (column.dLevelMax > 0) {
    dLevelsBuf = encodeValues(
      PARQUET_RDLVL_TYPE,
      PARQUET_RDLVL_ENCODING,
      dlevels, {
        bitWidth: parquet_util.getBitWidth(column.dLevelMax),
        disableEnvelope: true
      });
  }

  /* build page header */
  let pageHeader = new parquet_thrift.PageHeader({
    type: parquet_thrift.PageType.DATA_PAGE_V2,
    data_page_header_v2: new parquet_thrift.DataPageHeaderV2({
      num_values: valueCount,
      num_nulls: valueCount - values.length,
      num_rows: rowCount,
      encoding: parquet_thrift.Encoding[column.encoding] as any,
      definition_levels_byte_length: dLevelsBuf.length,
      repetition_levels_byte_length: rLevelsBuf.length,
      is_compressed: column.compression !== 'UNCOMPRESSED'
    }),
    uncompressed_page_size: rLevelsBuf.length + dLevelsBuf.length + valuesBuf.length,
    compressed_page_size: rLevelsBuf.length + dLevelsBuf.length + valuesBufCompressed.length
  });

  /* concat page header, repetition and definition levels and values */
  return Buffer.concat([
    parquet_util.serializeThrift(pageHeader),
    rLevelsBuf,
    dLevelsBuf,
    valuesBufCompressed]);
}

/**
 * Encode an array of values into a parquet column chunk
 */
function encodeColumnChunk(values, opts) {
  /* encode data page(s) */
  let pages = [];

  {
    let dataPage;
    if (opts.useDataPageV2) {
      dataPage = encodeDataPageV2(
        opts.column,
        values.count,
        opts.rowCount,
        values.values,
        values.rlevels,
        values.dlevels);
    } else {
      dataPage = encodeDataPage(
        opts.column,
        values.count,
        values.values,
        values.rlevels,
        values.dlevels);
    }

    pages.push(dataPage);
  }

  let pagesBuf = Buffer.concat(pages);

  /* prepare metadata header */
  let metadata = new parquet_thrift.ColumnMetaData({
    path_in_schema: opts.column.path,
    num_values: values.count,
    data_page_offset: opts.baseOffset,
    encodings: [],
    total_uncompressed_size: pagesBuf.length,
    total_compressed_size: pagesBuf.length,
    type: parquet_thrift.Type[opts.column.primitiveType] as any,
    codec: parquet_thrift.CompressionCodec[opts.useDataPageV2 ? opts.column.compression : 'UNCOMPRESSED'] as any
  });

  /* list encodings */
  let encodingsSet = {};
  encodingsSet[PARQUET_RDLVL_ENCODING] = true;
  encodingsSet[opts.column.encoding] = true;
  for (let k in encodingsSet) {
    metadata.encodings.push(parquet_thrift.Encoding[k]);
  }

  /* concat metadata header and data pages */
  let metadataOffset = opts.baseOffset + pagesBuf.length;
  let body = Buffer.concat([pagesBuf, parquet_util.serializeThrift(metadata)]);
  return { body, metadata, metadataOffset };
}

/**
 * Encode a list of column values into a parquet row group
 */
function encodeRowGroup(schema: ParquetSchema, data, opts): { body: Buffer, metadata: TODO } {
  let metadata = new parquet_thrift.RowGroup({
    num_rows: data.rowCount,
    columns: [],
    total_byte_size: 0
  });

  let body = Buffer.alloc(0);
  for (let field of schema.fieldList) {
    if (field.isNested) {
      continue;
    }

    let cchunkData = encodeColumnChunk(
      data.columnData[field.path as any],
      {
        column: field,
        baseOffset: opts.baseOffset + body.length,
        pageSize: opts.pageSize,
        encoding: field.encoding,
        rowCount: data.rowCount,
        useDataPageV2: opts.useDataPageV2
      }
    );

    let cchunk = new parquet_thrift.ColumnChunk({
      file_offset: cchunkData.metadataOffset,
      meta_data: cchunkData.metadata
    });

    metadata.columns.push(cchunk);
    metadata.total_byte_size += cchunkData.body.length;

    body = Buffer.concat([body, cchunkData.body]);
  }

  return { body, metadata };
}

/**
 * Encode a parquet file metadata footer
 */
function encodeFooter(schema, rowCount, rowGroups, userMetadata) {
  let metadata = new parquet_thrift.FileMetaData({
    version: PARQUET_VERSION,
    created_by: 'parquets',
    num_rows: rowCount,
    row_groups: rowGroups,
    schema: [],
    key_value_metadata: []
  });

  for (let key in userMetadata) {
    let kv = new parquet_thrift.KeyValue({
      key: key,
      value: userMetadata[key]
    });
    metadata.key_value_metadata.push(kv);
  }

  {
    let schemaRoot = new parquet_thrift.SchemaElement({
      name: 'root',
      num_children: Object.keys(schema.fields).length
    });
    metadata.schema.push(schemaRoot);
  }

  for (let field of schema.fieldList) {
    let relt = parquet_thrift.FieldRepetitionType[field.repetitionType];
    let schemaElem = new parquet_thrift.SchemaElement({
      name: field.name,
      repetition_type: relt as any
    });

    if (field.isNested) {
      schemaElem.num_children = field.fieldCount;
    } else {
      schemaElem.type = parquet_thrift.Type[field.primitiveType] as TODO;
    }

    if (field.originalType) {
      schemaElem.converted_type = parquet_thrift.ConvertedType[field.originalType] as TODO;
    }

    schemaElem.type_length = field.typeLength;

    metadata.schema.push(schemaElem);
  }

  let metadataEncoded = parquet_util.serializeThrift(metadata);
  let footerEncoded = new Buffer(metadataEncoded.length + 8);
  metadataEncoded.copy(footerEncoded);
  footerEncoded.writeUInt32LE(metadataEncoded.length, metadataEncoded.length);
  footerEncoded.write(PARQUET_MAGIC, metadataEncoded.length + 4);
  return footerEncoded;
}
