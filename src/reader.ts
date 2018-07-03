import { PARQUET_CODEC } from './codec';
import * as Compression from './compression';
import { ColumnData, CursorBuffer, ParquetCodec, ParquetType, RecordBuffer, SchemaDefinition, TODO } from './declare';
// tslint:disable-next-line:max-line-length
import { ColumnChunk, CompressionCodec, ConvertedType, Encoding, FieldRepetitionType, FileMetaData, PageHeader, PageType, RowGroup, Type } from './gen/parquet_types';
import { ParquetSchema } from './schema';
import * as Shred from './shred';
import * as Util from './util';

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = 'PAR1';

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 1;

/**
 * Internal type used for repetition/definition levels
 */
const PARQUET_RDLVL_TYPE = 'INT32';
const PARQUET_RDLVL_ENCODING = 'RLE';

/**
 * A parquet cursor is used to retrieve rows from a parquet file in order
 */
export class ParquetCursor<T> {

  public metadata: TODO;
  public envelopeReader: ParquetEnvelopeReader;
  public schema: ParquetSchema;
  public columnList: string[][];
  public rowGroup: any[];
  public rowGroupIndex: number;

  /**
   * Create a new parquet reader from the file metadata and an envelope reader.
   * It is usually not recommended to call this constructor directly except for
   * advanced and internal use cases. Consider using getCursor() on the
   * ParquetReader instead
   */
  constructor(metadata: TODO, envelopeReader: ParquetEnvelopeReader, schema: ParquetSchema, columnList: string[][]) {
    this.metadata = metadata;
    this.envelopeReader = envelopeReader;
    this.schema = schema;
    this.columnList = columnList;
    this.rowGroup = [];
    this.rowGroupIndex = 0;
  }

  /**
   * Retrieve the next row from the cursor. Returns a row or NULL if the end
   * of the file was reached
   */
  async next(): Promise<T> {
    if (this.rowGroup.length === 0) {
      if (this.rowGroupIndex >= this.metadata.row_groups.length) {
        return null;
      }

      const rowBuffer = await this.envelopeReader.readRowGroup(
        this.schema,
        this.metadata.row_groups[this.rowGroupIndex],
        this.columnList);

      this.rowGroup = Shred.materializeRecords(this.schema, rowBuffer);
      this.rowGroupIndex++;
    }

    return this.rowGroup.shift();
  }

  /**
   * Rewind the cursor the the beginning of the file
   */
  rewind(): void {
    this.rowGroup = [];
    this.rowGroupIndex = 0;
  }

}

/**
 * A parquet reader allows retrieving the rows from a parquet file in order.
 * The basic usage is to create a reader and then retrieve a cursor/iterator
 * which allows you to consume row after row until all rows have been read. It is
 * important that you call close() after you are finished reading the file to
 * avoid leaking file descriptors.
 */
export class ParquetReader<T> {

  /**
   * Open the parquet file pointed to by the specified path and return a new
   * parquet reader
   */
  static async openFile<T>(filePath: string): Promise<ParquetReader<T>> {
    const envelopeReader = await ParquetEnvelopeReader.openFile(filePath);
    try {
      await envelopeReader.readHeader();
      const metadata = await envelopeReader.readFooter();
      return new ParquetReader<T>(metadata, envelopeReader);
    } catch (err) {
      await envelopeReader.close();
      throw err;
    }
  }

  public metadata: FileMetaData;
  public envelopeReader: ParquetEnvelopeReader;
  public schema: ParquetSchema;

  /**
   * Create a new parquet reader from the file metadata and an envelope reader.
   * It is not recommended to call this constructor directly except for advanced
   * and internal use cases. Consider using one of the open{File,Buffer} methods
   * instead
   */
  constructor(metadata: FileMetaData, envelopeReader: ParquetEnvelopeReader) {
    if (metadata.version !== PARQUET_VERSION) {
      throw new Error('invalid parquet version');
    }

    this.metadata = metadata;
    this.envelopeReader = envelopeReader;
    this.schema = new ParquetSchema(decodeSchema(this.metadata.schema.splice(1)));
  }

  /**
   * Return a cursor to the file. You may open more than one cursor and use
   * them concurrently. All cursors become invalid once close() is called on
   * the reader object.
   *
   * The required_columns parameter controls which columns are actually read
   * from disk. An empty array or no value implies all columns. A list of column
   * names means that only those columns should be loaded from disk.
   */
  getCursor(): ParquetCursor<T>;
  getCursor(columnList: (string | string[])[]): ParquetCursor<Partial<T>>;
  getCursor(columnList?: (string | string[])[]): ParquetCursor<Partial<T>> {
    if (!columnList) {
      // tslint:disable-next-line:no-parameter-reassignment
      columnList = [];
    }

    // tslint:disable-next-line:no-parameter-reassignment
    columnList = columnList.map(x => Array.isArray(x) ? x : [x]);

    return new ParquetCursor<T>(
      this.metadata,
      this.envelopeReader,
      this.schema,
      columnList as string[][]
    );
  }

  /**
   * Return the number of rows in this file. Note that the number of rows is
   * not neccessarily equal to the number of rows in each column.
   */
  getRowCount(): number {
    return this.metadata.num_rows;
  }

  /**
   * Returns the ParquetSchema for this file
   */
  getSchema(): ParquetSchema {
    return this.schema;
  }

  /**
   * Returns the user (key/value) metadata for this file
   */
  getMetadata(): Record<string, TODO> {
    const md = {};
    for (const kv of this.metadata.key_value_metadata) {
      md[kv.key] = kv.value;
    }

    return md;
  }

  /**
   * Close this parquet reader. You MUST call this method once you're finished
   * reading rows
   */
  async close(): Promise<void> {
    await this.envelopeReader.close();
    this.envelopeReader = null;
    this.metadata = null;
  }

}

/**
 * The parquet envelope reader allows direct, unbuffered access to the individual
 * sections of the parquet file, namely the header, footer and the row groups.
 * This class is intended for advanced/internal users; if you just want to retrieve
 * rows from a parquet file use the ParquetReader instead
 */
export class ParquetEnvelopeReader {

  static async openFile(filePath): Promise<ParquetEnvelopeReader> {
    const fileStat = await Util.fstat(filePath);
    const fileDescriptor = await Util.fopen(filePath);

    const readFn = Util.fread.bind(undefined, fileDescriptor);
    const closeFn = Util.fclose.bind(undefined, fileDescriptor);

    return new ParquetEnvelopeReader(readFn, closeFn, fileStat.size);
  }

  constructor(
    public read: (position: number, length: number) => Promise<Buffer>,
    public close: () => Promise<void>,
    public fileSize: number
  ) {
  }

  async readHeader(): Promise<void> {
    const buf = await this.read(0, PARQUET_MAGIC.length);

    if (buf.toString() !== PARQUET_MAGIC) {
      throw new Error('not valid parquet file');
    }
  }

  async readRowGroup(schema: ParquetSchema, rowGroup: RowGroup, columnList: TODO[]): Promise<RecordBuffer> {
    const buffer: RecordBuffer = {
      rowCount: +rowGroup.num_rows,
      columnData: {}
    };

    for (const colChunk of rowGroup.columns) {
      const colMetadata = colChunk.meta_data;
      const colKey = colMetadata.path_in_schema;

      if (columnList.length > 0 && Util.fieldIndexOf(columnList, colKey) < 0) {
        continue;
      }

      buffer.columnData[colKey as any] = await this.readColumnChunk(schema, colChunk);
    }

    return buffer;
  }

  async readColumnChunk(schema: ParquetSchema, colChunk: ColumnChunk): Promise<ColumnData> {
    if (colChunk.file_path !== undefined && colChunk.file_path !== null) {
      throw new Error('external references are not supported');
    }

    const field = schema.findField(colChunk.meta_data.path_in_schema);
    const type = Util.getThriftEnum(
      Type,
      colChunk.meta_data.type);

    const compression = Util.getThriftEnum(
      CompressionCodec,
      colChunk.meta_data.codec);

    const pagesOffset = +colChunk.meta_data.data_page_offset;
    const pagesSize = +colChunk.meta_data.total_compressed_size;
    const pagesBuf = await this.read(pagesOffset, pagesSize);

    return decodeDataPages(pagesBuf, {
      type,
      rLevelMax: field.rLevelMax,
      dLevelMax: field.dLevelMax,
      compression,
      column: field
    });
  }

  async readFooter(): Promise<FileMetaData> {
    const trailerLen = PARQUET_MAGIC.length + 4;
    const trailerBuf = await this.read(this.fileSize - trailerLen, trailerLen);

    if (trailerBuf.slice(4).toString() !== PARQUET_MAGIC) {
      throw new Error('not a valid parquet file');
    }

    const metadataSize = trailerBuf.readUInt32LE(0);
    const metadataOffset = this.fileSize - metadataSize - trailerLen;
    if (metadataOffset < PARQUET_MAGIC.length) {
      throw new Error('invalid metadata size');
    }

    const metadataBuf = await this.read(metadataOffset, metadataSize);
    // let metadata = new parquet_thrift.FileMetaData();
    // parquet_util.decodeThrift(metadata, metadataBuf);
    const { metadata } = Util.decodeFileMetadata(metadataBuf);
    return metadata;
  }

}

/**
 * Decode a consecutive array of data using one of the parquet encodings
 */
function decodeValues(type: ParquetType, encoding: ParquetCodec, cursor: CursorBuffer, count: number, opts: TODO): any[] {
  if (!(encoding in PARQUET_CODEC)) {
    throw new Error('invalid encoding: ' + encoding);
  }

  return PARQUET_CODEC[encoding].decodeValues(type, cursor, count, opts);
}

function decodeDataPages(buffer: Buffer, opts: TODO): ColumnData {
  const cursor = {
    buffer,
    offset: 0,
    size: buffer.length
  };

  const data = {
    rlevels: [],
    dlevels: [],
    values: [],
    count: 0
  };

  while (cursor.offset < cursor.size) {
    // const pageHeader = new parquet_thrift.PageHeader();
    // cursor.offset += parquet_util.decodeThrift(pageHeader, cursor.buffer);

    const { pageHeader, length } = Util.decodePageHeader(cursor.buffer);
    cursor.offset += length;

    const pageType = Util.getThriftEnum(
      PageType,
      pageHeader.type);

    let pageData = null;
    switch (pageType) {
      case 'DATA_PAGE':
        pageData = decodeDataPage(cursor, pageHeader, opts);
        break;
      case 'DATA_PAGE_V2':
        pageData = decodeDataPageV2(cursor, pageHeader, opts);
        break;
      default:
        throw new Error('invalid page type: ' + pageType);
    }

    Array.prototype.push.apply(data.rlevels, pageData.rlevels);
    Array.prototype.push.apply(data.dlevels, pageData.dlevels);
    Array.prototype.push.apply(data.values, pageData.values);
    data.count += pageData.count;
  }

  return data;
}

function decodeDataPage(cursor: CursorBuffer, header: PageHeader, opts: TODO): ColumnData {
  const cursorEnd = cursor.offset + header.compressed_page_size;
  const valueCount = header.data_page_header.num_values;
  const valueEncoding = Util.getThriftEnum(
    Encoding,
    header.data_page_header.encoding
  ) as ParquetCodec;

  /* read repetition levels */
  const rLevelEncoding = Util.getThriftEnum(
    Encoding,
    header.data_page_header.repetition_level_encoding
  ) as ParquetCodec;

  // tslint:disable-next-line:prefer-array-literal
  let rLevels = new Array(valueCount);
  if (opts.rLevelMax > 0) {
    rLevels = decodeValues(
      PARQUET_RDLVL_TYPE,
      rLevelEncoding,
      cursor,
      valueCount,
      {
        bitWidth: Util.getBitWidth(opts.rLevelMax),
        disableEnvelope: false
      }
    );
  } else {
    rLevels.fill(0);
  }

  /* read definition levels */
  const dLevelEncoding = Util.getThriftEnum(
    Encoding,
    header.data_page_header.definition_level_encoding
  ) as ParquetCodec;

  // tslint:disable-next-line:prefer-array-literal
  let dLevels = new Array(valueCount);
  if (opts.dLevelMax > 0) {
    dLevels = decodeValues(
      PARQUET_RDLVL_TYPE,
      dLevelEncoding,
      cursor,
      valueCount,
      {
        bitWidth: Util.getBitWidth(opts.dLevelMax),
        disableEnvelope: false
      }
    );
  } else {
    dLevels.fill(0);
  }

  /* read values */
  let valueCountNonNull = 0;
  for (const dlvl of dLevels) {
    if (dlvl === opts.dLevelMax) {
      ++valueCountNonNull;
    }
  }
  /* read values */
  let valuesBufCursor = cursor;

  if (opts.compression !== 'UNCOMPRESSED') {
    const valuesBuf = Compression.inflate(
      opts.compression,
      cursor.buffer.slice(cursor.offset, cursorEnd),
      header.uncompressed_page_size
    );

    valuesBufCursor = {
      buffer: valuesBuf,
      offset: 0,
      size: valuesBuf.length
    };

    cursor.offset = cursorEnd;
  }

  const values = decodeValues(
    opts.type,
    valueEncoding,
    valuesBufCursor,
    valueCountNonNull,
    {
      typeLength: opts.column.typeLength,
      bitWidth: opts.column.typeLength
    });

  return {
    dlevels: dLevels,
    rlevels: rLevels,
    values,
    count: valueCount
  };
}

function decodeDataPageV2(cursor: CursorBuffer, header: PageHeader, opts: TODO): ColumnData {
  const cursorEnd = cursor.offset + header.compressed_page_size;

  const valueCount = header.data_page_header_v2.num_values;
  const valueCountNonNull = valueCount - header.data_page_header_v2.num_nulls;
  const valueEncoding = Util.getThriftEnum(
    Encoding,
    header.data_page_header_v2.encoding
  ) as ParquetCodec;

  /* read repetition levels */
  // tslint:disable-next-line:prefer-array-literal
  let rLevels = new Array(valueCount);
  if (opts.rLevelMax > 0) {
    rLevels = decodeValues(
      PARQUET_RDLVL_TYPE,
      PARQUET_RDLVL_ENCODING,
      cursor,
      valueCount,
      {
        bitWidth: Util.getBitWidth(opts.rLevelMax),
        disableEnvelope: true
      });
  } else {
    rLevels.fill(0);
  }

  /* read definition levels */
  // tslint:disable-next-line:prefer-array-literal
  let dLevels = new Array(valueCount);
  if (opts.dLevelMax > 0) {
    dLevels = decodeValues(
      PARQUET_RDLVL_TYPE,
      PARQUET_RDLVL_ENCODING,
      cursor,
      valueCount,
      {
        bitWidth: Util.getBitWidth(opts.dLevelMax),
        disableEnvelope: true
      });
  } else {
    dLevels.fill(0);
  }

  /* read values */
  let valuesBufCursor = cursor;

  if (header.data_page_header_v2.is_compressed) {
    const valuesBuf = Compression.inflate(
      opts.compression,
      cursor.buffer.slice(cursor.offset, cursorEnd),
      header.uncompressed_page_size
    );

    valuesBufCursor = {
      buffer: valuesBuf,
      offset: 0,
      size: valuesBuf.length
    };

    cursor.offset = cursorEnd;
  }

  const values = decodeValues(
    opts.type,
    valueEncoding,
    valuesBufCursor,
    valueCountNonNull,
    {
      typeLength: opts.column.typeLength,
      bitWidth: opts.column.typeLength
    });

  return {
    dlevels: dLevels,
    rlevels: rLevels,
    values,
    count: valueCount
  };
}

function decodeSchema(schemaElements: TODO[]): SchemaDefinition {
  const schema: SchemaDefinition = {};
  for (let idx = 0; idx < schemaElements.length;) {
    const schemaElement = schemaElements[idx];

    const repetitionType = Util.getThriftEnum(
      FieldRepetitionType,
      schemaElement.repetition_type);

    let optional = false;
    let repeated = false;
    switch (repetitionType) {
      case 'REQUIRED':
        break;
      case 'OPTIONAL':
        optional = true;
        break;
      case 'REPEATED':
        repeated = true;
        break;
    }

    if (schemaElement.num_children > 0) {
      schema[schemaElement.name] = {
        // type: undefined,
        optional,
        repeated,
        fields: decodeSchema(
          schemaElements.slice(idx + 1, idx + 1 + schemaElement.num_children))
      };
    } else {
      let logicalType = Util.getThriftEnum(
        Type,
        schemaElement.type);

      if (schemaElement.converted_type != null) {
        logicalType = Util.getThriftEnum(
          ConvertedType,
          schemaElement.converted_type);
      }

      schema[schemaElement.name] = {
        type: logicalType as ParquetType,
        typeLength: schemaElement.type_length,
        optional,
        repeated
      };
    }

    idx += (schemaElement.num_children || 0) + 1;
  }

  return schema;
}
