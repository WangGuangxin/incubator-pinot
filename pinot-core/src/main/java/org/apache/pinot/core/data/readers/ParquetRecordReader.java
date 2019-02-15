/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.data.readers;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.util.AvroUtils;

import java.io.File;
import java.io.IOException;

/**
 * Record reader for Parquet file.
 */
public class ParquetRecordReader implements RecordReader {
    private final File _dataFile;
    private final Schema _schema;

    private ParquetReader<GenericRecord> _reader;
    private GenericRecord _next;
    private boolean _hasNext;

    public ParquetRecordReader(File dataFile, Schema schema) throws IOException {
        _dataFile = dataFile;
        _schema = schema;

        init();
    }

    private void advance() {
        try {
            _next = _reader.read();
            _hasNext = _next != null;
        } catch (IOException e) {
            throw new RuntimeException("Failed while reading parquet file: " + _dataFile.getAbsolutePath(), e);
        }
    }

    @Override
    public boolean hasNext() {
        return _hasNext;
    }

    @Override
    public GenericRow next() throws IOException {
        return next(new GenericRow());
    }

    @Override
    public GenericRow next(GenericRow reuse) throws IOException {
        AvroUtils.fillGenericRow(_next, reuse, _schema);
        advance();

        return reuse;
    }

    @Override
    public void rewind() throws IOException {
        _reader.close();
        init();
    }

    @Override
    public Schema getSchema() {
        return _schema;
    }

    @Override
    public void close() throws IOException {
        _reader.close();
    }

    private void init() throws IOException {
        Path dataFsPath = new Path("file://" + _dataFile.getAbsolutePath());

        Configuration conf = new Configuration();
        _reader = AvroParquetReader.<GenericRecord>builder(dataFsPath)
                .disableCompatibility()
                .withDataModel(GenericData.get())
                .withConf(conf)
                .build();

        try {
            AvroUtils.validateSchema(_schema, getParquetSchema(conf, dataFsPath));
            advance();
        } catch (Exception e) {
            _reader.close();
            throw e;
        }
    }

    private org.apache.avro.Schema getParquetSchema(Configuration conf, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(conf);

        ParquetMetadata footer = ParquetFileReader.readFooter(fs.getConf(), path);

        String schemaString = footer.getFileMetaData()
                .getKeyValueMetaData().get("parquet.avro.schema");
        if (schemaString == null) {
            // try the older property
            schemaString = footer.getFileMetaData()
                    .getKeyValueMetaData().get("avro.schema");
        }

        if (schemaString != null) {
            return new org.apache.avro.Schema.Parser().parse(schemaString);
        } else {
            return new AvroSchemaConverter().convert(footer.getFileMetaData().getSchema());
        }
    }
}
