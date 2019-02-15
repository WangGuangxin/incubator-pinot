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

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.util.AvroUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Record reader for AVRO file.
 */
public class AvroRecordReader implements RecordReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroRecordReader.class);

  private final File _dataFile;
  private final Schema _schema;

  private DataFileStream<GenericRecord> _avroReader;
  private GenericRecord _reusableAvroRecord = null;

  public AvroRecordReader(File dataFile, Schema schema)
      throws IOException {
    _dataFile = dataFile;
    _schema = schema;
    _avroReader = AvroUtils.getAvroReader(dataFile);
    try {
      AvroUtils.validateSchema(_schema, _avroReader.getSchema());
    } catch (Exception e) {
      _avroReader.close();
      throw e;
    }
  }

  @Override
  public boolean hasNext() {
    return _avroReader.hasNext();
  }

  @Override
  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    _reusableAvroRecord = _avroReader.next(_reusableAvroRecord);
    AvroUtils.fillGenericRow(_reusableAvroRecord, reuse, _schema);
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _avroReader.close();
    _avroReader = AvroUtils.getAvroReader(_dataFile);
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void close()
      throws IOException {
    _avroReader.close();
  }
}
