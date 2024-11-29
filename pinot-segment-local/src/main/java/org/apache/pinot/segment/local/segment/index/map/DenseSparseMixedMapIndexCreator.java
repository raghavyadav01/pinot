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
package org.apache.pinot.segment.local.segment.index.map;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriter;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.MapUtils;


public class DenseSparseMixedMapIndexCreator extends BaseMapIndexCreator {
  private final File _indexDir;
  private final String _name;
  private final MapIndexConfig _indexConfig;
  private VarByteChunkForwardIndexWriter _indexWriter;

  private static final ChunkCompressionType DEFAULT_COMPRESSION_TYPE = ChunkCompressionType.LZ4;
  private static final int DEFAULT_WRITER_VERSION = 2;
  private static final int DEFAULT_TARGET_DOCS_PER_CHUNK = 2048;

  /**
   * Constructor for DenseSparseMixedMapIndexCreator.
   *
   * @param indexDir Directory where the index will be created
   * @param name Column name
   * @param indexConfig Map index configuration
   * @param numDocs Total number of documents
   * @param maxLength Maximum length of entries
   * @throws IOException if index creation fails
   */
  public DenseSparseMixedMapIndexCreator(File indexDir, String name, MapIndexConfig indexConfig, int numDocs,
      int maxLength)
      throws IOException {
    super(indexDir, name, indexConfig);

    Preconditions.checkNotNull(indexDir);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(indexConfig);
    Preconditions.checkArgument(numDocs > 0, "Number of documents must be positive");
    Preconditions.checkArgument(maxLength > 0, "Maximum length must be positive");

    _indexDir = indexDir;
    _name = name;
    _indexConfig = indexConfig;

    File indexFile = new File(_indexDir, name + V1Constants.Indexes.MAP_INDEX_FILE_EXTENSION);
    _indexWriter =
        new VarByteChunkForwardIndexWriter(indexFile, DEFAULT_COMPRESSION_TYPE, numDocs, DEFAULT_TARGET_DOCS_PER_CHUNK,
            maxLength, DEFAULT_WRITER_VERSION);
  }

  @Override
  public void add(@Nullable Object cellValue, int dictId) {
    if (cellValue instanceof Map) {
      _indexWriter.putBytes(MapUtils.serializeMap((Map<String, Object>) cellValue));
    } else {
      throw new IllegalStateException(
          "Unsupported value type: " + getValueType() + ", cellValue type is: " + cellValue.getClass());
    }
  }

  @Override
  public void add(Map<String, Object> mapValue) {
    _indexWriter.putBytes(MapUtils.serializeMap(mapValue));
  }

  @Override
  public void seal()
      throws IOException {
    //_indexWriter.close();
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return FieldSpec.DataType.MAP;
  }

  @Override
  public void close()
      throws IOException {
    if (_indexWriter != null) {
      _indexWriter.close();
      _indexWriter = null;
    }
  }
}