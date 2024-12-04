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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.MapUtils;


public class DenseSparseMixedMapIndexReader implements MapIndexReader<ForwardIndexReaderContext, IndexReader> {
  protected final PinotDataBuffer _dataBuffer;
  private final FieldSpec _valueFieldSpec;
  private final ColumnMetadata _columnMetadata;

  // Add new fields for metadata
  private final long _sparseDataStart;
  private final long _sparseDataSize;
  private final Map<String, KeyIndexMetadata> _denseKeyMetadata;
  private final ForwardIndexReader _sparseIndexReader;
  private final Map<String, ForwardIndexReader> _denseKeyReaders;
  private ForwardIndexReader _denseReader;

  public DenseSparseMixedMapIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata) {
    _dataBuffer = dataBuffer;
    _columnMetadata = columnMetadata;

    // Read metadata size from the last 8 bytes
    long metadataSize = dataBuffer.getLong(dataBuffer.size() - Long.BYTES);
    long metadataOffset = dataBuffer.size() - Long.BYTES - metadataSize;

    // Read metadata header
    MetadataInfo metadataInfo = readMetadataHeader(dataBuffer, metadataOffset);
    _sparseDataStart = metadataInfo.sparseDataStart;
    _sparseDataSize = metadataInfo.sparseDataSize;
    _denseKeyMetadata = metadataInfo.denseKeyMetadata;

    // Initialize sparse index reader
    PinotDataBuffer sparseBuffer = dataBuffer.view(_sparseDataStart, _sparseDataStart + _sparseDataSize);
    _sparseIndexReader = new VarByteChunkSVForwardIndexReader(sparseBuffer, FieldSpec.DataType.BYTES);

    // Initialize dense key readers
    _denseKeyReaders = new HashMap<>();
    for (Map.Entry<String, KeyIndexMetadata> entry : _denseKeyMetadata.entrySet()) {
      String key = entry.getKey();
      KeyIndexMetadata metadata = entry.getValue();
      PinotDataBuffer denseBuffer = dataBuffer.view(metadata.offset, metadata.offset + metadata.size);
      _denseReader = new VarByteChunkSVForwardIndexReader(denseBuffer, FieldSpec.DataType.BYTES);
      _denseKeyReaders.put(key, _denseReader);

      // Test reading: Iterate through all documents for this dense key
      try (ForwardIndexReaderContext readerContext = _denseReader.createContext()) {
        for (int docId = 0; docId < _columnMetadata.getTotalDocs(); docId++) {
          byte[] bytes = _denseReader.getBytes(docId, readerContext);
          if (bytes != null) {
            Map<String, Object> value = MapUtils.deserializeMap(bytes);
            //System.out.println(value);
            // Log or process the map value as needed
          }
        }
      } catch (IOException e) {
      }
    }

    ComplexFieldSpec complexFieldSpec = (ComplexFieldSpec) columnMetadata.getFieldSpec();
    Preconditions.checkState(
        complexFieldSpec.getChildFieldSpec(ComplexFieldSpec.KEY_FIELD).getDataType() == FieldSpec.DataType.STRING,
        "Only String key is supported in Map");
    _valueFieldSpec = complexFieldSpec.getChildFieldSpec(ComplexFieldSpec.VALUE_FIELD);
  }

  private static class MetadataInfo {
    final long sparseDataStart;
    final long sparseDataSize;
    final Map<String, KeyIndexMetadata> denseKeyMetadata;

    MetadataInfo(long sparseDataStart, long sparseDataSize, Map<String, KeyIndexMetadata> denseKeyMetadata) {
      this.sparseDataStart = sparseDataStart;
      this.sparseDataSize = sparseDataSize;
      this.denseKeyMetadata = denseKeyMetadata;
    }
  }

  private static class KeyIndexMetadata {
    final long offset;
    final long size;

    KeyIndexMetadata(long offset, long size) {
      this.offset = offset;
      this.size = size;
    }
  }

  private MetadataInfo readMetadataHeader(PinotDataBuffer dataBuffer, long metadataOffset) {
    long currentOffset = metadataOffset;

    // Read sparse data metadata
    long sparseDataStart = dataBuffer.getLong(currentOffset);
    currentOffset += Long.BYTES;
    long sparseDataSize = dataBuffer.getLong(currentOffset);
    currentOffset += Long.BYTES;

    // Read dense keys count
    int numDenseKeys = dataBuffer.getInt(currentOffset);
    currentOffset += Integer.BYTES;

    // Read dense keys metadata
    Map<String, KeyIndexMetadata> denseKeyMetadata = new HashMap<>();
    for (int i = 0; i < numDenseKeys; i++) {
      // Read key
      int keyLength = dataBuffer.getInt(currentOffset);
      currentOffset += Integer.BYTES;
      byte[] keyBytes = new byte[keyLength];
      dataBuffer.copyTo(currentOffset, keyBytes);
      String key = new String(keyBytes, StandardCharsets.UTF_8);
      currentOffset += keyLength;

      // Read offset and size
      long offset = dataBuffer.getLong(currentOffset);
      currentOffset += Long.BYTES;
      long size = dataBuffer.getLong(currentOffset);
      currentOffset += Long.BYTES;

      denseKeyMetadata.put(key, new KeyIndexMetadata(offset, size));
    }

    return new MetadataInfo(sparseDataStart, sparseDataSize, denseKeyMetadata);
  }

  @Override
  public Map<String, Object> getMap(int docId, ForwardIndexReaderContext context) {
    // Merge sparse and dense data
    Map<String, Object> result = new HashMap<>();

    // Read sparse data
    byte[] sparseBytes = _sparseIndexReader.getBytes(docId, context);
    if (sparseBytes != null) {
      Map<String, Object> sparseData = MapUtils.deserializeMap(sparseBytes);
      // Flatten sparse data
      sparseData.forEach((k, v) -> result.put(k, v instanceof Map ? ((Map) v).values().iterator().next() : v));
    }

    // Read dense data
    for (Map.Entry<String, ForwardIndexReader> entry : _denseKeyReaders.entrySet()) {
      String key = entry.getKey();
      ForwardIndexReader reader = entry.getValue();
      byte[] bytes = reader.getBytes(docId, reader.createContext());
      if (bytes != null) {
        Map<String, Object> value = MapUtils.deserializeMap(bytes);
        if (!value.isEmpty()) {
          // Flatten nested values before adding to result
          value.forEach((k, v) -> result.put(k, v instanceof Map ? ((Map) v).values().iterator().next() : v));
        }
      }
    }

    return result;
  }

  @Override
  public void close()
      throws IOException {
    _sparseIndexReader.close();
    for (ForwardIndexReader reader : _denseKeyReaders.values()) {
      reader.close();
    }
  }

  @Override
  public Set<String> getKeys() {
    // Return empty set since we don't store keys separately
    return Set.of();
  }

  @Override
  public IndexReader getKeyReader(String key, IndexType indexType) {
    return null;
  }

  @Override
  public FieldSpec getKeyFieldSpec(String key) {
    return _valueFieldSpec;
  }

  @Override
  public byte[] getBytes(int docId, ForwardIndexReaderContext context) {
    Map<String, Object> map = getMap(docId, context);
    return MapUtils.serializeMap(map);
  }

  @Override
  public Map<IndexType, IndexReader> getKeyIndexes(String key) {
    // Check if the key is a dense key
    ForwardIndexReader reader = _denseKeyReaders.get(key);
    if (reader != null) {
      // For dense keys, use the corresponding dense reader
      return Map.of(StandardIndexes.forward(), new MapKeyIndexReader(reader, key, getKeyFieldSpec(key)));
    }
    // For sparse keys, use the sparse reader
    return Map.of(StandardIndexes.forward(), new MapKeyIndexReader(_sparseIndexReader, key, getKeyFieldSpec(key)));
  }

  @Override
  public FieldSpec.DataType getKeyStoredType(String key) {
    return _valueFieldSpec.getDataType();
  }

  @Override
  public ColumnMetadata getKeyMetadata(String key) {
    return new ColumnMetadata() {
      @Override
      public FieldSpec getFieldSpec() {
        return _valueFieldSpec;
      }

      @Override
      public int getTotalDocs() {
        return _columnMetadata.getTotalDocs();
      }

      @Override
      public int getCardinality() {
        return 0;
      }

      @Override
      public boolean isSorted() {
        return false;
      }

      @Override
      public Comparable getMinValue() {
        return null;
      }

      @Override
      public Comparable getMaxValue() {
        return null;
      }

      @Override
      public boolean hasDictionary() {
        return false;
      }

      @Override
      public int getColumnMaxLength() {
        return 0;
      }

      @Override
      public int getBitsPerElement() {
        return 0;
      }

      @Override
      public int getMaxNumberOfMultiValues() {
        return 0;
      }

      @Override
      public int getTotalNumberOfEntries() {
        return 0;
      }

      @Nullable
      @Override
      public PartitionFunction getPartitionFunction() {
        return null;
      }

      @Nullable
      @Override
      public Set<Integer> getPartitions() {
        return null;
      }

      @Override
      public Map<IndexType<?, ?, ?>, Long> getIndexSizeMap() {
        return Map.of();
      }

      @Override
      public boolean isAutoGenerated() {
        return false;
      }
    };
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
  public FieldSpec.DataType getStoredType() {
    return FieldSpec.DataType.MAP;
  }

  @Nullable
  @Override
  public ForwardIndexReaderContext createContext() {
    return _sparseIndexReader.createContext();
  }
}
