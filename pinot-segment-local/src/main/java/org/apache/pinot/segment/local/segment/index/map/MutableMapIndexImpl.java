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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.forward.VarByteSVMutableForwardIndex;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.IndexUtil;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.MapUtils;


public class MutableMapIndexImpl implements MapIndexReader, MutableIndex {
  private final MutableIndexContext _context;
  private final ColumnMetadata _columnMetadata;
  private VarByteSVMutableForwardIndex _forwardIndex;
  private final FieldSpec _valueFieldSpec;
  private final String _name;
  private final File _indexDir;
  private final String _segmentName;

  public static final int MAX_MULTI_VALUES_PER_ROW = 1000;
  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT = 100;
  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT = 100_000;
  private static final int DEFAULT_TOP_K_KEYS = 100;
  private final int _topKKeys;
  private final Map<String, Integer> _keyFrequencies = new HashMap<>();

  public MutableMapIndexImpl(MutableIndexContext context, MapIndexConfig config) {
    _context = context;
    _columnMetadata = buildColumnMetadata(context);
    _name = context.getFieldSpec().getName();
    _indexDir = context.getConsumerDir();
    _segmentName = context.getSegmentName();
    _topKKeys = config != null && config.getConfigs().containsKey("maxDenseKeys") ? 
        (Integer) config.getConfigs().get("maxDenseKeys") : DEFAULT_TOP_K_KEYS;
    String column = _name;
    String segmentName = _segmentName;
    FieldSpec.DataType storedType = context.getFieldSpec().getDataType().getStoredType();
    int fixedLengthBytes = context.getFixedLengthBytes();
    boolean isSingleValue = context.getFieldSpec().isSingleValueField();
    String allocationContext =
        IndexUtil.buildAllocationContext(context.getSegmentName(), context.getFieldSpec().getName(),
            V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    int initialCapacity = Math.min(context.getCapacity(), NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT);
    _forwardIndex =
        new VarByteSVMutableForwardIndex(FieldSpec.DataType.BYTES, context.getMemoryManager(), allocationContext,
            initialCapacity, NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT);
    ComplexFieldSpec complexFieldSpec = (ComplexFieldSpec) _context.getFieldSpec();
    _valueFieldSpec = complexFieldSpec.getChildFieldSpec(ComplexFieldSpec.VALUE_FIELD);
  }

  private static ColumnMetadata buildColumnMetadata(MutableIndexContext context) {
    FieldSpec fieldSpec = context.getFieldSpec();
    return ColumnMetadataImpl.builder().setFieldSpec(fieldSpec).setTotalDocs(context.getCapacity())
        .setCardinality(context.getEstimatedCardinality()).setHasDictionary(context.hasDictionary())
        .setColumnMaxLength(context.getFixedLengthBytes()).setBitsPerElement(0)
        .setMaxNumberOfMultiValues(context.getAvgNumMultiValues()).setTotalNumberOfEntries(context.getCapacity())
        .setSorted(false)
        .setAutoGenerated(false).build();
  }

  @Override
  public void add(@Nonnull Object value, int dictId, int docId) {
    if (value instanceof Map) {
      Map<?, ?> mapValue = (Map<?, ?>) value;
      // Track key frequencies
      for (Object key : mapValue.keySet()) {
        _keyFrequencies.merge(key.toString(), 1, Integer::sum);
      }
      _forwardIndex.setBytes(docId, MapUtils.serializeMap((Map) value));
    } else {
      throw new IllegalStateException("Unsupported value type for MAP column: " + value.getClass());
    }
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds, int docId) {
  }

  @Override
  public void commit() {
    PriorityQueue<SimpleEntry<String, Integer>> topKHeap =
        new PriorityQueue<>((a, b) -> a.getValue().compareTo(b.getValue()));

    for (Map.Entry<String, Integer> entry : _keyFrequencies.entrySet()) {
      topKHeap.offer(new SimpleEntry<>(entry.getKey(), entry.getValue()));
      if (topKHeap.size() > _topKKeys) {
        topKHeap.poll();
      }
    }

    // Convert to sorted set of keys (highest frequency first)
    Set<String> topKeys =
        topKHeap.stream().sorted((a, b) -> b.getValue().compareTo(a.getValue())).map(SimpleEntry::getKey)
            .collect(Collectors.toSet());

    // Write topKeys to a file in the index directory
    File topKeysFile = new File(_indexDir, _name + ".topkeys");
    try {
      Files.write(topKeysFile.toPath(), String.join("\n", topKeys).getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeException("Failed to write top keys to file: " + topKeysFile, e);
    }
  }

  @Override
  public void close()
      throws IOException {
  }

  @Override
  public Set<String> getKeys() {
    return _keyFrequencies.keySet();
  }

  public Map getKeyIndexes(String key) {
    return Map.of(StandardIndexes.forward(), new MapKeyIndexReader(_forwardIndex, key, getKeyFieldSpec(key)));
  }

  @Override
  public FieldSpec getKeyFieldSpec(String key) {
    ComplexFieldSpec complexFieldSpec = (ComplexFieldSpec) _context.getFieldSpec();
    return complexFieldSpec.getChildFieldSpec(ComplexFieldSpec.VALUE_FIELD);
  }

  @Override
  public FieldSpec.DataType getKeyStoredType(String key) {
    return null;
  }

  @Override
  public IndexReader getKeyReader(String key, IndexType indexType) {
    return null;
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return false;
  }

  @Override
  public FieldSpec.DataType getStoredType() {
    return _context.getFieldSpec().getDataType().getStoredType();
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
}
