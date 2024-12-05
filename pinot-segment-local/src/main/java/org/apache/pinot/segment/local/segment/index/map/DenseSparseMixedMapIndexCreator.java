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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final Map<String, VarByteChunkForwardIndexWriter> _denseKeyWriters;
  private VarByteChunkForwardIndexWriter _sparseKeyIndexWriter;

  private final Set<String> _denseKeys;

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
    _denseKeyWriters = new HashMap<>();

    // Initialize configuration from indexConfig's config map
    Map<String, Object> configMap = indexConfig.getConfigs();

    // Parse dynamicallyCreateDenseKeys with proper type checking
    boolean dynamicallyCreateDenseKeys =
        configMap.containsKey("dynamicallyCreateDenseKeys") ? (Boolean) configMap.get("dynamicallyCreateDenseKeys")
            : false;

    // Determine dense keys based on configuration
    if (dynamicallyCreateDenseKeys) {
      _denseKeys = readTopKeysFromFile(_indexDir, _name);
      if (_denseKeys.isEmpty()) {
        throw new IllegalStateException(
            "dynamicallyCreateDenseKeys is true but no .topkeys file found for column: " + _name);
      }
    } else {
      // Parse dense keys from config
      if (configMap.containsKey("denseKeys")) {
        Object denseKeysObj = configMap.get("denseKeys");
        if (denseKeysObj instanceof List) {
          _denseKeys = Set.copyOf((List<String>) denseKeysObj);
        } else {
          throw new IllegalArgumentException(
              "Invalid denseKeys format in config. Expected List<String> or Set<String>");
        }
      } else {
        _denseKeys = Set.of();
      }
    }

    // Parse maxNumDenseKeys with proper type checking
    int maxNumDenseKeys =
        configMap.containsKey("maxNumDenseKeys") ? ((Number) configMap.get("maxNumDenseKeys")).intValue() : 0;

    // Create a separate writer for each dense key
    for (String denseKey : _denseKeys) {
      File denseKeyFile = new File(_indexDir, name + "." + denseKey + V1Constants.Indexes.MAP_INDEX_FILE_EXTENSION);
      _denseKeyWriters.put(denseKey, new VarByteChunkForwardIndexWriter(denseKeyFile, DEFAULT_COMPRESSION_TYPE, numDocs,
          DEFAULT_TARGET_DOCS_PER_CHUNK, maxLength, DEFAULT_WRITER_VERSION));
    }

    File sparseKeyFile = new File(_indexDir, name + ".sparse" + V1Constants.Indexes.MAP_INDEX_FILE_EXTENSION);
    _sparseKeyIndexWriter = new VarByteChunkForwardIndexWriter(sparseKeyFile, DEFAULT_COMPRESSION_TYPE, numDocs,
        DEFAULT_TARGET_DOCS_PER_CHUNK, maxLength, DEFAULT_WRITER_VERSION);
  }

  @Override
  public void add(@Nullable Object cellValue, int dictId) {
    if (cellValue instanceof Map) {
      Map<String, Object> mapValue = (Map<String, Object>) cellValue;
      processMap(mapValue);
    } else {
      throw new IllegalStateException(
          "Unsupported value type: " + getValueType() + ", cellValue type is: " + cellValue.getClass());
    }
  }

  @Override
  public void add(Map<String, Object> mapValue) {
    processMap(mapValue);
  }

  private void processMap(Map<String, Object> mapValue) {
    // Process dense keys - create separate maps for dense and sparse keys
    Map<String, Object> sparseKeyMap = new HashMap<>();

    // Copy all entries to sparseKeyMap initially
    sparseKeyMap.putAll(mapValue);

    // Process dense keys
    for (String denseKey : _denseKeys) {
      Object value = mapValue.get(denseKey);
      Map<String, Object> singleKeyMap = new HashMap<>();
      if (value != null) {
        singleKeyMap.put(denseKey, value);
        // Remove from sparseKeyMap since it's handled as dense
        sparseKeyMap.remove(denseKey);
      }
      _denseKeyWriters.get(denseKey).putBytes(MapUtils.serializeMap(singleKeyMap));
    }

    // Write remaining entries (sparse keys)
    _sparseKeyIndexWriter.putBytes(MapUtils.serializeMap(sparseKeyMap));
  }

  @Override
  public void seal()
      throws IOException {
    // Close all dense key writers
    for (VarByteChunkForwardIndexWriter writer : _denseKeyWriters.values()) {
      if (writer != null) {
        writer.close();
      }
    }

    if (_sparseKeyIndexWriter != null) {
      _sparseKeyIndexWriter.close();
      _sparseKeyIndexWriter = null;
    }

    // Create the final combined index file
    File combinedIndexFile = new File(_indexDir, _name + V1Constants.Indexes.MAP_INDEX_FILE_EXTENSION);

    // Delete existing index file if it exists
    if (combinedIndexFile.exists()) {
      combinedIndexFile.delete();
    }

    try (FileChannel outputChannel = FileChannel.open(combinedIndexFile.toPath(), StandardOpenOption.CREATE,
        StandardOpenOption.WRITE)) {

      // Track positions for metadata
      long currentPosition = 0;
      Map<String, Long> denseKeyPositions = new HashMap<>();
      Map<String, Long> denseKeySizes = new HashMap<>();

      // First write sparse data
      File sparseFile = new File(_indexDir, _name + ".sparse" + V1Constants.Indexes.MAP_INDEX_FILE_EXTENSION);
      long sparseDataStart = currentPosition;
      long sparseDataSize = Files.size(sparseFile.toPath());
      try (FileChannel sparseChannel = FileChannel.open(sparseFile.toPath(), StandardOpenOption.READ)) {
        currentPosition += sparseChannel.transferTo(0, sparseDataSize, outputChannel);
      }

      // Write dense key data
      for (Map.Entry<String, VarByteChunkForwardIndexWriter> entry : _denseKeyWriters.entrySet()) {
        String denseKey = entry.getKey();
        File denseKeyFile = new File(_indexDir, _name + "." + denseKey + V1Constants.Indexes.MAP_INDEX_FILE_EXTENSION);
        denseKeyPositions.put(denseKey, currentPosition);
        long denseKeySize = Files.size(denseKeyFile.toPath());
        denseKeySizes.put(denseKey, denseKeySize);

        try (FileChannel denseKeyChannel = FileChannel.open(denseKeyFile.toPath(), StandardOpenOption.READ)) {
          currentPosition += denseKeyChannel.transferTo(0, denseKeySize, outputChannel);
        }
      }

      // Write metadata header
      ByteBuffer metadataBuffer = ByteBuffer.allocate(1024); // Initial size, will resize if needed

      // Write sparse data metadata
      metadataBuffer.putLong(sparseDataStart);
      metadataBuffer.putLong(sparseDataSize);

      // Write dense keys count
      metadataBuffer.putInt(_denseKeys.size());

      // Write dense keys metadata
      for (String denseKey : _denseKeys) {
        byte[] keyBytes = denseKey.getBytes(StandardCharsets.UTF_8);
        metadataBuffer.putInt(keyBytes.length);
        metadataBuffer.put(keyBytes);
        metadataBuffer.putLong(denseKeyPositions.get(denseKey));
        metadataBuffer.putLong(denseKeySizes.get(denseKey));
      }

      // Record metadata size and position
      int metadataSize = metadataBuffer.position();
      metadataBuffer.putLong(metadataSize);

      // Write metadata to file
      metadataBuffer.flip();
      outputChannel.write(metadataBuffer);

      // Delete individual files
      sparseFile.delete();
      for (String denseKey : _denseKeys) {
        File denseKeyFile = new File(_indexDir, _name + "." + denseKey + V1Constants.Indexes.MAP_INDEX_FILE_EXTENSION);
        denseKeyFile.delete();
      }
    }
  }

  @Override
  public void close()
      throws IOException {
    _denseKeyWriters.clear();
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

  /**
   * Reads the top keys from the .topkeys file.
   *
   * @param indexDir Directory containing the .topkeys file
   * @param columnName Name of the column
   * @return Set of dense keys read from the file
   * @throws IOException if reading the file fails
   */
  private Set<String> readTopKeysFromFile(File indexDir, String columnName)
      throws IOException {
    File topKeysFile = new File(indexDir, "../../../consumers/" + columnName + ".topkeys");
    if (!topKeysFile.exists()) {
      return Set.of();
    }

    List<String> lines = Files.readAllLines(topKeysFile.toPath(), StandardCharsets.UTF_8);
    return Set.copyOf(lines);
  }
}
