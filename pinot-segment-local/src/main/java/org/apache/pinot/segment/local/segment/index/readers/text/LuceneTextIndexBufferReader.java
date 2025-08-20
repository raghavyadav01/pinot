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
package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.lucene.store.Directory;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for reading Lucene text index from a combined buffer.
 * This class provides methods to extract files and create Lucene Directory
 * from a PinotDataBuffer containing the combined text index data.
 */
public class LuceneTextIndexBufferReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneTextIndexBufferReader.class);

  private LuceneTextIndexBufferReader() {
    // Utility class
  }

  /** Magic number for V2 format */
  private static final String MAGIC_NUMBER = "LUCENE_V2";

  /** Version number for V2 format */
  private static final int VERSION = 2;

  /** Header size in bytes */
  private static final int HEADER_SIZE = 29; // 9 + 4 + 8 + 4 + 4

  /** File metadata entry size (excluding variable length filename) */
  private static final int FILE_METADATA_ENTRY_SIZE = 18; // 2 + 8 + 8

  /**
   * Creates a Lucene Directory from a PinotDataBuffer containing combined text index
   *
   * @param indexBuffer the buffer containing combined text index data
   * @param column the column name
   * @return Lucene Directory that reads from the buffer
   * @throws IOException if buffer parsing fails
   */
  public static Directory createLuceneDirectory(PinotDataBuffer indexBuffer, String column)
      throws IOException {
    // Parse buffer header and metadata
    LuceneTextIndexHeader.TextIndexMetadata metadata = parseBufferMetadata(indexBuffer);

    // Create file map from metadata
    Map<String, LuceneTextIndexHeader.FileInfo> fileMap = buildFileMap(indexBuffer, metadata);

    // Return custom Directory implementation
    return new PinotBufferLuceneDirectory(indexBuffer, fileMap, column);
  }

  /**
   * Extracts docId mapping buffer from combined index buffer
   *
   * @param indexBuffer the buffer containing combined text index data
   * @param column the column name
   * @param numDocs number of documents
   * @return PinotDataBuffer for docId mapping, or null if not found
   * @throws IOException if extraction fails
   */
  public static PinotDataBuffer extractDocIdMappingBuffer(PinotDataBuffer indexBuffer, String column, int numDocs)
      throws IOException {
    String mappingFileName = column + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION;
    LuceneTextIndexHeader.FileInfo fileInfo = getFileInfo(indexBuffer, mappingFileName);

    if (fileInfo != null) {
      return indexBuffer.view(fileInfo.getOffset(), fileInfo.getOffset() + fileInfo.getSize());
    }
    return null;
  }

  /**
   * Extracts properties from combined index buffer
   *
   * @param indexBuffer the buffer containing combined text index data
   * @param column the column name
   * @return Properties object, or empty Properties if not found
   * @throws IOException if extraction fails
   */
  public static Properties extractProperties(PinotDataBuffer indexBuffer, String column)
      throws IOException {
    String propertiesFileName = V1Constants.Indexes.LUCENE_TEXT_INDEX_PROPERTIES_FILE;
    LuceneTextIndexHeader.FileInfo fileInfo = getFileInfo(indexBuffer, propertiesFileName);

    if (fileInfo != null) {
      PinotDataBuffer propertiesBuffer =
          indexBuffer.view(fileInfo.getOffset(), fileInfo.getOffset() + fileInfo.getSize());
      return parsePropertiesFromBuffer(propertiesBuffer);
    }
    return new Properties();
  }

  /**
   * Checks if buffer contains a specific file
   *
   * @param indexBuffer the buffer containing combined text index data
   * @param fileName the file name to check
   * @return true if file exists in buffer
   * @throws IOException if buffer parsing fails
   */
  public static boolean hasFile(PinotDataBuffer indexBuffer, String fileName)
      throws IOException {
    return getFileInfo(indexBuffer, fileName) != null;
  }

  /**
   * Gets list of all files in the buffer
   *
   * @param indexBuffer the buffer containing combined text index data
   * @return list of file names
   * @throws IOException if buffer parsing fails
   */
  public static List<String> listFiles(PinotDataBuffer indexBuffer)
      throws IOException {
    LuceneTextIndexHeader.TextIndexMetadata metadata = parseBufferMetadata(indexBuffer);
    return metadata.getFileNames();
  }

  /**
   * Gets file info for a specific file
   *
   * @param indexBuffer the buffer containing combined text index data
   * @param fileName the file name
   * @return FileInfo object, or null if not found
   * @throws IOException if buffer parsing fails
   */
  public static LuceneTextIndexHeader.FileInfo getFileInfo(PinotDataBuffer indexBuffer, String fileName)
      throws IOException {
    LuceneTextIndexHeader.TextIndexMetadata metadata = parseBufferMetadata(indexBuffer);
    return metadata.getFileInfoMap().get(fileName);
  }

  /**
   * Parses buffer metadata from the combined index buffer
   */
  private static LuceneTextIndexHeader.TextIndexMetadata parseBufferMetadata(PinotDataBuffer indexBuffer)
      throws IOException {
    //indexBuffer.order();

    // Read and validate header
    byte[] magicBytes = new byte[9];
    indexBuffer.copyTo(0, magicBytes, 0, 9);
    String magic = new String(magicBytes);

    if (!MAGIC_NUMBER.equals(magic)) {
      throw new IOException("Invalid magic number: " + magic);
    }

    int version = indexBuffer.getInt(9);
    if (version != VERSION) {
      throw new IOException("Unsupported version: " + version);
    }

    long totalSize = indexBuffer.getLong(13);
    int fileCount = indexBuffer.getInt(21);
    // Skip reserved bytes at position 25

    LOGGER.debug("Parsing buffer metadata: {} files, total size: {} bytes", fileCount, totalSize);

    // Read file metadata
    List<String> fileNames = new ArrayList<>();
    Map<String, LuceneTextIndexHeader.FileInfo> fileInfoMap = new HashMap<>();
    long metadataOffset = HEADER_SIZE;

    for (int i = 0; i < fileCount; i++) {
      short nameLength = indexBuffer.getShort(metadataOffset);
      metadataOffset += 2;

      byte[] nameBytes = new byte[nameLength];
      indexBuffer.copyTo(metadataOffset, nameBytes, 0, nameLength);
      String fileName = new String(nameBytes);
      metadataOffset += nameLength;

      long fileOffset = indexBuffer.getLong(metadataOffset);
      metadataOffset += 8;
      long fileSize = indexBuffer.getLong(metadataOffset);
      metadataOffset += 8;

      fileNames.add(fileName);
      fileInfoMap.put(fileName, new LuceneTextIndexHeader.FileInfo(fileName, fileOffset, fileSize));
    }

    return new LuceneTextIndexHeader.TextIndexMetadata(magic, version, totalSize, fileCount, fileNames, fileInfoMap);
  }

  /**
   * Builds file map from buffer metadata
   */
  private static Map<String, LuceneTextIndexHeader.FileInfo> buildFileMap(PinotDataBuffer indexBuffer,
      LuceneTextIndexHeader.TextIndexMetadata metadata) {
    return metadata.getFileInfoMap();
  }

  /**
   * Parses properties from buffer
   */
  private static Properties parsePropertiesFromBuffer(PinotDataBuffer propertiesBuffer)
      throws IOException {
    Properties properties = new Properties();
    try {
      // Convert buffer to byte array for Properties.load()
      byte[] propertiesData = new byte[(int) propertiesBuffer.size()];
      propertiesBuffer.copyTo(0, propertiesData, 0, (int) propertiesBuffer.size());
      properties.load(new java.io.ByteArrayInputStream(propertiesData));
    } catch (Exception e) {
      throw new IOException("Failed to parse properties", e);
    }
    return properties;
  }
}
