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
package org.apache.pinot.segment.local.segment.creator.impl.text;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to combine all Lucene text index files into a single buffer in V2 format.
 * This class handles the serialization of Lucene index directory into a compact buffer format
 * that can be efficiently stored and loaded.
 *
 * <p>The V2 format structure:</p>
 * <pre>
 * [Header Section]
 * - Magic number: "LUCENE_V2" (9 bytes)
 * - Version: 2 (4 bytes)
 * - Total buffer size: 8 bytes
 * - File count: 4 bytes
 * - Reserved: 4 bytes (for future use)
 *
 * [File Metadata Section]
 * - File name length: 2 bytes
 * - File name: variable length
 * - File offset: 8 bytes
 * - File size: 8 bytes
 *
 * [File Data Section]
 * - Raw file data concatenated in order
 * </pre>
 */
public class LuceneTextIndexCombined {
  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneTextIndexCombined.class);

  /**
   * Private constructor to prevent instantiation of utility class.
   */
  private LuceneTextIndexCombined() {
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
   * Combines all files from a Lucene text index directory into a single file.
   *
   * @param luceneIndexDir the Lucene index directory to combine
   * @param outputFilePath the output file path to write the combined data
   * @throws IOException if any file operations fail
   */
  public static void combineLuceneIndexFiles(File luceneIndexDir, String outputFilePath)
      throws IOException {
    if (!luceneIndexDir.exists() || !luceneIndexDir.isDirectory()) {
      throw new IllegalArgumentException(
          "Lucene index directory does not exist or is not a directory: " + luceneIndexDir);
    }

    LOGGER.info("Combining Lucene text index files from directory: {}", luceneIndexDir.getAbsolutePath());

    // Step 1: Collect all files and calculate total size
    Map<String, FileInfo> fileInfoMap = collectFiles(luceneIndexDir);
    int fileCount = fileInfoMap.size();

    if (fileCount == 0) {
      throw new IOException("No files found in Lucene index directory: " + luceneIndexDir);
    }

    // Step 2: Calculate total buffer size
    long totalSize = calculateTotalBufferSize(fileInfoMap);

    if (totalSize > Integer.MAX_VALUE) {
      throw new IOException("Combined index size too large: " + totalSize + " bytes");
    }

    // Step 3: Create output file and write data
    File outputFile = new File(outputFilePath);
    try (FileOutputStream fos = new FileOutputStream(outputFile)) {
      // Write header
      writeHeader(fos, fileCount, (int) totalSize);

      // Write file metadata
      long dataOffset = HEADER_SIZE + calculateMetadataSize(fileInfoMap);
      writeFileMetadata(fos, fileInfoMap, dataOffset);

      // Write file data
      writeFileData(fos, fileInfoMap);
    }

    LOGGER.info("Successfully combined {} files into file: {} (size: {} bytes)", fileCount, outputFilePath, totalSize);
  }

  /**
   * Collects all files from the Lucene index directory and their metadata.
   */
  private static Map<String, FileInfo> collectFiles(File luceneIndexDir)
      throws IOException {
    Map<String, FileInfo> fileInfoMap = new TreeMap<>(); // Use TreeMap for consistent ordering

    File[] files = luceneIndexDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isFile()) {
          String fileName = file.getName();
          long fileSize = file.length();

          fileInfoMap.put(fileName, new FileInfo(file, fileName, fileSize));
        }
      }
    }

    return fileInfoMap;
  }

  /**
   * Calculates the total buffer size needed.
   */
  private static long calculateTotalBufferSize(Map<String, FileInfo> fileInfoMap) {
    long totalSize = HEADER_SIZE;
    totalSize += calculateMetadataSize(fileInfoMap);

    for (FileInfo fileInfo : fileInfoMap.values()) {
      totalSize += fileInfo._size;
    }

    return totalSize;
  }

  /**
   * Calculates the size needed for file metadata section.
   */
  private static long calculateMetadataSize(Map<String, FileInfo> fileInfoMap) {
    long metadataSize = 0;
    for (FileInfo fileInfo : fileInfoMap.values()) {
      metadataSize += FILE_METADATA_ENTRY_SIZE + fileInfo._name.length();
    }
    return metadataSize;
  }

  /**
   * Writes the header section to the file.
   */
  private static void writeHeader(FileOutputStream fos, int fileCount, int totalSize)
      throws IOException {
    // Magic number (9 bytes)
    fos.write(MAGIC_NUMBER.getBytes());

    // Version (4 bytes)
    writeInt(fos, VERSION);

    // Total buffer size (8 bytes)
    writeLong(fos, totalSize);

    // File count (4 bytes)
    writeInt(fos, fileCount);

    // Reserved (4 bytes)
    writeInt(fos, 0);
  }

  /**
   * Writes the file metadata section to the file.
   */
  private static void writeFileMetadata(FileOutputStream fos, Map<String, FileInfo> fileInfoMap, long dataOffset)
      throws IOException {
    for (FileInfo fileInfo : fileInfoMap.values()) {
      // File name length (2 bytes)
      writeShort(fos, (short) fileInfo._name.length());

      // File name (variable length)
      fos.write(fileInfo._name.getBytes());

      // File offset (8 bytes)
      writeLong(fos, dataOffset);

      // File size (8 bytes)
      writeLong(fos, fileInfo._size);

      dataOffset += fileInfo._size;
    }
  }

  /**
   * Writes the file data section to the file.
   */
  private static void writeFileData(FileOutputStream fos, Map<String, FileInfo> fileInfoMap)
      throws IOException {
    for (FileInfo fileInfo : fileInfoMap.values()) {
      try {
        byte[] fileData = Files.readAllBytes(fileInfo._file.toPath());
        fos.write(fileData);
        LOGGER.debug("Wrote file {} ({} bytes) to file", fileInfo._name, fileData.length);
      } catch (IOException e) {
        throw new IOException("Failed to read file: " + fileInfo._file.getAbsolutePath(), e);
      }
    }
  }

  /**
   * Extracts files from a combined buffer back to a directory.
   *
   * @param buffer the combined buffer
   * @param outputDir the output directory to extract files to
   * @throws IOException if extraction fails
   */
  public static void extractLuceneIndexFiles(ByteBuffer buffer, File outputDir)
      throws IOException {
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }

    buffer.order(ByteOrder.LITTLE_ENDIAN);

    // Read and validate header
    byte[] magicBytes = new byte[9];
    buffer.get(magicBytes);
    String magic = new String(magicBytes);

    if (!MAGIC_NUMBER.equals(magic)) {
      throw new IOException("Invalid magic number: " + magic);
    }

    int version = buffer.getInt();
    if (version != VERSION) {
      throw new IOException("Unsupported version: " + version);
    }

    long totalSize = buffer.getLong();
    int fileCount = buffer.getInt();
    buffer.getInt(); // Skip reserved bytes

    LOGGER.info("Extracting {} files from buffer (version {}, total size: {} bytes)", fileCount, version, totalSize);

    // Read file metadata
    List<FileInfo> fileInfos = new ArrayList<>();
    for (int i = 0; i < fileCount; i++) {
      short nameLength = buffer.getShort();
      byte[] nameBytes = new byte[nameLength];
      buffer.get(nameBytes);
      String fileName = new String(nameBytes);

      long fileOffset = buffer.getLong();
      long fileSize = buffer.getLong();

      fileInfos.add(new FileInfo(null, fileName, fileSize));
    }

    // Extract file data
    for (FileInfo fileInfo : fileInfos) {
      File outputFile = new File(outputDir, fileInfo._name);
      byte[] fileData = new byte[(int) fileInfo._size];
      buffer.get(fileData);

      FileUtils.writeByteArrayToFile(outputFile, fileData);
      LOGGER.debug("Extracted file {} ({} bytes) to {}", fileInfo._name, fileData.length, outputFile.getAbsolutePath());
    }

    LOGGER.info("Successfully extracted {} files to directory: {}", fileCount, outputDir.getAbsolutePath());
  }

  /**
   * Helper method to write an integer in little-endian format.
   */
  private static void writeInt(FileOutputStream fos, int value)
      throws IOException {
    fos.write(value & 0xFF);
    fos.write((value >> 8) & 0xFF);
    fos.write((value >> 16) & 0xFF);
    fos.write((value >> 24) & 0xFF);
  }

  /**
   * Helper method to write a long in little-endian format.
   */
  private static void writeLong(FileOutputStream fos, long value)
      throws IOException {
    fos.write((int) (value & 0xFF));
    fos.write((int) ((value >> 8) & 0xFF));
    fos.write((int) ((value >> 16) & 0xFF));
    fos.write((int) ((value >> 24) & 0xFF));
    fos.write((int) ((value >> 32) & 0xFF));
    fos.write((int) ((value >> 40) & 0xFF));
    fos.write((int) ((value >> 48) & 0xFF));
    fos.write((int) ((value >> 56) & 0xFF));
  }

  /**
   * Helper method to write a short in little-endian format.
   */
  private static void writeShort(FileOutputStream fos, short value)
      throws IOException {
    fos.write(value & 0xFF);
    fos.write((value >> 8) & 0xFF);
  }

  /**
   * Internal class to hold file information.
   */
  private static class FileInfo {
    final File _file;
    final String _name;
    final long _size;

    FileInfo(File file, String name, long size) {
      _file = file;
      _name = name;
      _size = size;
    }
  }
}
