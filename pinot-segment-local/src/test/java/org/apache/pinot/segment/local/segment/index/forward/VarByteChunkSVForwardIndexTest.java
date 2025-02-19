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
package org.apache.pinot.segment.local.segment.index.forward;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.fail;


/**
 * Unit test for {@link VarByteChunkSVForwardIndexReader} and {@link VarByteChunkForwardIndexWriter} classes.
 */
public class VarByteChunkSVForwardIndexTest implements PinotBuffersAfterMethodCheckRule {
  private static final int NUM_ENTRIES = 5003;
  private static final int NUM_DOCS_PER_CHUNK = 1009;
  private static final int MAX_STRING_LENGTH = 101;
  private static final String TEST_FILE = System.getProperty("java.io.tmpdir") + File.separator + "varByteSVRTest";

  @Test
  public void testWithCompression()
      throws Exception {
    test(ChunkCompressionType.SNAPPY);
  }

  @Test
  public void testWithoutCompression()
      throws Exception {
    test(ChunkCompressionType.PASS_THROUGH);
  }

  @Test
  public void testWithZstandardCompression()
      throws Exception {
    test(ChunkCompressionType.ZSTANDARD);
  }

  @Test
  public void testWithLZ4Compression()
      throws Exception {
    test(ChunkCompressionType.LZ4);
  }

  @Test
  public void testWithGZIPCompression()
      throws Exception {
    test(ChunkCompressionType.GZIP);
  }

  /**
   * This test writes {@link #NUM_ENTRIES} using {@link VarByteChunkForwardIndexWriter}. It then reads
   * the strings & bytes using {@link VarByteChunkSVForwardIndexReader}, and asserts that what was written is the
   * same as
   * what was read in.
   *
   * Number of docs and docs per chunk are chosen to generate complete as well partial chunks.
   *
   * @param compressionType Compression type
   * @throws Exception
   */
  public void test(ChunkCompressionType compressionType)
      throws Exception {
    String[] expected = new String[NUM_ENTRIES];
    Random random = new Random();

    File outFileFourByte = new File(TEST_FILE);
    File outFileEightByte = new File(TEST_FILE + "8byte");
    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);

    int maxStringLengthInBytes = 0;
    for (int i = 0; i < NUM_ENTRIES; i++) {
      String value = RandomStringUtils.random(random.nextInt(MAX_STRING_LENGTH));
      expected[i] = value;
      maxStringLengthInBytes = Math.max(maxStringLengthInBytes, value.getBytes(UTF_8).length);
    }

    // test both formats (4-byte chunk offsets and 8-byte chunk offsets)
    try (VarByteChunkForwardIndexWriter fourByteOffsetWriter = new VarByteChunkForwardIndexWriter(outFileFourByte,
        compressionType, NUM_ENTRIES, NUM_DOCS_PER_CHUNK, maxStringLengthInBytes, 2);
        VarByteChunkForwardIndexWriter eightByteOffsetWriter = new VarByteChunkForwardIndexWriter(outFileEightByte,
            compressionType, NUM_ENTRIES, NUM_DOCS_PER_CHUNK, maxStringLengthInBytes, 3)) {
      // NOTE: No need to test BYTES explicitly because STRING is handled as UTF-8 encoded bytes
      for (int i = 0; i < NUM_ENTRIES; i++) {
        fourByteOffsetWriter.putString(expected[i]);
        eightByteOffsetWriter.putString(expected[i]);
      }
    }

    try (PinotDataBuffer buffer1 = PinotDataBuffer.mapReadOnlyBigEndianFile(outFileFourByte);
        VarByteChunkSVForwardIndexReader fourByteOffsetReader = new VarByteChunkSVForwardIndexReader(
            buffer1, DataType.STRING);
        ChunkReaderContext fourByteOffsetReaderContext = fourByteOffsetReader.createContext();
        PinotDataBuffer buffer2 = PinotDataBuffer.mapReadOnlyBigEndianFile(outFileEightByte);
        VarByteChunkSVForwardIndexReader eightByteOffsetReader = new VarByteChunkSVForwardIndexReader(
            buffer2, DataType.STRING);
        ChunkReaderContext eightByteOffsetReaderContext = eightByteOffsetReader.createContext()) {
      for (int i = 0; i < NUM_ENTRIES; i++) {
        Assert.assertEquals(fourByteOffsetReader.getString(i, fourByteOffsetReaderContext), expected[i]);
        Assert.assertEquals(eightByteOffsetReader.getString(i, eightByteOffsetReaderContext), expected[i]);
      }
    }

    FileUtils.deleteQuietly(outFileFourByte);
    FileUtils.deleteQuietly(outFileEightByte);
  }

  /**
   * This test ensures that the reader can read in an data file from version 1.
   */
  @Test
  public void testBackwardCompatibilityV1()
      throws Exception {
    String[] expected = new String[]{"abcde", "fgh", "ijklmn", "12345"};
    testBackwardCompatibilityHelper("data/varByteStrings.v1", expected, 1009);
  }

  /**
   * This test ensures that the reader can read in an data file from version 2.
   */
  @Test
  public void testBackwardCompatibilityV2()
      throws Exception {
    String[] data = {"abcdefghijk", "12456887", "pqrstuv", "500"};
    testBackwardCompatibilityHelper("data/varByteStringsCompressed.v2", data, 1000);
    testBackwardCompatibilityHelper("data/varByteStringsRaw.v2", data, 1000);
  }

  private void testBackwardCompatibilityHelper(String fileName, String[] data, int numDocs)
      throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL resource = classLoader.getResource(fileName);
    if (resource == null) {
      throw new RuntimeException("Input file not found: " + fileName);
    }
    File file = new File(resource.getFile());
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        VarByteChunkSVForwardIndexReader reader = new VarByteChunkSVForwardIndexReader(buffer, DataType.STRING);
        ChunkReaderContext readerContext = reader.createContext()) {
      for (int i = 0; i < numDocs; i++) {
        String actual = reader.getString(i, readerContext);
        Assert.assertEquals(actual, data[i % data.length]);
      }
    }
  }

  @Test
  public void testVarCharWithDifferentSizes()
      throws Exception {
    testLargeVarcharHelper(ChunkCompressionType.SNAPPY, 10, 1000);
    testLargeVarcharHelper(ChunkCompressionType.PASS_THROUGH, 10, 1000);
    testLargeVarcharHelper(ChunkCompressionType.ZSTANDARD, 10, 1000);
    testLargeVarcharHelper(ChunkCompressionType.LZ4, 10, 1000);
    testLargeVarcharHelper(ChunkCompressionType.GZIP, 10, 1000);

    testLargeVarcharHelper(ChunkCompressionType.SNAPPY, 100, 1000);
    testLargeVarcharHelper(ChunkCompressionType.PASS_THROUGH, 100, 1000);
    testLargeVarcharHelper(ChunkCompressionType.ZSTANDARD, 100, 1000);
    testLargeVarcharHelper(ChunkCompressionType.LZ4, 100, 1000);
    testLargeVarcharHelper(ChunkCompressionType.GZIP, 100, 1000);

    testLargeVarcharHelper(ChunkCompressionType.SNAPPY, 1000, 1000);
    testLargeVarcharHelper(ChunkCompressionType.PASS_THROUGH, 1000, 1000);
    testLargeVarcharHelper(ChunkCompressionType.ZSTANDARD, 1000, 1000);
    testLargeVarcharHelper(ChunkCompressionType.LZ4, 1000, 1000);
    testLargeVarcharHelper(ChunkCompressionType.GZIP, 1000, 1000);

    testLargeVarcharHelper(ChunkCompressionType.SNAPPY, 10000, 100);
    testLargeVarcharHelper(ChunkCompressionType.PASS_THROUGH, 10000, 100);
    testLargeVarcharHelper(ChunkCompressionType.ZSTANDARD, 10000, 100);
    testLargeVarcharHelper(ChunkCompressionType.LZ4, 10000, 100);
    testLargeVarcharHelper(ChunkCompressionType.GZIP, 10000, 100);

    testLargeVarcharHelper(ChunkCompressionType.SNAPPY, 100000, 10);
    testLargeVarcharHelper(ChunkCompressionType.PASS_THROUGH, 100000, 10);
    testLargeVarcharHelper(ChunkCompressionType.ZSTANDARD, 100000, 10);
    testLargeVarcharHelper(ChunkCompressionType.LZ4, 100000, 10);
    testLargeVarcharHelper(ChunkCompressionType.GZIP, 100000, 10);

    testLargeVarcharHelper(ChunkCompressionType.SNAPPY, 1000000, 10);
    testLargeVarcharHelper(ChunkCompressionType.PASS_THROUGH, 1000000, 10);
    testLargeVarcharHelper(ChunkCompressionType.ZSTANDARD, 1000000, 10);
    testLargeVarcharHelper(ChunkCompressionType.LZ4, 1000000, 10);
    testLargeVarcharHelper(ChunkCompressionType.GZIP, 1000000, 10);

    testLargeVarcharHelper(ChunkCompressionType.SNAPPY, 2000000, 10);
    testLargeVarcharHelper(ChunkCompressionType.PASS_THROUGH, 2000000, 10);
    testLargeVarcharHelper(ChunkCompressionType.ZSTANDARD, 2000000, 10);
    testLargeVarcharHelper(ChunkCompressionType.LZ4, 2000000, 10);
    testLargeVarcharHelper(ChunkCompressionType.GZIP, 2000000, 10);
  }

  private void testLargeVarcharHelper(ChunkCompressionType compressionType, int numChars, int numDocs)
      throws Exception {
    String[] expected = new String[numDocs];
    Random random = new Random();

    File outFile = new File(TEST_FILE);
    FileUtils.deleteQuietly(outFile);

    int maxStringLengthInBytes = 0;
    for (int i = 0; i < numDocs; i++) {
      String value = RandomStringUtils.random(random.nextInt(numChars));
      expected[i] = value;
      maxStringLengthInBytes = Math.max(maxStringLengthInBytes, value.getBytes(UTF_8).length);
    }

    int numDocsPerChunk = SingleValueVarByteRawIndexCreator.getNumDocsPerChunk(maxStringLengthInBytes, 1024 * 1024);
    try (VarByteChunkForwardIndexWriter writer = new VarByteChunkForwardIndexWriter(outFile, compressionType, numDocs,
        numDocsPerChunk, maxStringLengthInBytes, 3)) {
      // NOTE: No need to test BYTES explicitly because STRING is handled as UTF-8 encoded bytes
      for (int i = 0; i < numDocs; i++) {
        writer.putString(expected[i]);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(outFile);
        VarByteChunkSVForwardIndexReader reader = new VarByteChunkSVForwardIndexReader(buffer, DataType.STRING);
        ChunkReaderContext readerContext = reader.createContext()) {
      for (int i = 0; i < numDocs; i++) {
        Assert.assertEquals(reader.getString(i, readerContext), expected[i]);
      }
    }

    // For large variable width column values (where total size of data
    // across all rows in the segment is > 2GB), Pinot may try to use different buffer
    // implementation when the fwd index. However, to test this scenario the unit test
    // will take a long time to execute due to comparison
    // (75000 characters in each row and 10000 rows will hit this scenario).
    // So we specifically test for mapping the index file using the default factory
    // trying to exercise the buffer used in larger cases
    try (PinotDataBuffer buffer = PinotDataBuffer.createDefaultFactory(false)
        .mapFile(outFile, outFile.canRead(), 0, outFile.length(), ByteOrder.BIG_ENDIAN)) {

      assert !(buffer instanceof PinotByteBuffer) : "This test tries to exercise the long buffer algorithm";

      try (VarByteChunkSVForwardIndexReader reader = new VarByteChunkSVForwardIndexReader(buffer, DataType.STRING);
          ChunkReaderContext readerContext = reader.createContext()) {
        for (int i = 0; i < numDocs; i++) {
          Assert.assertEquals(reader.getString(i, readerContext), expected[i]);
        }
      }

      FileUtils.deleteQuietly(outFile);
    }
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testV2IntegerOverflow()
      throws IOException {
    File file = Files.createTempFile(getClass().getSimpleName(), "big-file").toFile();
    file.deleteOnExit();
    int docSize = 21475;
    byte[] value = StringUtils.repeat("a", docSize).getBytes(UTF_8);
    try (VarByteChunkForwardIndexWriter writer = new VarByteChunkForwardIndexWriter(file,
        ChunkCompressionType.PASS_THROUGH, 100_001, 1000, docSize, 2)) {
      try {
        for (int i = 0; i < 100_000; i++) {
          writer.putBytes(value);
        }
      } catch (Throwable t) {
        fail("failed too early", t);
      }
      writer.putBytes(value);
    }
  }
}
