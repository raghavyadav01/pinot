/**
 * Copyright 2021 StarTree Inc. All rights reserved. Confidential and Proprietary Information of StarTree Inc.
 */
package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.store.Directory;

/**
 * Factory interface for creating Lucene Directory instances from a Path.
 * This allows for dependency injection and easy testing with different directory implementations.
 */
public interface LuceneDirectoryFactory {
  /**
   * Opens a Lucene Directory from the given path
   * @param path the path to the directory
   * @return Directory instance
   * @throws IOException if the directory cannot be opened
   */
  Directory open(Path path) throws IOException;
}
