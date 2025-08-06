/**
 * Copyright 2021 StarTree Inc. All rights reserved. Confidential and Proprietary Information of StarTree Inc.
 */
package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Registry for LuceneDirectoryFactory that provides a centralized way to manage
 * directory factory instances. Defaults to FSDirectory for backward compatibility.
 */
public class LuceneDirectoryFactoryRegistry {
  private static LuceneDirectoryFactory _factory = new DefaultLuceneDirectoryFactory();

  /**
   * Sets the directory factory to use
   * @param factory the factory to set
   */
  public static void setFactory(LuceneDirectoryFactory factory) {
    _factory = factory;
  }

  /**
   * Gets the current directory factory
   * @return the current factory
   */
  public static LuceneDirectoryFactory getFactory() {
    return _factory;
  }

  /**
   * Opens a directory using the current factory
   * @param path the path to open
   * @return the opened directory
   * @throws IOException if the directory cannot be opened
   */
  public static Directory open(Path path) throws IOException {
    return _factory.open(path);
  }

  /**
   * Default implementation that uses FSDirectory
   */
  private static class DefaultLuceneDirectoryFactory implements LuceneDirectoryFactory {
    @Override
    public Directory open(Path path) throws IOException {
      return FSDirectory.open(path);
    }
  }
}
