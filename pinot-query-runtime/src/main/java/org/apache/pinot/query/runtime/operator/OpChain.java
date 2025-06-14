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
package org.apache.pinot.query.runtime.operator;

import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An {@code OpChain} represents a chain of operators that are separated
 * by send/receive stages.
 */
public class OpChain implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpChain.class);

  private final OpChainId _id;
  private final OpChainExecutionContext _context;
  private final MultiStageOperator _root;
  private final Consumer<OpChainId> _finishCallback;
  private final ThreadExecutionContext _parentContext;

  public OpChain(OpChainExecutionContext context, MultiStageOperator root) {
    this(context, root, (id) -> {
    });
  }

  public OpChain(OpChainExecutionContext context, MultiStageOperator root, Consumer<OpChainId> finishCallback) {
    _context = context;
    _id = context.getId();
    _root = root;
    _finishCallback = finishCallback;
    _parentContext = context.getParentContext();
  }

  public OpChainExecutionContext getContext() {
    return _context;
  }

  public OpChainId getId() {
    return _id;
  }

  public MultiStageOperator getRoot() {
    return _root;
  }

  @Nullable
  public ThreadExecutionContext getParentContext() {
    return _parentContext;
  }

  @Override
  public String toString() {
    return "OpChain{" + _id + "}";
  }

  /**
   * close() is called when we finish execution successfully.
   *
   * Once the {@link OpChain} is being executed, this method should only be called from the thread that is actually
   * executing it.
   */
  @Override
  public void close() {
    try {
      _root.close();
    } finally {
      _finishCallback.accept(getId());
      LOGGER.trace("OpChain callback called");
    }
  }

  /**
   * cancel() is called when execution runs into error.
   *
   * Once the {@link OpChain} is being executed, this method should only be called from the thread that is actually
   * executing it.
   * @param e
   */
  public void cancel(Throwable e) {
    try {
      _root.cancel(e);
    } finally {
      _finishCallback.accept(getId());
      LOGGER.trace("OpChain callback called");
    }
  }
}
