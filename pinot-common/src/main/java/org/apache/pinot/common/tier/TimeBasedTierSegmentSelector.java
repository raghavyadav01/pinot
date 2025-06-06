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
package org.apache.pinot.common.tier;

import com.google.common.base.Preconditions;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.utils.TimeUtils;


/**
 * A {@link TierSegmentSelector} strategy which selects segments for a tier based on the age of the segment
 */
public class TimeBasedTierSegmentSelector implements TierSegmentSelector {
  private final long _segmentAgeMillis;

  public TimeBasedTierSegmentSelector(String segmentAge) {
    _segmentAgeMillis = TimeUtils.convertPeriodToMillis(segmentAge);
  }

  @Override
  public String getType() {
    return TierFactory.TIME_SEGMENT_SELECTOR_TYPE;
  }

  @Override
  public boolean selectSegment(String tableNameWithType, SegmentZKMetadata segmentZKMetadata) {
    // don't try to move consuming segments
    if (!segmentZKMetadata.getStatus().isCompleted()) {
      return false;
    }

    // get segment end time to decide if segment gets selected
    long endTimeMs = segmentZKMetadata.getEndTimeMs();
    Preconditions.checkState(endTimeMs > 0, "Invalid endTimeMs: %s for segment: %s of table: %s", endTimeMs,
        segmentZKMetadata.getSegmentName(), tableNameWithType);
    return (System.currentTimeMillis() - endTimeMs) > _segmentAgeMillis;
  }

  /**
   * Gets the age cutoff for segments accepted by this strategy
   */
  public long getSegmentAgeMillis() {
    return _segmentAgeMillis;
  }

  @Override
  public String toString() {
    return "TimeBasedTierSegmentSelector{_segmentAgeMillis=" + _segmentAgeMillis + "}";
  }
}
