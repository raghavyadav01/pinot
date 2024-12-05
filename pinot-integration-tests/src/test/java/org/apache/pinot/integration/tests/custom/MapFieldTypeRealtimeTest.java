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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.create;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@Test(suiteName = "CustomClusterIntegrationTest")
public class MapFieldTypeRealtimeTest extends CustomDataQueryClusterIntegrationTest {

  // Default settings
  protected static final String DEFAULT_TABLE_NAME = "MapFieldTypeRealtimeTest";
  private static final int NUM_DOCS = 1000;
  private static final String STRING_MAP_FIELD_NAME = "stringMap";
  private static final String INT_MAP_FIELD_NAME = "intMap";
  private int _setSelectionDefaultDocCount = 10;

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    ComplexFieldSpec stringMapFieldSpec = new ComplexFieldSpec(STRING_MAP_FIELD_NAME, FieldSpec.DataType.MAP, true,
        Map.of(
            ComplexFieldSpec.KEY_FIELD,
            new DimensionFieldSpec(ComplexFieldSpec.KEY_FIELD, FieldSpec.DataType.STRING, true),
            ComplexFieldSpec.VALUE_FIELD,
            new DimensionFieldSpec(ComplexFieldSpec.VALUE_FIELD, FieldSpec.DataType.STRING, true)
        ));
    ComplexFieldSpec intMapFieldSpec = new ComplexFieldSpec(INT_MAP_FIELD_NAME, FieldSpec.DataType.MAP, true,
        Map.of(
            ComplexFieldSpec.KEY_FIELD,
            new DimensionFieldSpec(ComplexFieldSpec.KEY_FIELD, FieldSpec.DataType.STRING, true),
            ComplexFieldSpec.VALUE_FIELD,
            new DimensionFieldSpec(ComplexFieldSpec.VALUE_FIELD, FieldSpec.DataType.INT, true)
        ));

    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addField(stringMapFieldSpec)
        .addField(intMapFieldSpec)
        .addDateTimeField(TIMESTAMP_FIELD_NAME, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS", "1:DAYS")
        .build();
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    List<FieldConfig> fieldConfigs = new ArrayList<>();

    // Create the indexes configuration
    Map<String, Object> indexConfig = new HashMap<>();
    Map<String, Object> mapConfig = new HashMap<>();
    mapConfig.put("mapIndexCreatorClassName",
        "org.apache.pinot.segment.local.segment.index.map.DenseSparseMixedMapIndexCreator");
    mapConfig.put("mapIndexReaderClassName",
        "org.apache.pinot.segment.local.segment.index.map.DenseSparseMixedMapIndexReader");
    mapConfig.put("mapIndexMutableClassName", "org.apache.pinot.segment.local.segment.index.map.MutableMapIndexImpl");
    mapConfig.put("maxDenseKeys", 5);
    mapConfig.put("denseKeys", Arrays.asList("k1", "k2"));
    mapConfig.put("dynamicallyCreateDenseKeys", true);

    Map<String, Object> config = new HashMap<>();
    config.put("configs", mapConfig);

    indexConfig.put("map", config);

    JsonNode indexes = JsonUtils.objectToJsonNode(indexConfig);

    fieldConfigs.add(
        new FieldConfig(STRING_MAP_FIELD_NAME, FieldConfig.EncodingType.RAW, null, null, null, null, indexes, null,
            null));

    fieldConfigs.add(new FieldConfig(INT_MAP_FIELD_NAME, FieldConfig.EncodingType.RAW, null, null, null, null, indexes,
        null, null));

    return fieldConfigs;
  }

  public File createAvroFile()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    org.apache.avro.Schema stringMapAvroSchema =
        org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
    org.apache.avro.Schema intMapAvroSchema =
        org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));
    List<org.apache.avro.Schema.Field> fields =
        Arrays.asList(
            new org.apache.avro.Schema.Field(STRING_MAP_FIELD_NAME, stringMapAvroSchema, null, null),
            new org.apache.avro.Schema.Field(INT_MAP_FIELD_NAME, intMapAvroSchema, null, null),
            new org.apache.avro.Schema.Field(TIMESTAMP_FIELD_NAME, create(org.apache.avro.Schema.Type.LONG), null,
                null));
    avroSchema.setFields(fields);

    File avroFile = new File(_tempDir, "data.avro");
    long tsBase = System.currentTimeMillis();
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < NUM_DOCS; i++) {
        Map<String, String> stringMap = new HashMap<>();
        Map<String, Integer> intMap = new HashMap<>();
        for (int j = 0; j < i; j++) {
          String key = "k" + j;
          stringMap.put(key, String.valueOf(i));
          intMap.put(key, i);
        }
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(STRING_MAP_FIELD_NAME, stringMap);
        record.put(INT_MAP_FIELD_NAME, intMap);
        record.put(TIMESTAMP_FIELD_NAME, tsBase + i);
        fileWriter.append(record);
      }
    }
    return avroFile;
  }

  protected int getSelectionDefaultDocCount() {
    return _setSelectionDefaultDocCount;
  }

  public Set<String> getConsumingSegmentsFromIdealState(String tableNameWithType) {
    IdealState tableIdealState = _controllerStarter.getHelixResourceManager().getTableIdealState(tableNameWithType);
    Map<String, Map<String, String>> segmentAssignment = tableIdealState.getRecord().getMapFields();
    Set<String> matchingSegments = new HashSet<>(HashUtil.getHashMapCapacity(segmentAssignment.size()));
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      Map<String, String> instanceStateMap = entry.getValue();
      if (instanceStateMap.containsValue(CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING)) {
        matchingSegments.add(entry.getKey());
      }
    }
    return matchingSegments;
  }

  private String forceCommit(String tableName)
      throws Exception {
    String response = sendPostRequest(_controllerRequestURLBuilder.forTableForceCommit(tableName), null);
    return JsonUtils.stringToJsonNode(response).get("forceCommitJobId").asText();
  }

  public boolean isForceCommitJobCompleted(String forceCommitJobId)
      throws Exception {
    String jobStatusResponse = sendGetRequest(_controllerRequestURLBuilder.forForceCommitJobStatus(forceCommitJobId));
    JsonNode jobStatus = JsonUtils.stringToJsonNode(jobStatusResponse);

    assertEquals(jobStatus.get("jobId").asText(), forceCommitJobId);
    assertEquals(jobStatus.get("jobType").asText(), "FORCE_COMMIT");
    return jobStatus.get("numberOfSegmentsYetToBeCommitted").asInt(-1) == 0;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {

    Set<String> consumingSegments = getConsumingSegmentsFromIdealState(getTableName() + "_REALTIME");
    String jobId = forceCommit(getTableName());

    TestUtils.waitForCondition(aVoid -> {
      try {
        if (isForceCommitJobCompleted(jobId)) {
          assertTrue(_controllerStarter.getHelixResourceManager()
              .getOnlineSegmentsFromIdealState(getTableName() + "_REALTIME", false).containsAll(consumingSegments));
          return true;
        }
        return false;
      } catch (Exception e) {
        return false;
      }
    }, 60000L, "Error verifying force commit operation on table!");

    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Selection only
    String query = "SELECT * FROM " + getTableName() + " ORDER BY ts";
    JsonNode pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      JsonNode intMap = rows.get(i).get(0);
      JsonNode stringMap = rows.get(i).get(1);
      assertEquals(intMap.size(), i);
      assertEquals(stringMap.size(), i);
      for (int j = 0; j < i; j++) {
        assertEquals(intMap.get("k" + j).intValue(), i);
        assertEquals(stringMap.get("k" + j).textValue(), String.valueOf(i));
      }
    }
    // Selection only
    query = "SELECT stringMap['k0'], intMap['k0'] FROM " + getTableName() + " ORDER BY ts";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());

    assertEquals(rows.get(0).get(0).textValue(), "null");
    assertEquals(rows.get(0).get(1).intValue(), -2147483648);
    for (int i = 1; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).textValue(), String.valueOf(i));
      assertEquals(rows.get(i).get(1).intValue(), i);
    }

    // Selection order-by
    query = "SELECT intMap['k0'], intMap['k1'], stringMap['k0'], stringMap['k1'] FROM " + getTableName()
        + " ORDER BY intMap['k0']";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());

    assertEquals(rows.get(0).get(0).intValue(), -2147483648);
    assertEquals(rows.get(0).get(1).intValue(), -2147483648);
    assertEquals(rows.get(0).get(2).textValue(), "null");
    assertEquals(rows.get(0).get(3).textValue(), "null");
    assertEquals(rows.get(1).get(0).intValue(), 1);
    assertEquals(rows.get(1).get(1).intValue(), -2147483648);
    assertEquals(rows.get(1).get(2).textValue(), "1");
    assertEquals(rows.get(1).get(3).textValue(), "null");
    for (int i = 2; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).intValue(), i);
      assertEquals(rows.get(i).get(1).intValue(), i);
      assertEquals(rows.get(i).get(2).textValue(), String.valueOf(i));
      assertEquals(rows.get(i).get(3).textValue(), String.valueOf(i));
    }

    // Aggregation only
    query = "SELECT MAX(intMap['k0']), MAX(intMap['k1']) FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    assertEquals(pinotResponse.get("resultTable").get("rows").get(0).get(0).intValue(), NUM_DOCS - 1);
    assertEquals(pinotResponse.get("resultTable").get("rows").get(0).get(1).intValue(), NUM_DOCS - 1);

    // Aggregation group-by
    query = "SELECT stringMap['k0'] AS key, MIN(intMap['k0']) AS value FROM " + getTableName()
        + " GROUP BY key ORDER BY value";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), getSelectionDefaultDocCount());
    assertEquals(rows.get(0).get(0).textValue(), "null");
    assertEquals(rows.get(0).get(1).intValue(), Integer.MIN_VALUE);
    for (int i = 1; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).textValue(), String.valueOf(i));
      assertEquals(rows.get(i).get(1).intValue(), i);
    }

    // Filter
    query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['k1']  = '25'";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).textValue(), "25");

    query = "SELECT intMap['k2'] FROM " + getTableName() + " WHERE intMap['k1']  = 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).intValue(), 25);

    // Filter on non-existing key
    query = "SELECT stringMap['k2'] FROM " + getTableName() + " WHERE stringMap['kk']  = '25'";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);
    query = "SELECT intMap['k2'] FROM " + getTableName() + " WHERE intMap['kk']  = 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);

    // Select non-existing key
    query = "SELECT stringMap['kkk'], intMap['kkk'] FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    rows = pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < getSelectionDefaultDocCount(); i++) {
      assertEquals(rows.get(i).get(0).textValue(), "null");
      assertEquals(rows.get(i).get(1).intValue(), Integer.MIN_VALUE);
    }
  }

  @Override
  protected void setUseMultiStageQueryEngine(boolean useMultiStageQueryEngine) {
    super.setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    _setSelectionDefaultDocCount = useMultiStageQueryEngine ? 1000 : 10;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }
}
