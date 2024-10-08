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
package org.apache.pinot.core.operator.docvalsets;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ArrayCopyUtils;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.roaringbitmap.RoaringBitmap;


/**
 * A {@link BlockValSet} implementation backed by row major data.
 *
 * TODO: Support MV
 */
public class RowBasedBlockValSet implements BlockValSet {
  private final DataType _dataType;
  private final DataType _storedType;
  private final List<Object[]> _rows;
  private final int _colId;
  private final RoaringBitmap _nullBitmap;
  private final Object _nullPlaceHolder;

  public RowBasedBlockValSet(ColumnDataType columnDataType, List<Object[]> rows, int colId,
      boolean nullHandlingEnabled) {
    _dataType = columnDataType.toDataType();
    _storedType = _dataType.getStoredType();
    _rows = rows;
    _colId = colId;
    _nullPlaceHolder = columnDataType.getNullPlaceholder();

    if (nullHandlingEnabled) {
      RoaringBitmap nullBitmap;
      int numRows = rows.size();
      if (_dataType == DataType.UNKNOWN) {
        nullBitmap = new RoaringBitmap();
        nullBitmap.add(0L, numRows);
      } else {
        nullBitmap = new RoaringBitmap();
        for (int i = 0; i < numRows; i++) {
          if (rows.get(i)[colId] == null) {
            nullBitmap.add(i);
          }
        }
      }
      _nullBitmap = nullBitmap.isEmpty() ? null : nullBitmap;
    } else {
      _nullBitmap = null;
    }
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap() {
    return _nullBitmap;
  }

  @Override
  public DataType getValueType() {
    return _dataType;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public int[] getDictionaryIdsSV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getIntValuesSV() {
    int numRows = _rows.size();
    int[] values = new int[numRows];
    if (numRows == 0 || _dataType == DataType.UNKNOWN) {
      return values;
    }
    if (_nullBitmap == null) {
      if (_storedType.isNumeric()) {
        for (int i = 0; i < numRows; i++) {
          values[i] = ((Number) _rows.get(i)[_colId]).intValue();
        }
      } else if (_storedType == DataType.STRING) {
        for (int i = 0; i < numRows; i++) {
          values[i] = Integer.parseInt((String) _rows.get(i)[_colId]);
        }
      } else {
        throw new IllegalStateException("Cannot read int values from data type: " + _dataType);
      }
    } else {
      if (_storedType.isNumeric()) {
        for (int i = 0; i < numRows; i++) {
          Number value = (Number) _rows.get(i)[_colId];
          if (value != null) {
            values[i] = value.intValue();
          }
        }
      } else if (_storedType == DataType.STRING) {
        for (int i = 0; i < numRows; i++) {
          String value = (String) _rows.get(i)[_colId];
          if (value != null) {
            values[i] = Integer.parseInt(value);
          }
        }
      } else {
        throw new IllegalStateException("Cannot read int values from data type: " + _dataType);
      }
    }
    return values;
  }

  @Override
  public long[] getLongValuesSV() {
    int numRows = _rows.size();
    long[] values = new long[numRows];
    if (numRows == 0 || _dataType == DataType.UNKNOWN) {
      return values;
    }
    if (_nullBitmap == null) {
      if (_storedType.isNumeric()) {
        for (int i = 0; i < numRows; i++) {
          values[i] = ((Number) _rows.get(i)[_colId]).longValue();
        }
      } else if (_storedType == DataType.STRING) {
        for (int i = 0; i < numRows; i++) {
          values[i] = Long.parseLong((String) _rows.get(i)[_colId]);
        }
      } else {
        throw new IllegalStateException("Cannot read long values from data type: " + _dataType);
      }
    } else {
      if (_storedType.isNumeric()) {
        for (int i = 0; i < numRows; i++) {
          Number value = (Number) _rows.get(i)[_colId];
          if (value != null) {
            values[i] = value.longValue();
          }
        }
      } else if (_storedType == DataType.STRING) {
        for (int i = 0; i < numRows; i++) {
          String value = (String) _rows.get(i)[_colId];
          if (value != null) {
            values[i] = Long.parseLong(value);
          }
        }
      } else {
        throw new IllegalStateException("Cannot read long values from data type: " + _dataType);
      }
    }
    return values;
  }

  @Override
  public float[] getFloatValuesSV() {
    int numRows = _rows.size();
    float[] values = new float[numRows];
    if (numRows == 0 || _dataType == DataType.UNKNOWN) {
      return values;
    }
    if (_nullBitmap == null) {
      if (_storedType.isNumeric()) {
        for (int i = 0; i < numRows; i++) {
          values[i] = ((Number) _rows.get(i)[_colId]).floatValue();
        }
      } else if (_storedType == DataType.STRING) {
        for (int i = 0; i < numRows; i++) {
          values[i] = Float.parseFloat((String) _rows.get(i)[_colId]);
        }
      } else {
        throw new IllegalStateException("Cannot read float values from data type: " + _dataType);
      }
    } else {
      if (_storedType.isNumeric()) {
        for (int i = 0; i < numRows; i++) {
          Number value = (Number) _rows.get(i)[_colId];
          if (value != null) {
            values[i] = value.floatValue();
          }
        }
      } else if (_storedType == DataType.STRING) {
        for (int i = 0; i < numRows; i++) {
          String value = (String) _rows.get(i)[_colId];
          if (value != null) {
            values[i] = Float.parseFloat(value);
          }
        }
      } else {
        throw new IllegalStateException("Cannot read float values from data type: " + _dataType);
      }
    }
    return values;
  }

  @Override
  public double[] getDoubleValuesSV() {
    int numRows = _rows.size();
    double[] values = new double[numRows];
    if (numRows == 0 || _dataType == DataType.UNKNOWN) {
      return values;
    }
    if (_nullBitmap == null) {
      if (_storedType.isNumeric()) {
        for (int i = 0; i < numRows; i++) {
          values[i] = ((Number) _rows.get(i)[_colId]).doubleValue();
        }
      } else if (_storedType == DataType.STRING) {
        for (int i = 0; i < numRows; i++) {
          values[i] = Double.parseDouble((String) _rows.get(i)[_colId]);
        }
      } else {
        throw new IllegalStateException("Cannot read double values from data type: " + _dataType);
      }
    } else {
      if (_storedType.isNumeric()) {
        for (int i = 0; i < numRows; i++) {
          Number value = (Number) _rows.get(i)[_colId];
          if (value != null) {
            values[i] = value.doubleValue();
          }
        }
      } else if (_storedType == DataType.STRING) {
        for (int i = 0; i < numRows; i++) {
          String value = (String) _rows.get(i)[_colId];
          if (value != null) {
            values[i] = Double.parseDouble(value);
          }
        }
      } else {
        throw new IllegalStateException("Cannot read double values from data type: " + _dataType);
      }
    }
    return values;
  }

  @Override
  public BigDecimal[] getBigDecimalValuesSV() {
    int numRows = _rows.size();
    BigDecimal[] values = new BigDecimal[numRows];
    if (numRows == 0) {
      return values;
    }
    if (_dataType == DataType.UNKNOWN) {
      Arrays.fill(values, NullValuePlaceHolder.BIG_DECIMAL);
      return values;
    }
    if (_nullBitmap == null) {
      switch (_storedType) {
        case INT:
        case LONG:
          for (int i = 0; i < numRows; i++) {
            values[i] = BigDecimal.valueOf(((Number) _rows.get(i)[_colId]).longValue());
          }
          break;
        case FLOAT:
        case DOUBLE:
        case STRING:
          for (int i = 0; i < numRows; i++) {
            values[i] = new BigDecimal(_rows.get(i)[_colId].toString());
          }
          break;
        case BIG_DECIMAL:
          for (int i = 0; i < numRows; i++) {
            values[i] = (BigDecimal) _rows.get(i)[_colId];
          }
          break;
        case BYTES:
          for (int i = 0; i < numRows; i++) {
            values[i] = BigDecimalUtils.deserialize((ByteArray) _rows.get(i)[_colId]);
          }
          break;
        default:
          throw new IllegalStateException("Cannot read BigDecimal values from data type: " + _dataType);
      }
    } else {
      switch (_storedType) {
        case INT:
        case LONG:
          for (int i = 0; i < numRows; i++) {
            Number value = (Number) _rows.get(i)[_colId];
            values[i] = value != null ? BigDecimal.valueOf(value.longValue()) : NullValuePlaceHolder.BIG_DECIMAL;
          }
          break;
        case FLOAT:
        case DOUBLE:
        case STRING:
          for (int i = 0; i < numRows; i++) {
            Object value = _rows.get(i)[_colId];
            values[i] = value != null ? new BigDecimal(value.toString()) : NullValuePlaceHolder.BIG_DECIMAL;
          }
          break;
        case BIG_DECIMAL:
          for (int i = 0; i < numRows; i++) {
            BigDecimal value = (BigDecimal) _rows.get(i)[_colId];
            values[i] = value != null ? value : NullValuePlaceHolder.BIG_DECIMAL;
          }
          break;
        case BYTES:
          for (int i = 0; i < numRows; i++) {
            ByteArray value = (ByteArray) _rows.get(i)[_colId];
            values[i] = value != null ? BigDecimalUtils.deserialize(value) : NullValuePlaceHolder.BIG_DECIMAL;
          }
          break;
        default:
          throw new IllegalStateException("Cannot read BigDecimal values from data type: " + _dataType);
      }
    }
    return values;
  }

  @Override
  public String[] getStringValuesSV() {
    int numRows = _rows.size();
    String[] values = new String[numRows];
    if (numRows == 0) {
      return values;
    }
    if (_dataType == DataType.UNKNOWN) {
      Arrays.fill(values, NullValuePlaceHolder.STRING);
      return values;
    }
    if (_nullBitmap == null) {
      for (int i = 0; i < numRows; i++) {
        values[i] = _rows.get(i)[_colId].toString();
      }
    } else {
      for (int i = 0; i < numRows; i++) {
        Object value = _rows.get(i)[_colId];
        values[i] = value != null ? value.toString() : NullValuePlaceHolder.STRING;
      }
    }
    return values;
  }

  @Override
  public byte[][] getBytesValuesSV() {
    int numRows = _rows.size();
    byte[][] values = new byte[numRows][];
    if (numRows == 0) {
      return values;
    }
    if (_dataType == DataType.UNKNOWN) {
      Arrays.fill(values, NullValuePlaceHolder.BYTES);
      return values;
    }
    if (_nullBitmap == null) {
      if (_storedType == DataType.BYTES) {
        for (int i = 0; i < numRows; i++) {
          values[i] = ((ByteArray) _rows.get(i)[_colId]).getBytes();
        }
      } else {
        throw new IllegalStateException("Cannot read bytes values from data type: " + _dataType);
      }
    } else {
      if (_storedType == DataType.BYTES) {
        for (int i = 0; i < numRows; i++) {
          ByteArray value = (ByteArray) _rows.get(i)[_colId];
          values[i] = value != null ? value.getBytes() : NullValuePlaceHolder.BYTES;
        }
      } else {
        throw new IllegalStateException("Cannot read bytes values from data type: " + _dataType);
      }
    }
    return values;
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] getIntValuesMV() {
    int numRows = _rows.size();
    int[][] values = new int[numRows][];
    if (numRows == 0) {
      return values;
    }
    if (_dataType == DataType.UNKNOWN) {
      Arrays.fill(values, new int[0]);
      return values;
    }

    for (int i = 0; i < numRows; i++) {
      Object storedValue = _rows.get(i)[_colId];
      if (storedValue instanceof int[]) {
        values[i] = (int[]) storedValue;
      } else if (storedValue instanceof long[]) {
        long[] longArray = (long[]) storedValue;
        values[i] = new int[longArray.length];
        ArrayCopyUtils.copy(longArray, values[i], longArray.length);
      } else if (storedValue instanceof float[]) {
        float[] floatArray = (float[]) storedValue;
        values[i] = new int[floatArray.length];
        ArrayCopyUtils.copy(floatArray, values[i], floatArray.length);
      } else if (storedValue instanceof double[]) {
        double[] doubleArray = (double[]) storedValue;
        values[i] = new int[doubleArray.length];
        ArrayCopyUtils.copy(doubleArray, values[i], doubleArray.length);
      } else if (storedValue instanceof String[]) {
        String[] stringArray = (String[]) storedValue;
        values[i] = new int[stringArray.length];
        for (int j = 0; j < stringArray.length; j++) {
          values[i][j] = Integer.parseInt(stringArray[j]);
        }
      } else {
        throw new IllegalStateException("Unsupported data type: " + storedValue.getClass().getName());
      }
    }
    return values;
  }

  @Override
  public long[][] getLongValuesMV() {
    int numRows = _rows.size();
    long[][] values = new long[numRows][];
    if (numRows == 0) {
      return values;
    }
    if (_dataType == DataType.UNKNOWN) {
      Arrays.fill(values, new long[0]);
      return values;
    }
    for (int i = 0; i < numRows; i++) {
      Object storedValue = _rows.get(i)[_colId];
      if (storedValue instanceof int[]) {
        int[] intArray = (int[]) storedValue;
        values[i] = new long[intArray.length];
        ArrayCopyUtils.copy(intArray, values[i], intArray.length);
      } else if (storedValue instanceof long[]) {
        values[i] = (long[]) storedValue;
      } else if (storedValue instanceof float[]) {
        float[] floatArray = (float[]) storedValue;
        values[i] = new long[floatArray.length];
        ArrayCopyUtils.copy(floatArray, values[i], floatArray.length);
      } else if (storedValue instanceof double[]) {
        double[] doubleArray = (double[]) storedValue;
        values[i] = new long[doubleArray.length];
        ArrayCopyUtils.copy(doubleArray, values[i], doubleArray.length);
      } else if (storedValue instanceof String[]) {
        String[] stringArray = (String[]) storedValue;
        values[i] = new long[stringArray.length];
        for (int j = 0; j < stringArray.length; j++) {
          values[i][j] = Long.parseLong(stringArray[j]);
        }
      } else {
        throw new IllegalStateException("Unsupported data type: " + storedValue.getClass().getName());
      }
    }
    return values;
  }

  @Override
  public float[][] getFloatValuesMV() {
    int numRows = _rows.size();
    float[][] values = new float[numRows][];
    if (numRows == 0) {
      return values;
    }
    if (_dataType == DataType.UNKNOWN) {
      Arrays.fill(values, new float[0]);
      return values;
    }
    for (int i = 0; i < numRows; i++) {
      Object storedValue = _rows.get(i)[_colId];
      if (storedValue instanceof int[]) {
        int[] intArray = (int[]) storedValue;
        values[i] = new float[intArray.length];
        ArrayCopyUtils.copy(intArray, values[i], intArray.length);
      } else if (storedValue instanceof long[]) {
        long[] longArray = (long[]) storedValue;
        values[i] = new float[longArray.length];
        ArrayCopyUtils.copy(longArray, values[i], longArray.length);
      } else if (storedValue instanceof float[]) {
        values[i] = (float[]) storedValue;
      } else if (storedValue instanceof double[]) {
        double[] doubleArray = (double[]) storedValue;
        values[i] = new float[doubleArray.length];
        ArrayCopyUtils.copy(doubleArray, values[i], doubleArray.length);
      } else if (storedValue instanceof String[]) {
        String[] stringArray = (String[]) storedValue;
        values[i] = new float[stringArray.length];
        for (int j = 0; j < stringArray.length; j++) {
          values[i][j] = Float.parseFloat(stringArray[j]);
        }
      } else {
        throw new IllegalStateException("Unsupported data type: " + storedValue.getClass().getName());
      }
    }
    return values;
  }

  @Override
  public double[][] getDoubleValuesMV() {
    int numRows = _rows.size();
    double[][] values = new double[numRows][];
    if (numRows == 0) {
      return values;
    }
    if (_dataType == DataType.UNKNOWN) {
      Arrays.fill(values, new double[0]);
      return values;
    }
    for (int i = 0; i < numRows; i++) {
      Object storedValue = _rows.get(i)[_colId];
      if (storedValue instanceof int[]) {
        int[] intArray = (int[]) storedValue;
        values[i] = new double[intArray.length];
        ArrayCopyUtils.copy(intArray, values[i], intArray.length);
      } else if (storedValue instanceof long[]) {
        long[] longArray = (long[]) storedValue;
        values[i] = new double[longArray.length];
        ArrayCopyUtils.copy(longArray, values[i], longArray.length);
      } else if (storedValue instanceof float[]) {
        float[] floatArray = (float[]) storedValue;
        values[i] = new double[floatArray.length];
        ArrayCopyUtils.copy(floatArray, values[i], floatArray.length);
      } else if (storedValue instanceof double[]) {
        values[i] = (double[]) storedValue;
      } else if (storedValue instanceof String[]) {
        String[] stringArray = (String[]) storedValue;
        values[i] = new double[stringArray.length];
        for (int j = 0; j < stringArray.length; j++) {
          values[i][j] = Double.parseDouble(stringArray[j]);
        }
      } else {
        throw new IllegalStateException("Unsupported data type: " + storedValue.getClass().getName());
      }
    }
    return values;
  }

  @Override
  public String[][] getStringValuesMV() {
    int numRows = _rows.size();
    String[][] values = new String[numRows][];
    if (numRows == 0) {
      return values;
    }
    if (_dataType == DataType.UNKNOWN) {
      Arrays.fill(values, new String[0]);
      return values;
    }
    for (int i = 0; i < numRows; i++) {
      Object storedValue = _rows.get(i)[_colId];
      if (storedValue instanceof int[]) {
        int[] intArray = (int[]) storedValue;
        values[i] = new String[intArray.length];
        for (int j = 0; j < intArray.length; j++) {
          values[i][j] = Integer.toString(intArray[j]);
        }
      } else if (storedValue instanceof long[]) {
        long[] longArray = (long[]) storedValue;
        values[i] = new String[longArray.length];
        for (int j = 0; j < longArray.length; j++) {
          values[i][j] = Long.toString(longArray[j]);
        }
      } else if (storedValue instanceof float[]) {
        float[] floatArray = (float[]) storedValue;
        values[i] = new String[floatArray.length];
        for (int j = 0; j < floatArray.length; j++) {
          values[i][j] = Float.toString(floatArray[j]);
        }
      } else if (storedValue instanceof double[]) {
        double[] doubleArray = (double[]) storedValue;
        values[i] = new String[doubleArray.length];
        for (int j = 0; j < doubleArray.length; j++) {
          values[i][j] = Double.toString(doubleArray[j]);
        }
      } else if (storedValue instanceof String[]) {
        values[i] = (String[]) storedValue;
      } else {
        throw new IllegalStateException("Unsupported data type: " + storedValue.getClass().getName());
      }
    }
    return values;
  }

  @Override
  public byte[][][] getBytesValuesMV() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getNumMVEntries() {
    throw new UnsupportedOperationException();
  }
}
