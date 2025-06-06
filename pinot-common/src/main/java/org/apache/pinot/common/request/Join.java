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
/**
 * Autogenerated by Thrift Compiler (0.21.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.pinot.common.request;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.21.0)", date = "2025-04-16")
public class Join implements org.apache.thrift.TBase<Join, Join._Fields>, java.io.Serializable, Cloneable, Comparable<Join> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Join");

  private static final org.apache.thrift.protocol.TField TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("type", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField LEFT_FIELD_DESC = new org.apache.thrift.protocol.TField("left", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField RIGHT_FIELD_DESC = new org.apache.thrift.protocol.TField("right", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField CONDITION_FIELD_DESC = new org.apache.thrift.protocol.TField("condition", org.apache.thrift.protocol.TType.STRUCT, (short)4);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new JoinStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new JoinTupleSchemeFactory();

  private @org.apache.thrift.annotation.Nullable JoinType type; // required
  private @org.apache.thrift.annotation.Nullable DataSource left; // required
  private @org.apache.thrift.annotation.Nullable DataSource right; // required
  private @org.apache.thrift.annotation.Nullable Expression condition; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * 
     * @see JoinType
     */
    TYPE((short)1, "type"),
    LEFT((short)2, "left"),
    RIGHT((short)3, "right"),
    CONDITION((short)4, "condition");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // TYPE
          return TYPE;
        case 2: // LEFT
          return LEFT;
        case 3: // RIGHT
          return RIGHT;
        case 4: // CONDITION
          return CONDITION;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    @Override
    public short getThriftFieldId() {
      return _thriftId;
    }

    @Override
    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.CONDITION};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TYPE, new org.apache.thrift.meta_data.FieldMetaData("type", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, JoinType.class)));
    tmpMap.put(_Fields.LEFT, new org.apache.thrift.meta_data.FieldMetaData("left", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, DataSource.class)));
    tmpMap.put(_Fields.RIGHT, new org.apache.thrift.meta_data.FieldMetaData("right", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, DataSource.class)));
    tmpMap.put(_Fields.CONDITION, new org.apache.thrift.meta_data.FieldMetaData("condition", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Expression.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Join.class, metaDataMap);
  }

  public Join() {
  }

  public Join(
    JoinType type,
    DataSource left,
    DataSource right)
  {
    this();
    this.type = type;
    this.left = left;
    this.right = right;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Join(Join other) {
    if (other.isSetType()) {
      this.type = other.type;
    }
    if (other.isSetLeft()) {
      this.left = new DataSource(other.left);
    }
    if (other.isSetRight()) {
      this.right = new DataSource(other.right);
    }
    if (other.isSetCondition()) {
      this.condition = new Expression(other.condition);
    }
  }

  @Override
  public Join deepCopy() {
    return new Join(this);
  }

  @Override
  public void clear() {
    this.type = null;
    this.left = null;
    this.right = null;
    this.condition = null;
  }

  /**
   * 
   * @see JoinType
   */
  @org.apache.thrift.annotation.Nullable
  public JoinType getType() {
    return this.type;
  }

  /**
   * 
   * @see JoinType
   */
  public void setType(@org.apache.thrift.annotation.Nullable JoinType type) {
    this.type = type;
  }

  public void unsetType() {
    this.type = null;
  }

  /** Returns true if field type is set (has been assigned a value) and false otherwise */
  public boolean isSetType() {
    return this.type != null;
  }

  public void setTypeIsSet(boolean value) {
    if (!value) {
      this.type = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public DataSource getLeft() {
    return this.left;
  }

  public void setLeft(@org.apache.thrift.annotation.Nullable DataSource left) {
    this.left = left;
  }

  public void unsetLeft() {
    this.left = null;
  }

  /** Returns true if field left is set (has been assigned a value) and false otherwise */
  public boolean isSetLeft() {
    return this.left != null;
  }

  public void setLeftIsSet(boolean value) {
    if (!value) {
      this.left = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public DataSource getRight() {
    return this.right;
  }

  public void setRight(@org.apache.thrift.annotation.Nullable DataSource right) {
    this.right = right;
  }

  public void unsetRight() {
    this.right = null;
  }

  /** Returns true if field right is set (has been assigned a value) and false otherwise */
  public boolean isSetRight() {
    return this.right != null;
  }

  public void setRightIsSet(boolean value) {
    if (!value) {
      this.right = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public Expression getCondition() {
    return this.condition;
  }

  public void setCondition(@org.apache.thrift.annotation.Nullable Expression condition) {
    this.condition = condition;
  }

  public void unsetCondition() {
    this.condition = null;
  }

  /** Returns true if field condition is set (has been assigned a value) and false otherwise */
  public boolean isSetCondition() {
    return this.condition != null;
  }

  public void setConditionIsSet(boolean value) {
    if (!value) {
      this.condition = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case TYPE:
      if (value == null) {
        unsetType();
      } else {
        setType((JoinType)value);
      }
      break;

    case LEFT:
      if (value == null) {
        unsetLeft();
      } else {
        setLeft((DataSource)value);
      }
      break;

    case RIGHT:
      if (value == null) {
        unsetRight();
      } else {
        setRight((DataSource)value);
      }
      break;

    case CONDITION:
      if (value == null) {
        unsetCondition();
      } else {
        setCondition((Expression)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case TYPE:
      return getType();

    case LEFT:
      return getLeft();

    case RIGHT:
      return getRight();

    case CONDITION:
      return getCondition();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  @Override
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case TYPE:
      return isSetType();
    case LEFT:
      return isSetLeft();
    case RIGHT:
      return isSetRight();
    case CONDITION:
      return isSetCondition();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof Join)
      return this.equals((Join)that);
    return false;
  }

  public boolean equals(Join that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_type = true && this.isSetType();
    boolean that_present_type = true && that.isSetType();
    if (this_present_type || that_present_type) {
      if (!(this_present_type && that_present_type))
        return false;
      if (!this.type.equals(that.type))
        return false;
    }

    boolean this_present_left = true && this.isSetLeft();
    boolean that_present_left = true && that.isSetLeft();
    if (this_present_left || that_present_left) {
      if (!(this_present_left && that_present_left))
        return false;
      if (!this.left.equals(that.left))
        return false;
    }

    boolean this_present_right = true && this.isSetRight();
    boolean that_present_right = true && that.isSetRight();
    if (this_present_right || that_present_right) {
      if (!(this_present_right && that_present_right))
        return false;
      if (!this.right.equals(that.right))
        return false;
    }

    boolean this_present_condition = true && this.isSetCondition();
    boolean that_present_condition = true && that.isSetCondition();
    if (this_present_condition || that_present_condition) {
      if (!(this_present_condition && that_present_condition))
        return false;
      if (!this.condition.equals(that.condition))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetType()) ? 131071 : 524287);
    if (isSetType())
      hashCode = hashCode * 8191 + type.getValue();

    hashCode = hashCode * 8191 + ((isSetLeft()) ? 131071 : 524287);
    if (isSetLeft())
      hashCode = hashCode * 8191 + left.hashCode();

    hashCode = hashCode * 8191 + ((isSetRight()) ? 131071 : 524287);
    if (isSetRight())
      hashCode = hashCode * 8191 + right.hashCode();

    hashCode = hashCode * 8191 + ((isSetCondition()) ? 131071 : 524287);
    if (isSetCondition())
      hashCode = hashCode * 8191 + condition.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(Join other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetType(), other.isSetType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.type, other.type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetLeft(), other.isSetLeft());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLeft()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.left, other.left);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetRight(), other.isSetRight());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRight()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.right, other.right);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetCondition(), other.isSetCondition());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCondition()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.condition, other.condition);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  @Override
  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  @Override
  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("Join(");
    boolean first = true;

    sb.append("type:");
    if (this.type == null) {
      sb.append("null");
    } else {
      sb.append(this.type);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("left:");
    if (this.left == null) {
      sb.append("null");
    } else {
      sb.append(this.left);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("right:");
    if (this.right == null) {
      sb.append("null");
    } else {
      sb.append(this.right);
    }
    first = false;
    if (isSetCondition()) {
      if (!first) sb.append(", ");
      sb.append("condition:");
      if (this.condition == null) {
        sb.append("null");
      } else {
        sb.append(this.condition);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetType()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'type' is unset! Struct:" + toString());
    }

    if (!isSetLeft()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'left' is unset! Struct:" + toString());
    }

    if (!isSetRight()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'right' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (left != null) {
      left.validate();
    }
    if (right != null) {
      right.validate();
    }
    if (condition != null) {
      condition.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class JoinStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public JoinStandardScheme getScheme() {
      return new JoinStandardScheme();
    }
  }

  private static class JoinStandardScheme extends org.apache.thrift.scheme.StandardScheme<Join> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, Join struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.type = org.apache.pinot.common.request.JoinType.findByValue(iprot.readI32());
              struct.setTypeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // LEFT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.left = new DataSource();
              struct.left.read(iprot);
              struct.setLeftIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // RIGHT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.right = new DataSource();
              struct.right.read(iprot);
              struct.setRightIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // CONDITION
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.condition = new Expression();
              struct.condition.read(iprot);
              struct.setConditionIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot, Join struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.type != null) {
        oprot.writeFieldBegin(TYPE_FIELD_DESC);
        oprot.writeI32(struct.type.getValue());
        oprot.writeFieldEnd();
      }
      if (struct.left != null) {
        oprot.writeFieldBegin(LEFT_FIELD_DESC);
        struct.left.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.right != null) {
        oprot.writeFieldBegin(RIGHT_FIELD_DESC);
        struct.right.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.condition != null) {
        if (struct.isSetCondition()) {
          oprot.writeFieldBegin(CONDITION_FIELD_DESC);
          struct.condition.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class JoinTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public JoinTupleScheme getScheme() {
      return new JoinTupleScheme();
    }
  }

  private static class JoinTupleScheme extends org.apache.thrift.scheme.TupleScheme<Join> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Join struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeI32(struct.type.getValue());
      struct.left.write(oprot);
      struct.right.write(oprot);
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetCondition()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetCondition()) {
        struct.condition.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Join struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.type = org.apache.pinot.common.request.JoinType.findByValue(iprot.readI32());
      struct.setTypeIsSet(true);
      struct.left = new DataSource();
      struct.left.read(iprot);
      struct.setLeftIsSet(true);
      struct.right = new DataSource();
      struct.right.read(iprot);
      struct.setRightIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.condition = new Expression();
        struct.condition.read(iprot);
        struct.setConditionIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

