/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.voole.hobbit2.storm.test.avro.model;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class AvroModel2 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AvroModel2\",\"namespace\":\"com.voole.hobbit2.storm.test.avro.model\",\"fields\":[{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"total\",\"type\":[\"long\",\"null\"]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence name;
  @Deprecated public java.lang.Long total;
  
  
  

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public AvroModel2() {}

  /**
   * All-args constructor.
   */
  public AvroModel2(java.lang.CharSequence name, java.lang.Long total) {
    this.name = name;
    this.total = total;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return name;
    case 1: return total;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: name = (java.lang.CharSequence)value$; break;
    case 1: total = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'name' field.
   */
  public java.lang.CharSequence getName() {
    return name;
  }

  /**
   * Sets the value of the 'name' field.
   * @param value the value to set.
   */
  public void setName(java.lang.CharSequence value) {
    this.name = value;
  }

  /**
   * Gets the value of the 'total' field.
   */
  public java.lang.Long getTotal() {
    return total;
  }

  /**
   * Sets the value of the 'total' field.
   * @param value the value to set.
   */
  public void setTotal(java.lang.Long value) {
    this.total = value;
  }

  /** Creates a new AvroModel2 RecordBuilder */
  public static com.voole.hobbit2.storm.test.avro.model.AvroModel2.Builder newBuilder() {
    return new com.voole.hobbit2.storm.test.avro.model.AvroModel2.Builder();
  }
  
  /** Creates a new AvroModel2 RecordBuilder by copying an existing Builder */
  public static com.voole.hobbit2.storm.test.avro.model.AvroModel2.Builder newBuilder(com.voole.hobbit2.storm.test.avro.model.AvroModel2.Builder other) {
    return new com.voole.hobbit2.storm.test.avro.model.AvroModel2.Builder(other);
  }
  
  /** Creates a new AvroModel2 RecordBuilder by copying an existing AvroModel2 instance */
  public static com.voole.hobbit2.storm.test.avro.model.AvroModel2.Builder newBuilder(com.voole.hobbit2.storm.test.avro.model.AvroModel2 other) {
    return new com.voole.hobbit2.storm.test.avro.model.AvroModel2.Builder(other);
  }
  
  /**
   * RecordBuilder for AvroModel2 instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<AvroModel2>
    implements org.apache.avro.data.RecordBuilder<AvroModel2> {

    private java.lang.CharSequence name;
    private java.lang.Long total;

    /** Creates a new Builder */
    private Builder() {
      super(com.voole.hobbit2.storm.test.avro.model.AvroModel2.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.voole.hobbit2.storm.test.avro.model.AvroModel2.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.total)) {
        this.total = data().deepCopy(fields()[1].schema(), other.total);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing AvroModel2 instance */
    private Builder(com.voole.hobbit2.storm.test.avro.model.AvroModel2 other) {
            super(com.voole.hobbit2.storm.test.avro.model.AvroModel2.SCHEMA$);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.total)) {
        this.total = data().deepCopy(fields()[1].schema(), other.total);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'name' field */
    public java.lang.CharSequence getName() {
      return name;
    }
    
    /** Sets the value of the 'name' field */
    public com.voole.hobbit2.storm.test.avro.model.AvroModel2.Builder setName(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.name = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'name' field has been set */
    public boolean hasName() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'name' field */
    public com.voole.hobbit2.storm.test.avro.model.AvroModel2.Builder clearName() {
      name = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'total' field */
    public java.lang.Long getTotal() {
      return total;
    }
    
    /** Sets the value of the 'total' field */
    public com.voole.hobbit2.storm.test.avro.model.AvroModel2.Builder setTotal(java.lang.Long value) {
      validate(fields()[1], value);
      this.total = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'total' field has been set */
    public boolean hasTotal() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'total' field */
    public com.voole.hobbit2.storm.test.avro.model.AvroModel2.Builder clearTotal() {
      total = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public AvroModel2 build() {
      try {
        AvroModel2 record = new AvroModel2();
        record.name = fieldSetFlags()[0] ? this.name : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.total = fieldSetFlags()[1] ? this.total : (java.lang.Long) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}