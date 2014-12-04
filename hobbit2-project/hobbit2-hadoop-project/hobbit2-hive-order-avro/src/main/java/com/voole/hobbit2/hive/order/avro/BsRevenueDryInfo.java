/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.voole.hobbit2.hive.order.avro;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class BsRevenueDryInfo extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"BsRevenueDryInfo\",\"namespace\":\"com.voole.hobbit2.hive.order.avro\",\"fields\":[{\"name\":\"sessID\",\"type\":[\"string\",\"null\"]},{\"name\":\"datasource\",\"type\":[\"string\",\"null\"]},{\"name\":\"accesstime\",\"type\":[\"string\",\"null\"]},{\"name\":\"hid\",\"type\":[\"string\",\"null\"]},{\"name\":\"oemid\",\"type\":[\"string\",\"null\"]},{\"name\":\"uid\",\"type\":[\"string\",\"null\"]},{\"name\":\"userip\",\"type\":[\"string\",\"null\"]},{\"name\":\"epgid\",\"type\":[\"string\",\"null\"]},{\"name\":\"pid\",\"type\":[\"string\",\"null\"]},{\"name\":\"result\",\"type\":[\"string\",\"null\"]},{\"name\":\"perfip\",\"type\":[\"string\",\"null\"]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence sessID;
  @Deprecated public java.lang.CharSequence datasource;
  @Deprecated public java.lang.CharSequence accesstime;
  @Deprecated public java.lang.CharSequence hid;
  @Deprecated public java.lang.CharSequence oemid;
  @Deprecated public java.lang.CharSequence uid;
  @Deprecated public java.lang.CharSequence userip;
  @Deprecated public java.lang.CharSequence epgid;
  @Deprecated public java.lang.CharSequence pid;
  @Deprecated public java.lang.CharSequence result;
  @Deprecated public java.lang.CharSequence perfip;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public BsRevenueDryInfo() {}

  /**
   * All-args constructor.
   */
  public BsRevenueDryInfo(java.lang.CharSequence sessID, java.lang.CharSequence datasource, java.lang.CharSequence accesstime, java.lang.CharSequence hid, java.lang.CharSequence oemid, java.lang.CharSequence uid, java.lang.CharSequence userip, java.lang.CharSequence epgid, java.lang.CharSequence pid, java.lang.CharSequence result, java.lang.CharSequence perfip) {
    this.sessID = sessID;
    this.datasource = datasource;
    this.accesstime = accesstime;
    this.hid = hid;
    this.oemid = oemid;
    this.uid = uid;
    this.userip = userip;
    this.epgid = epgid;
    this.pid = pid;
    this.result = result;
    this.perfip = perfip;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return sessID;
    case 1: return datasource;
    case 2: return accesstime;
    case 3: return hid;
    case 4: return oemid;
    case 5: return uid;
    case 6: return userip;
    case 7: return epgid;
    case 8: return pid;
    case 9: return result;
    case 10: return perfip;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: sessID = (java.lang.CharSequence)value$; break;
    case 1: datasource = (java.lang.CharSequence)value$; break;
    case 2: accesstime = (java.lang.CharSequence)value$; break;
    case 3: hid = (java.lang.CharSequence)value$; break;
    case 4: oemid = (java.lang.CharSequence)value$; break;
    case 5: uid = (java.lang.CharSequence)value$; break;
    case 6: userip = (java.lang.CharSequence)value$; break;
    case 7: epgid = (java.lang.CharSequence)value$; break;
    case 8: pid = (java.lang.CharSequence)value$; break;
    case 9: result = (java.lang.CharSequence)value$; break;
    case 10: perfip = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'sessID' field.
   */
  public java.lang.CharSequence getSessID() {
    return sessID;
  }

  /**
   * Sets the value of the 'sessID' field.
   * @param value the value to set.
   */
  public void setSessID(java.lang.CharSequence value) {
    this.sessID = value;
  }

  /**
   * Gets the value of the 'datasource' field.
   */
  public java.lang.CharSequence getDatasource() {
    return datasource;
  }

  /**
   * Sets the value of the 'datasource' field.
   * @param value the value to set.
   */
  public void setDatasource(java.lang.CharSequence value) {
    this.datasource = value;
  }

  /**
   * Gets the value of the 'accesstime' field.
   */
  public java.lang.CharSequence getAccesstime() {
    return accesstime;
  }

  /**
   * Sets the value of the 'accesstime' field.
   * @param value the value to set.
   */
  public void setAccesstime(java.lang.CharSequence value) {
    this.accesstime = value;
  }

  /**
   * Gets the value of the 'hid' field.
   */
  public java.lang.CharSequence getHid() {
    return hid;
  }

  /**
   * Sets the value of the 'hid' field.
   * @param value the value to set.
   */
  public void setHid(java.lang.CharSequence value) {
    this.hid = value;
  }

  /**
   * Gets the value of the 'oemid' field.
   */
  public java.lang.CharSequence getOemid() {
    return oemid;
  }

  /**
   * Sets the value of the 'oemid' field.
   * @param value the value to set.
   */
  public void setOemid(java.lang.CharSequence value) {
    this.oemid = value;
  }

  /**
   * Gets the value of the 'uid' field.
   */
  public java.lang.CharSequence getUid() {
    return uid;
  }

  /**
   * Sets the value of the 'uid' field.
   * @param value the value to set.
   */
  public void setUid(java.lang.CharSequence value) {
    this.uid = value;
  }

  /**
   * Gets the value of the 'userip' field.
   */
  public java.lang.CharSequence getUserip() {
    return userip;
  }

  /**
   * Sets the value of the 'userip' field.
   * @param value the value to set.
   */
  public void setUserip(java.lang.CharSequence value) {
    this.userip = value;
  }

  /**
   * Gets the value of the 'epgid' field.
   */
  public java.lang.CharSequence getEpgid() {
    return epgid;
  }

  /**
   * Sets the value of the 'epgid' field.
   * @param value the value to set.
   */
  public void setEpgid(java.lang.CharSequence value) {
    this.epgid = value;
  }

  /**
   * Gets the value of the 'pid' field.
   */
  public java.lang.CharSequence getPid() {
    return pid;
  }

  /**
   * Sets the value of the 'pid' field.
   * @param value the value to set.
   */
  public void setPid(java.lang.CharSequence value) {
    this.pid = value;
  }

  /**
   * Gets the value of the 'result' field.
   */
  public java.lang.CharSequence getResult() {
    return result;
  }

  /**
   * Sets the value of the 'result' field.
   * @param value the value to set.
   */
  public void setResult(java.lang.CharSequence value) {
    this.result = value;
  }

  /**
   * Gets the value of the 'perfip' field.
   */
  public java.lang.CharSequence getPerfip() {
    return perfip;
  }

  /**
   * Sets the value of the 'perfip' field.
   * @param value the value to set.
   */
  public void setPerfip(java.lang.CharSequence value) {
    this.perfip = value;
  }

  /** Creates a new BsRevenueDryInfo RecordBuilder */
  public static com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder newBuilder() {
    return new com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder();
  }
  
  /** Creates a new BsRevenueDryInfo RecordBuilder by copying an existing Builder */
  public static com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder newBuilder(com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder other) {
    return new com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder(other);
  }
  
  /** Creates a new BsRevenueDryInfo RecordBuilder by copying an existing BsRevenueDryInfo instance */
  public static com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder newBuilder(com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo other) {
    return new com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder(other);
  }
  
  /**
   * RecordBuilder for BsRevenueDryInfo instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<BsRevenueDryInfo>
    implements org.apache.avro.data.RecordBuilder<BsRevenueDryInfo> {

    private java.lang.CharSequence sessID;
    private java.lang.CharSequence datasource;
    private java.lang.CharSequence accesstime;
    private java.lang.CharSequence hid;
    private java.lang.CharSequence oemid;
    private java.lang.CharSequence uid;
    private java.lang.CharSequence userip;
    private java.lang.CharSequence epgid;
    private java.lang.CharSequence pid;
    private java.lang.CharSequence result;
    private java.lang.CharSequence perfip;

    /** Creates a new Builder */
    private Builder() {
      super(com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.sessID)) {
        this.sessID = data().deepCopy(fields()[0].schema(), other.sessID);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.datasource)) {
        this.datasource = data().deepCopy(fields()[1].schema(), other.datasource);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.accesstime)) {
        this.accesstime = data().deepCopy(fields()[2].schema(), other.accesstime);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.hid)) {
        this.hid = data().deepCopy(fields()[3].schema(), other.hid);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.oemid)) {
        this.oemid = data().deepCopy(fields()[4].schema(), other.oemid);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.uid)) {
        this.uid = data().deepCopy(fields()[5].schema(), other.uid);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.userip)) {
        this.userip = data().deepCopy(fields()[6].schema(), other.userip);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.epgid)) {
        this.epgid = data().deepCopy(fields()[7].schema(), other.epgid);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.pid)) {
        this.pid = data().deepCopy(fields()[8].schema(), other.pid);
        fieldSetFlags()[8] = true;
      }
      if (isValidValue(fields()[9], other.result)) {
        this.result = data().deepCopy(fields()[9].schema(), other.result);
        fieldSetFlags()[9] = true;
      }
      if (isValidValue(fields()[10], other.perfip)) {
        this.perfip = data().deepCopy(fields()[10].schema(), other.perfip);
        fieldSetFlags()[10] = true;
      }
    }
    
    /** Creates a Builder by copying an existing BsRevenueDryInfo instance */
    private Builder(com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo other) {
            super(com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.SCHEMA$);
      if (isValidValue(fields()[0], other.sessID)) {
        this.sessID = data().deepCopy(fields()[0].schema(), other.sessID);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.datasource)) {
        this.datasource = data().deepCopy(fields()[1].schema(), other.datasource);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.accesstime)) {
        this.accesstime = data().deepCopy(fields()[2].schema(), other.accesstime);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.hid)) {
        this.hid = data().deepCopy(fields()[3].schema(), other.hid);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.oemid)) {
        this.oemid = data().deepCopy(fields()[4].schema(), other.oemid);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.uid)) {
        this.uid = data().deepCopy(fields()[5].schema(), other.uid);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.userip)) {
        this.userip = data().deepCopy(fields()[6].schema(), other.userip);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.epgid)) {
        this.epgid = data().deepCopy(fields()[7].schema(), other.epgid);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.pid)) {
        this.pid = data().deepCopy(fields()[8].schema(), other.pid);
        fieldSetFlags()[8] = true;
      }
      if (isValidValue(fields()[9], other.result)) {
        this.result = data().deepCopy(fields()[9].schema(), other.result);
        fieldSetFlags()[9] = true;
      }
      if (isValidValue(fields()[10], other.perfip)) {
        this.perfip = data().deepCopy(fields()[10].schema(), other.perfip);
        fieldSetFlags()[10] = true;
      }
    }

    /** Gets the value of the 'sessID' field */
    public java.lang.CharSequence getSessID() {
      return sessID;
    }
    
    /** Sets the value of the 'sessID' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder setSessID(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.sessID = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'sessID' field has been set */
    public boolean hasSessID() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'sessID' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder clearSessID() {
      sessID = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'datasource' field */
    public java.lang.CharSequence getDatasource() {
      return datasource;
    }
    
    /** Sets the value of the 'datasource' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder setDatasource(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.datasource = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'datasource' field has been set */
    public boolean hasDatasource() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'datasource' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder clearDatasource() {
      datasource = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'accesstime' field */
    public java.lang.CharSequence getAccesstime() {
      return accesstime;
    }
    
    /** Sets the value of the 'accesstime' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder setAccesstime(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.accesstime = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'accesstime' field has been set */
    public boolean hasAccesstime() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'accesstime' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder clearAccesstime() {
      accesstime = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'hid' field */
    public java.lang.CharSequence getHid() {
      return hid;
    }
    
    /** Sets the value of the 'hid' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder setHid(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.hid = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'hid' field has been set */
    public boolean hasHid() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'hid' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder clearHid() {
      hid = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /** Gets the value of the 'oemid' field */
    public java.lang.CharSequence getOemid() {
      return oemid;
    }
    
    /** Sets the value of the 'oemid' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder setOemid(java.lang.CharSequence value) {
      validate(fields()[4], value);
      this.oemid = value;
      fieldSetFlags()[4] = true;
      return this; 
    }
    
    /** Checks whether the 'oemid' field has been set */
    public boolean hasOemid() {
      return fieldSetFlags()[4];
    }
    
    /** Clears the value of the 'oemid' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder clearOemid() {
      oemid = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /** Gets the value of the 'uid' field */
    public java.lang.CharSequence getUid() {
      return uid;
    }
    
    /** Sets the value of the 'uid' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder setUid(java.lang.CharSequence value) {
      validate(fields()[5], value);
      this.uid = value;
      fieldSetFlags()[5] = true;
      return this; 
    }
    
    /** Checks whether the 'uid' field has been set */
    public boolean hasUid() {
      return fieldSetFlags()[5];
    }
    
    /** Clears the value of the 'uid' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder clearUid() {
      uid = null;
      fieldSetFlags()[5] = false;
      return this;
    }

    /** Gets the value of the 'userip' field */
    public java.lang.CharSequence getUserip() {
      return userip;
    }
    
    /** Sets the value of the 'userip' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder setUserip(java.lang.CharSequence value) {
      validate(fields()[6], value);
      this.userip = value;
      fieldSetFlags()[6] = true;
      return this; 
    }
    
    /** Checks whether the 'userip' field has been set */
    public boolean hasUserip() {
      return fieldSetFlags()[6];
    }
    
    /** Clears the value of the 'userip' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder clearUserip() {
      userip = null;
      fieldSetFlags()[6] = false;
      return this;
    }

    /** Gets the value of the 'epgid' field */
    public java.lang.CharSequence getEpgid() {
      return epgid;
    }
    
    /** Sets the value of the 'epgid' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder setEpgid(java.lang.CharSequence value) {
      validate(fields()[7], value);
      this.epgid = value;
      fieldSetFlags()[7] = true;
      return this; 
    }
    
    /** Checks whether the 'epgid' field has been set */
    public boolean hasEpgid() {
      return fieldSetFlags()[7];
    }
    
    /** Clears the value of the 'epgid' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder clearEpgid() {
      epgid = null;
      fieldSetFlags()[7] = false;
      return this;
    }

    /** Gets the value of the 'pid' field */
    public java.lang.CharSequence getPid() {
      return pid;
    }
    
    /** Sets the value of the 'pid' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder setPid(java.lang.CharSequence value) {
      validate(fields()[8], value);
      this.pid = value;
      fieldSetFlags()[8] = true;
      return this; 
    }
    
    /** Checks whether the 'pid' field has been set */
    public boolean hasPid() {
      return fieldSetFlags()[8];
    }
    
    /** Clears the value of the 'pid' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder clearPid() {
      pid = null;
      fieldSetFlags()[8] = false;
      return this;
    }

    /** Gets the value of the 'result' field */
    public java.lang.CharSequence getResult() {
      return result;
    }
    
    /** Sets the value of the 'result' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder setResult(java.lang.CharSequence value) {
      validate(fields()[9], value);
      this.result = value;
      fieldSetFlags()[9] = true;
      return this; 
    }
    
    /** Checks whether the 'result' field has been set */
    public boolean hasResult() {
      return fieldSetFlags()[9];
    }
    
    /** Clears the value of the 'result' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder clearResult() {
      result = null;
      fieldSetFlags()[9] = false;
      return this;
    }

    /** Gets the value of the 'perfip' field */
    public java.lang.CharSequence getPerfip() {
      return perfip;
    }
    
    /** Sets the value of the 'perfip' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder setPerfip(java.lang.CharSequence value) {
      validate(fields()[10], value);
      this.perfip = value;
      fieldSetFlags()[10] = true;
      return this; 
    }
    
    /** Checks whether the 'perfip' field has been set */
    public boolean hasPerfip() {
      return fieldSetFlags()[10];
    }
    
    /** Clears the value of the 'perfip' field */
    public com.voole.hobbit2.hive.order.avro.BsRevenueDryInfo.Builder clearPerfip() {
      perfip = null;
      fieldSetFlags()[10] = false;
      return this;
    }

    @Override
    public BsRevenueDryInfo build() {
      try {
        BsRevenueDryInfo record = new BsRevenueDryInfo();
        record.sessID = fieldSetFlags()[0] ? this.sessID : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.datasource = fieldSetFlags()[1] ? this.datasource : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.accesstime = fieldSetFlags()[2] ? this.accesstime : (java.lang.CharSequence) defaultValue(fields()[2]);
        record.hid = fieldSetFlags()[3] ? this.hid : (java.lang.CharSequence) defaultValue(fields()[3]);
        record.oemid = fieldSetFlags()[4] ? this.oemid : (java.lang.CharSequence) defaultValue(fields()[4]);
        record.uid = fieldSetFlags()[5] ? this.uid : (java.lang.CharSequence) defaultValue(fields()[5]);
        record.userip = fieldSetFlags()[6] ? this.userip : (java.lang.CharSequence) defaultValue(fields()[6]);
        record.epgid = fieldSetFlags()[7] ? this.epgid : (java.lang.CharSequence) defaultValue(fields()[7]);
        record.pid = fieldSetFlags()[8] ? this.pid : (java.lang.CharSequence) defaultValue(fields()[8]);
        record.result = fieldSetFlags()[9] ? this.result : (java.lang.CharSequence) defaultValue(fields()[9]);
        record.perfip = fieldSetFlags()[10] ? this.perfip : (java.lang.CharSequence) defaultValue(fields()[10]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
