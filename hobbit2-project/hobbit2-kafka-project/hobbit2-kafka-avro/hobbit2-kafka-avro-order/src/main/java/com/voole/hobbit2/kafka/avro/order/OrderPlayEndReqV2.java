/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.voole.hobbit2.kafka.avro.order;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class OrderPlayEndReqV2 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"OrderPlayEndReqV2\",\"namespace\":\"com.voole.hobbit2.kafka.avro.order\",\"fields\":[{\"name\":\"HID\",\"type\":[\"string\",\"null\"]},{\"name\":\"UID\",\"type\":[\"string\",\"null\"]},{\"name\":\"localIP\",\"type\":[\"long\",\"null\"]},{\"name\":\"sessID\",\"type\":[\"string\",\"null\"]},{\"name\":\"adjPlayTime\",\"type\":[\"long\",\"null\"]},{\"name\":\"accID\",\"type\":[\"long\",\"null\"]},{\"name\":\"endTick\",\"type\":[\"long\",\"null\"]},{\"name\":\"seekNum\",\"type\":[\"long\",\"null\"]},{\"name\":\"readNum\",\"type\":[\"long\",\"null\"]},{\"name\":\"unsuccRead\",\"type\":[\"long\",\"null\"]},{\"name\":\"stopPos\",\"type\":[\"long\",\"null\"]},{\"name\":\"sessAvgSpeed\",\"type\":[\"long\",\"null\"]},{\"name\":\"linkNum\",\"type\":[\"int\",\"null\"]},{\"name\":\"_srvs\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"OrderPlayEndReqSrvV2\",\"fields\":[{\"name\":\"srvIP\",\"type\":[\"long\",\"null\"]},{\"name\":\"connTimes\",\"type\":[\"long\",\"null\"]},{\"name\":\"transNum\",\"type\":[\"long\",\"null\"]},{\"name\":\"avgRTT\",\"type\":[\"long\",\"null\"]},{\"name\":\"accBytes\",\"type\":[\"long\",\"null\"]},{\"name\":\"accTime\",\"type\":[\"long\",\"null\"]},{\"name\":\"avgSpeed\",\"type\":[\"long\",\"null\"]}]}}]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence HID;
  @Deprecated public java.lang.CharSequence UID;
  @Deprecated public java.lang.Long localIP;
  @Deprecated public java.lang.CharSequence sessID;
  @Deprecated public java.lang.Long adjPlayTime;
  @Deprecated public java.lang.Long accID;
  @Deprecated public java.lang.Long endTick;
  @Deprecated public java.lang.Long seekNum;
  @Deprecated public java.lang.Long readNum;
  @Deprecated public java.lang.Long unsuccRead;
  @Deprecated public java.lang.Long stopPos;
  @Deprecated public java.lang.Long sessAvgSpeed;
  @Deprecated public java.lang.Integer linkNum;
  @Deprecated public java.util.List<com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqSrvV2> _srvs;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public OrderPlayEndReqV2() {}

  /**
   * All-args constructor.
   */
  public OrderPlayEndReqV2(java.lang.CharSequence HID, java.lang.CharSequence UID, java.lang.Long localIP, java.lang.CharSequence sessID, java.lang.Long adjPlayTime, java.lang.Long accID, java.lang.Long endTick, java.lang.Long seekNum, java.lang.Long readNum, java.lang.Long unsuccRead, java.lang.Long stopPos, java.lang.Long sessAvgSpeed, java.lang.Integer linkNum, java.util.List<com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqSrvV2> _srvs) {
    this.HID = HID;
    this.UID = UID;
    this.localIP = localIP;
    this.sessID = sessID;
    this.adjPlayTime = adjPlayTime;
    this.accID = accID;
    this.endTick = endTick;
    this.seekNum = seekNum;
    this.readNum = readNum;
    this.unsuccRead = unsuccRead;
    this.stopPos = stopPos;
    this.sessAvgSpeed = sessAvgSpeed;
    this.linkNum = linkNum;
    this._srvs = _srvs;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return HID;
    case 1: return UID;
    case 2: return localIP;
    case 3: return sessID;
    case 4: return adjPlayTime;
    case 5: return accID;
    case 6: return endTick;
    case 7: return seekNum;
    case 8: return readNum;
    case 9: return unsuccRead;
    case 10: return stopPos;
    case 11: return sessAvgSpeed;
    case 12: return linkNum;
    case 13: return _srvs;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: HID = (java.lang.CharSequence)value$; break;
    case 1: UID = (java.lang.CharSequence)value$; break;
    case 2: localIP = (java.lang.Long)value$; break;
    case 3: sessID = (java.lang.CharSequence)value$; break;
    case 4: adjPlayTime = (java.lang.Long)value$; break;
    case 5: accID = (java.lang.Long)value$; break;
    case 6: endTick = (java.lang.Long)value$; break;
    case 7: seekNum = (java.lang.Long)value$; break;
    case 8: readNum = (java.lang.Long)value$; break;
    case 9: unsuccRead = (java.lang.Long)value$; break;
    case 10: stopPos = (java.lang.Long)value$; break;
    case 11: sessAvgSpeed = (java.lang.Long)value$; break;
    case 12: linkNum = (java.lang.Integer)value$; break;
    case 13: _srvs = (java.util.List<com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqSrvV2>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'HID' field.
   */
  public java.lang.CharSequence getHID() {
    return HID;
  }

  /**
   * Sets the value of the 'HID' field.
   * @param value the value to set.
   */
  public void setHID(java.lang.CharSequence value) {
    this.HID = value;
  }

  /**
   * Gets the value of the 'UID' field.
   */
  public java.lang.CharSequence getUID() {
    return UID;
  }

  /**
   * Sets the value of the 'UID' field.
   * @param value the value to set.
   */
  public void setUID(java.lang.CharSequence value) {
    this.UID = value;
  }

  /**
   * Gets the value of the 'localIP' field.
   */
  public java.lang.Long getLocalIP() {
    return localIP;
  }

  /**
   * Sets the value of the 'localIP' field.
   * @param value the value to set.
   */
  public void setLocalIP(java.lang.Long value) {
    this.localIP = value;
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
   * Gets the value of the 'adjPlayTime' field.
   */
  public java.lang.Long getAdjPlayTime() {
    return adjPlayTime;
  }

  /**
   * Sets the value of the 'adjPlayTime' field.
   * @param value the value to set.
   */
  public void setAdjPlayTime(java.lang.Long value) {
    this.adjPlayTime = value;
  }

  /**
   * Gets the value of the 'accID' field.
   */
  public java.lang.Long getAccID() {
    return accID;
  }

  /**
   * Sets the value of the 'accID' field.
   * @param value the value to set.
   */
  public void setAccID(java.lang.Long value) {
    this.accID = value;
  }

  /**
   * Gets the value of the 'endTick' field.
   */
  public java.lang.Long getEndTick() {
    return endTick;
  }

  /**
   * Sets the value of the 'endTick' field.
   * @param value the value to set.
   */
  public void setEndTick(java.lang.Long value) {
    this.endTick = value;
  }

  /**
   * Gets the value of the 'seekNum' field.
   */
  public java.lang.Long getSeekNum() {
    return seekNum;
  }

  /**
   * Sets the value of the 'seekNum' field.
   * @param value the value to set.
   */
  public void setSeekNum(java.lang.Long value) {
    this.seekNum = value;
  }

  /**
   * Gets the value of the 'readNum' field.
   */
  public java.lang.Long getReadNum() {
    return readNum;
  }

  /**
   * Sets the value of the 'readNum' field.
   * @param value the value to set.
   */
  public void setReadNum(java.lang.Long value) {
    this.readNum = value;
  }

  /**
   * Gets the value of the 'unsuccRead' field.
   */
  public java.lang.Long getUnsuccRead() {
    return unsuccRead;
  }

  /**
   * Sets the value of the 'unsuccRead' field.
   * @param value the value to set.
   */
  public void setUnsuccRead(java.lang.Long value) {
    this.unsuccRead = value;
  }

  /**
   * Gets the value of the 'stopPos' field.
   */
  public java.lang.Long getStopPos() {
    return stopPos;
  }

  /**
   * Sets the value of the 'stopPos' field.
   * @param value the value to set.
   */
  public void setStopPos(java.lang.Long value) {
    this.stopPos = value;
  }

  /**
   * Gets the value of the 'sessAvgSpeed' field.
   */
  public java.lang.Long getSessAvgSpeed() {
    return sessAvgSpeed;
  }

  /**
   * Sets the value of the 'sessAvgSpeed' field.
   * @param value the value to set.
   */
  public void setSessAvgSpeed(java.lang.Long value) {
    this.sessAvgSpeed = value;
  }

  /**
   * Gets the value of the 'linkNum' field.
   */
  public java.lang.Integer getLinkNum() {
    return linkNum;
  }

  /**
   * Sets the value of the 'linkNum' field.
   * @param value the value to set.
   */
  public void setLinkNum(java.lang.Integer value) {
    this.linkNum = value;
  }

  /**
   * Gets the value of the '_srvs' field.
   */
  public java.util.List<com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqSrvV2> getSrvs$1() {
    return _srvs;
  }

  /**
   * Sets the value of the '_srvs' field.
   * @param value the value to set.
   */
  public void setSrvs$1(java.util.List<com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqSrvV2> value) {
    this._srvs = value;
  }

  /** Creates a new OrderPlayEndReqV2 RecordBuilder */
  public static com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder newBuilder() {
    return new com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder();
  }
  
  /** Creates a new OrderPlayEndReqV2 RecordBuilder by copying an existing Builder */
  public static com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder newBuilder(com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder other) {
    return new com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder(other);
  }
  
  /** Creates a new OrderPlayEndReqV2 RecordBuilder by copying an existing OrderPlayEndReqV2 instance */
  public static com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder newBuilder(com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2 other) {
    return new com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder(other);
  }
  
  /**
   * RecordBuilder for OrderPlayEndReqV2 instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<OrderPlayEndReqV2>
    implements org.apache.avro.data.RecordBuilder<OrderPlayEndReqV2> {

    private java.lang.CharSequence HID;
    private java.lang.CharSequence UID;
    private java.lang.Long localIP;
    private java.lang.CharSequence sessID;
    private java.lang.Long adjPlayTime;
    private java.lang.Long accID;
    private java.lang.Long endTick;
    private java.lang.Long seekNum;
    private java.lang.Long readNum;
    private java.lang.Long unsuccRead;
    private java.lang.Long stopPos;
    private java.lang.Long sessAvgSpeed;
    private java.lang.Integer linkNum;
    private java.util.List<com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqSrvV2> _srvs;

    /** Creates a new Builder */
    private Builder() {
      super(com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.HID)) {
        this.HID = data().deepCopy(fields()[0].schema(), other.HID);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.UID)) {
        this.UID = data().deepCopy(fields()[1].schema(), other.UID);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.localIP)) {
        this.localIP = data().deepCopy(fields()[2].schema(), other.localIP);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.sessID)) {
        this.sessID = data().deepCopy(fields()[3].schema(), other.sessID);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.adjPlayTime)) {
        this.adjPlayTime = data().deepCopy(fields()[4].schema(), other.adjPlayTime);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.accID)) {
        this.accID = data().deepCopy(fields()[5].schema(), other.accID);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.endTick)) {
        this.endTick = data().deepCopy(fields()[6].schema(), other.endTick);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.seekNum)) {
        this.seekNum = data().deepCopy(fields()[7].schema(), other.seekNum);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.readNum)) {
        this.readNum = data().deepCopy(fields()[8].schema(), other.readNum);
        fieldSetFlags()[8] = true;
      }
      if (isValidValue(fields()[9], other.unsuccRead)) {
        this.unsuccRead = data().deepCopy(fields()[9].schema(), other.unsuccRead);
        fieldSetFlags()[9] = true;
      }
      if (isValidValue(fields()[10], other.stopPos)) {
        this.stopPos = data().deepCopy(fields()[10].schema(), other.stopPos);
        fieldSetFlags()[10] = true;
      }
      if (isValidValue(fields()[11], other.sessAvgSpeed)) {
        this.sessAvgSpeed = data().deepCopy(fields()[11].schema(), other.sessAvgSpeed);
        fieldSetFlags()[11] = true;
      }
      if (isValidValue(fields()[12], other.linkNum)) {
        this.linkNum = data().deepCopy(fields()[12].schema(), other.linkNum);
        fieldSetFlags()[12] = true;
      }
      if (isValidValue(fields()[13], other._srvs)) {
        this._srvs = data().deepCopy(fields()[13].schema(), other._srvs);
        fieldSetFlags()[13] = true;
      }
    }
    
    /** Creates a Builder by copying an existing OrderPlayEndReqV2 instance */
    private Builder(com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2 other) {
            super(com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.SCHEMA$);
      if (isValidValue(fields()[0], other.HID)) {
        this.HID = data().deepCopy(fields()[0].schema(), other.HID);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.UID)) {
        this.UID = data().deepCopy(fields()[1].schema(), other.UID);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.localIP)) {
        this.localIP = data().deepCopy(fields()[2].schema(), other.localIP);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.sessID)) {
        this.sessID = data().deepCopy(fields()[3].schema(), other.sessID);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.adjPlayTime)) {
        this.adjPlayTime = data().deepCopy(fields()[4].schema(), other.adjPlayTime);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.accID)) {
        this.accID = data().deepCopy(fields()[5].schema(), other.accID);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.endTick)) {
        this.endTick = data().deepCopy(fields()[6].schema(), other.endTick);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.seekNum)) {
        this.seekNum = data().deepCopy(fields()[7].schema(), other.seekNum);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.readNum)) {
        this.readNum = data().deepCopy(fields()[8].schema(), other.readNum);
        fieldSetFlags()[8] = true;
      }
      if (isValidValue(fields()[9], other.unsuccRead)) {
        this.unsuccRead = data().deepCopy(fields()[9].schema(), other.unsuccRead);
        fieldSetFlags()[9] = true;
      }
      if (isValidValue(fields()[10], other.stopPos)) {
        this.stopPos = data().deepCopy(fields()[10].schema(), other.stopPos);
        fieldSetFlags()[10] = true;
      }
      if (isValidValue(fields()[11], other.sessAvgSpeed)) {
        this.sessAvgSpeed = data().deepCopy(fields()[11].schema(), other.sessAvgSpeed);
        fieldSetFlags()[11] = true;
      }
      if (isValidValue(fields()[12], other.linkNum)) {
        this.linkNum = data().deepCopy(fields()[12].schema(), other.linkNum);
        fieldSetFlags()[12] = true;
      }
      if (isValidValue(fields()[13], other._srvs)) {
        this._srvs = data().deepCopy(fields()[13].schema(), other._srvs);
        fieldSetFlags()[13] = true;
      }
    }

    /** Gets the value of the 'HID' field */
    public java.lang.CharSequence getHID() {
      return HID;
    }
    
    /** Sets the value of the 'HID' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setHID(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.HID = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'HID' field has been set */
    public boolean hasHID() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'HID' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearHID() {
      HID = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'UID' field */
    public java.lang.CharSequence getUID() {
      return UID;
    }
    
    /** Sets the value of the 'UID' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setUID(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.UID = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'UID' field has been set */
    public boolean hasUID() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'UID' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearUID() {
      UID = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'localIP' field */
    public java.lang.Long getLocalIP() {
      return localIP;
    }
    
    /** Sets the value of the 'localIP' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setLocalIP(java.lang.Long value) {
      validate(fields()[2], value);
      this.localIP = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'localIP' field has been set */
    public boolean hasLocalIP() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'localIP' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearLocalIP() {
      localIP = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'sessID' field */
    public java.lang.CharSequence getSessID() {
      return sessID;
    }
    
    /** Sets the value of the 'sessID' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setSessID(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.sessID = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'sessID' field has been set */
    public boolean hasSessID() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'sessID' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearSessID() {
      sessID = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /** Gets the value of the 'adjPlayTime' field */
    public java.lang.Long getAdjPlayTime() {
      return adjPlayTime;
    }
    
    /** Sets the value of the 'adjPlayTime' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setAdjPlayTime(java.lang.Long value) {
      validate(fields()[4], value);
      this.adjPlayTime = value;
      fieldSetFlags()[4] = true;
      return this; 
    }
    
    /** Checks whether the 'adjPlayTime' field has been set */
    public boolean hasAdjPlayTime() {
      return fieldSetFlags()[4];
    }
    
    /** Clears the value of the 'adjPlayTime' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearAdjPlayTime() {
      adjPlayTime = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /** Gets the value of the 'accID' field */
    public java.lang.Long getAccID() {
      return accID;
    }
    
    /** Sets the value of the 'accID' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setAccID(java.lang.Long value) {
      validate(fields()[5], value);
      this.accID = value;
      fieldSetFlags()[5] = true;
      return this; 
    }
    
    /** Checks whether the 'accID' field has been set */
    public boolean hasAccID() {
      return fieldSetFlags()[5];
    }
    
    /** Clears the value of the 'accID' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearAccID() {
      accID = null;
      fieldSetFlags()[5] = false;
      return this;
    }

    /** Gets the value of the 'endTick' field */
    public java.lang.Long getEndTick() {
      return endTick;
    }
    
    /** Sets the value of the 'endTick' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setEndTick(java.lang.Long value) {
      validate(fields()[6], value);
      this.endTick = value;
      fieldSetFlags()[6] = true;
      return this; 
    }
    
    /** Checks whether the 'endTick' field has been set */
    public boolean hasEndTick() {
      return fieldSetFlags()[6];
    }
    
    /** Clears the value of the 'endTick' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearEndTick() {
      endTick = null;
      fieldSetFlags()[6] = false;
      return this;
    }

    /** Gets the value of the 'seekNum' field */
    public java.lang.Long getSeekNum() {
      return seekNum;
    }
    
    /** Sets the value of the 'seekNum' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setSeekNum(java.lang.Long value) {
      validate(fields()[7], value);
      this.seekNum = value;
      fieldSetFlags()[7] = true;
      return this; 
    }
    
    /** Checks whether the 'seekNum' field has been set */
    public boolean hasSeekNum() {
      return fieldSetFlags()[7];
    }
    
    /** Clears the value of the 'seekNum' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearSeekNum() {
      seekNum = null;
      fieldSetFlags()[7] = false;
      return this;
    }

    /** Gets the value of the 'readNum' field */
    public java.lang.Long getReadNum() {
      return readNum;
    }
    
    /** Sets the value of the 'readNum' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setReadNum(java.lang.Long value) {
      validate(fields()[8], value);
      this.readNum = value;
      fieldSetFlags()[8] = true;
      return this; 
    }
    
    /** Checks whether the 'readNum' field has been set */
    public boolean hasReadNum() {
      return fieldSetFlags()[8];
    }
    
    /** Clears the value of the 'readNum' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearReadNum() {
      readNum = null;
      fieldSetFlags()[8] = false;
      return this;
    }

    /** Gets the value of the 'unsuccRead' field */
    public java.lang.Long getUnsuccRead() {
      return unsuccRead;
    }
    
    /** Sets the value of the 'unsuccRead' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setUnsuccRead(java.lang.Long value) {
      validate(fields()[9], value);
      this.unsuccRead = value;
      fieldSetFlags()[9] = true;
      return this; 
    }
    
    /** Checks whether the 'unsuccRead' field has been set */
    public boolean hasUnsuccRead() {
      return fieldSetFlags()[9];
    }
    
    /** Clears the value of the 'unsuccRead' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearUnsuccRead() {
      unsuccRead = null;
      fieldSetFlags()[9] = false;
      return this;
    }

    /** Gets the value of the 'stopPos' field */
    public java.lang.Long getStopPos() {
      return stopPos;
    }
    
    /** Sets the value of the 'stopPos' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setStopPos(java.lang.Long value) {
      validate(fields()[10], value);
      this.stopPos = value;
      fieldSetFlags()[10] = true;
      return this; 
    }
    
    /** Checks whether the 'stopPos' field has been set */
    public boolean hasStopPos() {
      return fieldSetFlags()[10];
    }
    
    /** Clears the value of the 'stopPos' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearStopPos() {
      stopPos = null;
      fieldSetFlags()[10] = false;
      return this;
    }

    /** Gets the value of the 'sessAvgSpeed' field */
    public java.lang.Long getSessAvgSpeed() {
      return sessAvgSpeed;
    }
    
    /** Sets the value of the 'sessAvgSpeed' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setSessAvgSpeed(java.lang.Long value) {
      validate(fields()[11], value);
      this.sessAvgSpeed = value;
      fieldSetFlags()[11] = true;
      return this; 
    }
    
    /** Checks whether the 'sessAvgSpeed' field has been set */
    public boolean hasSessAvgSpeed() {
      return fieldSetFlags()[11];
    }
    
    /** Clears the value of the 'sessAvgSpeed' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearSessAvgSpeed() {
      sessAvgSpeed = null;
      fieldSetFlags()[11] = false;
      return this;
    }

    /** Gets the value of the 'linkNum' field */
    public java.lang.Integer getLinkNum() {
      return linkNum;
    }
    
    /** Sets the value of the 'linkNum' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setLinkNum(java.lang.Integer value) {
      validate(fields()[12], value);
      this.linkNum = value;
      fieldSetFlags()[12] = true;
      return this; 
    }
    
    /** Checks whether the 'linkNum' field has been set */
    public boolean hasLinkNum() {
      return fieldSetFlags()[12];
    }
    
    /** Clears the value of the 'linkNum' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearLinkNum() {
      linkNum = null;
      fieldSetFlags()[12] = false;
      return this;
    }

    /** Gets the value of the '_srvs' field */
    public java.util.List<com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqSrvV2> getSrvs$1() {
      return _srvs;
    }
    
    /** Sets the value of the '_srvs' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder setSrvs$1(java.util.List<com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqSrvV2> value) {
      validate(fields()[13], value);
      this._srvs = value;
      fieldSetFlags()[13] = true;
      return this; 
    }
    
    /** Checks whether the '_srvs' field has been set */
    public boolean hasSrvs$1() {
      return fieldSetFlags()[13];
    }
    
    /** Clears the value of the '_srvs' field */
    public com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqV2.Builder clearSrvs$1() {
      _srvs = null;
      fieldSetFlags()[13] = false;
      return this;
    }

    @Override
    public OrderPlayEndReqV2 build() {
      try {
        OrderPlayEndReqV2 record = new OrderPlayEndReqV2();
        record.HID = fieldSetFlags()[0] ? this.HID : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.UID = fieldSetFlags()[1] ? this.UID : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.localIP = fieldSetFlags()[2] ? this.localIP : (java.lang.Long) defaultValue(fields()[2]);
        record.sessID = fieldSetFlags()[3] ? this.sessID : (java.lang.CharSequence) defaultValue(fields()[3]);
        record.adjPlayTime = fieldSetFlags()[4] ? this.adjPlayTime : (java.lang.Long) defaultValue(fields()[4]);
        record.accID = fieldSetFlags()[5] ? this.accID : (java.lang.Long) defaultValue(fields()[5]);
        record.endTick = fieldSetFlags()[6] ? this.endTick : (java.lang.Long) defaultValue(fields()[6]);
        record.seekNum = fieldSetFlags()[7] ? this.seekNum : (java.lang.Long) defaultValue(fields()[7]);
        record.readNum = fieldSetFlags()[8] ? this.readNum : (java.lang.Long) defaultValue(fields()[8]);
        record.unsuccRead = fieldSetFlags()[9] ? this.unsuccRead : (java.lang.Long) defaultValue(fields()[9]);
        record.stopPos = fieldSetFlags()[10] ? this.stopPos : (java.lang.Long) defaultValue(fields()[10]);
        record.sessAvgSpeed = fieldSetFlags()[11] ? this.sessAvgSpeed : (java.lang.Long) defaultValue(fields()[11]);
        record.linkNum = fieldSetFlags()[12] ? this.linkNum : (java.lang.Integer) defaultValue(fields()[12]);
        record._srvs = fieldSetFlags()[13] ? this._srvs : (java.util.List<com.voole.hobbit2.kafka.avro.order.OrderPlayEndReqSrvV2>) defaultValue(fields()[13]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
