/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.voole.hobbit2.camus.order;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class OrderPlayBgnReqV2 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"OrderPlayBgnReqV2\",\"namespace\":\"com.voole.hobbit2.camus.order\",\"fields\":[{\"name\":\"OEMID\",\"type\":[\"long\",\"null\"]},{\"name\":\"curVer\",\"type\":[\"long\",\"null\"]},{\"name\":\"HID\",\"type\":[\"string\",\"null\"]},{\"name\":\"UID\",\"type\":[\"string\",\"null\"]},{\"name\":\"localIP\",\"type\":[\"long\",\"null\"]},{\"name\":\"sessID\",\"type\":[\"string\",\"null\"]},{\"name\":\"FID\",\"type\":[\"string\",\"null\"]},{\"name\":\"URL\",\"type\":[\"string\",\"null\"]},{\"name\":\"playTick\",\"type\":[\"long\",\"null\"]},{\"name\":\"srvNum\",\"type\":[\"int\",\"null\"]},{\"name\":\"_srvs\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"OrderPlayBgnReqSrvV2\",\"fields\":[{\"name\":\"srvIP\",\"type\":[\"long\",\"null\"]},{\"name\":\"srvPort\",\"type\":[\"int\",\"null\"]},{\"name\":\"srvType\",\"type\":[\"int\",\"null\"]}]}}]},{\"name\":\"natip\",\"type\":[\"long\",\"null\"]},{\"name\":\"sessStatus\",\"type\":[\"int\",\"null\"]},{\"name\":\"perfIp\",\"type\":[\"string\",\"null\"]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.Long OEMID;
  @Deprecated public java.lang.Long curVer;
  @Deprecated public java.lang.CharSequence HID;
  @Deprecated public java.lang.CharSequence UID;
  @Deprecated public java.lang.Long localIP;
  @Deprecated public java.lang.CharSequence sessID;
  @Deprecated public java.lang.CharSequence FID;
  @Deprecated public java.lang.CharSequence URL;
  @Deprecated public java.lang.Long playTick;
  @Deprecated public java.lang.Integer srvNum;
  @Deprecated public java.util.List<com.voole.hobbit2.camus.order.OrderPlayBgnReqSrvV2> _srvs;
  @Deprecated public java.lang.Long natip;
  @Deprecated public java.lang.Integer sessStatus;
  @Deprecated public java.lang.CharSequence perfIp;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public OrderPlayBgnReqV2() {}

  /**
   * All-args constructor.
   */
  public OrderPlayBgnReqV2(java.lang.Long OEMID, java.lang.Long curVer, java.lang.CharSequence HID, java.lang.CharSequence UID, java.lang.Long localIP, java.lang.CharSequence sessID, java.lang.CharSequence FID, java.lang.CharSequence URL, java.lang.Long playTick, java.lang.Integer srvNum, java.util.List<com.voole.hobbit2.camus.order.OrderPlayBgnReqSrvV2> _srvs, java.lang.Long natip, java.lang.Integer sessStatus, java.lang.CharSequence perfIp) {
    this.OEMID = OEMID;
    this.curVer = curVer;
    this.HID = HID;
    this.UID = UID;
    this.localIP = localIP;
    this.sessID = sessID;
    this.FID = FID;
    this.URL = URL;
    this.playTick = playTick;
    this.srvNum = srvNum;
    this._srvs = _srvs;
    this.natip = natip;
    this.sessStatus = sessStatus;
    this.perfIp = perfIp;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return OEMID;
    case 1: return curVer;
    case 2: return HID;
    case 3: return UID;
    case 4: return localIP;
    case 5: return sessID;
    case 6: return FID;
    case 7: return URL;
    case 8: return playTick;
    case 9: return srvNum;
    case 10: return _srvs;
    case 11: return natip;
    case 12: return sessStatus;
    case 13: return perfIp;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: OEMID = (java.lang.Long)value$; break;
    case 1: curVer = (java.lang.Long)value$; break;
    case 2: HID = (java.lang.CharSequence)value$; break;
    case 3: UID = (java.lang.CharSequence)value$; break;
    case 4: localIP = (java.lang.Long)value$; break;
    case 5: sessID = (java.lang.CharSequence)value$; break;
    case 6: FID = (java.lang.CharSequence)value$; break;
    case 7: URL = (java.lang.CharSequence)value$; break;
    case 8: playTick = (java.lang.Long)value$; break;
    case 9: srvNum = (java.lang.Integer)value$; break;
    case 10: _srvs = (java.util.List<com.voole.hobbit2.camus.order.OrderPlayBgnReqSrvV2>)value$; break;
    case 11: natip = (java.lang.Long)value$; break;
    case 12: sessStatus = (java.lang.Integer)value$; break;
    case 13: perfIp = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'OEMID' field.
   */
  public java.lang.Long getOEMID() {
    return OEMID;
  }

  /**
   * Sets the value of the 'OEMID' field.
   * @param value the value to set.
   */
  public void setOEMID(java.lang.Long value) {
    this.OEMID = value;
  }

  /**
   * Gets the value of the 'curVer' field.
   */
  public java.lang.Long getCurVer() {
    return curVer;
  }

  /**
   * Sets the value of the 'curVer' field.
   * @param value the value to set.
   */
  public void setCurVer(java.lang.Long value) {
    this.curVer = value;
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
   * Gets the value of the 'FID' field.
   */
  public java.lang.CharSequence getFID() {
    return FID;
  }

  /**
   * Sets the value of the 'FID' field.
   * @param value the value to set.
   */
  public void setFID(java.lang.CharSequence value) {
    this.FID = value;
  }

  /**
   * Gets the value of the 'URL' field.
   */
  public java.lang.CharSequence getURL() {
    return URL;
  }

  /**
   * Sets the value of the 'URL' field.
   * @param value the value to set.
   */
  public void setURL(java.lang.CharSequence value) {
    this.URL = value;
  }

  /**
   * Gets the value of the 'playTick' field.
   */
  public java.lang.Long getPlayTick() {
    return playTick;
  }

  /**
   * Sets the value of the 'playTick' field.
   * @param value the value to set.
   */
  public void setPlayTick(java.lang.Long value) {
    this.playTick = value;
  }

  /**
   * Gets the value of the 'srvNum' field.
   */
  public java.lang.Integer getSrvNum() {
    return srvNum;
  }

  /**
   * Sets the value of the 'srvNum' field.
   * @param value the value to set.
   */
  public void setSrvNum(java.lang.Integer value) {
    this.srvNum = value;
  }

  /**
   * Gets the value of the '_srvs' field.
   */
  public java.util.List<com.voole.hobbit2.camus.order.OrderPlayBgnReqSrvV2> getSrvs$1() {
    return _srvs;
  }

  /**
   * Sets the value of the '_srvs' field.
   * @param value the value to set.
   */
  public void setSrvs$1(java.util.List<com.voole.hobbit2.camus.order.OrderPlayBgnReqSrvV2> value) {
    this._srvs = value;
  }

  /**
   * Gets the value of the 'natip' field.
   */
  public java.lang.Long getNatip() {
    return natip;
  }

  /**
   * Sets the value of the 'natip' field.
   * @param value the value to set.
   */
  public void setNatip(java.lang.Long value) {
    this.natip = value;
  }

  /**
   * Gets the value of the 'sessStatus' field.
   */
  public java.lang.Integer getSessStatus() {
    return sessStatus;
  }

  /**
   * Sets the value of the 'sessStatus' field.
   * @param value the value to set.
   */
  public void setSessStatus(java.lang.Integer value) {
    this.sessStatus = value;
  }

  /**
   * Gets the value of the 'perfIp' field.
   */
  public java.lang.CharSequence getPerfIp() {
    return perfIp;
  }

  /**
   * Sets the value of the 'perfIp' field.
   * @param value the value to set.
   */
  public void setPerfIp(java.lang.CharSequence value) {
    this.perfIp = value;
  }

  /** Creates a new OrderPlayBgnReqV2 RecordBuilder */
  public static com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder newBuilder() {
    return new com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder();
  }
  
  /** Creates a new OrderPlayBgnReqV2 RecordBuilder by copying an existing Builder */
  public static com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder newBuilder(com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder other) {
    return new com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder(other);
  }
  
  /** Creates a new OrderPlayBgnReqV2 RecordBuilder by copying an existing OrderPlayBgnReqV2 instance */
  public static com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder newBuilder(com.voole.hobbit2.camus.order.OrderPlayBgnReqV2 other) {
    return new com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder(other);
  }
  
  /**
   * RecordBuilder for OrderPlayBgnReqV2 instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<OrderPlayBgnReqV2>
    implements org.apache.avro.data.RecordBuilder<OrderPlayBgnReqV2> {

    private java.lang.Long OEMID;
    private java.lang.Long curVer;
    private java.lang.CharSequence HID;
    private java.lang.CharSequence UID;
    private java.lang.Long localIP;
    private java.lang.CharSequence sessID;
    private java.lang.CharSequence FID;
    private java.lang.CharSequence URL;
    private java.lang.Long playTick;
    private java.lang.Integer srvNum;
    private java.util.List<com.voole.hobbit2.camus.order.OrderPlayBgnReqSrvV2> _srvs;
    private java.lang.Long natip;
    private java.lang.Integer sessStatus;
    private java.lang.CharSequence perfIp;

    /** Creates a new Builder */
    private Builder() {
      super(com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.OEMID)) {
        this.OEMID = data().deepCopy(fields()[0].schema(), other.OEMID);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.curVer)) {
        this.curVer = data().deepCopy(fields()[1].schema(), other.curVer);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.HID)) {
        this.HID = data().deepCopy(fields()[2].schema(), other.HID);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.UID)) {
        this.UID = data().deepCopy(fields()[3].schema(), other.UID);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.localIP)) {
        this.localIP = data().deepCopy(fields()[4].schema(), other.localIP);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.sessID)) {
        this.sessID = data().deepCopy(fields()[5].schema(), other.sessID);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.FID)) {
        this.FID = data().deepCopy(fields()[6].schema(), other.FID);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.URL)) {
        this.URL = data().deepCopy(fields()[7].schema(), other.URL);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.playTick)) {
        this.playTick = data().deepCopy(fields()[8].schema(), other.playTick);
        fieldSetFlags()[8] = true;
      }
      if (isValidValue(fields()[9], other.srvNum)) {
        this.srvNum = data().deepCopy(fields()[9].schema(), other.srvNum);
        fieldSetFlags()[9] = true;
      }
      if (isValidValue(fields()[10], other._srvs)) {
        this._srvs = data().deepCopy(fields()[10].schema(), other._srvs);
        fieldSetFlags()[10] = true;
      }
      if (isValidValue(fields()[11], other.natip)) {
        this.natip = data().deepCopy(fields()[11].schema(), other.natip);
        fieldSetFlags()[11] = true;
      }
      if (isValidValue(fields()[12], other.sessStatus)) {
        this.sessStatus = data().deepCopy(fields()[12].schema(), other.sessStatus);
        fieldSetFlags()[12] = true;
      }
      if (isValidValue(fields()[13], other.perfIp)) {
        this.perfIp = data().deepCopy(fields()[13].schema(), other.perfIp);
        fieldSetFlags()[13] = true;
      }
    }
    
    /** Creates a Builder by copying an existing OrderPlayBgnReqV2 instance */
    private Builder(com.voole.hobbit2.camus.order.OrderPlayBgnReqV2 other) {
            super(com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.SCHEMA$);
      if (isValidValue(fields()[0], other.OEMID)) {
        this.OEMID = data().deepCopy(fields()[0].schema(), other.OEMID);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.curVer)) {
        this.curVer = data().deepCopy(fields()[1].schema(), other.curVer);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.HID)) {
        this.HID = data().deepCopy(fields()[2].schema(), other.HID);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.UID)) {
        this.UID = data().deepCopy(fields()[3].schema(), other.UID);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.localIP)) {
        this.localIP = data().deepCopy(fields()[4].schema(), other.localIP);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.sessID)) {
        this.sessID = data().deepCopy(fields()[5].schema(), other.sessID);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.FID)) {
        this.FID = data().deepCopy(fields()[6].schema(), other.FID);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.URL)) {
        this.URL = data().deepCopy(fields()[7].schema(), other.URL);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.playTick)) {
        this.playTick = data().deepCopy(fields()[8].schema(), other.playTick);
        fieldSetFlags()[8] = true;
      }
      if (isValidValue(fields()[9], other.srvNum)) {
        this.srvNum = data().deepCopy(fields()[9].schema(), other.srvNum);
        fieldSetFlags()[9] = true;
      }
      if (isValidValue(fields()[10], other._srvs)) {
        this._srvs = data().deepCopy(fields()[10].schema(), other._srvs);
        fieldSetFlags()[10] = true;
      }
      if (isValidValue(fields()[11], other.natip)) {
        this.natip = data().deepCopy(fields()[11].schema(), other.natip);
        fieldSetFlags()[11] = true;
      }
      if (isValidValue(fields()[12], other.sessStatus)) {
        this.sessStatus = data().deepCopy(fields()[12].schema(), other.sessStatus);
        fieldSetFlags()[12] = true;
      }
      if (isValidValue(fields()[13], other.perfIp)) {
        this.perfIp = data().deepCopy(fields()[13].schema(), other.perfIp);
        fieldSetFlags()[13] = true;
      }
    }

    /** Gets the value of the 'OEMID' field */
    public java.lang.Long getOEMID() {
      return OEMID;
    }
    
    /** Sets the value of the 'OEMID' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setOEMID(java.lang.Long value) {
      validate(fields()[0], value);
      this.OEMID = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'OEMID' field has been set */
    public boolean hasOEMID() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'OEMID' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearOEMID() {
      OEMID = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'curVer' field */
    public java.lang.Long getCurVer() {
      return curVer;
    }
    
    /** Sets the value of the 'curVer' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setCurVer(java.lang.Long value) {
      validate(fields()[1], value);
      this.curVer = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'curVer' field has been set */
    public boolean hasCurVer() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'curVer' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearCurVer() {
      curVer = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'HID' field */
    public java.lang.CharSequence getHID() {
      return HID;
    }
    
    /** Sets the value of the 'HID' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setHID(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.HID = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'HID' field has been set */
    public boolean hasHID() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'HID' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearHID() {
      HID = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'UID' field */
    public java.lang.CharSequence getUID() {
      return UID;
    }
    
    /** Sets the value of the 'UID' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setUID(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.UID = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'UID' field has been set */
    public boolean hasUID() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'UID' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearUID() {
      UID = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /** Gets the value of the 'localIP' field */
    public java.lang.Long getLocalIP() {
      return localIP;
    }
    
    /** Sets the value of the 'localIP' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setLocalIP(java.lang.Long value) {
      validate(fields()[4], value);
      this.localIP = value;
      fieldSetFlags()[4] = true;
      return this; 
    }
    
    /** Checks whether the 'localIP' field has been set */
    public boolean hasLocalIP() {
      return fieldSetFlags()[4];
    }
    
    /** Clears the value of the 'localIP' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearLocalIP() {
      localIP = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /** Gets the value of the 'sessID' field */
    public java.lang.CharSequence getSessID() {
      return sessID;
    }
    
    /** Sets the value of the 'sessID' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setSessID(java.lang.CharSequence value) {
      validate(fields()[5], value);
      this.sessID = value;
      fieldSetFlags()[5] = true;
      return this; 
    }
    
    /** Checks whether the 'sessID' field has been set */
    public boolean hasSessID() {
      return fieldSetFlags()[5];
    }
    
    /** Clears the value of the 'sessID' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearSessID() {
      sessID = null;
      fieldSetFlags()[5] = false;
      return this;
    }

    /** Gets the value of the 'FID' field */
    public java.lang.CharSequence getFID() {
      return FID;
    }
    
    /** Sets the value of the 'FID' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setFID(java.lang.CharSequence value) {
      validate(fields()[6], value);
      this.FID = value;
      fieldSetFlags()[6] = true;
      return this; 
    }
    
    /** Checks whether the 'FID' field has been set */
    public boolean hasFID() {
      return fieldSetFlags()[6];
    }
    
    /** Clears the value of the 'FID' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearFID() {
      FID = null;
      fieldSetFlags()[6] = false;
      return this;
    }

    /** Gets the value of the 'URL' field */
    public java.lang.CharSequence getURL() {
      return URL;
    }
    
    /** Sets the value of the 'URL' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setURL(java.lang.CharSequence value) {
      validate(fields()[7], value);
      this.URL = value;
      fieldSetFlags()[7] = true;
      return this; 
    }
    
    /** Checks whether the 'URL' field has been set */
    public boolean hasURL() {
      return fieldSetFlags()[7];
    }
    
    /** Clears the value of the 'URL' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearURL() {
      URL = null;
      fieldSetFlags()[7] = false;
      return this;
    }

    /** Gets the value of the 'playTick' field */
    public java.lang.Long getPlayTick() {
      return playTick;
    }
    
    /** Sets the value of the 'playTick' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setPlayTick(java.lang.Long value) {
      validate(fields()[8], value);
      this.playTick = value;
      fieldSetFlags()[8] = true;
      return this; 
    }
    
    /** Checks whether the 'playTick' field has been set */
    public boolean hasPlayTick() {
      return fieldSetFlags()[8];
    }
    
    /** Clears the value of the 'playTick' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearPlayTick() {
      playTick = null;
      fieldSetFlags()[8] = false;
      return this;
    }

    /** Gets the value of the 'srvNum' field */
    public java.lang.Integer getSrvNum() {
      return srvNum;
    }
    
    /** Sets the value of the 'srvNum' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setSrvNum(java.lang.Integer value) {
      validate(fields()[9], value);
      this.srvNum = value;
      fieldSetFlags()[9] = true;
      return this; 
    }
    
    /** Checks whether the 'srvNum' field has been set */
    public boolean hasSrvNum() {
      return fieldSetFlags()[9];
    }
    
    /** Clears the value of the 'srvNum' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearSrvNum() {
      srvNum = null;
      fieldSetFlags()[9] = false;
      return this;
    }

    /** Gets the value of the '_srvs' field */
    public java.util.List<com.voole.hobbit2.camus.order.OrderPlayBgnReqSrvV2> getSrvs$1() {
      return _srvs;
    }
    
    /** Sets the value of the '_srvs' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setSrvs$1(java.util.List<com.voole.hobbit2.camus.order.OrderPlayBgnReqSrvV2> value) {
      validate(fields()[10], value);
      this._srvs = value;
      fieldSetFlags()[10] = true;
      return this; 
    }
    
    /** Checks whether the '_srvs' field has been set */
    public boolean hasSrvs$1() {
      return fieldSetFlags()[10];
    }
    
    /** Clears the value of the '_srvs' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearSrvs$1() {
      _srvs = null;
      fieldSetFlags()[10] = false;
      return this;
    }

    /** Gets the value of the 'natip' field */
    public java.lang.Long getNatip() {
      return natip;
    }
    
    /** Sets the value of the 'natip' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setNatip(java.lang.Long value) {
      validate(fields()[11], value);
      this.natip = value;
      fieldSetFlags()[11] = true;
      return this; 
    }
    
    /** Checks whether the 'natip' field has been set */
    public boolean hasNatip() {
      return fieldSetFlags()[11];
    }
    
    /** Clears the value of the 'natip' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearNatip() {
      natip = null;
      fieldSetFlags()[11] = false;
      return this;
    }

    /** Gets the value of the 'sessStatus' field */
    public java.lang.Integer getSessStatus() {
      return sessStatus;
    }
    
    /** Sets the value of the 'sessStatus' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setSessStatus(java.lang.Integer value) {
      validate(fields()[12], value);
      this.sessStatus = value;
      fieldSetFlags()[12] = true;
      return this; 
    }
    
    /** Checks whether the 'sessStatus' field has been set */
    public boolean hasSessStatus() {
      return fieldSetFlags()[12];
    }
    
    /** Clears the value of the 'sessStatus' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearSessStatus() {
      sessStatus = null;
      fieldSetFlags()[12] = false;
      return this;
    }

    /** Gets the value of the 'perfIp' field */
    public java.lang.CharSequence getPerfIp() {
      return perfIp;
    }
    
    /** Sets the value of the 'perfIp' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder setPerfIp(java.lang.CharSequence value) {
      validate(fields()[13], value);
      this.perfIp = value;
      fieldSetFlags()[13] = true;
      return this; 
    }
    
    /** Checks whether the 'perfIp' field has been set */
    public boolean hasPerfIp() {
      return fieldSetFlags()[13];
    }
    
    /** Clears the value of the 'perfIp' field */
    public com.voole.hobbit2.camus.order.OrderPlayBgnReqV2.Builder clearPerfIp() {
      perfIp = null;
      fieldSetFlags()[13] = false;
      return this;
    }

    @Override
    public OrderPlayBgnReqV2 build() {
      try {
        OrderPlayBgnReqV2 record = new OrderPlayBgnReqV2();
        record.OEMID = fieldSetFlags()[0] ? this.OEMID : (java.lang.Long) defaultValue(fields()[0]);
        record.curVer = fieldSetFlags()[1] ? this.curVer : (java.lang.Long) defaultValue(fields()[1]);
        record.HID = fieldSetFlags()[2] ? this.HID : (java.lang.CharSequence) defaultValue(fields()[2]);
        record.UID = fieldSetFlags()[3] ? this.UID : (java.lang.CharSequence) defaultValue(fields()[3]);
        record.localIP = fieldSetFlags()[4] ? this.localIP : (java.lang.Long) defaultValue(fields()[4]);
        record.sessID = fieldSetFlags()[5] ? this.sessID : (java.lang.CharSequence) defaultValue(fields()[5]);
        record.FID = fieldSetFlags()[6] ? this.FID : (java.lang.CharSequence) defaultValue(fields()[6]);
        record.URL = fieldSetFlags()[7] ? this.URL : (java.lang.CharSequence) defaultValue(fields()[7]);
        record.playTick = fieldSetFlags()[8] ? this.playTick : (java.lang.Long) defaultValue(fields()[8]);
        record.srvNum = fieldSetFlags()[9] ? this.srvNum : (java.lang.Integer) defaultValue(fields()[9]);
        record._srvs = fieldSetFlags()[10] ? this._srvs : (java.util.List<com.voole.hobbit2.camus.order.OrderPlayBgnReqSrvV2>) defaultValue(fields()[10]);
        record.natip = fieldSetFlags()[11] ? this.natip : (java.lang.Long) defaultValue(fields()[11]);
        record.sessStatus = fieldSetFlags()[12] ? this.sessStatus : (java.lang.Integer) defaultValue(fields()[12]);
        record.perfIp = fieldSetFlags()[13] ? this.perfIp : (java.lang.CharSequence) defaultValue(fields()[13]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
