interface ZIF_MQBA_PINS_AD_MGR
  public .


  methods SET_TYPE
    importing
      !IV_TYPE type DATA
    returning
      value(RR_SELF) type ref to ZIF_MQBA_PINS_AD_MGR .
  methods SET_ID
    importing
      !IV_ID type DATA
    returning
      value(RR_SELF) type ref to ZIF_MQBA_PINS_AD_MGR .
  methods GET_TYPE
    returning
      value(RV_TYPE) type STRING .
  methods GET_ID
    returning
      value(RV_ID) type STRING .
  methods GET_GUID
    returning
      value(RV_GUID) type STRING .
  methods START
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods STOP
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods SEND_CMD
    importing
      !IV_CMD type DATA
      !IV_PARAM type DATA optional
      !IV_PAYLOAD type DATA
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods GET_ERROR
    returning
      value(RV_ERROR) type STRING .
  methods GET_CMD_RESULT
    returning
      value(RV_RESULT) type STRING .
  methods IS_VALID
    returning
      value(RV_VALID) type ABAP_BOOL .
endinterface.
