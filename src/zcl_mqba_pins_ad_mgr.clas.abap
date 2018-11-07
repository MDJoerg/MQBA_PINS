class ZCL_MQBA_PINS_AD_MGR definition
  public
  create public .

public section.

  interfaces ZIF_MQBA_PINS_AD_MGR .

  class-methods CREATE
    returning
      value(RR_MGR) type ref to ZIF_MQBA_PINS_AD_MGR .
protected section.

  data MV_ADF_TYPE type STRING .
  data MV_ADF_ID type STRING .
  data MV_ERROR type STRING .
  data MV_RESULT type STRING .
  data C_ERROR_INVALID type STRING value 'invalid configuration' ##NO_TEXT.
  data C_ERROR_EXISTS type STRING value 'already registered' ##NO_TEXT.
  data C_ERROR_START type STRING value 'could not start' ##NO_TEXT.
  data C_ERROR_STOP type STRING value 'could not stop' ##NO_TEXT.
  data C_ERROR_NOT_FOUND type STRING value 'instance not found' ##NO_TEXT.

  methods RESET .
  methods SET_GUID
    importing
      !IV_GUID type DATA optional
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
private section.
ENDCLASS.



CLASS ZCL_MQBA_PINS_AD_MGR IMPLEMENTATION.


  method CREATE.
*   future use: test double or enhancement concept
    rr_mgr = new ZCL_MQBA_PINS_AD_MGR( ).
  endmethod.


  METHOD reset.
    CLEAR: mv_error, mv_result.
  ENDMETHOD.


  METHOD set_guid.
    CHECK zif_mqba_pins_ad_mgr~is_valid( ) EQ abap_true.
    DATA(lv_memid) = CONV char40( |{ mv_adf_type }:{ mv_adf_id }| ).

    IF iv_guid IS NOT INITIAL.
      DATA(lv_guid) = CONV string( iv_guid ).
      EXPORT lv_memid FROM lv_guid TO MEMORY ID lv_memid.
    ELSE.
      FREE MEMORY ID lv_memid.
    ENDIF.
  ENDMETHOD.


  method ZIF_MQBA_PINS_AD_MGR~GET_CMD_RESULT.
    rv_result = mv_result.
  endmethod.


  method ZIF_MQBA_PINS_AD_MGR~GET_ERROR.
    rv_error = mv_error.
  endmethod.


  METHOD zif_mqba_pins_ad_mgr~get_guid.
    CHECK zif_mqba_pins_ad_mgr~is_valid( ) EQ abap_true.
    DATA(lv_memid) = CONV char40( |{ mv_adf_type }:{ mv_adf_id }| ).
    IMPORT lv_memid TO rv_guid FROM MEMORY ID lv_memid.
  ENDMETHOD.


  method ZIF_MQBA_PINS_AD_MGR~GET_ID.
    rv_id = mv_adf_id.
  endmethod.


  method ZIF_MQBA_PINS_AD_MGR~GET_TYPE.
    rv_type = mv_adf_type.
  endmethod.


  METHOD zif_mqba_pins_ad_mgr~is_valid.
    CHECK mv_adf_id   IS NOT INITIAL.
    CHECK mv_adf_type IS NOT INITIAL.
    rv_valid = abap_true.
  ENDMETHOD.


  METHOD zif_mqba_pins_ad_mgr~send_cmd.

* ------ check
    reset( ).
    CHECK zif_mqba_pins_ad_mgr~is_valid( ) EQ abap_true.


* ------ get existing
    DATA(lv_guid) = zif_mqba_pins_ad_mgr~get_guid( ).
    IF lv_guid IS INITIAL.
      mv_error = c_error_not_found.
      RETURN.
    ENDIF.

    TRY.
* ------ create pcp message
        DATA(pcp) = cl_ac_message_type_pcp=>create( ).
        pcp->set_text( CONV string( iv_cmd ) ).
        pcp->set_field(
            i_name  = 'PAYLOAD'
            i_value = CONV string( iv_payload )
        ).

* ------ send
        cl_abap_daemon_client_manager=>attach( i_instance_id = lv_guid )->send( pcp ).

* ------ success
        rv_success = abap_true.

* ------ error handling
      CATCH cx_abap_daemon_error
            cx_ac_message_type_pcp_error
            INTO DATA(lx_error).
        mv_error = lx_error->get_text( ).
    ENDTRY.

  ENDMETHOD.


  method ZIF_MQBA_PINS_AD_MGR~SET_ID.
    mv_adf_id = iv_id.
    rr_self = me.
  endmethod.


  METHOD zif_mqba_pins_ad_mgr~set_type.
    mv_adf_type = iv_type.
    rr_self = me.
  ENDMETHOD.


  METHOD zif_mqba_pins_ad_mgr~start.

* ------ check
    reset( ).
    CHECK zif_mqba_pins_ad_mgr~is_valid( ) EQ abap_true.


* ------ get existing
    DATA(lv_guid) = zif_mqba_pins_ad_mgr~get_guid( ).
    IF lv_guid IS NOT INITIAL.
      mv_error = c_error_invalid.
      RETURN.
    ENDIF.

    TRY.
* ------ start
        cl_abap_daemon_client_manager=>start(
         EXPORTING
           i_class_name = CONV abap_daemon_class_name( mv_adf_type )
           i_name       = CONV abap_daemon_name( mv_adf_id )
        IMPORTING
           e_instance_id = lv_guid ).

        IF lv_guid IS INITIAL.
          mv_error = c_error_start.
          RETURN.
        ENDIF.


* ------- save to memory
        set_guid( lv_guid ).
        rv_success = abap_true.

* ------ error handling
      CATCH cx_abap_daemon_error
            cx_ac_message_type_pcp_error
            INTO DATA(lx_error).
        mv_error = lx_error->get_text( ).
    ENDTRY.


  ENDMETHOD.


  METHOD zif_mqba_pins_ad_mgr~stop.
* ------ check
    reset( ).
    CHECK zif_mqba_pins_ad_mgr~is_valid( ) EQ abap_true.


* ------ get existing
    DATA(lv_guid) = zif_mqba_pins_ad_mgr~get_guid( ).
    IF lv_guid IS INITIAL.
      mv_error = c_error_stop.
      RETURN.
    ENDIF.

    TRY.
* ------ start
        cl_abap_daemon_client_manager=>stop(
         i_instance_id = lv_guid ).

* ------- save to memory
        set_guid( ).
        rv_success = abap_true.

* ------ error handling
      CATCH cx_abap_daemon_error
            cx_ac_message_type_pcp_error
            INTO DATA(lx_error).
        mv_error = lx_error->get_text( ).
    ENDTRY.

  ENDMETHOD.
ENDCLASS.
