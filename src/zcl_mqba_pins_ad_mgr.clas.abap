CLASS zcl_mqba_pins_ad_mgr DEFINITION
  PUBLIC
  CREATE PUBLIC .

  PUBLIC SECTION.

    INTERFACES zif_mqba_pins_ad_mgr .

    CLASS-METHODS create
      RETURNING
        VALUE(rr_mgr) TYPE REF TO zif_mqba_pins_ad_mgr .
  PROTECTED SECTION.

    DATA mv_adf_type TYPE string .
    DATA mv_adf_id TYPE string .
    DATA mv_error TYPE string .
    DATA mv_result TYPE string .
    DATA c_error_invalid TYPE string VALUE 'invalid configuration' ##NO_TEXT.
    DATA c_error_exists TYPE string VALUE 'already registered' ##NO_TEXT.
    DATA c_error_start TYPE string VALUE 'could not start' ##NO_TEXT.
    DATA c_error_stop TYPE string VALUE 'could not stop' ##NO_TEXT.
    DATA c_error_not_found TYPE string VALUE 'instance not found' ##NO_TEXT.
    DATA c_shm_group TYPE string VALUE 'ZCL_MQBA_PINS_AD_MGR' ##NO_TEXT.

    METHODS reset .
    METHODS set_guid
      IMPORTING
        !iv_guid          TYPE data OPTIONAL
      RETURNING
        VALUE(rv_success) TYPE abap_bool .
  PRIVATE SECTION.
ENDCLASS.



CLASS ZCL_MQBA_PINS_AD_MGR IMPLEMENTATION.


  METHOD create.
*   future use: test double or enhancement concept
    rr_mgr = NEW zcl_mqba_pins_ad_mgr( ).
  ENDMETHOD.


  METHOD reset.
    CLEAR: mv_error, mv_result.
  ENDMETHOD.


  METHOD set_guid.
    IF zif_mqba_pins_ad_mgr~is_valid( ) EQ abap_false.
      rv_success = abap_false.
      RETURN.
    ENDIF.

*    DATA(lv_memid) = CONV char40( |{ mv_adf_type }:{ mv_adf_id }| ).
*
*    IF iv_guid IS NOT INITIAL.
*      DATA(lv_guid) = CONV string( iv_guid ).
*      EXPORT lv_memid FROM lv_guid TO MEMORY ID lv_memid.
*    ELSE.
*      FREE MEMORY ID lv_memid.
*    ENDIF.

    DATA(lr_util) = zcl_mqba_factory=>get_shm_context( ).
    lr_util->set_group( mv_adf_type ).
    rv_success = lr_util->put(
        iv_param   = mv_adf_id
        iv_value   = iv_guid ).

  ENDMETHOD.


  METHOD zif_mqba_pins_ad_mgr~get_cmd_result.
    rv_result = mv_result.
  ENDMETHOD.


  METHOD zif_mqba_pins_ad_mgr~get_error.
    rv_error = mv_error.
  ENDMETHOD.


  METHOD zif_mqba_pins_ad_mgr~get_guid.
    IF zif_mqba_pins_ad_mgr~is_valid( ) EQ abap_false.
      RETURN.
    ENDIF.

*    DATA(lv_memid) = CONV char40( |{ mv_adf_type }:{ mv_adf_id }| ).
*    IMPORT lv_memid TO rv_guid FROM MEMORY ID lv_memid.

    DATA(lr_util) = zcl_mqba_factory=>get_shm_context( ).
    lr_util->set_group( mv_adf_type ).
    rv_guid = lr_util->get( mv_adf_id ).
  ENDMETHOD.


  METHOD zif_mqba_pins_ad_mgr~get_id.
    rv_id = mv_adf_id.
  ENDMETHOD.


  METHOD zif_mqba_pins_ad_mgr~get_type.
    rv_type = mv_adf_type.
  ENDMETHOD.


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

        IF iv_param IS NOT INITIAL.
          pcp->set_field(
              i_name  = 'PARAM'
              i_value = CONV string( iv_param )
          ).
        ENDIF.


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


  METHOD zif_mqba_pins_ad_mgr~set_id.
    mv_adf_id = iv_id.
    rr_self = me.
  ENDMETHOD.


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
