class ZCL_MQBA_PINS_SYSTEM_INFO definition
  public
  create public .

public section.

  interfaces ZIF_MQBA_PINS_SYSTEM_INFO .

  class-methods CREATE
    returning
      value(RR_INSTANCE) type ref to ZIF_MQBA_PINS_SYSTEM_INFO .
protected section.

  data MV_EXTERNAL type ABAP_BOOL value ABAP_TRUE ##NO_TEXT.
  data MV_GATEWAY type ZMQBA_BROKER_ID .
  data MV_TOPIC type STRING value '/sc0001/SystemInfo' ##NO_TEXT.
  data MV_CONFIG_ID type STRING .

  methods DO_PUBLISH
    importing
      !IV_TOPIC type STRING
      !IV_PAYLOAD type DATA
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods GET_WPINFO
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods COLLECT_FINISH
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods COLLECT_PREPARE
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods COLLECT_PROCESS
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods GET_TOPIC_PREFIX
    returning
      value(RV_PREFIX) type STRING .
private section.
ENDCLASS.



CLASS ZCL_MQBA_PINS_SYSTEM_INFO IMPLEMENTATION.


  method COLLECT_FINISH.

* ------- init
  rv_success = abap_true.

  endmethod.


  method COLLECT_PREPARE.

* ------- init
  rv_success = abap_true.

  endmethod.


  METHOD collect_process.

* ------- init
    rv_success = abap_false.
    DATA(lv_error) = abap_false.

* ------- get work process info
    IF get_wpinfo( ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.



* ------ final success
    rv_success = COND #( WHEN lv_error EQ abap_true
                         THEN abap_false
                         ELSE abap_true ).

  ENDMETHOD.


  METHOD create.
    rr_instance = NEW zcl_mqba_pins_system_info( ).
  ENDMETHOD.


  METHOD do_publish.

* ------ local data
    DATA: lv_error    TYPE abap_bool.
    DATA: lv_errtext  TYPE zmqba_error_text.


* ------ build topic
    DATA(lv_topic) = get_topic_prefix( ).
    lv_topic = lv_topic && iv_topic.


* ------ build prefix
    DATA: lv_payload TYPE string.
    lv_payload = iv_payload.

* ------ public
    CALL FUNCTION 'Z_MQBA_API_BROKER_PUBLISH'
      EXPORTING
        iv_topic      = lv_topic
        iv_payload    = lv_payload
*       IV_SESSION_ID =
        iv_external   = mv_external
        iv_gateway    = mv_gateway
*       IV_CONTEXT    =
*       IT_PROPS      =
      IMPORTING
        ev_error_text = lv_errtext
        ev_error      = lv_error
*       EV_GUID       =
*       EV_SCOPE      =
      .

    IF lv_error EQ abap_true.
      IF 1 = 1.

      ENDIF.
    ENDIF.


* ------ build return
    rv_success = COND #( WHEN lv_error EQ abap_true
                         THEN abap_false
                         ELSE abap_true ).


  ENDMETHOD.


  method GET_TOPIC_PREFIX.
    rv_prefix = mv_topic.
  endmethod.


  METHOD get_wpinfo.

* ---------- local data
    DATA: lt_data TYPE TABLE OF wpinfos.
    DATA: BEGIN OF ls_calc,
            wp_typ     TYPE  wptyp,
            count      TYPE  i,
            used       TYPE  i,
            wp_eltime  TYPE  wpelzeit,
            wp_irestrt TYPE  wpirestart,
            wp_dumps   TYPE  wpdumps,
          END OF ls_calc.
    DATA: ls_sum  LIKE ls_calc.
    DATA: lt_calc LIKE TABLE OF ls_calc.
    FIELD-SYMBOLS: <lfs_calc> LIKE ls_calc.

* --------- local macro
    DEFINE collect_calc.
      <lfs_calc>-&1 = <lfs_calc>-&1 + <lfs_data>-&1.
    END-OF-DEFINITION.
    DEFINE collect_sum.
      ls_sum-&1 = ls_sum-&1 + <lfs_data>-&1.
    END-OF-DEFINITION.



* ---------- call api
    CALL FUNCTION 'TH_SYSTEMWIDE_WPINFO'
      TABLES
        wplist         = lt_data
      EXCEPTIONS
        argument_error = 1
        send_error     = 2
        OTHERS         = 3.
    IF sy-subrc <> 0.
      rv_success = abap_false.
      RETURN.
    ENDIF.


* ---------- calc
    LOOP AT lt_data ASSIGNING FIELD-SYMBOL(<lfs_data>).

*     get index
      UNASSIGN <lfs_calc>.
      READ TABLE lt_calc ASSIGNING <lfs_calc>
        WITH KEY wp_typ = <lfs_data>-wp_typ.
      IF sy-subrc NE 0.
        APPEND INITIAL LINE TO lt_calc ASSIGNING <lfs_calc>.
        <lfs_calc>-wp_typ = <lfs_data>-wp_typ.
      ENDIF.

*     collect data
      ADD 1 TO <lfs_calc>-count.
      collect_calc wp_dumps.
      collect_calc wp_eltime.
      collect_calc wp_irestrt.

*    collect sum
      ADD 1 TO ls_sum-count.
      collect_sum wp_dumps.
      collect_sum wp_eltime.
      collect_sum wp_irestrt.

*     collect used
      IF <lfs_data>-wp_istatus NE 2.
        ADD 1 TO <lfs_calc>-used.
        ADD 1 TO ls_sum-used.
      ENDIF.
    ENDLOOP.



* ---------- publish per type
    DATA(lv_prefix) = '/WP'.
    DATA(lv_error)  = abap_false.

    LOOP AT lt_calc INTO ls_calc.

      DATA(lv_usage) = 100 * ls_calc-used / ls_calc-count.
      IF  do_publish(
              iv_topic = |{ lv_prefix }/Type/{ ls_calc-wp_typ }/Usage|
              iv_payload = lv_usage ) EQ abap_false.
        lv_error = abap_true.
      ENDIF.

      IF  do_publish(
              iv_topic = |{ lv_prefix }/Type/{ ls_calc-wp_typ }/Runtime|
              iv_payload = ls_calc-wp_eltime ) EQ abap_false.
        lv_error = abap_true.
      ENDIF.

      IF  do_publish(
              iv_topic = |{ lv_prefix }/Type/{ ls_calc-wp_typ }/Dumps|
              iv_payload = ls_calc-wp_dumps ) EQ abap_false.
        lv_error = abap_true.
      ENDIF.

      IF  do_publish(
              iv_topic = |{ lv_prefix }/Type/{ ls_calc-wp_typ }/Restarts|
              iv_payload = ls_calc-wp_irestrt ) EQ abap_false.
        lv_error = abap_true.
      ENDIF.

    ENDLOOP.

* --------- publish sums
    lv_usage = 100 * ls_sum-used / ls_sum-count.
    IF  do_publish(
            iv_topic = |{ lv_prefix }Usage|
            iv_payload = lv_usage ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

    IF  do_publish(
            iv_topic = |{ lv_prefix }Runtime|
            iv_payload = ls_sum-wp_eltime ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

    IF  do_publish(
            iv_topic = |{ lv_prefix }Dumps|
            iv_payload = ls_sum-wp_dumps ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

    IF  do_publish(
            iv_topic = |{ lv_prefix }Restarts|
            iv_payload = ls_sum-wp_irestrt ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

* ---------- return
    rv_success = COND #( WHEN lv_error EQ abap_false
                         THEN abap_true
                         ELSE abap_false ).

  ENDMETHOD.


  METHOD zif_mqba_pins_system_info~collect.

* ------- init
    rv_success = abap_false.

* ------- prepare
    IF collect_prepare( ) EQ abap_false.
      RETURN.
    ENDIF.


* ------- process
    IF collect_process( ) EQ abap_false.
      RETURN.
    ENDIF.



* ------- finish
    IF collect_finish( ) EQ abap_false.
      RETURN.
    ENDIF.


* ------- finally true
    rv_success = abap_true.

  ENDMETHOD.


  method ZIF_MQBA_PINS_SYSTEM_INFO~DESTROY.
  endmethod.


  METHOD zif_mqba_pins_system_info~set_config_id.
    mv_config_id = iv_config_id.
    rr_self = me.
  ENDMETHOD.
ENDCLASS.
