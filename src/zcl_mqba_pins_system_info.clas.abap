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
  data MV_TOPIC type STRING value '/sap/SystemInfo' ##NO_TEXT.
  data MV_CONFIG_ID type STRING .
  data MV_TOPIC_ALL type STRING value 'Today' ##NO_TEXT.
  data MV_TOPIC_LAST_HOUR type STRING value 'LastHour' ##NO_TEXT.
  data MV_TOPIC_CURRENT type STRING value 'Current' ##NO_TEXT.

  methods DO_PUBLISH
    importing
      !IV_TOPIC type STRING
      !IV_PAYLOAD type DATA
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods DO_COLLECT_DUMPS
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods DO_COLLECT_QRFC
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods DO_COLLECT_USERS
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods DO_COLLECT_WPINFO
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
    IF do_collect_wpinfo( ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

* ------- get dump info
    IF do_collect_dumps( ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

* ------- get qrfc info
    IF do_collect_qrfc( ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

* ------- get user info
    IF do_collect_users( ) EQ abap_false.
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


  METHOD do_collect_dumps.

* -------- select
    GET TIME.
    DATA(lv_60m_before) = sy-uzeit - 3600.

    SELECT COUNT(*)
      FROM snap
      INTO @DATA(lv_today)
     WHERE datum EQ @sy-datum
       AND seqno EQ '000'.

    SELECT COUNT(*)
      FROM snap
      INTO @DATA(lv_last60m)
     WHERE datum EQ @sy-datum
       AND uzeit GE @lv_60m_before
       AND seqno EQ '000'.


* --------- publish sums
    DATA(lv_error)  = abap_false.

    IF  do_publish(
            iv_topic = |/{ mv_topic_all }/Dumps|
            iv_payload = lv_today ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

    IF  do_publish(
            iv_topic = |/{ mv_topic_last_hour }/Dumps|
            iv_payload = lv_last60m ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

* ---------- return
    rv_success = COND #( WHEN lv_error EQ abap_false
                         THEN abap_true
                         ELSE abap_false ).

  ENDMETHOD.


  METHOD do_collect_qrfc.

* ---------- local data
    DATA: lt_qin_err    TYPE TABLE OF trfcqview.
    DATA: lt_qout_err   TYPE TABLE OF trfcqview.

* ---------- call api
    CALL FUNCTION 'TRFC_QIN_GET_HANGING_QUEUES'
      TABLES
        err_queue = lt_qin_err.
    DESCRIBE TABLE lt_qin_err LINES DATA(lv_cnt_qin_err).

    CALL FUNCTION 'TRFC_QOUT_GET_HANGING_QUEUES'
      TABLES
        err_queue = lt_qout_err.
    DESCRIBE TABLE lt_qin_err LINES DATA(lv_cnt_qout_err).

* --------- calc
    DATA(lv_cnt_all) = lv_cnt_qin_err + lv_cnt_qout_err.


* --------- publish sums
    DATA(lv_error)  = abap_false.
    DATA(lv_prefix) = |/{ mv_topic_current }|.

    IF  do_publish(
            iv_topic = |{ lv_prefix }/QRFC/Hanging|
            iv_payload = lv_cnt_all ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

* ---------- return
    rv_success = COND #( WHEN lv_error EQ abap_false
                         THEN abap_true
                         ELSE abap_false ).

  ENDMETHOD.


  METHOD do_collect_users.

* ---------- local data
    DATA: lt_data   TYPE TABLE OF uinfos.
    DATA: lt_users  TYPE zmqba_t_string.

* ---------- call api
    CALL FUNCTION 'TH_SYSTEMWIDE_USER_LIST'
      TABLES
        list           = lt_data
      EXCEPTIONS
        argument_error = 1
        send_error     = 2
        OTHERS         = 3.


* --------- calc
* get users and modes
    DATA(lv_cnt_mod_intern) = 0.
    DATA(lv_cnt_mod_extern) = 0.
    LOOP AT lt_data ASSIGNING FIELD-SYMBOL(<lfs_data>).
      READ TABLE lt_users TRANSPORTING NO FIELDS
        WITH KEY table_line = <lfs_data>-bname.
      IF sy-subrc NE 0.
        APPEND <lfs_data>-bname TO lt_users.
      ENDIF.

      IF <lfs_data>-intmodi NE '*'.
        ADD <lfs_data>-intmodi TO lv_cnt_mod_intern.
      ENDIF.

      IF <lfs_data>-extmodi NE '*'.
        ADD <lfs_data>-extmodi TO lv_cnt_mod_extern.
      ENDIF.
    ENDLOOP.

* calc
    DESCRIBE TABLE lt_data LINES DATA(lv_cnt_sessions).
    DESCRIBE TABLE lt_users LINES DATA(lv_cnt_users).
    DATA(lv_cnt_mod_all) = lv_cnt_mod_intern + lv_cnt_mod_extern.
    DATA(lv_cnt_mod_per_user) = CONV float( lv_cnt_mod_all / lv_cnt_users ).

* --------- publish sums
    DATA(lv_error)  = abap_false.
    DATA(lv_prefix) = |/{ mv_topic_current }|.

    IF  do_publish(
            iv_topic = |{ lv_prefix }/Users/Sessions|
            iv_payload = lv_cnt_sessions ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

    IF  do_publish(
            iv_topic = |{ lv_prefix }/Users/LoggedOn|
            iv_payload = lv_cnt_users ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

    IF  do_publish(
            iv_topic = |{ lv_prefix }/Users/ModeInternal|
            iv_payload = lv_cnt_mod_intern ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

    IF  do_publish(
            iv_topic = |{ lv_prefix }/Users/ModeExternal|
            iv_payload = lv_cnt_mod_extern ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

    IF  do_publish(
            iv_topic = |{ lv_prefix }/Users/ModeAll|
            iv_payload = lv_cnt_mod_all ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

    IF  do_publish(
            iv_topic = |{ lv_prefix }/Users/ModePerUser|
            iv_payload = lv_cnt_mod_per_user ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

* ---------- return
    rv_success = COND #( WHEN lv_error EQ abap_false
                         THEN abap_true
                         ELSE abap_false ).

  ENDMETHOD.


  METHOD DO_COLLECT_WPINFO.

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



* ---------- publish per work process type
    DATA(lv_prefix) = |/WP|.
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
    lv_prefix = |/{ mv_topic_all }|.
    lv_usage = 100 * ls_sum-used / ls_sum-count.
    IF  do_publish(
            iv_topic = |{ lv_prefix }/WP/Usage|
            iv_payload = lv_usage ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

    IF  do_publish(
            iv_topic = |{ lv_prefix }/WP/Runtime|
            iv_payload = ls_sum-wp_eltime ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

    IF  do_publish(
            iv_topic = |{ lv_prefix }/WP/Dumps|
            iv_payload = ls_sum-wp_dumps ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

    IF  do_publish(
            iv_topic = |{ lv_prefix }/WP/Restarts|
            iv_payload = ls_sum-wp_irestrt ) EQ abap_false.
      lv_error = abap_true.
    ENDIF.

* ---------- return
    rv_success = COND #( WHEN lv_error EQ abap_false
                         THEN abap_true
                         ELSE abap_false ).

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
