class ZCL_MQBA_PINS_AD_BASE definition
  public
  inheriting from CL_ABAP_DAEMON_EXT_BASE
  create public .

public section.

  interfaces IF_ABAP_TIMER_HANDLER .

  methods TASK_EXECUTE .
  methods DO_CMD
    importing
      !IR_MESSAGE type ref to IF_AC_MESSAGE_TYPE_PCP
      !IR_CONTEXT type ref to IF_ABAP_DAEMON_CONTEXT
      !IV_CMD type STRING
      !IV_PARAM type STRING
      !IV_PAYLOAD type STRING
    returning
      value(RV_SUCCESS) type ABAP_BOOL .

  methods IF_ABAP_DAEMON_EXTENSION~ON_ACCEPT
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_BEFORE_RESTART_BY_SYSTEM
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_ERROR
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_MESSAGE
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_RESTART
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_SERVER_SHUTDOWN
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_START
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_STOP
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_SYSTEM_SHUTDOWN
    redefinition .
protected section.

  class-data GV_NAME type STRING .
  class-data GV_STOPPED type ABAP_BOOL .
  class-data GV_INTERVAL type I value 300 ##NO_TEXT.

  methods DO_CMD_PING
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods DO_CMD_TASK_SCHEDULE
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods TASK_SCHEDULE_NEXT .
private section.
ENDCLASS.



CLASS ZCL_MQBA_PINS_AD_BASE IMPLEMENTATION.


  METHOD do_cmd.

* ------- init
    rv_success = abap_true.

* ------- process
    CASE iv_cmd.
      WHEN 'PING'.
        rv_success = do_cmd_ping( ).
      WHEN 'TASK_SCHEDULE'.
        rv_success = do_cmd_task_schedule( ).
      WHEN OTHERS.
    ENDCASE.

  ENDMETHOD.


  method DO_CMD_PING.
    rv_success = abap_true.
  endmethod.


  METHOD do_cmd_task_schedule.
    IF gv_stopped EQ abap_false.
      task_schedule_next( ).
      rv_success = abap_true.
    ELSE.
      rv_success = abap_false.
    ENDIF.
  ENDMETHOD.


  METHOD IF_ABAP_DAEMON_EXTENSION~ON_ACCEPT.

    TRY.
*     get the caller info and store to static context
        DATA(ls_caller_info) = i_context_base->get_start_caller_info( ).
        gv_name = ls_caller_info-name.

        e_setup_mode = co_setup_mode-accept.


        " apply the access rights, e.g. based on caller program or proper authority checks
        " IF lv_caller_info-program = <permitted program>.
        " e_setup_mode = co_setup_mode-accept.
        " ELSE.
        " e_setup_mode = co_setup_mode-reject.
        " ENDIF.


      CATCH cx_abap_daemon_error INTO DATA(lx_ad_error).
        " todo: error handling , e.g. write error log !
        e_setup_mode = co_setup_mode-reject.
    ENDTRY.

** ------- set status
*    IF e_setup_mode = co_setup_mode-accept.
*      send_mqtt_message( 'ACCEPTED' ).
*    ELSE.
*      send_mqtt_message( 'REJECTED' ).
*    ENDIF.


  ENDMETHOD.


  METHOD IF_ABAP_DAEMON_EXTENSION~ON_BEFORE_RESTART_BY_SYSTEM.
  ENDMETHOD.


  METHOD IF_ABAP_DAEMON_EXTENSION~ON_ERROR.
  ENDMETHOD.


  METHOD if_abap_daemon_extension~on_message.

* -------- local data
    DATA: lv_msg     TYPE string.
    DATA: lv_cmd     TYPE string.
    DATA: lv_param   TYPE string.
    DATA: lv_payload TYPE string.
    DATA: lv_success TYPE abap_bool.

* -------- get command from message
    TRY.
        lv_cmd     = i_message->get_text( ).
        lv_param   = i_message->get_field( 'PARAM' ).
        lv_payload = i_message->get_field( 'PAYLOAD' ).
      CATCH cx_ac_message_type_pcp_error INTO DATA(lx_pcp).
        lv_msg = |Error getting channel data for command (PCP): { lx_pcp->get_text( ) }|.
    ENDTRY.

* ------- process
    IF lv_cmd IS NOT INITIAL.
      lv_success = do_cmd(
                       ir_message = i_message
                       ir_context = i_context
                       iv_cmd     = lv_cmd
                       iv_param   = lv_param
                       iv_payload = lv_payload
      ).
    ENDIF.


* -------- check success
    IF lv_success EQ abap_false.
      IF lv_msg IS INITIAL.
        lv_msg = |Error processing command { lv_cmd }: { lv_param }/{ lv_payload }|.
      ENDIF.
      zcl_mqba_factory=>create_exception( lv_msg  ).
    ENDIF.

  ENDMETHOD.


  METHOD IF_ABAP_DAEMON_EXTENSION~ON_RESTART.
  ENDMETHOD.


  METHOD IF_ABAP_DAEMON_EXTENSION~ON_SERVER_SHUTDOWN.
  ENDMETHOD.


  METHOD IF_ABAP_DAEMON_EXTENSION~ON_START.

* execute task first time
    task_execute( ).

*  schedule for next call
    task_schedule_next( ).

  ENDMETHOD.


  METHOD IF_ABAP_DAEMON_EXTENSION~ON_STOP.
    gv_stopped = abap_true.
    TRY.
        cl_abap_timer_manager=>get_timer_manager( )->stop_timer( i_timer_handler = me ).
      CATCH cx_abap_timer_error.
    ENDTRY.
  ENDMETHOD.


  METHOD IF_ABAP_DAEMON_EXTENSION~ON_SYSTEM_SHUTDOWN.
  ENDMETHOD.


  METHOD if_abap_timer_handler~on_timeout.

* ----- execute task
    task_execute( ).

* ----- schedule for next
    IF gv_stopped EQ abap_false.
      task_schedule_next( ).
    ENDIF.

  ENDMETHOD.


  METHOD TASK_EXECUTE.
  ENDMETHOD.


  METHOD task_schedule_next.

    TRY.
        cl_abap_timer_manager=>get_timer_manager( )->start_timer(
          EXPORTING
            i_timer_handler = me
            i_timeout       = gv_interval
        ).
      CATCH cx_abap_timer_error INTO DATA(lx_error).
        zcl_mqba_factory=>create_exception( |Error while rescheduling task: { lx_error->get_text( ) }| ).
    ENDTRY.
  ENDMETHOD.
ENDCLASS.
