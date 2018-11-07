class ZCL_MQBA_PINS_AD_BASE definition
  public
  inheriting from CL_ABAP_DAEMON_EXT_BASE
  create public .

public section.

  interfaces IF_ABAP_TIMER_HANDLER .

  methods TASK_EXECUTE .

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

  methods TASK_SCHEDULE_NEXT .
private section.
ENDCLASS.



CLASS ZCL_MQBA_PINS_AD_BASE IMPLEMENTATION.


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


  METHOD IF_ABAP_DAEMON_EXTENSION~ON_MESSAGE.
    TRY.
        DATA(msg) = i_message->get_text( ).

      CATCH cx_ac_message_type_pcp_error.
        RETURN.
    ENDTRY.
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


  METHOD IF_ABAP_TIMER_HANDLER~ON_TIMEOUT.

* ----- execute task
    task_execute( ).

* ----- schedule for next
    CHECK gv_stopped EQ abap_false.
    task_schedule_next( ).

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
    ENDTRY.
  ENDMETHOD.
ENDCLASS.
