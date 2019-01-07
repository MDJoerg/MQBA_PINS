class ZCL_MQBA_PINS_AD_SYSINFO definition
  public
  inheriting from ZCL_MQBA_PINS_AD_BASE
  create public .

public section.

  methods IF_ABAP_DAEMON_EXTENSION~ON_START
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_STOP
    redefinition .
  methods TASK_EXECUTE
    redefinition .
protected section.

  data MR_SYSINFO type ref to ZIF_MQBA_PINS_SYSTEM_INFO .
private section.
ENDCLASS.



CLASS ZCL_MQBA_PINS_AD_SYSINFO IMPLEMENTATION.


  METHOD if_abap_daemon_extension~on_start.

* -------- init system info service
    mr_sysinfo = zcl_mqba_pins_system_info=>create( ).
    mr_sysinfo->set_config_id( gv_name ).
    gv_interval = 60000. " set default to 60 seconds

* -------- call super
    super->if_abap_daemon_extension~on_start( i_context ).

  ENDMETHOD.


  METHOD if_abap_daemon_extension~on_stop.

* ------- cleanup
    IF mr_sysinfo IS NOT INITIAL.
      mr_sysinfo->destroy( ).
      CLEAR mr_sysinfo.
    ENDIF.

* ------- call super
    super->if_abap_daemon_extension~on_stop(
        i_message = i_message
        i_context = i_context
       ).

  ENDMETHOD.


  METHOD task_execute.
    IF mr_sysinfo IS NOT INITIAL.
      mr_sysinfo->collect( ).
    ELSE.
      zcl_mqba_factory=>create_exception( |invalid system info in TASK_EXECUTE| ).
    ENDIF.
  ENDMETHOD.
ENDCLASS.
