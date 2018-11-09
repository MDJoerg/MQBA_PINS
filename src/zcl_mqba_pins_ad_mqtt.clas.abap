class ZCL_MQBA_PINS_AD_MQTT definition
  public
  inheriting from ZCL_MQBA_PINS_AD_BASE
  create public .

public section.

  methods IF_ABAP_DAEMON_EXTENSION~ON_ACCEPT
    redefinition .
  methods TASK_EXECUTE
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_STOP
    redefinition .
protected section.

  data MR_PROXY type ref to ZIF_MQBA_API_MQTT_PROXY .
private section.
ENDCLASS.



CLASS ZCL_MQBA_PINS_AD_MQTT IMPLEMENTATION.


  METHOD if_abap_daemon_extension~on_accept.



* ------------
    TRY.
*     default reject
        e_setup_mode = co_setup_mode-reject.

*     get the caller info and store to static context
        DATA(ls_caller_info) = i_context_base->get_start_caller_info( ).
        gv_name = ls_caller_info-name.

*     log
        LOG-POINT ID zmqba_gw
           SUBKEY 'IF_ABAP_DAEMON_EXTENSION~ON_ACCEPT'
           FIELDS gv_name
                  ls_caller_info.


*     get the broker configuration
        DATA(lr_brkc) = zcl_mqba_factory=>get_broker_config( CONV zmqba_broker_id( gv_name ) ).
        CHECK lr_brkc IS NOT INITIAL.

*     todo: set check interval
        gv_interval = 30.


*     get the broker proxy and try to set config
        DATA(lr_proxy) = zcl_mqba_pins_mqtt_protocol=>create( ).
        CHECK lr_proxy->set_config( lr_brkc ) EQ abap_true.

*     try to connect
        CHECK lr_proxy->connect( ) EQ abap_true.

*     set the broker proxy and set to accept
        mr_proxy = lr_proxy.
        e_setup_mode = co_setup_mode-accept.

*     error handling
      CATCH cx_abap_daemon_error INTO DATA(lx_ad_error).
        e_setup_mode = co_setup_mode-reject.
        DATA(lv_error) = lx_ad_error->get_text( ).

        ASSERT ID zmqba_gw
           SUBKEY 'IF_ABAP_DAEMON_EXTENSION~ON_ACCEPT'
           FIELDS gv_name
                  lv_error
          CONDITION 1 = 2.
    ENDTRY.

  ENDMETHOD.


  METHOD if_abap_daemon_extension~on_stop.

    LOG-POINT ID zmqba_gw
        SUBKEY 'IF_ABAP_DAEMON_EXTENSION~ON_STOP'.

    mr_proxy->disconnect( ).

    CALL METHOD super->if_abap_daemon_extension~on_stop
      EXPORTING
        i_message = i_message
        i_context = i_context.


  ENDMETHOD.


  METHOD task_execute.

* --------- check
    ASSERT ID zmqba_gw
      SUBKEY 'TASK_EXECUTE'
      CONDITION mr_proxy IS BOUND.


* --------- check connected
    DATA(lv_connected) = mr_proxy->is_connected( ).

    LOG-POINT ID zmqba_gw
       SUBKEY 'TASK_EXECUTE:is_connected'
       FIELDS lv_connected.

    CHECK lv_connected EQ abap_true.


* -------- reconnect
    DATA(lv_reconnect) = mr_proxy->reconnect( ).

    ASSERT ID zmqba_gw
      SUBKEY 'TASK_EXECUTE:reconnect'
      CONDITION lv_reconnect EQ abap_true.

  ENDMETHOD.
ENDCLASS.
