class ZCL_MQBA_PINS_AD_MQTT definition
  public
  inheriting from ZCL_MQBA_PINS_AD_BASE
  create public .

public section.
  type-pools ABAP .

  methods IF_ABAP_DAEMON_EXTENSION~ON_ACCEPT
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_START
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_STOP
    redefinition .
  methods TASK_EXECUTE
    redefinition .
protected section.

  data MR_PROXY type ref to ZIF_MQBA_API_MQTT_PROXY .
  data MV_BROKER_ID type STRING .
  data MS_CONFIG type ZMQBA_API_S_BRK_CFG .

  methods BROKER_CONNECT
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods BROKER_DISCONNECT
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods BROKER_INIT_AFTER_CONNECT
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods BROKER_IS_CONNECTED
    returning
      value(RV_CONNECTED) type ABAP_BOOL .
  methods BROKER_RECONNECT
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods GET_BROKER_ID_FROM_CONTEXT
    importing
      !IR_CONTEXT type ref to IF_ABAP_DAEMON_CONTEXT
    returning
      value(RV_BROKER_ID) type STRING .
private section.
ENDCLASS.



CLASS ZCL_MQBA_PINS_AD_MQTT IMPLEMENTATION.


  METHOD broker_connect.

* ---------- log
    LOG-POINT ID zmqba_gw
       SUBKEY 'BROKER_CONNECT'
       FIELDS mv_broker_id.

    ASSERT ID  zmqba_gw
    SUBKEY 'BROKER_CONNECT:No_ID'
       CONDITION mv_broker_id IS NOT INITIAL.


* --------- check for existing connection
    IF mr_proxy IS NOT INITIAL.
      broker_disconnect( ).
    ENDIF.

* --------- init
    rv_success = abap_false.

* --------- get broker config handler
    DATA(lr_brkc) = zcl_mqba_factory=>get_broker_config( CONV zmqba_broker_id( mv_broker_id ) ).

    ASSERT ID  zmqba_gw
    SUBKEY 'BROKER_CONNECT:No_ConfigHandler'
       CONDITION lr_brkc IS NOT INITIAL.


* --------- get broker config
    ms_config = lr_brkc->get_config( ).

    ASSERT ID  zmqba_gw
    SUBKEY 'BROKER_CONNECT:No_Config'
       CONDITION ms_config IS NOT INITIAL.


* --------- create a proxy
    DATA(lr_proxy) = zcl_mqba_pins_mqtt_protocol=>create( ).
    IF lr_proxy->set_config( lr_brkc ) EQ abap_false.
      ASSERT ID  zmqba_gw
      SUBKEY 'BROKER_CONNECT:Init_Failed'
         CONDITION 1 = 2.
    ENDIF.

* --------- connect
*     try to connect
    IF lr_proxy->connect( ) EQ abap_false.
      ASSERT ID  zmqba_gw
      SUBKEY 'BROKER_CONNECT:Connect_Failed'
         CONDITION 1 = 2.
    ENDIF.


* --------- set to instance and start post actions
    mr_proxy = lr_proxy.

    IF broker_init_after_connect( ) EQ abap_false.
      ASSERT ID  zmqba_gw
      SUBKEY 'BROKER_CONNECT:AfterStart_Init_Failed'
         CONDITION 1 = 2.
    ENDIF.

* -------- finally set true
    rv_success = abap_true.


  ENDMETHOD.


  METHOD broker_disconnect.

* -------- init
    rv_success = abap_true.
    IF mr_proxy IS INITIAL.
      RETURN.
    ENDIF.

* -------- check connected
    IF mr_proxy->is_connected( ) EQ abap_true.
      mr_proxy->disconnect( ).
      mr_proxy->destroy( ).
      CLEAR mr_proxy.
    ENDIF.

  ENDMETHOD.


  METHOD broker_init_after_connect.

* ------- init
    rv_success = abap_false.
    CHECK ms_config IS NOT INITIAL.


* ------- subscribe
    DATA(lv_success) = mr_proxy->subscribe_to(
      EXPORTING
        iv_topic      = '#'
        iv_use_prefix = abap_true        " use external prefix
*            iv_qos        = 0                " MQTT Quality of service option
    ).


* ------ set true
    rv_success = abap_true.

  ENDMETHOD.


  METHOD broker_is_connected.

    rv_connected = abap_false.

    IF mr_proxy IS NOT INITIAL.
      rv_connected = mr_proxy->is_connected( ).
    ENDIF.

  ENDMETHOD.


  METHOD broker_reconnect.

    broker_disconnect( ).
    rv_success = broker_connect( ).

  ENDMETHOD.


  METHOD get_broker_id_from_context.
*     get the caller info and store to static context
    DATA(ls_caller_info) = ir_context->get_start_caller_info( ).
    rv_broker_id = ls_caller_info-name.
  ENDMETHOD.


  METHOD if_abap_daemon_extension~on_accept.

* ------------
    TRY.
*     log
        LOG-POINT ID zmqba_gw
           SUBKEY 'IF_ABAP_DAEMON_EXTENSION~ON_ACCEPT'.

*     default reject
        e_setup_mode = co_setup_mode-reject.

*     get the broker id
        DATA(ls_caller) = i_context_base->get_start_caller_info( ).
        DATA(lv_name) = ls_caller-name.
        CHECK lv_name IS NOT INITIAL.
        DATA(lr_brkc) = zcl_mqba_factory=>get_broker_config( CONV zmqba_broker_id( lv_name ) ).
        CHECK lr_brkc IS NOT INITIAL.
        DATA(ls_cfg) = lr_brkc->get_config( ).
        CHECK ls_cfg IS NOT INITIAL.

*     todo: set check interval
        gv_interval = 5.

*     set to accepted
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


  METHOD if_abap_daemon_extension~on_start.

* -------- log
    LOG-POINT ID zmqba_gw
        SUBKEY 'IF_ABAP_DAEMON_EXTENSION~ON_START'.

* -------- reset
    CLEAR:  mv_broker_id,
            ms_config.

* -------- get broker id from contect
    mv_broker_id = get_broker_id_from_context( i_context ).

* -------- connect now
    IF broker_connect( ) EQ abap_false.
      ASSERT ID zmqba_gw
         SUBKEY 'IF_ABAP_DAEMON_EXTENSION~ON_START'
         FIELDS  mv_broker_id
         CONDITION 1 = 2.
    ENDIF.

* -------- call super
    super->if_abap_daemon_extension~on_start( i_context ).

  ENDMETHOD.


  METHOD if_abap_daemon_extension~on_stop.

    LOG-POINT ID zmqba_gw
        SUBKEY 'IF_ABAP_DAEMON_EXTENSION~ON_STOP'.

    broker_disconnect( ).

    CALL METHOD super->if_abap_daemon_extension~on_stop
      EXPORTING
        i_message = i_message
        i_context = i_context.


  ENDMETHOD.


  METHOD task_execute.

* --------- local data
    DATA: lv_error TYPE abap_bool.
    DATA: lv_cnt   TYPE i.


* --------- check
    ASSERT ID zmqba_gw
      SUBKEY 'TASK_EXECUTE'
      CONDITION mr_proxy IS BOUND.

* --------- get received messages and delete
    DATA(lt_msg) = mr_proxy->get_received_messages( ).
    DESCRIBE TABLE lt_msg LINES lv_cnt.
    IF lt_msg[] IS NOT INITIAL.

* ------------ check qrfc
*      DATA(lr_qrfc) = zcl_mqba_factory=>create_util_qrfc( ).
*      DATA(lv_quid) = CONV trfcqnam( ms_config-queue_name ).
*      IF lv_quid IS INITIAL.
*        lv_quid = 'ZMQBA-' && mv_broker_id.
*      ENDIF.
*
**          start in queue
*      lr_qrfc->set_qrfc_inbound( lv_quid ).
*
      CALL FUNCTION 'Z_MQBA_API_EBROKER_MSGS_ADD'
*        IN BACKGROUND TASK
*        AS SEPARATE UNIT
        EXPORTING
          iv_broker = mv_broker_id
          it_msg    = lt_msg.

*      lr_qrfc->transaction_end( ).
    ENDIF.


* --------- check connected
    IF broker_is_connected( ) EQ abap_false.
      broker_reconnect( ).
    ENDIF.

  ENDMETHOD.
ENDCLASS.
