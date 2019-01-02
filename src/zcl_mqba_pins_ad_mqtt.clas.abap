class ZCL_MQBA_PINS_AD_MQTT definition
  public
  inheriting from ZCL_MQBA_PINS_AD_BASE
  create public .

public section.
  type-pools ABAP .

  interfaces ZIF_MQBA_CALLBACK_NEW_MSG .

  methods IF_ABAP_DAEMON_EXTENSION~ON_ACCEPT
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_START
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_STOP
    redefinition .
  methods TASK_EXECUTE
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_MESSAGE
    redefinition .
protected section.

  class-data MR_PROXY type ref to ZIF_MQBA_API_MQTT_PROXY .
  class-data MV_BROKER_ID type STRING .
  class-data MS_CONFIG type ZMQBA_API_S_BRK_CFG .

  methods ON_CMD_PUBLISH
    importing
      !I_MESSAGE type ref to IF_AC_MESSAGE_TYPE_PCP
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
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


* ------- set callback
    IF ms_config-push_mode EQ abap_true.
      mr_proxy->set_callback_new_msg( me ).
    ENDIF.


* ------- subscribe
    DATA(lv_use_prefix) = COND #( WHEN ms_config-prefix IS NOT INITIAL
                                  THEN abap_true
                                  ELSE abap_false ).

    DATA(lv_success) = mr_proxy->subscribe_to(
      EXPORTING
        iv_topic      = '#'
        iv_use_prefix = lv_use_prefix
        iv_qos        = ms_config-qos_in
    ).


** ------ set true
*    rv_success = abap_true.

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


  METHOD if_abap_daemon_extension~on_message.

* -------- local data
    DATA: lv_cmd     TYPE string.
    DATA: lv_success TYPE abap_bool.

* -------- get command from message
    TRY.
        lv_cmd = i_message->get_text( ).
      CATCH cx_ac_message_type_pcp_error.
        RETURN.
    ENDTRY.


* -------- process command
    CASE lv_cmd.
      WHEN 'PUBLISH'.
        lv_success = on_cmd_publish( i_message ).
      WHEN OTHERS.
* -------- not handled here
        CALL METHOD super->if_abap_daemon_extension~on_message
          EXPORTING
            i_message = i_message
            i_context = i_context.
        RETURN.
    ENDCASE.


* -------- check success
    IF lv_success EQ abap_false.
      RAISE EXCEPTION TYPE cx_mqtt_error. "|Error processing command { lv_cmd }|.
    ENDIF.

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


  METHOD on_cmd_publish.

* -------- init
    rv_success = abap_false.
    IF mr_proxy IS INITIAL.
      RETURN.
    ENDIF.


* -------- check reconnect
    IF mr_proxy->is_connected( ) EQ abap_false.
      RETURN.
    ENDIF.

* -------- unpack message
    DATA(lv_topic)   = i_message->get_field( 'TOPIC' ).
    DATA(lv_payload) = i_message->get_field( 'PAYLOAD' ).

* -------- publish
    rv_success = mr_proxy->publish(
      EXPORTING
        iv_topic   = lv_topic
        iv_payload = lv_payload
*      iv_qos     = 0
*      iv_retain  = abap_false
    ).


  ENDMETHOD.


  METHOD task_execute.

* --------- local data
    DATA: lv_error  TYPE abap_bool.
    DATA: ls_result TYPE zmqba_api_s_ebr_msg_out.
    DATA: lv_cnt    TYPE i.


* --------- check
    ASSERT ID zmqba_gw
      SUBKEY 'TASK_EXECUTE'
      CONDITION mr_proxy IS BOUND.

* --------- get received messages and delete
    DATA(lt_msg) = mr_proxy->get_received_messages( ).
    DESCRIBE TABLE lt_msg LINES lv_cnt.
    IF lv_cnt GT 0.
      CALL FUNCTION 'Z_MQBA_API_EBROKER_MSGS_ADD'
        DESTINATION 'NONE'
        EXPORTING
          iv_broker = mv_broker_id
          it_msg    = lt_msg
        IMPORTING
          ev_error  = lv_error
          es_result = ls_result.

      ASSERT ID zmqba_gw
        SUBKEY 'TASK_EXECUTE:Forward'
        FIELDS mv_broker_id
               lt_msg
               ls_result
        CONDITION lv_error = abap_false.

    ENDIF.

* --------- check connected
    IF broker_is_connected( ) EQ abap_false.
      broker_reconnect( ).
      LOG-POINT ID zmqba_gw
         SUBKEY 'TASK_EXECUTE:reconnect'.
    ENDIF.

  ENDMETHOD.


  METHOD zif_mqba_callback_new_msg~on_message.

* --------- local data
    DATA: lt_props    TYPE  zmqba_msg_t_prp.
    DATA: lv_error_text TYPE  zmqba_error_text.
    DATA: lv_error      TYPE  zmqba_flag_error.
    DATA: lv_guid       TYPE  zmqba_msg_guid.
    DATA: lv_scope      TYPE  zmqba_msg_scope.


* --------- init
    rv_success = abap_false.
    CHECK mr_proxy IS NOT INITIAL.


* --------- forward to internal message broker
    CALL FUNCTION 'Z_MQBA_API_EBROKER_MSG_ADD'
      DESTINATION 'NONE'
      EXPORTING
        iv_broker     = mv_broker_id
        iv_topic      = is_msg-topic
        iv_payload    = is_msg-payload
        iv_msg_id     = is_msg-msg_id
        it_props      = lt_props
      IMPORTING
        ev_error_text = lv_error_text
        ev_error      = lv_error
        ev_guid       = lv_guid
        ev_scope      = lv_scope.

* -------- log when error
    IF lv_error EQ abap_true.
      LOG-POINT ID zmqba_gw
        SUBKEY 'ZIF_MQBA_CALLBACK_NEW_MSG~ON_MESSAGE:ERROR'
        FIELDS mv_broker_id
               is_msg
               lv_error_text.
    ENDIF.

* -------- sucess??
    rv_success = COND #( WHEN lv_error EQ abap_false
                         THEN abap_true
                         ELSE abap_false ).


  ENDMETHOD.
ENDCLASS.
