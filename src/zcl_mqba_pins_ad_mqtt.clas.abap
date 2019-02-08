class ZCL_MQBA_PINS_AD_MQTT definition
  public
  inheriting from ZCL_MQBA_PINS_AD_BASE
  create public .

public section.
  type-pools ABAP .

  interfaces ZIF_MQBA_CALLBACK_NEW_MSG .
  interfaces ZIF_MQBA_DAEMON_MGR .

  methods DO_CMD
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_ACCEPT
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_START
    redefinition .
  methods IF_ABAP_DAEMON_EXTENSION~ON_STOP
    redefinition .
  methods TASK_EXECUTE
    redefinition .
protected section.

  class-data MR_PROXY type ref to ZIF_MQBA_API_MQTT_PROXY .
  class-data MV_BROKER_ID type STRING .
  class-data MS_CONFIG type ZMQBA_API_S_BRK_CFG .
  data MR_CONTEXT type ref to ZIF_MQBA_SHM_CONTEXT .
  data MV_CNT_RECEIVED type SADL_COUNT .
  data MV_CNT_PROCESSED type SADL_COUNT .
  data MV_STAT_TIMESTAMP type TIMESTAMP .
  data MV_CNT_RECEIVED_60S type SADL_COUNT .
  data MV_CNT_PROCESSED_60S type SADL_COUNT .
  data MV_CNT_RECONNECTING type SADL_COUNT .
  data MV_CNT_BYTES_RECEIVED type SADL_COUNT .
  data MV_CNT_FILTERED type SADL_COUNT .
  data MR_AD_MGR type ref to ZIF_MQBA_PINS_AD_MGR .
  data MV_STAT_LAST_CALL type SYUZEIT .
  constants GC_TOPIC_DEFAULT type STRING value '/sap/MQBADaemon' ##NO_TEXT.

  methods DO_CMD_STATISTIC
    returning
      value(RV_SUCCESS) type ABAP_BOOL .
  methods INIT_CONTEXT .
  methods IS_MSG_FILTERED
    importing
      !IS_MSG type ZMQBA_API_S_EBR_MSG
    returning
      value(RV_FILTERED) type ABAP_BOOL .
  methods DO_CMD_PUBLISH
    importing
      !IV_TOPIC type STRING
      !IV_PAYLOAD type STRING
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
*     disconnnected by by external?
      IF rv_connected EQ abap_false.
        DATA(lv_err) =  mr_proxy->get_error_text( ).
        IF lv_err IS INITIAL.
          lv_err = |unknown disconnect reason|.
        ENDIF.
        mr_context->put( iv_param = 'DISCONNECT_REASON' iv_value = lv_err ).
      ENDIF.
    ENDIF.

  ENDMETHOD.


  METHOD broker_reconnect.

    broker_disconnect( ).
    rv_success = broker_connect( ).

  ENDMETHOD.


  METHOD do_cmd.

* -------- local data
    DATA: lv_msg TYPE string.


* -------- init and log to context
    init_context( ).
    GET TIME STAMP FIELD DATA(lv_now).
    DATA(lv_log) = |{ lv_now }: command arrived { iv_cmd } - { iv_param } - { iv_payload }|.
    mr_context->put( iv_param = 'LAST_COMMAND' iv_value = lv_log ).


* -------- process
    CASE iv_cmd.
      WHEN 'PUBLISH'.
        rv_success = do_cmd_publish( iv_topic = iv_param iv_payload = iv_payload ).
      WHEN 'STATISTIC'.
        rv_success = do_cmd_statistic( ).
      WHEN OTHERS.
        rv_success = super->do_cmd(
            ir_message = ir_message
            ir_context = ir_context
            iv_cmd     = iv_cmd
            iv_param   = iv_param
            iv_payload = iv_payload
        ).
    ENDCASE.

* ------- log to context
    IF rv_success EQ abap_false.
      IF lv_msg IS INITIAL.
        lv_msg = |Error processing command { iv_cmd }|.
      ENDIF.
      mr_context->put( iv_param = 'LAST_COMMAND_ERROR' iv_value = lv_msg ).
    ENDIF.

  ENDMETHOD.


  METHOD do_cmd_publish.

* -------- init
    rv_success = abap_false.
    IF mr_proxy IS INITIAL.
      RETURN.
    ENDIF.

* -------- check reconnect
    IF mr_proxy->is_connected( ) EQ abap_false.
      RETURN.
    ENDIF.

* -------- publish
    rv_success = mr_proxy->publish(
      EXPORTING
        iv_topic   = iv_topic
        iv_payload = iv_payload
*      iv_qos     = 0
*      iv_retain  = abap_false
    ).

  ENDMETHOD.


  METHOD do_cmd_statistic.

* ----------- local vars
    DATA lv_topic     TYPE string.
    DATA lv_payload   TYPE string.
    DATA lv_external  TYPE abap_bool.
    DATA lv_gateway   TYPE zmqba_broker_id.
    DATA lv_prefix    TYPE string.


* ----------- local macros
    DEFINE distribute_broker.
      IF lv_prefix IS NOT INITIAL.
*       main fields
        lv_topic     = &1.
        lv_topic     = |{ lv_prefix }/{ lv_topic }|.
        lv_payload   = &2.
        CONDENSE lv_payload.

*       call mqba api
        CALL FUNCTION 'Z_MQBA_API_BROKER_PUBLISH'
          EXPORTING
            iv_topic            = lv_topic
            iv_payload          = lv_payload
            iv_external         = lv_external
            iv_gateway          = lv_gateway
                  .
      ENDIF.
    END-OF-DEFINITION.

* ===================== prepare mqtt distribution
*   check time
    GET TIME.
    IF mv_stat_last_call EQ sy-uzeit.
      CLEAR lv_prefix.  " <- no external distribution in this call (1 per second)
    ELSE.
*   set prefix
      lv_prefix = ms_config-param1.
      IF lv_prefix IS INITIAL.
        DATA(lv_broker_id) = mv_broker_id.
        IF lv_broker_id IS INITIAL.
          lv_broker_id = 'default'.
        ELSE.
          TRANSLATE lv_broker_id TO LOWER CASE.
        ENDIF.
        lv_prefix = |{ gc_topic_default }/{ lv_broker_id }|.
      ENDIF.

*   external handling: param2 activated -> no external distribution
      lv_external  = COND #( WHEN ms_config-param2 EQ abap_true
                             THEN abap_false
                             ELSE abap_true ).
      lv_gateway   = ms_config-param3.
*   set current time
      mv_stat_last_call = sy-uzeit.
    ENDIF.

* ====================== get and distribute statistics
* ----------- main fields
    GET TIME STAMP FIELD DATA(lv_now).
    mr_context->put( iv_param = 'STAT_UPDATED' iv_value = lv_now ).
    mr_context->put( iv_param = 'STAT_RECEIVED' iv_value = mv_cnt_received ).
    mr_context->put( iv_param = 'STAT_PROCESSED' iv_value = mv_cnt_processed ).
    mr_context->put( iv_param = 'STAT_RECONNECTING' iv_value = mv_cnt_reconnecting ).

* ---------- distribute kpi to broker
    IF lv_prefix IS NOT INITIAL.
      distribute_broker 'updated'         lv_now.
      distribute_broker 'received'        mv_cnt_received.
      distribute_broker 'processed'       mv_cnt_processed.
      distribute_broker 'filtered'        mv_cnt_filtered.
      distribute_broker 'bytes_received'  mv_cnt_bytes_received.
      distribute_broker 'reconnecting'    mv_cnt_reconnecting.

      DATA(lv_error) = mv_cnt_received - mv_cnt_processed - mv_cnt_filtered.
      distribute_broker 'failed'  lv_error.
    ENDIF.

* ---------- check 60s statistics
    DATA(lv_delta) = lv_now - mv_stat_timestamp.
    IF mv_stat_timestamp IS INITIAL
      OR lv_delta >= 60.

      IF mv_cnt_received_60s GT 0.
        DATA(lv_rec) = CONV integer( ( mv_cnt_received - mv_cnt_received_60s ) * 60 / lv_delta ).
        mr_context->put( iv_param = 'STAT_RECEIVED_60S' iv_value = lv_rec ).
        distribute_broker 'received_60s' lv_rec.
      ENDIF.

      IF mv_cnt_processed_60s GT 0.
        DATA(lv_proc) = CONV integer( ( mv_cnt_processed - mv_cnt_processed_60s ) * 60 / lv_delta ).
        mr_context->put( iv_param = 'STAT_PROCESSED_60S' iv_value = lv_proc ).
        distribute_broker 'processed_60s' lv_proc.
      ENDIF.

*     calc errors
      distribute_broker 'updated_60s' lv_now.
      DATA(lv_err60) = lv_rec - lv_proc.
      distribute_broker 'failed_60s' lv_err60.

*     remember the values
      mv_stat_timestamp = lv_now.
      mv_cnt_received_60s = mv_cnt_received.
      mv_cnt_processed_60s = mv_cnt_processed.

    ENDIF.


* ------------- client state
    DATA(lv_state) = zif_mqba_api_mqtt_proxy=>c_state_not_initialized.
    IF mr_proxy IS NOT INITIAL.
      lv_state = mr_proxy->get_client_state( ).
    ENDIF.
    mr_context->put( iv_param = 'CLIENT_STATE' iv_value = lv_state ).
    distribute_broker 'client_state' lv_state.


* ------------- success
    rv_success = abap_true.

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
    GET TIME STAMP FIELD DATA(lv_now).
    LOG-POINT ID zmqba_gw
        SUBKEY 'IF_ABAP_DAEMON_EXTENSION~ON_START'
        FIELDS lv_now.

* -------- reset
    CLEAR:  mv_broker_id,
            ms_config.

* -------- get broker id from contect
    mv_broker_id = get_broker_id_from_context( i_context ).

* -------- get memory context
    init_context( ).

* -------- connect now
    IF broker_connect( ) EQ abap_false.
      ASSERT ID zmqba_gw
         SUBKEY 'IF_ABAP_DAEMON_EXTENSION~ON_START'
         FIELDS  mv_broker_id
         CONDITION 1 = 2.
    ENDIF.

* ------- check interval
    gv_interval = 5000. " default 5 sec

*  if ms_config is NOT INITIAL.
*
*  else.
*
*  endif.


* -------- call super
    super->if_abap_daemon_extension~on_start( i_context ).

* -------- context
    mr_context->put( iv_param = 'STARTED' iv_value = lv_now ).
    IF mr_proxy IS NOT INITIAL.
      mr_context->put( iv_param = 'CLIENT_ID' iv_value = mr_proxy->get_client_id( ) ).
    ENDIF.

  ENDMETHOD.


  METHOD if_abap_daemon_extension~on_stop.

* -------- prepare
    GET TIME STAMP FIELD DATA(lv_now).
    LOG-POINT ID zmqba_gw
        SUBKEY 'IF_ABAP_DAEMON_EXTENSION~ON_STOP'
        FIELDS lv_now.


* -------- call super
    super->if_abap_daemon_extension~on_stop(
        i_message = i_message                  " ABAP Channels message type Push Channel Protocol (PCP)
        i_context = i_context                  " ABAP Daemon framework Context interface (do not implement)
    ).

* -------- stop broker
    broker_disconnect( ).

* -------- context
    mr_context->put( iv_param = 'STOPPED' iv_value = lv_now ).

  ENDMETHOD.


  METHOD init_context.
    IF mr_context IS INITIAL.
      mr_context = zcl_mqba_factory=>get_shm_context( ).
      mr_context->set_group( |ZCL_MQBA_PINS_AD_MQTT:{ mv_broker_id }| ).
      mr_context->clear( ).
    ENDIF.
  ENDMETHOD.


  METHOD is_msg_filtered.

* ----- init
    rv_filtered = abap_false.

* ----- check topic
    IF is_msg-topic IS INITIAL.
      rv_filtered = abap_true.
    ENDIF.

* ----- check payload empty
    IF is_msg-payload IS INITIAL.
      rv_filtered = abap_true.
    ELSE.
      DATA(lv_len) = strlen( is_msg-payload ).
      ADD lv_len TO mv_cnt_bytes_received.

*   check payload len not allowed
      IF ms_config-max_payload GT 0
        AND lv_len GT ms_config-max_payload.
        rv_filtered = abap_true.
      ENDIF.
    ENDIF.

* ------ count?
    IF rv_filtered EQ abap_true.
      ADD 1 TO mv_cnt_filtered.
    ENDIF.

  ENDMETHOD.


  METHOD task_execute.

* --------- local data
    DATA: lv_error  TYPE abap_bool.
    DATA: ls_result TYPE zmqba_api_s_ebr_msg_out.
    DATA: lv_cnt    TYPE i.


* --------- init
    init_context( ).

* --------- check
    GET TIME STAMP FIELD DATA(lv_start).
    IF mr_proxy IS INITIAL.
      mr_context->put( iv_param = 'TASK_EXECUTE_NO_PROXY' iv_value = |{ lv_start }| ).
      IF broker_connect( ) EQ abap_false
        OR mr_proxy IS INITIAL.
        RETURN.
      ENDIF.
    ENDIF.


* --------- get received messages and delete filtered
    DATA(lt_msg) = mr_proxy->get_received_messages( ).
    DESCRIBE TABLE lt_msg LINES lv_cnt.

* --------- not in push mode - count received here
    IF ms_config-push_mode EQ abap_false.
      ADD lv_cnt TO mv_cnt_received.
    ENDIF.

* --------- check filtering and count again
    LOOP AT lt_msg INTO DATA(ls_msg).
      IF is_msg_filtered( ls_msg ) EQ abap_true.
        DELETE lt_msg.
      ENDIF.
    ENDLOOP.
    DESCRIBE TABLE lt_msg LINES lv_cnt.


* --------- process
    IF lv_cnt LE 0.
      DATA(lv_empty_log) = |{ lv_start }: Periodic Task executed: nothing to do|.
      mr_context->put( iv_param = 'TASK_EXECUTED_LAST' iv_value = lv_empty_log ).
    ELSE.

* --------- forward to mqba api
      CALL FUNCTION 'Z_MQBA_API_EBROKER_MSGS_ADD'
        DESTINATION 'NONE'
        EXPORTING
          iv_broker = mv_broker_id
          it_msg    = lt_msg
        IMPORTING
          ev_error  = lv_error
          es_result = ls_result.

* ---------- statistics
      GET TIME STAMP FIELD DATA(lv_end).
      DATA(lv_delta) = lv_end - lv_start.
      DATA(lv_stat) = |{ lv_start }: Periodic Task executed: { lv_cnt } messages in { lv_delta } msec|.

      IF lv_error EQ abap_true.
        lv_stat = 'ERROR: ' && lv_stat.
      ELSE.
        ADD lv_cnt TO mv_cnt_processed.
      ENDIF.

      mr_context->put( iv_param = 'TASK_EXECUTED_LAST' iv_value = lv_stat ).
    ENDIF.

* --------- check connected
    DATA(lv_connected) = broker_is_connected( ).
    IF lv_connected EQ abap_false.
      DATA(lv_succ_rec) = broker_reconnect( ).
      DATA(lv_msg_rc) = |{ lv_start }: |.

      IF lv_succ_rec EQ abap_false.
        lv_msg_rc = lv_msg_rc && 'ERROR'.
      ELSE.
        lv_msg_rc = lv_msg_rc && 'SUCCESS'.
      ENDIF.

      mr_context->put( iv_param = 'LAST_RECONNECTING_AT' iv_value = lv_msg_rc ).
      ADD 1 TO mv_cnt_reconnecting.
    ENDIF.


* ---------- statistics and other info distribution
    do_cmd_statistic( ).


  ENDMETHOD.


  METHOD zif_mqba_callback_new_msg~on_message.

* --------- local data
    DATA: lt_props    TYPE  zmqba_msg_t_prp.
    DATA: lv_error_text TYPE  zmqba_error_text.
    DATA: lv_error      TYPE  zmqba_flag_error.
    DATA: lv_guid       TYPE  zmqba_msg_guid.
    DATA: lv_scope      TYPE  zmqba_msg_scope.


* --------- log message
    init_context( ).
    GET TIME STAMP FIELD DATA(lv_now).
    mr_context->put( iv_param = 'LAST_MESSAGE_ARRIVED' iv_value = |{ lv_now }: { is_msg-topic }| ).
    ADD 1 TO mv_cnt_received.

* --------- init
    rv_success = abap_false.
    IF mr_proxy IS INITIAL.
      RETURN.
    ENDIF.


* --------- check msg filter
    IF is_msg_filtered( is_msg ) EQ abap_true.
      rv_success = abap_true.
      RETURN.
    ENDIF.


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

* -------- runtime check
    GET TIME STAMP FIELD DATA(lv_end).

* -------- log when error
    IF lv_error EQ abap_true.
      mr_context->put( iv_param = 'LAST_MESSAGE_ERROR' iv_value = |{ lv_now }..{ lv_end }: { is_msg-topic } : { lv_error_text }| ).
    ELSE.
      " statistics
      ADD 1 TO mv_cnt_processed.
    ENDIF.

* -------- sucess??
    rv_success = COND #( WHEN lv_error EQ abap_false
                         THEN abap_true
                         ELSE abap_false ).

  ENDMETHOD.


  METHOD zif_mqba_daemon_mgr~is_available.

* ------- init
    rv_available = abap_false.
    IF mr_ad_mgr IS INITIAL.
      RETURN.
    ENDIF.

* ------- get a ping command
    rv_available = mr_ad_mgr->send_cmd( iv_cmd = 'PING' ).

  ENDMETHOD.


  METHOD zif_mqba_daemon_mgr~publish.
* ------- init
    rv_success = abap_false.
    IF mr_ad_mgr IS INITIAL.
      RETURN.
    ENDIF.

* ------- get a ping command
    rv_success = mr_ad_mgr->send_cmd(
      iv_cmd      = 'PUBLISH'
      iv_param    = iv_topic
      iv_payload  = iv_payload
    ).
  ENDMETHOD.


  METHOD zif_mqba_daemon_mgr~restart.
* ------- init
    rv_success = abap_false.
    IF mr_ad_mgr IS INITIAL.
      RETURN.
    ENDIF.

* ------- stop
    zif_mqba_daemon_mgr~stop( ).

* ------- start
    rv_success = zif_mqba_daemon_mgr~start( ).

  ENDMETHOD.


  METHOD zif_mqba_daemon_mgr~set_config.

* ------- init and check
    rv_success = abap_false.
    IF ir_cfg IS INITIAL.
      RETURN.
    ENDIF.

* -------- get config
    ms_config    = ir_cfg->get_config( ).
    mv_broker_id = ir_cfg->get_id( ).

* -------- check again
    IF ms_config IS INITIAL
      OR mv_broker_id IS INITIAL
      OR ms_config-impl_class IS INITIAL.
      RETURN.
    ENDIF.

* ------- get ad manager
    mr_ad_mgr = zcl_mqba_pins_ad_mgr=>create( ).
    IF mr_ad_mgr IS INITIAL.
      RETURN.
    ELSE.
      mr_ad_mgr->set_id( mv_broker_id ).
      mr_ad_mgr->set_type( ms_config-impl_class ).
    ENDIF.

* ------- finally true
    rv_success = abap_true.

  ENDMETHOD.


  METHOD zif_mqba_daemon_mgr~start.

* ------- init
    rv_success = abap_false.
    IF mr_ad_mgr IS INITIAL.
      RETURN.
    ENDIF.

* -------- call ad manager
    rv_success = mr_ad_mgr->start( ).

  ENDMETHOD.


  METHOD zif_mqba_daemon_mgr~stop.
* ------- init
    rv_success = abap_false.
    IF mr_ad_mgr IS INITIAL.
      RETURN.
    ENDIF.

* -------- call ad manager
    rv_success = mr_ad_mgr->stop( ).

  ENDMETHOD.
ENDCLASS.
