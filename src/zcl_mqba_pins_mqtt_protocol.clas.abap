class ZCL_MQBA_PINS_MQTT_PROTOCOL definition
  public
  create public .

public section.

  interfaces ZIF_MQBA_API_MQTT_PROXY .
  interfaces IF_MQTT_EVENT_HANDLER .

  methods CONSTRUCTOR .
  class-methods CREATE
    returning
      value(RR_PROXY) type ref to ZIF_MQBA_API_MQTT_PROXY .
PROTECTED SECTION.

  DATA ms_config_apc TYPE apc_connect_options .
  DATA ms_will_topic TYPE zmqba_api_s_ebr_msg .
  DATA mt_rec_msg TYPE zmqba_api_t_ebr_msg .
  DATA mv_client_id TYPE string .
  DATA mv_keep_alive TYPE i VALUE 60 ##NO_TEXT.
  DATA mv_prefix TYPE string .
  DATA mr_cfg TYPE REF TO zif_mqba_cfg_broker .
  DATA mr_client TYPE REF TO if_mqtt_client .
  DATA mv_error TYPE i .
  DATA mv_error_text TYPE string .

  METHODS error_reset .
  METHODS error_set
    IMPORTING
      !iv_text TYPE data
      !iv_code TYPE i DEFAULT 500 .
  METHODS topic_to_external
    IMPORTING
      !iv_topic       TYPE string
    RETURNING
      VALUE(rv_topic) TYPE string .
  METHODS topic_to_internal
    IMPORTING
      !iv_topic       TYPE string
    RETURNING
      VALUE(rv_topic) TYPE string .
private section.
ENDCLASS.



CLASS ZCL_MQBA_PINS_MQTT_PROTOCOL IMPLEMENTATION.


  method CONSTRUCTOR.
      mv_client_id = |MQBA{ sy-sysid }{ sy-mandt }{ sy-datum }{ sy-uzeit }|.
  endmethod.


  method CREATE.
    rr_proxy = new ZCL_MQBA_PINS_MQTT_PROTOCOL( ).
  endmethod.


  method ERROR_RESET.
    clear: mv_error, mv_error_text.
  endmethod.


  method ERROR_SET.
    mv_error_text = iv_text.
    mv_error      = iv_code.
  endmethod.


  method IF_MQTT_EVENT_HANDLER~ON_CONNECT.
  endmethod.


  method IF_MQTT_EVENT_HANDLER~ON_DISCONNECT.
  endmethod.


  METHOD if_mqtt_event_handler~on_message.

    TRY.
        DATA: ls_message TYPE zmqba_api_s_ebr_msg.

        ls_message-topic      = topic_to_internal( i_topic_name ).
        ls_message-qos        = CONV #( i_message->get_qos( ) ).
        ls_message-retain     = i_message->get_retain( ).
        ls_message-payload    = i_message->get_text( ).
        ls_message-msg_id     = i_envelope->get_message_number( ).

        APPEND ls_message TO mt_rec_msg.

      CATCH cx_mqtt_error INTO DATA(lx_mqtt_error).
        mv_error_text = |ERROR: { lx_mqtt_error->get_text( ) }|.
        mv_error      = 500.
    ENDTRY.

  ENDMETHOD.


  method IF_MQTT_EVENT_HANDLER~ON_PUBLISH.
  endmethod.


  method IF_MQTT_EVENT_HANDLER~ON_SUBSCRIBE.
  endmethod.


  method IF_MQTT_EVENT_HANDLER~ON_UNSUBSCRIBE.
  endmethod.


  METHOD topic_to_external.
    rv_topic = iv_topic.
    CHECK iv_topic IS NOT INITIAL.

    CHECK mr_cfg IS NOT INITIAL.
    DATA(ls_cfg) = mr_cfg->get_config( ).
    CHECK ls_cfg-prefix IS NOT INITIAL.

    DATA(lv_prefix) = |{ ls_cfg-prefix }|.
    IF NOT iv_topic CP '*/'
       AND NOT iv_topic CP '/*'.
      rv_topic =  ls_cfg-prefix && '/' && iv_topic.
    ELSE.
      rv_topic = ls_cfg-prefix &&  iv_topic.
    ENDIF.

    IF rv_topic CS '//'.
      REPLACE ALL OCCURRENCES OF '//' IN rv_topic WITH '/'.
    ENDIF.
  ENDMETHOD.


  METHOD topic_to_internal.
    rv_topic = iv_topic.
    CHECK mr_cfg IS NOT INITIAL.
    DATA(ls_cfg) = mr_cfg->get_config( ).
    CHECK ls_cfg-prefix IS NOT INITIAL.

    DATA(lv_check) = ls_cfg-prefix && '*'.
    CHECK iv_topic CP lv_check.

    DATA(lv_len) = strlen( ls_cfg-prefix ).
    rv_topic = iv_topic+lv_len.
  ENDMETHOD.


  METHOD zif_mqba_api_mqtt_proxy~connect.

* ------ local data
    DATA: ls_proxy               TYPE apc_proxy.


* ------ init/check
    rv_success = abap_false.
    error_set( 'mqtt connect' ).
    CHECK mr_client IS INITIAL.
    CHECK mr_cfg IS NOT INITIAL.
    CHECK mv_client_id IS NOT INITIAL.


* ------- prepare
    DATA(ls_cfg) = mr_cfg->get_config( ).
    IF ls_cfg-proxy_host IS NOT INITIAL.
      ls_proxy-host     = ls_cfg-proxy_host.
      ls_proxy-port     = ls_cfg-proxy_port.
      ls_proxy-username = ls_cfg-proxy_user.
      ls_proxy-password = ls_cfg-proxy_pw.
    ENDIF.

* ------- create client
    TRY.
        " set HTTP(S) proxy setting, if specified
        mr_client = cl_mqtt_client_manager=>create_by_url(
             i_url              = CONV string( ls_cfg-broker_url )
*         i_protocol_version = pversion
             i_event_handler    = me
             i_proxy            = ls_proxy
*         i_ssl_id           = sslid
         ). " MQTT Client


        " MQTT connect options
        DATA(lo_mqtt_connect_options) = cl_mqtt_connect_options=>create( ).
        lo_mqtt_connect_options->set_client_id( mv_client_id ).
        lo_mqtt_connect_options->set_username( CONV string( ls_cfg-broker_user ) ).
        lo_mqtt_connect_options->set_password_as_text( CONV string( ls_cfg-broker_pw ) ).
        lo_mqtt_connect_options->set_keep_alive( mv_keep_alive ).


*      " set MQTT connection options last will
        IF ms_will_topic IS NOT INITIAL.
          DATA(lo_will_message) = cl_mqtt_message=>create( ).
          lo_will_message->set_qos( CONV if_mqtt_types=>ty_mqtt_qos( ms_will_topic-qos ) ).
          lo_will_message->set_retain( ms_will_topic-retain ).
          lo_will_message->set_text( ms_will_topic-payload ).
          lo_mqtt_connect_options->set_will( i_topic_name = ms_will_topic-topic i_message = lo_will_message ).
        ENDIF.

* ------------ connect
        mr_client->connect(
          EXPORTING
            i_mqtt_options = lo_mqtt_connect_options   " MQTT connect options
            i_apc_options  = ms_config_apc ). " Structure APC connect options

* ------------- log
        LOG-POINT ID zmqba_gw
           SUBKEY 'ZIF_MQBA_API_MQTT_PROXY~CONNECT'
           FIELDS ls_cfg-broker_url.

      CATCH cx_mqtt_error INTO DATA(lx_error).
        error_set( lx_error->get_text( ) ).
        CLEAR mr_client.
        RETURN.
    ENDTRY.


* ----- success
    error_reset( ).
    rv_success = abap_true.

  ENDMETHOD.


  METHOD zif_mqba_api_mqtt_proxy~destroy.
    zif_mqba_api_mqtt_proxy~disconnect( ).
  ENDMETHOD.


  METHOD zif_mqba_api_mqtt_proxy~disconnect.

* ------ init
    rv_success = abap_false.
    error_set( 'mqtt disconnect' ).

* ------- check client
    IF mr_client IS BOUND.
      TRY.
          mr_client->disconnect( ).

        CATCH cx_mqtt_error INTO DATA(lx_mqtt_error).
          DATA(lv_error_text) = lx_mqtt_error->get_text( ).
          error_set( lv_error_text ).
          RETURN.
      ENDTRY.

      CLEAR mr_client.
    ENDIF.

* ------ success
    error_reset( ).
    rv_success = abap_true.

  ENDMETHOD.


  method ZIF_MQBA_API_MQTT_PROXY~GET_ERROR.
    rv_error = mv_error.
  endmethod.


  method ZIF_MQBA_API_MQTT_PROXY~GET_ERROR_TEXT.
    rv_error = mv_error_text.
  endmethod.


  METHOD zif_mqba_api_mqtt_proxy~get_received_messages.
    rt_msg = mt_rec_msg.
    IF iv_delete EQ abap_true.
      CLEAR: mt_rec_msg.
    ENDIF.
  ENDMETHOD.


  METHOD zif_mqba_api_mqtt_proxy~is_connected.

*   init
    rv_success = abap_false.
    error_set( 'mqtt isconnected' ).
    CHECK mr_client IS NOT INITIAL.

*   check
    TRY.
        DATA(lv_conn_state) = mr_client->get_context( )->get_connection_state( ).
        DATA(lv_conn_id)    = mr_client->get_context( )->get_connection_id( ).

        CASE lv_conn_state.
          WHEN if_mqtt_types=>connection_state-connecting
            OR if_mqtt_types=>connection_state-connected
            OR if_mqtt_types=>connection_state-disconnecting.
            rv_success = abap_true.
          WHEN if_mqtt_types=>connection_state-disconnected.
          WHEN OTHERS.
        ENDCASE.
      CATCH cx_mqtt_error INTO DATA(lx_mqtt_error).
        DATA(lv_error_text) = lx_mqtt_error->get_text( ).
        error_set( lv_error_text ).
        RETURN.
    ENDTRY.

* -------- finally
    error_reset( ).

  ENDMETHOD.


  method ZIF_MQBA_API_MQTT_PROXY~IS_ERROR.
    rv_error = cond #( when mv_error eq 0 and mv_error_text is INITIAL
                       then abap_false
                       else abap_true ).
  endmethod.


  METHOD zif_mqba_api_mqtt_proxy~reconnect.
* disconnect
    zif_mqba_api_mqtt_proxy~disconnect( ).

* connect
    rv_success = zif_mqba_api_mqtt_proxy~connect( ).
  ENDMETHOD.


  method ZIF_MQBA_API_MQTT_PROXY~SET_CLIENT_ID.
    mv_client_id = iv_client_id.
    rr_self = me.
  endmethod.


  METHOD zif_mqba_api_mqtt_proxy~set_config.

*     init
    rv_success = abap_false.
    CHECK mr_cfg IS INITIAL.
    CHECK ir_cfg IS NOT INITIAL.

*     check config
    DATA(ls_cfg) = ir_cfg->get_config( ).
    CHECK ls_cfg IS NOT INITIAL AND ls_cfg-broker_url IS NOT INITIAL.


*   set some important infos
    IF ls_cfg-client_id IS NOT INITIAL.
      mv_client_id = ls_cfg-client_id.
    ENDIF.
    IF ls_cfg-prefix IS NOT INITIAL.
      mv_prefix    = ls_cfg-prefix.
    ENDIF.
    IF ls_cfg-keep_alive GT 0.
      mv_keep_alive = ls_cfg-keep_alive.
    ENDIF.

*     set to internal and leave
    mr_cfg = ir_cfg.
    rv_success = abap_true.

  ENDMETHOD.


  METHOD zif_mqba_api_mqtt_proxy~set_config_apc.
    ms_config_apc = is_apc.
  ENDMETHOD.


  METHOD zif_mqba_api_mqtt_proxy~set_last_will.
    ms_will_topic-topic     = iv_topic.
    ms_will_topic-payload   = iv_payload.
    ms_will_topic-qos       = iv_qos.
    ms_will_topic-retain    = iv_retain.
  ENDMETHOD.


  METHOD zif_mqba_api_mqtt_proxy~subscribe_to.

* ------ init
    rv_success = abap_false.
    CHECK iv_topic IS NOT INITIAL.
    CHECK zif_mqba_api_mqtt_proxy~is_connected( ) EQ abap_true.


* ------ subscribe
    TRY.
        DATA(lv_topic) = CONV string( iv_topic ).
        IF iv_use_prefix EQ abap_true.
          lv_topic = topic_to_external( iv_topic ).
        ENDIF.

        DATA: lt_topic_filter_qos TYPE if_mqtt_types=>tt_mqtt_topic_filter_qos.
        lt_topic_filter_qos = VALUE #( ( topic_filter = lv_topic
                                         qos          = CONV if_mqtt_types=>ty_mqtt_qos( iv_qos ) ) ).

        mr_client->subscribe( EXPORTING i_topic_filter_qos = lt_topic_filter_qos
                                                      IMPORTING e_envelope    = DATA(lo_envelope_subscribe) ).
        DATA(lv_id) = lo_envelope_subscribe->get_message_number( ).
        rv_success = abap_true.

      CATCH cx_mqtt_error INTO DATA(lx_mqtt_error).
        mv_error_text = lx_mqtt_error->get_text( ).
        mv_error = 500.
    ENDTRY.

  ENDMETHOD.
ENDCLASS.
