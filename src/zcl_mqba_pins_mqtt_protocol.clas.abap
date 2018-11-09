class ZCL_MQBA_PINS_MQTT_PROTOCOL definition
  public
  create public .

public section.

  interfaces ZIF_MQBA_API_MQTT_PROXY .
  interfaces IF_MQTT_EVENT_HANDLER .

  class-methods CREATE
    returning
      value(RR_PROXY) type ref to ZIF_MQBA_API_MQTT_PROXY .
protected section.

  data MR_CFG type ref to ZIF_MQBA_CFG_BROKER .
  data MR_CLIENT type ref to IF_MQTT_CLIENT .
  data MV_ERROR type I .
  data MV_ERROR_TEXT type STRING .

  methods ERROR_RESET .
  methods ERROR_SET
    importing
      !IV_TEXT type DATA
      !IV_CODE type I default 500 .
private section.
ENDCLASS.



CLASS ZCL_MQBA_PINS_MQTT_PROTOCOL IMPLEMENTATION.


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


  METHOD zif_mqba_api_mqtt_proxy~connect.

* ------ local data
    DATA: ls_proxy               TYPE apc_proxy.


* ------ init/check
    rv_success = abap_false.
    error_set( 'mqtt connect' ).
    CHECK mr_client IS INITIAL.
    CHECK mr_cfg IS NOT INITIAL.

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
        lo_mqtt_connect_options->set_client_id( 'ACS88098098435' ).
        lo_mqtt_connect_options->set_username( CONV string( ls_cfg-broker_user ) ).
        lo_mqtt_connect_options->set_password_as_text( CONV string( ls_cfg-broker_pw ) ).
        lo_mqtt_connect_options->set_keep_alive( 60 ).


*      " set MQTT connection options last will
*      DATA(lo_mqtt_conn_options) = cl_mqtt_connect_options=>create( ) ##NEEDED.
*      DATA(lo_will_message) = cl_mqtt_message=>create( ).
*      IF will_q1 IS INITIAL.
*        lo_will_message->set_qos( if_mqtt_types=>qos-at_least_once ).
*      ELSE.
*        lo_will_message->set_qos( if_mqtt_types=>qos-at_most_once ).
*      ENDIF.
*      lo_will_message->set_retain( will_r ).
*      lo_will_message->set_text( will_m ).
*      lo_mqtt_connect_options->set_will( i_topic_name = will_t i_message = lo_will_message ).

* ------------ APC Options
        DATA: ls_apc_connect_options TYPE apc_connect_options.
        " APC connect options
*      ls_apc_connect_options-timeout = timeout.
*      ls_apc_connect_options-vscan_profile_incoming_msg = vsin.
*      ls_apc_connect_options-vscan_profile_outgoing_msg = vsout.


* ------------ connect
        mr_client->connect(
          EXPORTING
            i_mqtt_options = lo_mqtt_connect_options   " MQTT connect options
            i_apc_options  = ls_apc_connect_options ). " Structure APC connect options

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


  METHOD zif_mqba_api_mqtt_proxy~set_config.

*     init
    rv_success = abap_false.
    CHECK mr_cfg IS INITIAL.
    CHECK ir_cfg IS NOT INITIAL.

*     check config
    DATA(ls_cfg) = ir_cfg->get_config( ).
    CHECK ls_cfg IS NOT INITIAL AND ls_cfg-broker_url IS NOT INITIAL.

*     set to internal and leave
    mr_cfg = ir_cfg.
    rv_success = abap_true.

  ENDMETHOD.
ENDCLASS.
