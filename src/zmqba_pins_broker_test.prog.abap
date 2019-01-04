*&---------------------------------------------------------------------*
*& Report ZMQBA_PINS_BROKER_TEST
*&---------------------------------------------------------------------*
*& Test program for mqtt protocol implementation (client)
*&---------------------------------------------------------------------*
REPORT zmqba_pins_broker_test NO STANDARD PAGE HEADING.

* ------------- interface
TABLES: ztc_mqbabrk.
PARAMETERS: p_broker       LIKE ztc_mqbabrk-broker_id OBLIGATORY.
PARAMETERS: p_topic        TYPE zmqba_topic_mask DEFAULT '#'.
PARAMETERS: p_waitf        TYPE zmqba_wait_for_sec DEFAULT 5.
SELECTION-SCREEN: ULINE.
PARAMETERS: p_usep         AS CHECKBOX DEFAULT 'X'. " use custimized prefix
PARAMETERS: p_forw         AS CHECKBOX DEFAULT 'X'. " forward to mqba
PARAMETERS: p_qrfc         AS CHECKBOX DEFAULT 'X'. " forward as qrfc

* -------------- local macros
DEFINE protocol.
  WRITE: / &1.
END-OF-DEFINITION.

DEFINE exit_error.
  WRITE: / &1 COLOR 6.
  RETURN.
END-OF-DEFINITION.


START-OF-SELECTION.

* ------------ local data
  DATA: lv_error_text TYPE  zmqba_error_text.
  DATA: lv_error      TYPE  zmqba_flag_error.
  DATA: lv_guid       TYPE  zmqba_msg_guid.
  DATA: lv_scope      TYPE  zmqba_msg_scope.


* ------------ get broker config
  DATA(lr_brkc) = zcl_mqba_factory=>get_broker_config( p_broker ).
  IF lr_brkc IS INITIAL.
    exit_error 'missing or wrong broker config'.
  ELSE.
    protocol 'broker config loaded'.
  ENDIF.


* ------------ create protocol handler
  DATA(lr_prot) = zcl_mqba_pins_mqtt_protocol=>create( ).
  IF lr_prot IS INITIAL.
    exit_error 'no protocol handler'.
  ELSE.
    protocol 'protocol handler for mqtt loaded'.
  ENDIF.


* ------------ set config
  IF lr_prot->set_config( lr_brkc ) EQ abap_false.
    exit_error 'wrong broker config'.
  ELSE.
    protocol 'configuration set to protocol handler'.
  ENDIF.

* ------------ get some config infos
  DATA(ls_cfg) = lr_brkc->get_config( ).
  WRITE: / |broker url: { ls_cfg-broker_url }|.
  WRITE: / |broker prefix: { ls_cfg-prefix }|.

* ------------ check qrfc
  DATA(lr_qrfc) = zcl_mqba_factory=>create_util_qrfc( ).
  DATA(lv_quid) = CONV trfcqnam( ls_cfg-queue_name ).
  IF lv_quid IS INITIAL.
    lv_quid = 'ZMQBA-' && p_broker.
  ENDIF.


* ------------ connect
  IF lr_prot->connect( ) EQ abap_false.
    exit_error 'conncection failed'.
  ELSE.
    protocol 'connection established'.
  ENDIF.

* ------------ subscribe
  IF p_topic IS NOT INITIAL.
    IF lr_prot->subscribe_to( iv_topic = CONV string( p_topic ) iv_use_prefix = p_usep ) EQ abap_false.
      protocol 'subribe failed'.
    ENDIF.
  ENDIF.


* ------------ wait
  WAIT UP TO p_waitf SECONDS.


* ---------- get received messages
  DATA(lt_msg) = lr_prot->get_received_messages( ).
  IF lt_msg[] IS INITIAL.
    protocol 'no messages received.'.
  ELSE.
    SKIP.
    WRITE: / 'received messages:'.
* ---------- loop and process received messages



    LOOP AT lt_msg INTO DATA(ls_msg).
*     standard output
      WRITE: / ls_msg-msg_id, ':', ls_msg-topic, ':', ls_msg-payload.
*     forward mqba

      IF p_forw EQ abap_true.
        DATA(lv_msgid) = |{ p_broker }:{ ls_msg-msg_id }|.

        IF p_qrfc EQ abap_true.
*          start in queue
          lr_qrfc->set_qrfc_inbound( lv_quid ).
          CALL FUNCTION 'Z_MQBA_API_EBROKER_MSG_ADD'
            IN BACKGROUND TASK
            AS SEPARATE UNIT
            EXPORTING
              iv_broker  = CONV string( p_broker )
              iv_topic   = ls_msg-topic
              iv_payload = ls_msg-payload
              iv_msg_id  = lv_msgid.
          lr_qrfc->transaction_end( ).
          WRITE: 'executed in queue', lv_quid.
        ELSE.
*         start synchronous
          CALL FUNCTION 'Z_MQBA_API_EBROKER_MSG_ADD'
            EXPORTING
              iv_broker     = CONV string( p_broker )
              iv_topic      = ls_msg-topic
              iv_payload    = ls_msg-payload
              iv_msg_id     = lv_msgid
*             IT_PROPS      =
            IMPORTING
              ev_error_text = lv_error_text
              ev_error      = lv_error
              ev_guid       = lv_guid
              ev_scope      = lv_scope.
          IF lv_guid IS INITIAL.
            WRITE: 'error forwarding to local MQBA broker.' COLOR 6.
          ELSE.
            WRITE: 'message forwared: ', lv_guid.
          ENDIF.

        ENDIF.

      ENDIF.
    ENDLOOP.
    SKIP.
  ENDIF.



* ---------- disconnect
  IF lr_prot->disconnect( ) EQ abap_false.
    exit_error 'disconnecting failed'.
  ELSE.
    protocol 'disconnected'.
  ENDIF.

* ---------- cleanup
  lr_prot->destroy( ).
  protocol 'protoccol handler destroyed'.
