*&---------------------------------------------------------------------*
*& Report ZBAD_ADMQ_DAEMON_CONTROL
*&---------------------------------------------------------------------*
*& Controls a Daemon for MQBA
*&---------------------------------------------------------------------*
REPORT zmqba_pins_ad_control NO STANDARD PAGE HEADING.

* -------- interface
TABLES: ztc_mqbabrk.
PARAMETERS: p_class   TYPE seoclsname   OBLIGATORY DEFAULT 'ZCL_MQBA_PINS_AD_MQTT'.
PARAMETERS: p_id      TYPE zmqba_param  OBLIGATORY DEFAULT 'DEFAULT'.
SELECTION-SCREEN: ULINE.
PARAMETERS: p_start   RADIOBUTTON GROUP grp1 DEFAULT 'X'.
PARAMETERS: p_stop    RADIOBUTTON GROUP grp1.
SELECTION-SCREEN: ULINE.
PARAMETERS: p_send    RADIOBUTTON GROUP grp1.
PARAMETERS: p_cmd     TYPE string DEFAULT 'PUBLISH'.
PARAMETERS: p_param   TYPE string DEFAULT '/mytopic'.
PARAMETERS: p_payl    TYPE string DEFAULT 'mydata'.


* -------- local macros
DEFINE exit_error.
  WRITE: / &1 COLOR 6.
  RETURN.
END-OF-DEFINITION.

DEFINE output.
  WRITE: / &1.
END-OF-DEFINITION.

START-OF-SELECTION.

* ------- get ad manager
  DATA(lr_ad_mgr) = zcl_mqba_pins_ad_mgr=>create( ).
  lr_ad_mgr->set_id( p_id ).
  lr_ad_mgr->set_type( p_class ).

* -------- process command
  CASE 'X'.
    WHEN p_start.
      IF lr_ad_mgr->start( ) EQ abap_true.
        output 'daemon started'.
      ELSE.
        exit_error 'start daemon failed'.
      ENDIF.
    WHEN p_stop.
      IF lr_ad_mgr->stop( ) EQ abap_true.
        output 'daemon stopped'.
      ELSE.
        exit_error 'stop daemon failed'.
      ENDIF.
    WHEN p_send.
      IF p_cmd IS INITIAL.
        exit_error 'command missing'.
      ELSE.
        IF lr_ad_mgr->send_cmd(
            iv_cmd     = p_cmd
            iv_param   = p_param
            iv_payload = p_payl ) EQ abap_true.
          output 'command sent to daemon'.
        ELSE.
          exit_error 'sending command failed'.
        ENDIF.
      ENDIF.
    WHEN OTHERS.
      exit_error 'unknown option'.
  ENDCASE.
