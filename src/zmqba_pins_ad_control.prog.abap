*&---------------------------------------------------------------------*
*& Report ZBAD_ADMQ_DAEMON_CONTROL
*&---------------------------------------------------------------------*
*& Controls a Daemon for MQBA
*&---------------------------------------------------------------------*
REPORT zmqba_pins_ad_control NO STANDARD PAGE HEADING.


TABLES: ztc_mqbabrk.
PARAMETERS: p_broker  LIKE ztc_mqbabrk-broker_id OBLIGATORY.
SELECTION-SCREEN: ULINE.
PARAMETERS: p_start   RADIOBUTTON GROUP grp1 DEFAULT 'X'.
PARAMETERS: p_stop    RADIOBUTTON GROUP grp1.
SELECTION-SCREEN: ULINE.
PARAMETERS: p_send    RADIOBUTTON GROUP grp1.
PARAMETERS: p_cmd     TYPE string DEFAULT 'mycmd'.
PARAMETERS: p_payl    TYPE string DEFAULT 'mydata'.


START-OF-SELECTION.

  DEFINE exit_error.
    WRITE: / &1 COLOR 6.
    RETURN.
  END-OF-DEFINITION.

  DEFINE output.
    WRITE: / &1.
  END-OF-DEFINITION.


* ------- create broker config
  DATA(lr_brk_cfg) = zcl_mqba_factory=>get_broker_config( p_broker ).
  IF lr_brk_cfg IS INITIAL.
    " wrong or missing config
    exit_error 'wrong broker config'.
  ENDIF.

* ------- get config and check
  DATA(ls_cfg) = lr_brk_cfg->get_config( ).
  IF ls_cfg-impl_class IS INITIAL.
    exit_error 'missing broker implementation class'.
  ENDIF.

* ------- get ad manager
  DATA(lr_ad_mgr) = zcl_mqba_pins_ad_mgr=>create( ).
  lr_ad_mgr->set_id( p_broker ).
  lr_ad_mgr->set_type( ls_cfg-impl_class ).

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
            iv_payload = p_payl ) EQ abap_true.
          output 'command sent to daemon'.
        ELSE.
          exit_error 'sending command failed'.
        ENDIF.
      ENDIF.
    WHEN OTHERS.
      exit_error 'unknown option'.
  ENDCASE.
