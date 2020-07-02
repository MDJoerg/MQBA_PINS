*&---------------------------------------------------------------------*
*& Report ZMQBA_PINS_TEST_SYSTEM_INFO
*&---------------------------------------------------------------------*
*& test the system info implemenation with broker integration
*&---------------------------------------------------------------------*
REPORT zmqba_pins_test_system_info NO STANDARD PAGE HEADING.

* ----------- interface
PARAMETERS: p_param TYPE zmqba_param OBLIGATORY DEFAULT 'DEFAULT' LOWER CASE.


START-OF-SELECTION.


* ----------- get the handler and init
  DATA(lr_handler) = zcl_mqba_pins_system_info=>create( ).
  lr_handler->set_config_id( p_param ).


* ----------- process
  IF lr_handler->collect( ) EQ abap_true.
    WRITE: / 'System info collected and published to local broker.'.
  ELSE.
    WRITE: / 'Errors occured.' COLOR 6.
  ENDIF.

* ----------- cleanup
  lr_handler->destroy( ).
