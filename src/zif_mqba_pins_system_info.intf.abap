INTERFACE zif_mqba_pins_system_info
  PUBLIC .


  METHODS set_config_id
    IMPORTING
      !iv_config_id  TYPE data
    RETURNING
      VALUE(rr_self) TYPE REF TO zif_mqba_pins_system_info .
  METHODS collect
    RETURNING
      VALUE(rv_success) TYPE abap_bool .
  METHODS destroy .
ENDINTERFACE.
