#!/bin/bash
  shopt -s expand_aliases
  alias alexandria='docker run -it --rm -v "$HOME/.peon/tools/alexandria_config.yml:/config/connections/alexandria_connections.yaml" -v "$PWD:/dax_data" avco/alexandria:cli'
  alexandria project-pipe -p tmob2514 -t i3_intrepid_census_block_pen_model_w_tmo_0_60_month_10142025 -D cb_id -S cb_id -u glenntachera --drop