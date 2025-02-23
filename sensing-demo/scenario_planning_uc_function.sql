create or replace function
main.sgfs.scenario_planning_sensing_forecast
(
  item_id int comment "item id",
  location_id int comment "location id",
  date string comment "date",
  current_base_forecast double comment "current baseline forecast",
  social_media_index double comment "social media index",
  local_events_count int comment "count of total local events",
  location_weather string comment "weather condition in string",
  adjusted_demand_forecast double comment "adjusted previous consensus demand forecast",
  lag1_pos_sales double comment "point of sale sales lag 1",
  lag2_pos_sales double comment "point of sale sales lag 2"
)
RETURNS TABLE (
  item_id int,
  location_id int,
  date string,
  current_base_forecast double,
  social_media_index double,
  local_events_count int,
  location_weather string,
  adjusted_demand_forecast double,
  ship_str_ds_forecast float comment 'new updated demand sensing forecast for shipments')
comment 'Produces updated demand sensing forecasts which represent the shipments to the store'
RETURN
SELECT 
scenario_planning_sensing_forecast.item_id,
scenario_planning_sensing_forecast.location_id,
scenario_planning_sensing_forecast.date,
scenario_planning_sensing_forecast.current_base_forecast,
scenario_planning_sensing_forecast.social_media_index,
scenario_planning_sensing_forecast.local_events_count,
scenario_planning_sensing_forecast.location_weather,
scenario_planning_sensing_forecast.adjusted_demand_forecast as previous_consensus_demand_forecast,
ai_query('ship_str_ds_forecast',
    request => 
        named_struct(
         "item_id", scenario_planning_sensing_forecast.item_id,
          "location_id",scenario_planning_sensing_forecast.location_id,
          "date",scenario_planning_sensing_forecast.date,
          "current_base_forecast",scenario_planning_sensing_forecast.current_base_forecast,
          "social_media_index",scenario_planning_sensing_forecast.social_media_index,
          "local_events_count",scenario_planning_sensing_forecast.local_events_count,
          "location_weather",scenario_planning_sensing_forecast.location_weather,
          "adjusted_demand_forecast",scenario_planning_sensing_forecast.adjusted_demand_forecast,
          "lag1_pos_sales",scenario_planning_sensing_forecast.lag1_pos_sales,
          "lag2_pos_sales",scenario_planning_sensing_forecast.lag2_pos_sales
        )
);
