create or replace table main.sgfs.ship_str_ds_forecast as 
SELECT 
item_id,
location_id,
cast(date as string) as date,
current_base_forecast,
social_media_index,
local_events_count,
location_weather,
adjusted_demand_forecast,
lag1_pos_sales,
lag2_pos_sales,
ai_query('ship_str_ds_forecast',
    request => 
        named_struct(
         "item_id", item_id,
          "location_id",location_id,
          "date",cast(date as string),
          "current_base_forecast",current_base_forecast,
          "social_media_index",social_media_index,
          "local_events_count",local_events_count,
          "location_weather",location_weather,
          "adjusted_demand_forecast",adjusted_demand_forecast,
          "lag1_pos_sales",lag1_pos_sales,
          "lag2_pos_sales",lag2_pos_sales
        )
) as ship_str_ds_forecast
from main.sgfs.generated_sensing_data;