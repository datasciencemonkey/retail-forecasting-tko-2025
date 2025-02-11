# Databricks notebook source
# MAGIC %md
# MAGIC Go here to see how this data was [generated](https://chatgpt.com/share/67a940d2-963c-8006-8d4b-802aedd46772).

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/sathish.gangichetty@databricks.com/demo-tko/sensing-demo/demand-sensing.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's explore the data generated.
# MAGIC select * from main.sgfs.generated_sensing_data where item_id=1 and location_id=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Eyeball 👀 how many records we have across the dataset.
# MAGIC select min(date), max(date), count(1) as rec_cnt, 
# MAGIC item_id, location_id from main.sgfs.generated_sensing_data
# MAGIC group by item_id, location_id
# MAGIC order by item_id, location_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- total time series in this dataset.
# MAGIC select count(distinct item_id) * count(distinct location_id) as total_time_series from main.sgfs.generated_sensing_data

# COMMAND ----------

import pandas as pd
# %%
df = spark.sql(f"select * from main.sgfs.generated_sensing_data").toPandas()
# split df into train and test by date. assign 90% to train and 10% to test by selecting from df.date.unique()
# train_dates = df.observed_date.unique()[:int(len(df.observed_date.unique())*0.8)]
# test_dates = df.observed_date.unique()[int(len(df.observed_date.unique())*0.8):]
# %%
# train_df = df[df['observed_date'].isin(train_dates)]
# test_df = df[df['observed_date'].isin(test_dates)]

cols_for_forecast = ['item_id', 'location_id', 'date', 'current_base_forecast',
                    'social_media_index', 'local_events_count', 'location_weather','adjusted_demand_forecast',
                    'lag1_pos_sales', 'lag2_pos_sales', 'ship_str']


train_df = df[cols_for_forecast]

# COMMAND ----------

train_df.head()

# COMMAND ----------

from databricks import automl

# Start the AutoML regression run
summary = automl.regress(
    dataset=train_df,
    target_col="ship_str",
    time_col = "date",
    primary_metric= "rmse",
    timeout_minutes=10
    )


# Display the summary of the AutoML run
display(summary)

# COMMAND ----------

from rich import print
print(summary.best_trial.metrics)

# COMMAND ----------

summary.best_trial.load_model()

# COMMAND ----------

# DBTITLE 1,Find the best run and register to UC
import mlflow
mlflow.set_registry_uri("databricks-uc")  # Point to Unity Catalog

best_run_id = summary.best_trial.mlflow_run_id
model_uri = f"runs:/{best_run_id}/model"

registered_name = "main.sgfs.ship_str_ds_forecast"  # UC 3-level naming
mlflow.register_model(model_uri, registered_name)

# COMMAND ----------

# DBTITLE 1,Get model version & set alias as Champion
client = mlflow.MlflowClient()
versions = client.search_model_versions(f"name='{registered_name}'")
latest_version = max(int(v.version) for v in versions)
print(f"The latest_version of the model is {latest_version}")
client.set_registered_model_alias(
    name=registered_name,
    alias="Champion",
    version=latest_version
)

# COMMAND ----------

# DBTITLE 1,Load Champ back into notebook context
model_uri = "models:/main.sgfs.ship_str_ds_forecast@Champion"
loaded_model = mlflow.pyfunc.load_model(model_uri)

# COMMAND ----------

# DBTITLE 1,Score with the model
import pandas as pd

# Convert the dictionary to a DataFrame
input_data = pd.DataFrame([train_df.iloc[-1][cols_for_forecast]])

# Use the DataFrame for prediction
prediction = loaded_model.predict(input_data)

print(f"The predicted value is {int(prediction[0])}")
print(f"Actual Shipments to Store was {train_df.iloc[-1]['ship_str']}")

# COMMAND ----------

# MAGIC %md
# MAGIC Benefits are far and wide... 
# MAGIC ![](/Workspace/Users/sathish.gangichetty@databricks.com/demo-tko/sensing-demo/ds-benefits.png)