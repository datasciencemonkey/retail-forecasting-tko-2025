# Databricks notebook source
# MAGIC %pip install -r requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from autogluon.timeseries import TimeSeriesDataFrame, TimeSeriesPredictor
from sklearn.metrics._regression import mean_absolute_error
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

TABLE = "main.sgfs.grocery_data"

# COMMAND ----------


df = spark.sql(f"SELECT * FROM {TABLE}").toPandas()
df.head()

# COMMAND ----------

data = TimeSeriesDataFrame.from_data_frame(df, timestamp_column='timestamp', id_column='item_id')
data.head()

# COMMAND ----------

from sklearn.metrics import mean_absolute_error
prediction_length = 8
train_data, test_data = data.train_test_split(prediction_length)

predictor = TimeSeriesPredictor(prediction_length=prediction_length,
                                target='unit_sales',
                                eval_metric='RMSE',
                                # known_covariates_names=["scaled_price", "promotion_email", "promotion_homepage"]
                                ).fit(
    train_data,
    hyperparameters={
        "DeepAR": {},
        "AutoARIMA": {},
        "PatchTST": {},
        "Chronos": [
            # Zero-shot model WITHOUT covariates
            {
                "model_path": "bolt_small",
                "ag_args": {"name_suffix": "ZeroShot"},
            },
            {
            # fine tune the base model without the covariates
                "model_path": "bolt_small", 
                "fine_tune": True, 
                "ag_args": {"name_suffix": "FineTuned"},
            }
        ],
    },
    enable_ensemble=False,
    time_limit=300,
)

# COMMAND ----------

predictor.leaderboard(test_data)

# COMMAND ----------

result_set = spark.createDataFrame(predictor.predict(train_data, model='ChronosFineTuned[bolt_small]').to_data_frame().reset_index())
display(result_set)

# COMMAND ----------

result_set.write.saveAsTable("main.sgfs.grocery_data_forecast") 