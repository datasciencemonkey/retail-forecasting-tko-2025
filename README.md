# Demand Forecasting and Sensing Demos

This repository contains demonstrations of demand forecasting and sensing capabilities using machine learning techniques on Databricks.

## 📁 Project Structure 

### forecasting-demo/
Contains time series forecasting implementation using AutoGluon:
- Uses grocery sales data to predict future demand
- Implements multiple forecasting models including DeepAR, AutoARIMA, PatchTST, and Chronos
- Evaluates models using RMSE metric
- Saves forecasts to a database table

### sensing-demo/
Demonstrates demand sensing capabilities:
- Incorporates multiple factors affecting demand:
  - Social media indices
  - Local events
  - Weather data
  - POS sales history
  - Base forecasts
- Uses Databricks AutoML for regression modeling
- Implements model versioning and registration in Unity Catalog
- Includes visualization of the demand sensing process
