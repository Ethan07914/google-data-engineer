CREATE OR REPLACE MODEL lakehouse.return_predictor
OPTIONS(model_type = 'LOGISTIC_REG') AS
SELECT
  customer_id,
  total_events,
  total_spent,
  (total_purchases > 1) AS label
FROM lakehouse.customer_summary_gold;

-- Evaluate
SELECT * FROM ML.EVALUATE(MODEL lakehouse.return_predictor);

-- Predict
SELECT * except (predicted_label), predicted_label as will_return FROM ML.PREDICT(MODEL lakehouse.return_predictor,
  TABLE lakehouse.customer_summary_gold);