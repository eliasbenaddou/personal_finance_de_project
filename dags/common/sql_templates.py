delete = """
DELETE FROM {schema}.{table} x
WHERE x.id IN ({transactions})
"""

exists = """
SELECT
"id",
"date",
"description",
"amount",
"category",
"merchant_id",
"merchant_category",
"merchant_description",
"notes",
"suggested_tags",
"emoji"
FROM {schema}.{table}
"""

past_week_transactions_agg = """
SELECT a.category,
sum(a.count) AS frequency,
sum(a.amount) AS total_amount
FROM (
SELECT count(*),
category,
amount
FROM {schema}.{table}
WHERE date < current_date
AND date > (current_date - interval '7 day')
GROUP BY date,
category,
amount
ORDER BY date desc
) a
GROUP BY a.category
ORDER BY sum(a.amount) DESC
"""
