CREATE VIEW monzo.v_monzo_transactions
AS SELECT tr.id,
    tr.date,
    round(tr.amount::numeric, 2) AS amount,
        CASE
            WHEN COALESCE(tr.merchant_description, tr.description) ~~ 'DD%'::text THEN 'Direct Debit'::text
            ELSE COALESCE(tr.merchant_description, tr.description)
        END AS merchant,
        category,
    COALESCE(NULLIF(tr.emoji, ''::text),
        CASE
            WHEN lower(tr.category) = 'pets'::text THEN '🐱'::text
            WHEN lower(tr.category) = 'groceries'::text THEN '🍏'::text
            WHEN lower(tr.category) = 'clothes'::text THEN '👕'::text
            WHEN lower(tr.category) = 'stocks'::text THEN '📉'::text
            WHEN lower(tr.category) = 'healthcare'::text THEN '💊'::text
            WHEN lower(tr.category) = 'bills'::text THEN '🔌'::text
            WHEN lower(tr.category) = 'education'::text THEN '📚'::text
            WHEN lower(tr.category) = 'shopping'::text THEN '💳'::text
            WHEN lower(tr.category) = 'fees'::text THEN '💸'::text
            WHEN lower(tr.category) = 'holidays'::text THEN '🏖'::text
            WHEN lower(tr.category) = 'home'::text THEN '🏠'::text
            WHEN lower(tr.category) = 'gym'::text THEN '🏋️‍♀️'::text
            WHEN lower(tr.category) = 'entertainment'::text THEN '🎮'::text
            WHEN lower(tr.category) = 'gifts'::text THEN '🎁'::text
            WHEN lower(tr.category) = 'eating_out'::text THEN '🍝'::text
            WHEN lower(tr.category) = 'subscriptions'::text THEN '📆'::text
            WHEN lower(tr.category) = 'transport'::text THEN '🚇'::text
            WHEN lower(tr.category) = 'travel'::text THEN '🚀'::text
            WHEN lower(tr.category) = 'hotels'::text THEN '🏩'::text
            ELSE '❓'::text
        END) AS emoji,
        CASE
            WHEN COALESCE(NULLIF(tr.notes, ''::text), tr.suggested_tags) ~~ 'DD%'::text THEN 'Direct Debit'::text
            ELSE COALESCE(NULLIF(tr.notes, ''::text), tr.suggested_tags)
        END AS notes,
    date_part('year'::text, tr.date)::numeric AS year,
    date_part('month'::text, tr.date)::numeric AS month,
    date_part('day'::text, tr.date)::numeric AS day
   FROM monzo.monzo_transactions tr
  WHERE tr.decline = 0::double precision AND tr.amount > 0::double precision
  ORDER BY tr.date;