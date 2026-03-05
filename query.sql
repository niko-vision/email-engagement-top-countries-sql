WITH
-- Account metrics: collect base dimensions for account activity
cte_account_sessions_dim AS (
  SELECT s.date,
         sp.country,
         a.id AS id_account,
         a.send_interval,
         a.is_verified,
         a.is_unsubscribed
   FROM `DA.account` a
   JOIN `DA.account_session` acs
       ON a.id = acs.account_id
   JOIN `DA.session` s
       ON acs.ga_session_id = s.ga_session_id
   JOIN `DA.session_params` sp
       ON  acs.ga_session_id = sp.ga_session_id
     ),
      
-- Collect email events (sent/open/visit) with account and country attributes
cte_email_activity_dim AS (
  SELECT DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS sent_date,
             sp.country,
             a.id AS id_account,
             a.send_interval,
             a.is_verified,
             a.is_unsubscribed,
             es.id_message AS sent_message,
             eo.id_message AS open_message,
             ev.id_message AS visit_message
     FROM `DA.email_sent` es
     JOIN `DA.account_session` acs
         ON es.id_account = acs.account_id
     JOIN `DA.session` s
         ON acs.ga_session_id = s.ga_session_id
     LEFT JOIN `DA.email_open` eo
         ON es.id_message = eo.id_message
     LEFT JOIN `DA.email_visit` ev
         ON es.id_message = ev.id_message
     JOIN `DA.account` a
         ON acs.account_id= a.id
     JOIN `DA.session_params` sp
         ON s.ga_session_id= sp.ga_session_id
         ),

-- Build daily metrics (accounts+emails) using UNION ALL
cte_union_daily_metrics AS (
  SELECT date,
         country,
         send_interval,
         is_verified,
         is_unsubscribed,
         COUNT(DISTINCT id_account) AS account_cnt,
         0 AS sent_msg,
         0 AS open_msg,
         0 AS visit_msg
     FROM  cte_account_sessions_dim
     GROUP BY date,country,send_interval,is_verified,is_unsubscribed

  UNION ALL

  SELECT sent_date AS date,
         country,
         send_interval,
         is_verified,
         is_unsubscribed,
         0 AS account_cnt,
         COUNT(DISTINCT sent_message) AS sent_msg,
         COUNT(DISTINCT open_message) AS open_msg,
         COUNT(DISTINCT visit_message) AS visit_msg
     FROM cte_email_activity_dim
     GROUP BY sent_date,country,send_interval,is_verified,is_unsubscribed
  ),
-- Add country totals using window functions (total accounts/sent emails per country)
cte_add_country_totals AS (
  SELECT date,
         country,
         send_interval,
         is_verified,
         is_unsubscribed,
         account_cnt,
         sent_msg,
         open_msg,
         visit_msg,
         SUM(account_cnt) OVER (PARTITION BY country ) AS total_country_account_cnt,
         SUM(sent_msg) OVER (PARTITION BY country )AS total_country_sent_cnt
         FROM cte_union_daily_metrics
  ),
-- Rank countries by total accounts/sent emails
cte_country_ranks AS (
  SELECT country,
         total_country_account_cnt,
         total_country_sent_cnt,
         RANK() OVER (ORDER BY total_country_account_cnt DESC ) AS rank_total_country_account_cnt,
         RANK() OVER (ORDER BY total_country_sent_cnt DESC ) AS rank_total_country_sent_cnt
         FROM (
  -- Reduce to one row per country (totals are repeated per day in previous CTE)
  SELECT country, 
         MAX(total_country_account_cnt) AS total_country_account_cnt,
         MAX (total_country_sent_cnt) AS total_country_sent_cnt
         FROM cte_add_country_totals
         GROUP BY country)
  ),
-- Join ranks back to daily metrics     
cte_final_join AS (
  SELECT act.date,
         act.country, 
         act.send_interval ,
         act.is_verified,
         act.is_unsubscribed,
         act.account_cnt,
         act.sent_msg,
         act.open_msg,
         act.visit_msg,
         ccr.total_country_account_cnt,
         ccr.total_country_sent_cnt,
         ccr.rank_total_country_account_cnt,
         ccr.rank_total_country_sent_cnt
    FROM cte_add_country_totals  act
    JOIN cte_country_ranks ccr
        ON act.country = ccr.country
  )

SELECT date,
       country,
       send_interval,
  -- Whether the user verified their email address (0 - not verified, 1 - verified)
          CASE
          WHEN is_verified = 1 THEN 'Verified'
          ELSE 'Not Verified'
          END AS is_verified,
          CASE
  -- Whether the user unsubscribed from emails (0 - subscribed, 1 - unsubscribed)  
          WHEN is_unsubscribed = 1 THEN 'Unsubscribed'
          ELSE 'Subscribed'
          END AS is_unsubscribed,
       account_cnt,
       sent_msg,
       open_msg,
       visit_msg,
       total_country_account_cnt,
       total_country_sent_cnt,
       rank_total_country_account_cnt,
       rank_total_country_sent_cnt
    FROM cte_final_join
    WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt<= 10
    ORDER BY date
 
