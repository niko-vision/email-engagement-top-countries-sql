# Email engagement: Top-10 countries (BigQuery SQL)

This project builds a dataset to analyze:
- account creation (account_cnt)
- email engagement: sent/open/visit (sent_msg, open_msg, visit_msg)
across countries.

## Dimensions
date, country, send_interval, is_verified, is_unsubscribed

## Output
The dataset includes country totals and country ranks and keeps only:
Top-10 countries by total created accounts OR total sent emails.

## Files
- `query.sql` — BigQuery SQL query
- `dashboard.png` — Looker Studio dashboard screenshot
