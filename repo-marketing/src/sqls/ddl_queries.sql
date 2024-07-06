CREATE OR REPLACE TABLE `data-427901.repo_marketing.dim_repos`
(
  id INT64,
  name STRING,
  url STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  deleted_at TIMESTAMP DEFAULT NULL
)
;

CREATE OR REPLACE TABLE `data-427901.repo_marketing.fact_repo_events`
(
  event_date_kst DATE,
  id INT64,
  metric STRING,
  value INT64
)
;

CREATE OR REPLACE TABLE  `data-427901.repo_marketing.dim_repo_readmes`
(
  id INT64,
  content STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  deteled_at TIMESTAMP DEFAULT NULL
)
;

