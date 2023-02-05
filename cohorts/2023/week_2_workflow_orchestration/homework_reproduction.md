### Q1
```bash
python etl_web_to_gcs.py
```

### Q2
```bash
prefect deployment build etl_web_to_gcs.py:main_flow -n etl --cron "0 5 1 * *" -a
```

### Q3
```bash
python etl_gcs_to_bq.py
```

### Q4
```bash
python create_gh_deployment_block.py
prefect deployment build etl_web_to_gcs_from_gh.py:main_flow --name etl-web-to-gcs-via-github --tag main -sb github/main
```