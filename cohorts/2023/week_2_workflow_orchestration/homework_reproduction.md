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