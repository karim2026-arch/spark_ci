# PySpark CI with GitHub Actions

`pyspark_job.py` has a function `clean_data(df)` that:

- removes rows where `name` is NULL
- removes rows where `amount <= 0`
- adds `amount_with_tax = amount * 1.20`

`test_pyspark_job.py` has pytest unit tests for it. They use one local SparkSession for all tests.

The workflow in `.github/workflows/ci.yml` runs the tests on every pull request (opened or updated) and on push to main. It installs Java 17 and Python 3.11 first because PySpark needs Java.

Run the tests locally:

```
pip install -r requirements.txt
pytest -v
```
