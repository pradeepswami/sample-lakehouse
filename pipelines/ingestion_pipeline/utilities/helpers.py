"""
Shared utilities for ingestion pipeline.
"""
from pyspark.sql import SparkSession
spark: SparkSession

def get_catalog() -> str:
  """
  Reads the config and return
  """
  spark = SparkSession.builder.getOrCreate()
  return spark.conf.get("stepright.catalog", "dev")

def landing_path(subfolder: str) -> str:
  """
  Returns the path to the landing zone.
  """
  return f"/Volumes/{get_catalog()}/stepright/landing/{subfolder}"
