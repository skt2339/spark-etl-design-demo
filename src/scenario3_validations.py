from pyspark.sql import functions as F


def hard_validations(df):
    # Hard rule: party_key must not be null
    if df.filter(F.col("party_key").isNull()).count() > 0:
        raise Exception("Hard validation failed: party_key contains nulls")

    # Hard rule: timestamp must be valid
    if df.filter(F.col("source_updated_at").isNull()).count() > 0:
        raise Exception("Hard validation failed: invalid source_updated_at")

    # Hard rule: duplicate explosion threshold
    total = df.count()
    distinct = df.select("party_key").distinct().count()
    if total - distinct > 0.5 * total:
        raise Exception("Hard validation failed: duplicate rate too high")


def soft_validations(df):
    # Soft rule: excessive null names
    total = df.count()
    null_name = df.filter(F.col("name").isNull()).count()

    if total > 0 and null_name / total > 0.1:
        print("Soft warning: High null percentage in name")

    # Soft rule: unexpected country codes
    allowed = ["SG", "IN", "US", "UK"]
    invalid_country = df.filter(~F.col("country").isin(allowed)).count()

    if invalid_country > 0:
        print(f"Soft warning: {invalid_country} rows with invalid country")
