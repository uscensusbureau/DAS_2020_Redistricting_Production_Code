try:
    from pyspark.sql import SparkSession
    from pyspark.sql import Row
    from pyspark.sql.types import StringType, IntegerType
    from pyspark.sql.functions import concat, col
except ImportError as e:
    pass


def load_grfc(grfc_path: str):
    """
    Loads the GRF-C file and return a dataframe with oidtb and geocode

    :param grfc_path: The location of the GRF-C file.
    :return: A DataFrame with oidtb and geocode columns
    """
    spark = SparkSession.builder.getOrCreate()

    # Read in the GRF-C CSV file as a dataframe
    grfc_df = spark.read.csv(grfc_path, sep="|", header=True)
    # Add in the geocode column by concatenating geography columns
    grfc_df = grfc_df.withColumn(
        "geocode",
        concat(
            col("TABBLKST"),
            col("TABBLKCOU"),
            col("TABTRACTCE"),
            col("TABBLKGRPCE"),
            col("TABBLK"),
        ),
    )
    # Only keep oidtb (renamed from OIDTABBLK) and geocode in the returned dataframe
    grfc_df = grfc_df.select(col("OIDTABBLK").alias("oidtb"), col("geocode"))
    grfc_df = grfc_df.distinct()

    # Ensure that all
    grfc_without_geocode = grfc_df.where(col('geocode').isNull())
    assert grfc_without_geocode.count() == 0, f'Some GRFC could not find geocode! {grfc_without_geocode.show()}'

    return grfc_df
