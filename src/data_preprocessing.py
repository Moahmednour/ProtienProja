## data_preprocessing.py
# Module for data loading and preprocessing

from pyspark.sql import SparkSession

def initialize_spark(app_name, executor_memory, driver_memory, shuffle_partitions):
    """
    Initialize and configure a Spark session.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.sql.shuffle.partitions", shuffle_partitions) \
        .getOrCreate()

def load_data(spark, file_path):
    """
    Load data from a TSV file into an RDD.

    Parameters:
        spark (SparkSession): The Spark session.
        file_path (str): Path to the TSV file.

    Returns:
        Tuple[RDD, List[str]]: The RDD of data rows and the header as a list of strings.
    """
    raw_rdd = spark.sparkContext.textFile(file_path)
    header = raw_rdd.first()
    data_rdd = raw_rdd.filter(lambda line: line != header).map(lambda line: line.split("\t"))
    return data_rdd, header.split("\t")

def clean_row(row):
    """
    Clean a single row by removing rows with critical missing fields.

    Parameters:
        row (dict): A dictionary representation of a data row.

    Returns:
        dict or None: Cleaned row or None if the row is invalid.
    """
    if not row["Sequence"] or not row["InterPro"]:
        return None
    return row

def preprocess_data(data_rdd):
    """
    Perform initial cleaning on the RDD.

    Parameters:
        data_rdd (RDD): Raw data RDD.

    Returns:
        RDD: Preprocessed RDD with cleaned and valid rows.
    """
    def parse_row(row):
        try:
            return {
                "Entry": row[0],
                "Entry Name": row[1],
                "Protein names": row[2],
                "Gene Names": row[3],
                "Organism": row[4],
                "Sequence": row[5],
                "EC number": row[6] if len(row) > 6 else None,
                "InterPro": row[7].split(";") if len(row) > 7 and row[7] else []
            }
        except IndexError:
            return None

    structured_rdd = data_rdd.map(parse_row).filter(lambda x: x is not None)
    cleaned_rdd = structured_rdd.map(clean_row).filter(lambda x: x is not None)
    return cleaned_rdd
