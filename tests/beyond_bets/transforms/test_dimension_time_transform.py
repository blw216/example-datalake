import pytest
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, TimestampType, DoubleType
) 
from beyond_bets.transforms.dimension_time_transform import DimensionTimeTransform
from beyond_bets.base.dataset import Dataset

@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder \
        .appName("MarketHourlyTest") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

@pytest.fixture
def sample_bets_data(spark):
    # Create a DataFrame with sample data matching the Bets schema
    data = [
        Row(market="MIA @ BOS Over 110.5", odds=-115,
            timestamp=datetime.datetime(2024, 1, 1, 1, 0, 0), bet_amount=100.0, player_id=1),
        Row(market="MIA @ BOS Over 110.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 1, 1, 15, 0), bet_amount=100.0, player_id=2),
        Row(market="DEN @ GSW Over 115.5", odds=-105,
            timestamp=datetime.datetime(2024, 1, 1, 3, 30, 0), bet_amount=200.0, player_id=3),
        Row(market="DET @ OKC Over 111.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 1, 4, 45, 0), bet_amount=250.0, player_id=4),
    ]
    
    schema = StructType([
        StructField("market", StringType(), False),
        StructField("odds", IntegerType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("bet_amount", DoubleType(), False),
        StructField("player_id", IntegerType(), False),
    ])
    
    return spark.createDataFrame(data, schema)

class SampleDataset(Dataset):
    def __init__(self, df):
        self.df = df

    def reader(self):
        return self.df

    def writer(self):
        pass

def test_dimension_time_transform(sample_bets_data):
    """
    Test the DimensionTimeTransform class.
    """
    dt = DimensionTimeTransform()
    dt._inputs["bets"] = SampleDataset(sample_bets_data)

    result_df = dt.result(dimensions=["market"], time_period="hour")

    # Check if the result DataFrame is the expected size
    assert result_df.count() == 3

    # Check if the result DataFrame is the expected size per market
    assert result_df.filter(F.col("market") == "MIA @ BOS Over 110.5").count() == 1
    assert result_df.filter(F.col("market") == "DEN @ GSW Over 115.5").count() == 1
    assert result_df.filter(F.col("market") == "DET @ OKC Over 111.5").count() == 1 

    # Check if the bet_amount is summed correctly
    assert result_df.filter(F.col("market") == "MIA @ BOS Over 110.5")\
        .select(F.sum("total_bets")).collect()[0][0] == 200.0
    assert result_df.filter(F.col("market") == "DEN @ GSW Over 115.5")\
        .select(F.sum("total_bets")).collect()[0][0] == 200.0
    assert result_df.filter(F.col("market") == "DET @ OKC Over 111.5")\
        .select(F.sum("total_bets")).collect()[0][0] == 250.0
