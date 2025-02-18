import pytest
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, TimestampType, DoubleType
) 
from beyond_bets.transforms.player_hourly import PlayerHourly
from beyond_bets.base.dataset import Dataset

@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder \
        .appName("PlayerHourlyTest") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

@pytest.fixture
def sample_bets_data(spark):
    # Create a DataFrame with sample data matching the Bets schema
    data = [
        Row(market="MIA @ BOS Over 110.5", odds=-115,
            timestamp=datetime.datetime(2024, 1, 1, 1, 0, 0), bet_amount=50.0, player_id=1),
        Row(market="MIA @ BOS Over 110.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 1, 1, 30, 0), bet_amount=50.0, player_id=1),
        Row(market="DEN @ GSW Over 115.5", odds=-105,
            timestamp=datetime.datetime(2024, 1, 1, 2, 0, 0), bet_amount=100.0, player_id=2),
        Row(market="DET @ OKC Over 111.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 2, 2, 30, 0), bet_amount=100.0, player_id=2),
        Row(market="MIA @ BOS Over 110.5", odds=-115,
            timestamp=datetime.datetime(2024, 1, 1, 3, 0, 0), bet_amount=150.0, player_id=3),
        Row(market="MIA @ BOS Over 110.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 2, 3, 30, 0), bet_amount=150.0, player_id=3),
        Row(market="DEN @ GSW Over 115.5", odds=-105,
            timestamp=datetime.datetime(2024, 1, 2, 1, 0, 0), bet_amount=200.0, player_id=4),
        Row(market="DET @ OKC Over 111.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 2, 1, 30, 0), bet_amount=200.0, player_id=4),
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

def test_player_hourly_transformation(sample_bets_data):
    """
    The PlayerHourly transform should group the bets by player and hour, 
    and sum the bet_amount for each player in that hour.
    """
    ph = PlayerHourly()
    
    # Override the randomly generated bets data
    ph._inputs["bets"] = SampleDataset(sample_bets_data)

    result_df = ph.result()

    print(result_df.show())

    # Check if the result DataFrame is the expected size
    assert result_df.count() == 6

    # Check if the result DataFrame is the expected size per player
    assert result_df.filter(F.col("player_id") == 1).count() == 1
    assert result_df.filter(F.col("player_id") == 2).count() == 2
    assert result_df.filter(F.col("player_id") == 3).count() == 2
    assert result_df.filter(F.col("player_id") == 4).count() == 1

    # Check if the bet_amount is summed correctly
    assert result_df.filter(F.col("player_id") == 1)\
        .select(F.col("total_bets")).collect()[0][0] == 100.0
    assert result_df.filter(F.col("player_id") == 4)\
        .select(F.col("total_bets")).collect()[0][0] == 400.0
