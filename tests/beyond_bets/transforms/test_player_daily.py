import pytest
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, TimestampType, DoubleType
) 
from beyond_bets.transforms.player_daily import PlayerDaily
from beyond_bets.base.dataset import Dataset

@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder \
        .appName("PlayerDailyTest") \
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
            timestamp=datetime.datetime(2024, 1, 1, 1, 15, 0), bet_amount=150.0, player_id=1),
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

def test_player_daily_transformation(sample_bets_data):
    """
    The PlayerDaily transform should group the bets by player and date, 
    and sum the bet_amount for each player on that date.
    """
    pd = PlayerDaily()
    
    # Override the randomly generated bets data
    pd._inputs["bets"] = SampleDataset(sample_bets_data)

    result_df = pd.result()

    print(result_df.show())

    # Check if the result DataFrame is the expected size
    assert result_df.count() == 3

    # Check if the result DataFrame is the expected size per player
    assert result_df.filter(F.col("player_id") == 1).count() == 1
    assert result_df.filter(F.col("player_id") == 3).count() == 1 
    assert result_df.filter(F.col("player_id") == 4).count() == 1 

    # Check if the bet_amount is summed correctly
    assert result_df.filter(F.col("player_id") == 1)\
        .select(F.sum("total_bets")).collect()[0][0] == 250.0
    assert result_df.filter(F.col("player_id") == 3)\
        .select(F.sum("total_bets")).collect()[0][0] == 200.0
    assert result_df.filter(F.col("player_id") == 4)\
        .select(F.sum("total_bets")).collect()[0][0] == 250.0
