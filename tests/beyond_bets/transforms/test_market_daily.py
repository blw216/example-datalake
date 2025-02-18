import pytest
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, TimestampType, DoubleType
) 
from beyond_bets.transforms.market_daily import MarketDaily
from beyond_bets.base.dataset import Dataset

@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder \
        .appName("MarketDailyTest") \
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
            timestamp=datetime.datetime(2024, 1, 1, 1, 30, 0), bet_amount=150.0, player_id=1),
        Row(market="DEN @ GSW Over 115.5", odds=-105,
            timestamp=datetime.datetime(2024, 1, 2, 2, 0, 0), bet_amount=200.0, player_id=2),
        Row(market="DEN @ GSW Over 115.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 2, 2, 30, 0), bet_amount=250.0, player_id=2),
        Row(market="MIA @ BOS Over 117.5", odds=-115,
            timestamp=datetime.datetime(2024, 1, 3, 1, 0, 0), bet_amount=300.0, player_id=3),
        Row(market="MIA @ BOS Over 117.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 3, 1, 30, 0), bet_amount=350.0, player_id=3),
        Row(market="DEN @ GSW Over 120.5", odds=-105,
            timestamp=datetime.datetime(2024, 1, 4, 2, 0, 0), bet_amount=400.0, player_id=1),
        Row(market="DEN @ GSW Over 120.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 4, 2, 30, 0), bet_amount=450.0, player_id=1),
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

def test_market_daily_transformation(sample_bets_data):
    """
    The MarketDaily transform should group the bets by market and date, 
    and sum the bet_amount for each market on that date.
    """
    md = MarketDaily()
    
    # Override the randomly generated bets data
    md._inputs["bets"] = SampleDataset(sample_bets_data)

    result_df = md.result()

    print(result_df.show())

    assert result_df.count() == 4

    # Check if the result DataFrame is the expected size per market
    assert result_df.filter(F.col("market") == "MIA @ BOS Over 110.5").count() == 1
    assert result_df.filter(F.col("market") == "DEN @ GSW Over 115.5").count() == 1
    assert result_df.filter(F.col("market") == "DEN @ GSW Over 120.5").count() == 1
    assert result_df.filter(F.col("market") == "MIA @ BOS Over 117.5").count() == 1

    # Check if the bet_amount is summed correctly
    assert result_df.filter(F.col("market") == "MIA @ BOS Over 110.5")\
        .select(F.col("total_bets")).collect()[0][0] == 250.0
    assert result_df.filter(F.col("market") == "DEN @ GSW Over 115.5")\
        .select(F.col("total_bets")).collect()[0][0] == 450.0
    assert result_df.filter(F.col("market") == "DEN @ GSW Over 120.5")\
        .select(F.col("total_bets")).collect()[0][0] == 850.0
    assert result_df.filter(F.col("market") == "MIA @ BOS Over 117.5")\
        .select(F.col("total_bets")).collect()[0][0] == 650.0
