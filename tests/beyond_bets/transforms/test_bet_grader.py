import pytest
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, TimestampType, DoubleType
) 
from beyond_bets.transforms.bet_grader import BetGrader
from beyond_bets.base.dataset import Dataset

@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder \
        .appName("BetGraderTest") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

@pytest.fixture
def sample_bets_data(spark):
    # Create a DataFrame with sample data matching the Bets schema
    data = [
        Row(market="MIA @ BOS Over 110.5", odds=-115,
            timestamp=datetime.datetime(2024, 1, 1, 1, 6, 0), bet_amount=723.5, player_id=1),
        Row(market="MIA @ BOS Over 110.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 1, 1, 18, 0), bet_amount=156.8, player_id=1),
        Row(market="DEN @ GSW Over 115.5", odds=-105,
            timestamp=datetime.datetime(2024, 1, 2, 2, 20, 0), bet_amount=892.3, player_id=2),
        Row(market="DEN @ GSW Over 115.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 2, 2, 35, 0), bet_amount=245.9, player_id=2),
        Row(market="MIA @ BOS Over 110.5", odds=-115,
            timestamp=datetime.datetime(2024, 1, 3, 1, 40, 0), bet_amount=567.2, player_id=3),
        Row(market="MIA @ BOS Over 110.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 3, 1, 57, 0), bet_amount=123.4, player_id=3),
        Row(market="DEN @ GSW Over 115.5", odds=-105,
            timestamp=datetime.datetime(2024, 1, 4, 2, 22, 0), bet_amount=789.1, player_id=1),
        Row(market="DEN @ GSW Over 115.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 4, 2, 13, 0), bet_amount=432.7, player_id=1),
        Row(market="LAL @ PHX Over 225.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 5, 3, 15, 0), bet_amount=678.9, player_id=4),
        Row(market="LAL @ PHX Over 225.5", odds=-115,
            timestamp=datetime.datetime(2024, 1, 5, 3, 28, 0), bet_amount=345.6, player_id=4),
        Row(market="LAL @ PHX Over 225.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 6, 4, 10, 0), bet_amount=901.2, player_id=5),
        Row(market="LAL @ PHX Over 225.5", odds=-105,
            timestamp=datetime.datetime(2024, 1, 6, 4, 25, 0), bet_amount=234.5, player_id=5),
        Row(market="MIA @ BOS Over 110.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 7, 5, 5, 0), bet_amount=567.8, player_id=6),
        Row(market="MIA @ BOS Over 110.5", odds=-115,
            timestamp=datetime.datetime(2024, 1, 7, 5, 20, 0), bet_amount=123.4, player_id=6),
        Row(market="DEN @ GSW Over 115.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 8, 6, 15, 0), bet_amount=789.0, player_id=7),
        Row(market="DEN @ GSW Over 115.5", odds=-105,
            timestamp=datetime.datetime(2024, 1, 8, 6, 30, 0), bet_amount=456.7, player_id=7),
        Row(market="LAL @ PHX Over 225.5", odds=-115,
            timestamp=datetime.datetime(2024, 1, 9, 7, 12, 0), bet_amount=234.5, player_id=8),
        Row(market="LAL @ PHX Over 225.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 9, 7, 25, 0), bet_amount=678.9, player_id=8)
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


def test_bet_grader_transformation(sample_bets_data):
    bg = BetGrader()
    bg._inputs["bets"] = SampleDataset(sample_bets_data)
    result_df = bg.result()
    print(result_df.show())