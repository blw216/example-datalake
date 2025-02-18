import pytest
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, TimestampType, DoubleType
) 
from beyond_bets.transforms.top_players import TopPlayers
from beyond_bets.base.dataset import Dataset

@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder \
        .appName("TopPlayersTest") \
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
            timestamp=datetime.datetime(2024, 1, 2, 1, 30, 0), bet_amount=150.0, player_id=1),
        Row(market="DEN @ GSW Over 115.5", odds=-105,
            timestamp=datetime.datetime(2024, 1, 1, 2, 0, 0), bet_amount=200.0, player_id=2),
        Row(market="DET @ OKC Over 111.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 4, 2, 30, 0), bet_amount=250.0, player_id=2),
        Row(market="MIA @ BOS Over 110.5", odds=-115,
            timestamp=datetime.datetime(2024, 1, 2, 1, 0, 0), bet_amount=300.0, player_id=3),
        Row(market="MIA @ BOS Over 110.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 6, 1, 30, 0), bet_amount=350.0, player_id=3),
        Row(market="DEN @ GSW Over 115.5", odds=-105,
            timestamp=datetime.datetime(2024, 1, 2, 2, 0, 0), bet_amount=400.0, player_id=1),
        Row(market="DET @ OKC Over 111.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 5, 2, 30, 0), bet_amount=450.0, player_id=1),
        Row(market="DET @ OKC Over 111.5", odds=-110,
            timestamp=datetime.datetime(2024, 1, 5, 2, 30, 0), bet_amount=1000000.0, player_id=10),
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

def test_top_players_transformation(sample_bets_data):
    tp = TopPlayers()
    
    tp._inputs["bets"] = SampleDataset(sample_bets_data)

    # Override the start and end dates to fit sample data
    kwargs = {
            "start_date": datetime.datetime(2024, 1, 1, 0, 0, 0),
            "end_date": datetime.datetime(2024, 1, 7, 0, 0, 0)
        }

    result_df = tp.result(**kwargs)

    assert result_df.count() == 1
    assert result_df.filter(F.col("player_id") == 10).count() == 1
    assert result_df.collect()[0][1] == 1000000.0
