from pyspark.sql import SparkSession

class SparkManager:
    _instance = None
    _session = None

    @classmethod
    def get_session(cls):
        if not cls._session:
            cls._session = SparkSession.builder.getOrCreate()
        return cls._session

    @classmethod
    def stop_session(cls):
        if cls._session:
            cls._session.stop()
            cls._session = None