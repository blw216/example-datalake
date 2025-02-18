Things I did:

- Added the `PlayerHourly` transform
- Added the `MarketDaily` transform
- Added the `PlayerMarketDaily` transform
- Added the `TopPlayers` transform
- Added the `BetGrader` transform
- Added the `SparkManager` class to manage the Spark session as a singleton
- Added tests for each transform
- Added some basic docstrings to the transforms
- Used the `num_partitions` parameter via kwargs to repartition the data in the transforms

Things I would do if I had more time:

- Add another layer of abstraction to the transforms since most of them are time-based,
  e.g. a `TimeBasedTransform` class that extends `Transform` and overrides the `_transformation` method.
  I would also parameterize the dimensions that the user can choose to group by.

- I would create a utility function for dataframe repartitioning.

- I would define/extend input validation and exception handling in the classes.