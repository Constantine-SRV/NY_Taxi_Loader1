# table
```sql

DROP TABLE IF EXISTS `taxi_trips`;

CREATE TABLE `taxi_trips` (
  `trip_id` int(11) NOT NULL AUTO_INCREMENT,
  `pickup_datetime` datetime NOT NULL COMMENT 'Pickup timestamp',
  `vendor_id` tinyint(4) NOT NULL COMMENT 'Vendor ID (1=Creative Mobile, 2=VeriFone)',
  `dropoff_datetime` datetime NOT NULL COMMENT 'Dropoff timestamp',
  `passenger_count` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'Number of passengers',
  `trip_distance` decimal(11,2) NOT NULL DEFAULT '0.00' COMMENT 'Trip distance in miles',
  `rate_code_id` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'Rate code (1=Standard, 2=JFK, etc)',
  `store_and_fwd_flag` char(1) DEFAULT 'N' COMMENT 'Store and forward flag',
  `pu_location_id` smallint(6) NOT NULL COMMENT 'Pickup location ID',
  `do_location_id` smallint(6) NOT NULL COMMENT 'Dropoff location ID',
  `payment_type` tinyint(4) NOT NULL COMMENT 'Payment type (1=Credit, 2=Cash, etc)',
  `fare_amount` decimal(12,2) NOT NULL DEFAULT '0.00' COMMENT 'Base fare',
  `extra` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT 'Extra charges',
  `mta_tax` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT 'MTA tax',
  `tip_amount` decimal(11,2) NOT NULL DEFAULT '0.00' COMMENT 'Tip amount',
  `tolls_amount` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT 'Tolls',
  `improvement_surcharge` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT 'Improvement surcharge',
  `total_amount` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT 'Total amount',
  `congestion_surcharge` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT 'Congestion surcharge',
  PRIMARY KEY (`pickup_datetime`, `trip_id`)
)
ORGANIZATION INDEX
AUTO_INCREMENT = 1
AUTO_INCREMENT_MODE = 'ORDER'
DEFAULT CHARSET = utf8mb4
ROW_FORMAT = DYNAMIC
COMPRESSION = 'zstd_1.3.8'
REPLICA_NUM = 1
BLOCK_SIZE = 16384
USE_BLOOM_FILTER = FALSE
ENABLE_MACRO_BLOCK_BLOOM_FILTER = FALSE
TABLET_SIZE = 134217728
PCTFREE = 0
COMMENT = 'NYC Taxi trip data'
PARTITION BY RANGE COLUMNS (`pickup_datetime`) (
  PARTITION p2010 VALUES LESS THAN ('2011-01-01 00:00:00'),
  PARTITION p2011 VALUES LESS THAN ('2012-01-01 00:00:00'),
  PARTITION p2012 VALUES LESS THAN ('2013-01-01 00:00:00'),
  PARTITION p2013 VALUES LESS THAN ('2014-01-01 00:00:00'),
  PARTITION p2014 VALUES LESS THAN ('2015-01-01 00:00:00'),
  PARTITION p2015 VALUES LESS THAN ('2016-01-01 00:00:00'),
  PARTITION p2016 VALUES LESS THAN ('2017-01-01 00:00:00'),
  PARTITION p2017 VALUES LESS THAN ('2018-01-01 00:00:00'),
  PARTITION p2018 VALUES LESS THAN ('2019-01-01 00:00:00'),
  PARTITION p2019 VALUES LESS THAN ('2020-01-01 00:00:00'),
  PARTITION p2020 VALUES LESS THAN ('2021-01-01 00:00:00'),
  PARTITION p2021 VALUES LESS THAN ('2022-01-01 00:00:00'),
  PARTITION p2022 VALUES LESS THAN ('2023-01-01 00:00:00'),
  PARTITION p2023 VALUES LESS THAN ('2024-01-01 00:00:00'),
  PARTITION p2024 VALUES LESS THAN ('2025-01-01 00:00:00'),
  PARTITION p2025 VALUES LESS THAN ('2026-01-01 00:00:00')
);
```

# indexes
```
-- vendor_id
CREATE INDEX idx_vendor_id
  ON taxi_trips(vendor_id)
  BLOCK_SIZE 16384 LOCAL;

-- rate_code_id
CREATE INDEX idx_rate_code_id
  ON taxi_trips(rate_code_id)
  BLOCK_SIZE 16384 LOCAL;

-- pu_location_id
CREATE INDEX idx_pu_location_id
  ON taxi_trips(pu_location_id)
  BLOCK_SIZE 16384 LOCAL;

-- do_location_id
CREATE INDEX idx_do_location_id
  ON taxi_trips(do_location_id)
  BLOCK_SIZE 16384 LOCAL;

-- payment_type
CREATE INDEX idx_payment_type
  ON taxi_trips(payment_type)
  BLOCK_SIZE 16384 LOCAL;

```
