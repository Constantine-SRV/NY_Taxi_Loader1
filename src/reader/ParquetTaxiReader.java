package reader;

import Logging.LogService;
import model.TaxiTrip;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Класс для чтения Parquet файлов с данными NYC Taxi.
 * Single Responsibility: только чтение и парсинг Parquet.
 * Version 2.0: С валидацией и ограничением значений по схеме БД
 */
public class ParquetTaxiReader {

    private final Configuration hadoopConfig;

    // Лимиты для TINYINT (0-255) и SMALLINT (0-65535)
    private static final int TINYINT_MAX = 255;
    private static final int SMALLINT_MAX = 65535;

    // Лимиты для DECIMAL полей
    private static final double TRIP_DISTANCE_MAX = 9999999.99;      // DECIMAL(9,2)
    private static final double FARE_AMOUNT_MAX = 9999999999.99;     // DECIMAL(12,2)
    private static final double MONEY_MAX = 99999999.99;             // DECIMAL(10,2)

    public ParquetTaxiReader() {
        this.hadoopConfig = new Configuration();
        hadoopConfig.set("fs.defaultFS", "file:///");
    }

    /**
     * Читает Parquet файл и возвращает список поездок.
     */
    public List<TaxiTrip> readFile(String filePath, int limit) throws IOException {
        List<TaxiTrip> trips = new ArrayList<>();

        LogService.infof("Reading Parquet file: %s", filePath);
        long startTime = System.currentTimeMillis();

        Path path = new Path(filePath);

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(hadoopConfig)
                .build()) {

            Group group;
            int count = 0;

            while ((group = reader.read()) != null) {
                TaxiTrip trip = parseGroup(group);
                trips.add(trip);
                count++;

                if (count % 100_000 == 0) {
                    LogService.infof("  Read %,d records...", count);
                }

                if (limit > 0 && count >= limit) {
                    LogService.infof("  Reached limit of %,d records", limit);
                    break;
                }
            }

            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > 0) {
                LogService.infof("✅ Read %,d records in %,d ms (%.2f records/sec)",
                        count, elapsed, (count * 1000.0) / elapsed);
            } else {
                LogService.infof("✅ Read %,d records", count);
            }
        }

        return trips;
    }

    /**
     * Парсит Group из Parquet в объект TaxiTrip с валидацией.
     */
    private TaxiTrip parseGroup(Group group) {
        TaxiTrip trip = new TaxiTrip();

        // VendorID - TINYINT(4): 0-255
        trip.setVendorId(clampTinyInt(getIntValue(group, "VendorID")));

        // Pickup datetime
        if (hasField(group, "tpep_pickup_datetime")) {
            long micros = getLongValue(group, "tpep_pickup_datetime");
            trip.setPickupDatetime(microsToLocalDateTime(micros));
        }

        // Dropoff datetime
        if (hasField(group, "tpep_dropoff_datetime")) {
            long micros = getLongValue(group, "tpep_dropoff_datetime");
            trip.setDropoffDatetime(microsToLocalDateTime(micros));
        }

        // Passenger count - TINYINT(4): 0-255
        trip.setPassengerCount(clampTinyInt((int) getDoubleValue(group, "passenger_count")));

        // Trip distance - DECIMAL(9,2): max 9999999.99
        trip.setTripDistance(clampDecimal(getDoubleValue(group, "trip_distance"), TRIP_DISTANCE_MAX));

        // Rate code - TINYINT(4): 0-255
        trip.setRateCodeId(clampTinyInt((int) getDoubleValue(group, "RatecodeID")));

        // Store and forward flag - CHAR(1)
        try {
            if (hasField(group, "store_and_fwd_flag")) {
                String flag = group.getString("store_and_fwd_flag", 0);
                // Гарантируем только 1 символ
                if (flag != null && !flag.isEmpty()) {
                    trip.setStoreAndFwdFlag(flag.substring(0, 1));
                } else {
                    trip.setStoreAndFwdFlag("N");
                }
            } else {
                trip.setStoreAndFwdFlag("N");
            }
        } catch (Exception e) {
            trip.setStoreAndFwdFlag("N");
        }

        // Pickup location - SMALLINT(6): 0-65535
        trip.setPuLocationId(clampSmallInt(getIntValue(group, "PULocationID")));

        // Dropoff location - SMALLINT(6): 0-65535
        trip.setDoLocationId(clampSmallInt(getIntValue(group, "DOLocationID")));

        // Payment type - TINYINT(4): 0-255
        trip.setPaymentType(clampTinyInt(getIntValue(group, "payment_type")));

        // Fare amount - DECIMAL(12,2): max 9999999999.99
        trip.setFareAmount(clampDecimal(getDoubleValue(group, "fare_amount"), FARE_AMOUNT_MAX));

        // Extra - DECIMAL(10,2): max 99999999.99
        trip.setExtra(clampDecimal(getDoubleValue(group, "extra"), MONEY_MAX));

        // MTA tax - DECIMAL(10,2)
        trip.setMtaTax(clampDecimal(getDoubleValue(group, "mta_tax"), MONEY_MAX));

        // Tip amount - DECIMAL(10,2)
        trip.setTipAmount(clampDecimal(getDoubleValue(group, "tip_amount"), MONEY_MAX));

        // Tolls amount - DECIMAL(10,2)
        trip.setTollsAmount(clampDecimal(getDoubleValue(group, "tolls_amount"), MONEY_MAX));

        // Improvement surcharge - DECIMAL(10,2)
        trip.setImprovementSurcharge(clampDecimal(getDoubleValue(group, "improvement_surcharge"), MONEY_MAX));

        // Total amount - DECIMAL(10,2)
        trip.setTotalAmount(clampDecimal(getDoubleValue(group, "total_amount"), MONEY_MAX));

        // Congestion surcharge - DECIMAL(10,2)
        trip.setCongestionSurcharge(clampDecimal(getDoubleValue(group, "congestion_surcharge"), MONEY_MAX));

        return trip;
    }

    /**
     * Ограничивает значение для TINYINT (0-255).
     * Отрицательные значения = 0, слишком большие = 255.
     */
    private int clampTinyInt(int value) {
        if (value < 0) return 0;
        if (value > TINYINT_MAX) return TINYINT_MAX;
        return value;
    }

    /**
     * Ограничивает значение для SMALLINT (0-65535).
     * Отрицательные значения = 0, слишком большие = 65535.
     */
    private int clampSmallInt(int value) {
        if (value < 0) return 0;
        if (value > SMALLINT_MAX) return SMALLINT_MAX;
        return value;
    }

    /**
     * Ограничивает значение для DECIMAL полей.
     * Отрицательные значения = 0, слишком большие = max.
     */
    private double clampDecimal(double value, double max) {
        // Проверка на NaN и Infinity
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return 0.0;
        }

        if (value < 0.0) return 0.0;
        if (value > max) return max;

        // Округление до 2 знаков после запятой
        return Math.round(value * 100.0) / 100.0;
    }

    /**
     * Безопасное чтение целочисленного значения (int или long).
     */
    private int getIntValue(Group group, String fieldName) {
        if (!hasField(group, fieldName)) {
            return 0;
        }

        try {
            return group.getInteger(fieldName, 0);
        } catch (ClassCastException e) {
            try {
                long longVal = group.getLong(fieldName, 0);
                // Безопасное преобразование long -> int
                if (longVal > Integer.MAX_VALUE) return Integer.MAX_VALUE;
                if (longVal < Integer.MIN_VALUE) return Integer.MIN_VALUE;
                return (int) longVal;
            } catch (Exception ex) {
                return 0;
            }
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Безопасное чтение long значения.
     */
    private long getLongValue(Group group, String fieldName) {
        if (!hasField(group, fieldName)) {
            return 0L;
        }

        try {
            return group.getLong(fieldName, 0);
        } catch (ClassCastException e) {
            try {
                return group.getInteger(fieldName, 0);
            } catch (Exception ex) {
                return 0L;
            }
        } catch (Exception e) {
            return 0L;
        }
    }

    /**
     * Безопасное чтение дробного значения (float или double).
     */
    private double getDoubleValue(Group group, String fieldName) {
        if (!hasField(group, fieldName)) {
            return 0.0;
        }

        try {
            double value = group.getDouble(fieldName, 0);
            // Проверка на некорректные значения
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                return 0.0;
            }
            return value;
        } catch (ClassCastException e) {
            try {
                float floatVal = group.getFloat(fieldName, 0);
                if (Float.isNaN(floatVal) || Float.isInfinite(floatVal)) {
                    return 0.0;
                }
                return floatVal;
            } catch (Exception ex) {
                return 0.0;
            }
        } catch (Exception e) {
            return 0.0;
        }
    }

    /**
     * Проверяет, существует ли поле в Group.
     */
    private boolean hasField(Group group, String fieldName) {
        try {
            group.getType().getFieldIndex(fieldName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Конвертирует микросекунды Unix timestamp в LocalDateTime.
     */
    private LocalDateTime microsToLocalDateTime(long micros) {
        try {
            long millis = micros / 1000;
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
        } catch (Exception e) {
            // Если конвертация не удалась, вернуть текущую дату
            return LocalDateTime.now();
        }
    }

    /**
     * Выводит схему Parquet файла (все поля и их типы).
     */
    public void printSchema(String filePath) throws IOException {
        LogService.infof("Analyzing schema: %s", filePath);
        Path path = new Path(filePath);

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(hadoopConfig)
                .build()) {

            Group firstRecord = reader.read();
            if (firstRecord != null) {
                LogService.info("=== Parquet File Schema ===");

                org.apache.parquet.schema.GroupType schema = firstRecord.getType();
                int fieldCount = schema.getFieldCount();

                for (int i = 0; i < fieldCount; i++) {
                    org.apache.parquet.schema.Type field = schema.getType(i);
                    String fieldName = field.getName();

                    String value = "null";
                    try {
                        value = firstRecord.getValueToString(i, 0);
                    } catch (Exception e) {
                        value = "(unable to read)";
                    }

                    String typeName = "UNKNOWN";
                    try {
                        typeName = field.asPrimitiveType().getPrimitiveTypeName().toString();
                    } catch (Exception e) {
                        typeName = field.toString();
                    }

                    LogService.infof("  Field %2d: %-30s | Type: %-15s | Sample: %s",
                            i, fieldName, typeName, value);
                }

                LogService.infof("Total fields: %d", fieldCount);
            }
        }
    }
}