package writer;

import config.AppConfig;
import logging.LogService;
import model.TaxiTrip;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Класс для записи данных TaxiTrip в PostgreSQL.
 * Использует batch INSERT для максимальной производительности.
 * Поддерживает многопоточную работу - каждый экземпляр имеет свое подключение.
 */
public class PostgreSQLWriter implements DatabaseWriter {

    private final Connection connection;
    private final PreparedStatement insertStatement;
    private final String taskId;
    private int batchCount = 0;
    private long totalInserted = 0;
    private long startTime;

    private static final String INSERT_SQL =
            "INSERT INTO " + AppConfig.TABLE_NAME + " (" +
                    "  pickup_datetime, " +
                    "  vendor_id, " +
                    "  dropoff_datetime, " +
                    "  passenger_count, " +
                    "  trip_distance, " +
                    "  rate_code_id, " +
                    "  store_and_fwd_flag, " +
                    "  pu_location_id, " +
                    "  do_location_id, " +
                    "  payment_type, " +
                    "  fare_amount, " +
                    "  extra, " +
                    "  mta_tax, " +
                    "  tip_amount, " +
                    "  tolls_amount, " +
                    "  improvement_surcharge, " +
                    "  total_amount, " +
                    "  congestion_surcharge" +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public PostgreSQLWriter(String taskId) throws SQLException {
        this.taskId = taskId;
        LogService.infof("[%s] Connecting to PostgreSQL...", taskId);

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            LogService.errorf("[%s] PostgreSQL driver not found", taskId);
        }

        this.connection = DriverManager.getConnection(
                AppConfig.getPostgreSQLUrl(),
                AppConfig.POSTGRESQL_USER,
                AppConfig.POSTGRESQL_PASSWORD
        );

        this.connection.setAutoCommit(false);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("SET work_mem = '256MB'");
            stmt.execute("SET maintenance_work_mem = '512MB'");
            stmt.execute("SET synchronous_commit = OFF");
            stmt.execute("SET statement_timeout = '300s'");
        } catch (SQLException e) {
            LogService.infof("[%s] Could not set session parameters: %s", taskId, e.getMessage());
        }

        this.insertStatement = connection.prepareStatement(INSERT_SQL);
        this.startTime = System.currentTimeMillis();

        LogService.infof("[%s] ✅ Connected to PostgreSQL successfully", taskId);
    }

    public PostgreSQLWriter() throws SQLException {
        this("Main");
    }

    @Override
    public void addTrip(TaxiTrip trip) throws SQLException {
        insertStatement.setTimestamp(1, toTimestamp(trip.getPickupDatetime()));
        insertStatement.setInt(2, trip.getVendorId());
        insertStatement.setTimestamp(3, toTimestamp(trip.getDropoffDatetime()));
        insertStatement.setInt(4, trip.getPassengerCount());
        insertStatement.setDouble(5, trip.getTripDistance());
        insertStatement.setInt(6, trip.getRateCodeId());
        insertStatement.setString(7, trip.getStoreAndFwdFlag());
        insertStatement.setInt(8, trip.getPuLocationId());
        insertStatement.setInt(9, trip.getDoLocationId());
        insertStatement.setInt(10, trip.getPaymentType());
        insertStatement.setDouble(11, trip.getFareAmount());
        insertStatement.setDouble(12, trip.getExtra());
        insertStatement.setDouble(13, trip.getMtaTax());
        insertStatement.setDouble(14, trip.getTipAmount());
        insertStatement.setDouble(15, trip.getTollsAmount());
        insertStatement.setDouble(16, trip.getImprovementSurcharge());
        insertStatement.setDouble(17, trip.getTotalAmount());
        insertStatement.setDouble(18, trip.getCongestionSurcharge());

        insertStatement.addBatch();
        batchCount++;

        if (batchCount >= AppConfig.BATCH_SIZE) {
            executeBatch();
        }
    }

    @Override
    public void addTrips(List<TaxiTrip> trips) throws SQLException {
        for (TaxiTrip trip : trips) {
            addTrip(trip);
        }
    }

    @Override
    public void executeBatch() throws SQLException {
        if (batchCount == 0) {
            return;
        }

        int recordsInBatch = batchCount;

        try {
            insertStatement.executeBatch();
            connection.commit();

            totalInserted += recordsInBatch;

            // ИСПРАВЛЕНО: Проверка на division by zero
            if (totalInserted % AppConfig.LOG_INTERVAL == 0) {
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed > 0) {
                    double rate = (totalInserted * 1000.0) / elapsed;
                    LogService.infof("[%s] Inserted %,d records (%.0f records/sec)",
                            taskId, totalInserted, rate);
                } else {
                    LogService.infof("[%s] Inserted %,d records", taskId, totalInserted);
                }
            }

            batchCount = 0;
            insertStatement.clearBatch();

        } catch (SQLException e) {
            connection.rollback();
            insertStatement.clearBatch();
            batchCount = 0;

            LogService.errorf("[%s] Batch insert failed: %s", taskId, e.getMessage());
            throw e;
        }
    }

    @Override
    public void flush() throws SQLException {
        if (batchCount > 0) {
            executeBatch();
        }
    }

    @Override
    public long getTotalInserted() {
        return totalInserted;
    }

    @Override
    public String getPerformanceStats() {
        long elapsed = System.currentTimeMillis() - startTime;

        // ИСПРАВЛЕНО: Проверка на division by zero
        if (elapsed > 0) {
            double rate = (totalInserted * 1000.0) / elapsed;
            double seconds = elapsed / 1000.0;
            return String.format(
                    "[%s] Total: %,d records | Time: %.1f sec | Rate: %.0f records/sec",
                    taskId, totalInserted, seconds, rate
            );
        } else {
            return String.format(
                    "[%s] Total: %,d records | Time: <1ms",
                    taskId, totalInserted
            );
        }
    }

    @Override
    public String getTaskId() {
        return taskId;
    }

    private Timestamp toTimestamp(LocalDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        return Timestamp.valueOf(dateTime);
    }

    @Override
    public void close() throws SQLException {
        try {
            flush();

            if (insertStatement != null && !insertStatement.isClosed()) {
                insertStatement.close();
            }

            if (connection != null && !connection.isClosed()) {
                connection.close();
            }

            LogService.infof("[%s] ✅ PostgreSQLWriter closed", taskId);

        } catch (SQLException e) {
            LogService.errorf("[%s] Error closing PostgreSQLWriter: %s", taskId, e.getMessage());
            throw e;
        }
    }

    @Override
    public void testInsert() throws SQLException {
        TaxiTrip testTrip = new TaxiTrip();
        testTrip.setVendorId(1);
        testTrip.setPickupDatetime(LocalDateTime.now());
        testTrip.setDropoffDatetime(LocalDateTime.now());
        testTrip.setPassengerCount(1);
        testTrip.setTripDistance(1.5);
        testTrip.setRateCodeId(1);
        testTrip.setStoreAndFwdFlag("N");
        testTrip.setPuLocationId(100);
        testTrip.setDoLocationId(200);
        testTrip.setPaymentType(1);
        testTrip.setFareAmount(10.0);
        testTrip.setExtra(0.5);
        testTrip.setMtaTax(0.5);
        testTrip.setTipAmount(2.0);
        testTrip.setTollsAmount(0.0);
        testTrip.setImprovementSurcharge(0.3);
        testTrip.setTotalAmount(13.3);
        testTrip.setCongestionSurcharge(0.0);

        addTrip(testTrip);
        flush();

        LogService.infof("[%s] ✅ Test insert successful", taskId);
    }
}