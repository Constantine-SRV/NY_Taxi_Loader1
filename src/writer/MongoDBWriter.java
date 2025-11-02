package writer;

import Logging.LogService;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import config.AppConfig;
import model.TaxiTrip;
import org.bson.Document;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Реализация DatabaseWriter для MongoDB.
 * Поддерживает batch-вставку документов в коллекцию.
 */
public class MongoDBWriter implements DatabaseWriter {

    private final String taskId;
    private final MongoClient mongoClient;
    private final MongoCollection<Document> collection;

    // batch buffer
    private final List<Document> batch = new ArrayList<>();
    private long totalInserted = 0;
    private long startTimeMs = System.currentTimeMillis();

    public MongoDBWriter(String taskId) throws SQLException {
        this.taskId = taskId;

        try {
            java.util.logging.Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);

            // Список серверов
            List<ServerAddress> servers = new ArrayList<>(AppConfig.MONGO_HOSTS.size());
            for (String h : AppConfig.MONGO_HOSTS) {
                String[] p = h.split(":");
                String host = p[0];
                int port = (p.length > 1) ? Integer.parseInt(p[1]) : 27017;
                servers.add(new ServerAddress(host, port));
            }

            // Аутентификация
            MongoCredential credential = MongoCredential.createCredential(
                    AppConfig.MONGO_USERNAME,
                    AppConfig.MONGO_AUTH_SOURCE,
                    AppConfig.MONGO_PASSWORD.toCharArray()
            );

            // Настройки клиента
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyToClusterSettings(b -> b.hosts(servers))
                    .credential(credential)
                    .applyToSslSettings(b -> b.enabled(AppConfig.MONGO_USE_SSL))
                    .build();

            this.mongoClient = MongoClients.create(settings);
            MongoDatabase db = mongoClient.getDatabase(AppConfig.MONGO_DATABASE);
            this.collection = db.getCollection(AppConfig.MONGO_COLLECTION);

            LogService.infof("[%s] MongoDB connected: %s / %s",
                    taskId, AppConfig.MONGO_DATABASE, AppConfig.MONGO_COLLECTION);
        } catch (Exception e) {
            throw new SQLException("MongoDB connection failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void addTrip(TaxiTrip trip) throws SQLException {
        if (trip == null) return;
        Document doc = new Document()
                .append("pickup_datetime", trip.getPickupDatetime())
                .append("dropoff_datetime", trip.getDropoffDatetime())
                .append("passenger_count", trip.getPassengerCount())
                .append("trip_distance", trip.getTripDistance())
                .append("pu_location_id", trip.getPuLocationId())
                .append("do_location_id", trip.getDoLocationId())
                .append("fare_amount", trip.getFareAmount())
                .append("total_amount", trip.getTotalAmount());
        batch.add(doc);

        if (batch.size() >= AppConfig.BATCH_SIZE) {
            executeBatch();
        }
    }

    @Override
    public void addTrips(List<TaxiTrip> trips) throws SQLException {
        if (trips == null || trips.isEmpty()) return;
        for (TaxiTrip t : trips) addTrip(t);
    }

    @Override
    public void executeBatch() throws SQLException {
        if (batch.isEmpty()) return;
        try {
            collection.insertMany(new ArrayList<>(batch));
            totalInserted += batch.size();
            batch.clear();

            LogService.infof("[%s] Inserted batch (%d docs), total: %,d",
                    taskId, AppConfig.BATCH_SIZE, totalInserted);
        } catch (Exception e) {
            throw new SQLException("MongoDB batch insert failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void flush() throws SQLException {
        executeBatch();
        long elapsed = System.currentTimeMillis() - startTimeMs;
        LogService.infof("[%s] MongoDB flush complete: %,d docs in %d ms (%.2f docs/s)",
                taskId, totalInserted, elapsed,
                (totalInserted * 1000.0) / Math.max(1, elapsed));
    }

    @Override
    public long getTotalInserted() {
        return totalInserted;
    }

    @Override
    public String getPerformanceStats() {
        long elapsed = System.currentTimeMillis() - startTimeMs;
        double rate = (totalInserted * 1000.0) / Math.max(1, elapsed);
        return String.format("MongoDBWriter: %,d docs in %d ms (%.2f docs/s)",
                totalInserted, elapsed, rate);
    }

    @Override
    public String getTaskId() {
        return taskId;
    }

    @Override
    public void testInsert() throws SQLException {
        try {
            Document testDoc = new Document("test_field", "connection_test")
                    .append("timestamp", System.currentTimeMillis());
            collection.insertOne(testDoc);
            LogService.infof("[%s] MongoDB test insert OK", taskId);
        } catch (Exception e) {
            LogService.errorf("[%s] MongoDB test insert FAILED: %s", taskId, e.getMessage());
            throw new SQLException("MongoDB test insert failed", e);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            flush();
            mongoClient.close();
            LogService.infof("[%s] MongoDB connection closed", taskId);
        } catch (Exception e) {
            throw new SQLException("MongoDB close error: " + e.getMessage(), e);
        }
    }
}
