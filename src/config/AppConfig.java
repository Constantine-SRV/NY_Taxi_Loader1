package config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Application configuration loaded from XML.
 * Fully static version — backward compatible with existing code.
 */
public final class AppConfig {

    private AppConfig() {
        // static-only class
    }

    // === Database type ===
    public static DatabaseType DATABASE_TYPE;

    // === OceanBase ===
    public static String OCEANBASE_URL;
    public static String OCEANBASE_USER;
    public static String OCEANBASE_PASSWORD;

    // === PostgreSQL ===
    public static String POSTGRESQL_URL;
    public static String POSTGRESQL_USER;
    public static String POSTGRESQL_PASSWORD;

    // === MSSQL ===
    public static String MSSQL_URL;
    public static String MSSQL_USER;
    public static String MSSQL_PASSWORD;

    // === JDBC compatibility (used in OceanBaseWriter) ===
    public static String JDBC_URL;
    public static String JDBC_USER;
    public static String JDBC_PASSWORD;

    // === MongoDB ===
    public static boolean MONGO_USE_SSL;
    public static String  MONGO_USERNAME;
    public static String  MONGO_PASSWORD;
    public static String  MONGO_AUTH_SOURCE;
    public static List<String> MONGO_HOSTS = new ArrayList<>();
    public static String  MONGO_DATABASE;
    public static String  MONGO_COLLECTION;

    // === Data source ===
    public static String PARQUET_DIR;
    public static String FIRST_FILE;

    // === Batch / limits / logs ===
    public static int BATCH_SIZE;
    public static int TEST_LIMIT;
    public static int LOG_INTERVAL;

    // === Table / DB name ===
    public static String TABLE_NAME;
    public static String DATABASE_NAME;

    // === Performance ===
    public static int THREAD_COUNT;

    // ---------------------------------------------------------------------
    // === Utility methods ===
    // ---------------------------------------------------------------------

    public static List<String> getMongoHostsList() {
        return MONGO_HOSTS;
    }

    public static List<String> getAllParquetFiles() {
        if (PARQUET_DIR == null) {
            System.err.println("ERROR: PARQUET_DIR not set");
            return new ArrayList<>();
        }
        File rootDir = new File(PARQUET_DIR);
        if (!rootDir.exists()) {
            System.err.println("ERROR: Parquet directory does not exist: " + PARQUET_DIR);
            return new ArrayList<>();
        }
        List<String> parquetFiles = new ArrayList<>();
        scanRecursively(rootDir, parquetFiles);
        parquetFiles.sort(String::compareTo);
        System.out.println("INFO: Found " + parquetFiles.size() + " .parquet files in " + PARQUET_DIR);
        return parquetFiles;
    }

    private static void scanRecursively(File dir, List<String> list) {
        File[] files = dir.listFiles();
        if (files == null) return;
        for (File file : files) {
            if (file.isDirectory()) {
                scanRecursively(file, list);
            } else if (file.getName().toLowerCase().endsWith(".parquet")) {
                list.add(file.getAbsolutePath());
            }
        }
    }

    public static String getRelativeFileName(String fullPath) {
        if (PARQUET_DIR != null && fullPath.startsWith(PARQUET_DIR)) {
            return fullPath.substring(PARQUET_DIR.length());
        }
        return new File(fullPath).getName();
    }

    // === Compatibility getters (used by old writer classes) ===
    public static String getPostgreSQLUrl() {
        return POSTGRESQL_URL;
    }

    public static String getMSSQLUrl() {
        return MSSQL_URL;
    }

    public static String getOceanBaseUrl() {
        return OCEANBASE_URL;
    }

    public static String getJdbcUrl() {
        if (DATABASE_TYPE == null) return null;
        switch (DATABASE_TYPE) {
            case POSTGRESQL: return POSTGRESQL_URL;
            case MSSQL:      return MSSQL_URL;
            case OCEANBASE:  return OCEANBASE_URL;
            default:         return null;
        }
    }

    public static void printConfig() {
        System.out.println("=== Application Configuration ===");
        System.out.println("Database Type: " + (DATABASE_TYPE != null ? DATABASE_TYPE.getDisplayName() : "null"));
        switch (DATABASE_TYPE) {
            case OCEANBASE:
                System.out.println("JDBC URL: " + OCEANBASE_URL);
                System.out.println("User: " + OCEANBASE_USER);
                break;
            case POSTGRESQL:
                System.out.println("JDBC URL: " + POSTGRESQL_URL);
                System.out.println("User: " + POSTGRESQL_USER);
                break;
            case MSSQL:
                System.out.println("JDBC URL: " + MSSQL_URL);
                System.out.println("User: " + MSSQL_USER);
                break;
            case MONGODB:
                System.out.println("Hosts: " + String.join(", ", MONGO_HOSTS));
                System.out.println("User: " + MONGO_USERNAME);
                System.out.println("Database: " + MONGO_DATABASE);
                System.out.println("Collection: " + MONGO_COLLECTION);
                System.out.println("SSL: " + (MONGO_USE_SSL ? "enabled" : "disabled"));
                break;
            default:
                System.out.println("No database type selected.");
        }

        System.out.println("Database Name: " + DATABASE_NAME);
        System.out.println("Table Name: " + TABLE_NAME);
        System.out.println("Parquet Dir: " + PARQUET_DIR);
        System.out.println("First File: " + FIRST_FILE);
        System.out.println("Batch Size: " + BATCH_SIZE);
        System.out.println("Test Limit: " + TEST_LIMIT);
        System.out.println("Log Interval: " + LOG_INTERVAL);
        System.out.println("Thread Count: " + THREAD_COUNT);
        System.out.println("=================================");
    }
}
