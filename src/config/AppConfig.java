package config;

import java.util.ArrayList;
import java.util.List;

/**
 * Конфигурация приложения (переведена на XML).
 * Сохраняем обратную совместимость: статические поля и методы остаются,
 * но теперь заполняются из AppConfigReader.read(...). См. репозиторий исходной версии. :contentReference[oaicite:0]{index=0}
 */
public final class AppConfig {

    // Активная (instance) конфигурация
    public static AppConfig ACTIVE;

    // ===== Database Type Selection =====
    public DatabaseType DATABASE_TYPE = DatabaseType.MSSQL;

    // ===== OceanBase Connection =====
    public String OCEANBASE_URL = "jdbc:mysql://192.168.55.201:2883/testdb";
    public String OCEANBASE_USER = "root@app_tenant";
    public String OCEANBASE_PASSWORD = "Ob500200";
    public String OCEANBASE_URL_FULL = null; // генерится

    // Для обратной совместимости (старые места, где дергают JDBC_* напрямую)
    public String JDBC_URL = null;
    public String JDBC_USER = null;
    public String JDBC_PASSWORD = null;

    // ===== PostgreSQL Connection =====
    public String POSTGRESQL_URL = "jdbc:postgresql://192.168.55.211:5432/testdb";
    public String POSTGRESQL_USER = "testdbuser";
    public String POSTGRESQL_PASSWORD = "qaz123";
    public String POSTGRESQL_URL_FULL = null; // генерится

    // ===== MS SQL Server Connection =====
    public String MSSQL_URL = "jdbc:sqlserver://sqlupdate2019:1433;databaseName=testdb;encrypt=false";
    public String MSSQL_USER = "testdbuser";
    public String MSSQL_PASSWORD = "qaz123";
    public String MSSQL_URL_FULL = null; // генерится

    // ===== MongoDB Connection =====
    public boolean MONGO_USE_SSL = false;
    public String  MONGO_USERNAME = "mongoAdmin";
    public String  MONGO_PASSWORD = "qaz123";
    public String  MONGO_AUTH_SOURCE = "admin";
    public List<String> MONGO_HOSTS = java.util.Arrays.asList(
            "192.168.55.30:27017",
            "192.168.55.40:27017",
            "192.168.0.60:27017"
    );
    public String MONGO_DATABASE = "testdb";
    public String MONGO_COLLECTION = "taxi_trips";

    // ===== Data Source =====
    public String PARQUET_DIR = "E:/NYCTaxi/";
    public String FIRST_FILE = "yellow_tripdata_2019-02.parquet";

    // ===== Batch Settings =====
    public int BATCH_SIZE = 10_000;
    public int TEST_LIMIT = 0;
    public int LOG_INTERVAL = 500_000;

    // ===== Table Settings =====
    public String TABLE_NAME = "taxi_trips1";
    public String DATABASE_NAME = "testdb";

    // ===== Performance Settings =====
    public int THREAD_COUNT = 8;

    // === Генерация производных строк + заливка в статические поля для обратной совместимости ===
    public void applyToStatics() {
        // OceanBase
        this.OCEANBASE_URL_FULL =
                OCEANBASE_URL
                        + "?rewriteBatchedStatements=true"
                        + "&useServerPrepStmts=true"
                        + "&cachePrepStmts=true"
                        + "&prepStmtCacheSize=250"
                        + "&prepStmtCacheSqlLimit=2048"
                        + "&useCompression=true"
                        + "&autoReconnect=true"
                        + "&socketTimeout=60000"
                        + "&connectTimeout=10000";

        // PostgreSQL
        this.POSTGRESQL_URL_FULL =
                POSTGRESQL_URL
                        + "?reWriteBatchedInserts=true"
                        + "&prepareThreshold=1"
                        + "&preparedStatementCacheQueries=250"
                        + "&preparedStatementCacheSizeMiB=5"
                        + "&connectTimeout=10"
                        + "&socketTimeout=60"
                        + "&tcpKeepAlive=true";

        // MSSQL
        this.MSSQL_URL_FULL =
                MSSQL_URL
                        + ";useBulkCopyForBatchInsert=true"
                        + ";sendStringParametersAsUnicode=false"
                        + ";loginTimeout=10"
                        + ";socketTimeout=60000"
                        + ";applicationName=TaxiLoader";

        // JDBC_* (совместимость как в старой версии)
        this.JDBC_URL = this.OCEANBASE_URL;
        this.JDBC_USER = this.OCEANBASE_USER;
        this.JDBC_PASSWORD = this.OCEANBASE_PASSWORD;

        // --- скопировать ВСЁ в статические, чтобы не перепиливать остальной код прямо сейчас ---
        DATABASE_TYPE_static = this.DATABASE_TYPE;
        OCEANBASE_URL_static = this.OCEANBASE_URL;
        OCEANBASE_USER_static = this.OCEANBASE_USER;
        OCEANBASE_PASSWORD_static = this.OCEANBASE_PASSWORD;
        OCEANBASE_URL_FULL_static = this.OCEANBASE_URL_FULL;

        JDBC_URL_static = this.JDBC_URL;
        JDBC_USER_static = this.JDBC_USER;
        JDBC_PASSWORD_static = this.JDBC_PASSWORD;

        POSTGRESQL_URL_static = this.POSTGRESQL_URL;
        POSTGRESQL_USER_static = this.POSTGRESQL_USER;
        POSTGRESQL_PASSWORD_static = this.POSTGRESQL_PASSWORD;
        POSTGRESQL_URL_FULL_static = this.POSTGRESQL_URL_FULL;

        MSSQL_URL_static = this.MSSQL_URL;
        MSSQL_USER_static = this.MSSQL_USER;
        MSSQL_PASSWORD_static = this.MSSQL_PASSWORD;
        MSSQL_URL_FULL_static = this.MSSQL_URL_FULL;

        MONGO_USE_SSL_static = this.MONGO_USE_SSL;
        MONGO_USERNAME_static = this.MONGO_USERNAME;
        MONGO_PASSWORD_static = this.MONGO_PASSWORD;
        MONGO_AUTH_SOURCE_static = this.MONGO_AUTH_SOURCE;
        MONGO_HOSTS_static = new ArrayList<>(this.MONGO_HOSTS);
        MONGO_DATABASE_static = this.MONGO_DATABASE;
        MONGO_COLLECTION_static = this.MONGO_COLLECTION;

        PARQUET_DIR_static = this.PARQUET_DIR;
        FIRST_FILE_static = this.FIRST_FILE;

        BATCH_SIZE_static = this.BATCH_SIZE;
        TEST_LIMIT_static = this.TEST_LIMIT;
        LOG_INTERVAL_static = this.LOG_INTERVAL;

        TABLE_NAME_static = this.TABLE_NAME;
        DATABASE_NAME_static = this.DATABASE_NAME;

        THREAD_COUNT_static = this.THREAD_COUNT;
    }

    // ======= СТАТИЧЕСКИЕ ПОЛЯ/МЕТОДЫ (обратная совместимость с текущим кодом) =======
    public static DatabaseType DATABASE_TYPE_static;
    public static String OCEANBASE_URL_static, OCEANBASE_USER_static, OCEANBASE_PASSWORD_static, OCEANBASE_URL_FULL_static;
    public static String JDBC_URL_static, JDBC_USER_static, JDBC_PASSWORD_static;
    public static String POSTGRESQL_URL_static, POSTGRESQL_USER_static, POSTGRESQL_PASSWORD_static, POSTGRESQL_URL_FULL_static;
    public static String MSSQL_URL_static, MSSQL_USER_static, MSSQL_PASSWORD_static, MSSQL_URL_FULL_static;
    public static boolean MONGO_USE_SSL_static;
    public static String  MONGO_USERNAME_static, MONGO_PASSWORD_static, MONGO_AUTH_SOURCE_static, MONGO_DATABASE_static, MONGO_COLLECTION_static;
    public static List<String> MONGO_HOSTS_static;
    public static String PARQUET_DIR_static, FIRST_FILE_static;
    public static int BATCH_SIZE_static, TEST_LIMIT_static, LOG_INTERVAL_static, THREAD_COUNT_static;
    public static String TABLE_NAME_static, DATABASE_NAME_static;

    // Инициализация дефолтами (как в исходной версии), чтобы работало даже без XML
    static {
        try {
            new AppConfig().applyToStatics();
        } catch (Exception ignored) {}
    }

    // ---- Статические методы, которые использует Main/другие классы без переписывания ----

    public static String getJdbcUrl() {
        switch (DATABASE_TYPE_static) {
            case OCEANBASE:  return OCEANBASE_URL_FULL_static;
            case POSTGRESQL: return POSTGRESQL_URL_FULL_static;
            case MSSQL:      return MSSQL_URL_FULL_static;
            default: throw new IllegalStateException("Unknown database type: " + DATABASE_TYPE_static);
        }
    }

    public static String getOceanBaseUrl() { return OCEANBASE_URL_FULL_static; }
    public static String getPostgreSQLUrl(){ return POSTGRESQL_URL_FULL_static; }
    public static String getMSSQLUrl()     { return MSSQL_URL_FULL_static; }

    public static String getRelativeFileName(String fullPath) {
        if (PARQUET_DIR_static != null && fullPath.startsWith(PARQUET_DIR_static)) {
            return fullPath.substring(PARQUET_DIR_static.length());
        }
        return new java.io.File(fullPath).getName();
    }

    public static List<String> getAllParquetFiles() {
        java.io.File rootDir = new java.io.File(PARQUET_DIR_static);
        if (!rootDir.exists()) {
            System.err.println("ERROR: Parquet directory does not exist: " + PARQUET_DIR_static);
            return new ArrayList<>();
        }
        if (!rootDir.isDirectory()) {
            System.err.println("ERROR: Parquet path is not a directory: " + PARQUET_DIR_static);
            return new ArrayList<>();
        }
        List<String> parquetFiles = new ArrayList<>();
        scanDirectoryRecursively(rootDir, parquetFiles);
        parquetFiles.sort(String::compareTo);
        System.out.println("INFO: Found " + parquetFiles.size() + " .parquet files in " + PARQUET_DIR_static);
        return parquetFiles;
    }

    private static void scanDirectoryRecursively(java.io.File directory, List<String> fileList) {
        java.io.File[] files = directory.listFiles();
        if (files == null) return;
        for (java.io.File file : files) {
            if (file.isDirectory()) {
                scanDirectoryRecursively(file, fileList);
            } else if (file.isFile() && file.getName().toLowerCase().endsWith(".parquet")) {
                fileList.add(file.getAbsolutePath());
            }
        }
    }

    public static void printConfig() {
        System.out.println("=== Application Configuration ===");
        System.out.println("Database Type: " + DATABASE_TYPE_static.getDisplayName());
        switch (DATABASE_TYPE_static) {
            case OCEANBASE:
                System.out.println("JDBC URL: " + OCEANBASE_URL_static);
                System.out.println("JDBC User: " + OCEANBASE_USER_static);
                break;
            case POSTGRESQL:
                System.out.println("JDBC URL: " + POSTGRESQL_URL_static);
                System.out.println("JDBC User: " + POSTGRESQL_USER_static);
                break;
            case MSSQL:
                System.out.println("JDBC URL: " + MSSQL_URL_static);
                System.out.println("JDBC User: " + MSSQL_USER_static);
                break;
            case MONGODB:
                System.out.println("Hosts: " + String.join(", ", MONGO_HOSTS_static));
                System.out.println("User: " + MONGO_USERNAME_static);
                System.out.println("Database: " + MONGO_DATABASE_static);
                System.out.println("Collection: " + MONGO_COLLECTION_static);
                System.out.println("SSL: " + (MONGO_USE_SSL_static ? "enabled" : "disabled"));
                break;
        }
        System.out.println("Database: " + DATABASE_NAME_static);
        System.out.println("Table: " + TABLE_NAME_static);
        System.out.println("Parquet Dir: " + PARQUET_DIR_static);
        System.out.println("First File: " + FIRST_FILE_static);
        System.out.println("Batch Size: " + BATCH_SIZE_static);
        System.out.println("Test Limit: " + (TEST_LIMIT_static == 0 ? "ALL" : TEST_LIMIT_static));
        System.out.println("Thread Count: " + THREAD_COUNT_static + (THREAD_COUNT_static > 1 ? " (MULTITHREADED)" : " (single-threaded)"));
        System.out.println("=================================");
    }
}
