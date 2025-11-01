package config;

import java.util.ArrayList;
import java.util.List;

/**
 * Конфигурация приложения для загрузки данных NYC Taxi в различные базы данных.
 * Version 4.0 - поддержка OceanBase, PostgreSQL, MS SQL Server
 */
public final class AppConfig {

    private AppConfig() {}

    // ===== Database Type Selection =====

    /**
     * Выбор базы данных для загрузки.
     * Измените это значение для переключения между базами данных.
     */
    public static final DatabaseType DATABASE_TYPE = DatabaseType.POSTGRESQL;

    // ===== OceanBase Connection =====

    /** OceanBase JDBC URL */
    public static final String OCEANBASE_URL = "jdbc:mysql://192.168.55.201:2883/testdb";

    /** OceanBase пользователь */
    public static final String OCEANBASE_USER = "root@app_tenant";

    /** OceanBase пароль */
    public static final String OCEANBASE_PASSWORD = "Ob500200";

    /** Параметры подключения для OceanBase оптимизации */
    public static final String OCEANBASE_URL_FULL = OCEANBASE_URL
            + "?rewriteBatchedStatements=true"
            + "&useServerPrepStmts=true"
            + "&cachePrepStmts=true"
            + "&prepStmtCacheSize=250"
            + "&prepStmtCacheSqlLimit=2048"
            + "&useCompression=true"
            + "&autoReconnect=true"
            + "&socketTimeout=60000"
            + "&connectTimeout=10000";

    // Для обратной совместимости
    public static final String JDBC_URL = OCEANBASE_URL;
    public static final String JDBC_USER = OCEANBASE_USER;
    public static final String JDBC_PASSWORD = OCEANBASE_PASSWORD;

    // ===== PostgreSQL Connection =====

    /** PostgreSQL JDBC URL */
    public static final String POSTGRESQL_URL = "jdbc:postgresql://192.168.55.211:5432/testdb";

    /** PostgreSQL пользователь */
    public static final String POSTGRESQL_USER = "testdbuser";

    /** PostgreSQL пароль */
    public static final String POSTGRESQL_PASSWORD = "qaz123";

    /** Параметры подключения для PostgreSQL оптимизации */
    public static final String POSTGRESQL_URL_FULL = POSTGRESQL_URL
            + "?reWriteBatchedInserts=true"          // Включить batch optimization
            + "&prepareThreshold=1"                   // Использовать prepared statements
            + "&preparedStatementCacheQueries=250"    // Кеш prepared statements
            + "&preparedStatementCacheSizeMiB=5"      // Размер кеша в MB
            + "&connectTimeout=10"                    // Connect timeout 10 сек
            + "&socketTimeout=60"                     // Socket timeout 60 сек
            + "&tcpKeepAlive=true";                   // Keep-alive для долгих операций

    // ===== MS SQL Server Connection =====

    /** MS SQL Server JDBC URL */
    public static final String MSSQL_URL = "jdbc:sqlserver://sqlupdate2019:1433;databaseName=testdb;encrypt=false";

    /** MS SQL Server пользователь */
    public static final String MSSQL_USER = "testdbuser";

    /** MS SQL Server пароль */
    public static final String MSSQL_PASSWORD = "qaz123";

    /** Параметры подключения для MSSQL оптимизации */
    public static final String MSSQL_URL_FULL = MSSQL_URL
         //   + ";useBulkCopyForBatchInsert=true"       // Использовать bulk copy
            + ";sendStringParametersAsUnicode=false"  // Производительность для ASCII
            + ";loginTimeout=10"                      // Login timeout 10 сек
            + ";socketTimeout=60000"                  // Socket timeout 60 сек
            + ";applicationName=TaxiLoader";          // Имя приложения

    // ===== Data Source =====

    /** Каталог с Parquet файлами */
    public static final String PARQUET_DIR = "E:/NYCTaxi/";

    /** Первый файл для загрузки (для теста) */
    public static final String FIRST_FILE = "yellow_tripdata_2019-02.parquet";

    /** Полный путь к первому файлу */
    public static final String FIRST_FILE_PATH = PARQUET_DIR + FIRST_FILE;

    // ===== Batch Settings =====

    /**
     * Размер batch для INSERT
     * Рекомендации:
     * - OceanBase: 1500-2000
     * - PostgreSQL: 5000-10000
     * - MS SQL Server: 1000-5000
     */
    public static final int BATCH_SIZE = 1000;

    /** Лимит записей для тестовой загрузки (0 = все) */
    public static final int TEST_LIMIT = 0;

    /** Интервал логирования прогресса (каждые N записей) */
    public static final int LOG_INTERVAL = 500_000;

    // ===== Table Settings =====

    /** Имя таблицы */
    public static final String TABLE_NAME = "taxi_trips1";

    /** База данных */
    public static final String DATABASE_NAME = "testdb";

    // ===== Performance Settings =====

    /**
     * Количество потоков для параллельной загрузки.
     * Установите 1 для однопоточного режима.
     */
    public static final int THREAD_COUNT = 8;


    // ===== Utility Methods =====

    /**
     * Возвращает JDBC URL для текущей базы данных с параметрами оптимизации.
     */
    public static String getJdbcUrl() {
        switch (DATABASE_TYPE) {
            case OCEANBASE:
                return OCEANBASE_URL_FULL;
            case POSTGRESQL:
                return POSTGRESQL_URL_FULL;
            case MSSQL:
                return MSSQL_URL_FULL;
            default:
                throw new IllegalStateException("Unknown database type: " + DATABASE_TYPE);
        }
    }

    /**
     * Возвращает JDBC URL для OceanBase.
     */
    public static String getOceanBaseUrl() {
        return OCEANBASE_URL_FULL;
    }

    /**
     * Возвращает JDBC URL для PostgreSQL.
     */
    public static String getPostgreSQLUrl() {
        return POSTGRESQL_URL_FULL;
    }

    /**
     * Возвращает JDBC URL для MS SQL Server.
     */
    public static String getMSSQLUrl() {
        return MSSQL_URL_FULL;
    }

    /**
     * Возвращает полный путь к Parquet файлу.
     */
    public static String getParquetFilePath(String filename) {
        return PARQUET_DIR + filename;
    }

    /**
     * Сканирует каталог PARQUET_DIR и все подкаталоги,
     * возвращает список всех *.parquet файлов, отсортированный по полному пути.
     */
    public static List<String> getAllParquetFiles() {
        java.io.File rootDir = new java.io.File(PARQUET_DIR);

        if (!rootDir.exists()) {
            System.err.println("ERROR: Parquet directory does not exist: " + PARQUET_DIR);
            return new ArrayList<>();
        }

        if (!rootDir.isDirectory()) {
            System.err.println("ERROR: Parquet path is not a directory: " + PARQUET_DIR);
            return new ArrayList<>();
        }

        List<String> parquetFiles = new ArrayList<>();
        scanDirectoryRecursively(rootDir, parquetFiles);

        parquetFiles.sort(String::compareTo);

        System.out.println("INFO: Found " + parquetFiles.size() + " .parquet files in " + PARQUET_DIR);

        return parquetFiles;
    }

    /**
     * Рекурсивно сканирует каталог и добавляет все .parquet файлы в список.
     */
    private static void scanDirectoryRecursively(java.io.File directory, List<String> fileList) {
        java.io.File[] files = directory.listFiles();

        if (files == null) {
            return;
        }

        for (java.io.File file : files) {
            if (file.isDirectory()) {
                scanDirectoryRecursively(file, fileList);
            } else if (file.isFile() && file.getName().toLowerCase().endsWith(".parquet")) {
                fileList.add(file.getAbsolutePath());
            }
        }
    }

    /**
     * Возвращает список относительных имен файлов из полного пути.
     */
    public static String getRelativeFileName(String fullPath) {
        if (fullPath.startsWith(PARQUET_DIR)) {
            return fullPath.substring(PARQUET_DIR.length());
        }
        return new java.io.File(fullPath).getName();
    }

    /**
     * Выводит текущую конфигурацию в консоль.
     */
    public static void printConfig() {
        System.out.println("=== Application Configuration ===");
        System.out.println("Database Type:   " + DATABASE_TYPE.getDisplayName());

        switch (DATABASE_TYPE) {
            case OCEANBASE:
                System.out.println("JDBC URL:        " + OCEANBASE_URL);
                System.out.println("JDBC User:       " + OCEANBASE_USER);
                break;
            case POSTGRESQL:
                System.out.println("JDBC URL:        " + POSTGRESQL_URL);
                System.out.println("JDBC User:       " + POSTGRESQL_USER);
                break;
            case MSSQL:
                System.out.println("JDBC URL:        " + MSSQL_URL);
                System.out.println("JDBC User:       " + MSSQL_USER);
                break;
        }

        System.out.println("Database:        " + DATABASE_NAME);
        System.out.println("Table:           " + TABLE_NAME);
        System.out.println("Parquet Dir:     " + PARQUET_DIR);
        System.out.println("First File:      " + FIRST_FILE);
        System.out.println("Batch Size:      " + BATCH_SIZE);
        System.out.println("Test Limit:      " + (TEST_LIMIT == 0 ? "ALL" : TEST_LIMIT));
        System.out.println("Thread Count:    " + THREAD_COUNT + (THREAD_COUNT > 1 ? " (MULTITHREADED)" : " (single-threaded)"));
        System.out.println("=================================");
    }
}