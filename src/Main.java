import config.AppConfig;
import Logging.LogService;
import model.TaxiTrip;
import reader.ParquetTaxiReader;
import writer.DatabaseWriter;
import writer.DatabaseWriterFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Точка входа приложения для загрузки NYC Taxi данных из Parquet в различные БД.
 * Поддерживает: OceanBase, PostgreSQL, MS SQL Server
 * Version 4.0 - универсальная загрузка с многопоточной вставкой
 */
public class Main {

    // Отключить избыточные логи Hadoop и Parquet
    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "ERROR");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.hadoop", "ERROR");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.parquet", "ERROR");
        System.setProperty("org.apache.commons.logging.Log",
                "org.apache.commons.logging.impl.NoOpLog");
    }

    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        LogService.info("=== NYC Taxi Parquet to Database Loader ===");
        LogService.infof("Version: 4.0 - Universal loader (OceanBase/PostgreSQL/MS SQL Server)");
        LogService.infof("Target Database: %s", DatabaseWriterFactory.getCurrentDatabaseName());
        LogService.info("");

        // Вывести конфигурацию
        AppConfig.printConfig();
        LogService.info("");



        try {
            // Проверить подключение к БД
            testConnection();

            // Загрузить все файлы из каталога
            loadAllFiles();

            LogService.info("");
            LogService.info("✅ All done successfully!");

        } catch (Exception e) {
            LogService.errorf("❌ Fatal error: %s", e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Загрузить все Parquet файлы из каталога.
     */
    private static void loadAllFiles() {
        LogService.info("=== Scanning for Parquet files ===");

        // Автоматически найти все .parquet файлы в каталоге и подкаталогах
        List<String> filesPaths = AppConfig.getAllParquetFiles();

        if (filesPaths.isEmpty()) {
            LogService.error("❌ No .parquet files found in directory: " + AppConfig.PARQUET_DIR);
            return;
        }

        LogService.infof("Found %d .parquet files to process", filesPaths.size());
        LogService.info("");

        // Вывести список найденных файлов для проверки
        LogService.info("Files to be processed:");
        for (int i = 0; i < filesPaths.size(); i++) {
            String relativeName = AppConfig.getRelativeFileName(filesPaths.get(i));
            LogService.infof("  %2d. %s", i + 1, relativeName);
        }
        LogService.info("");

        long totalRecords = 0;
        long totalStartTime = System.currentTimeMillis();

        // Загрузить каждый файл
        for (int i = 0; i < filesPaths.size(); i++) {
            String filePath = filesPaths.get(i);
            String relativeName = AppConfig.getRelativeFileName(filePath);

            LogService.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            LogService.infof("Processing file %d/%d: %s", i + 1, filesPaths.size(), relativeName);
            LogService.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

            try {
                long recordsLoaded = loadFile(filePath, AppConfig.TEST_LIMIT);
                totalRecords += recordsLoaded;

                LogService.infof("✅ File %d/%d completed: %,d records",
                        i + 1, filesPaths.size(), recordsLoaded);
                LogService.info("");

            } catch (Exception e) {
                LogService.errorf("❌ Error loading file %s: %s", relativeName, e.getMessage());
                e.printStackTrace();
                // Продолжить со следующим файлом
            }
        }

        // Итоговая статистика
        long totalElapsed = System.currentTimeMillis() - totalStartTime;
        double totalRate = (totalRecords * 1000.0) / totalElapsed;

        LogService.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        LogService.info("=== FINAL STATISTICS ===");
        LogService.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        LogService.infof("Files processed:  %d", filesPaths.size());
        LogService.infof("Total records:    %,d", totalRecords);
        LogService.infof("Total time:       %.1f minutes", totalElapsed / 60000.0);
        LogService.infof("Average rate:     %.0f records/sec", totalRate);
        LogService.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    /**
     * Проверить подключение к базе данных.
     */
    private static void testConnection() {
        LogService.infof("=== Testing %s Connection ===", DatabaseWriterFactory.getCurrentDatabaseName());

        try (DatabaseWriter writer = DatabaseWriterFactory.createWriter("Test")) {
            LogService.info("Connection test successful!");

        } catch (Exception e) {
            LogService.errorf("Connection test failed: %s", e.getMessage());
            throw new RuntimeException("Cannot connect to database", e);
        }

        LogService.info("");
    }

    /**
     * Загрузить данные из Parquet файла в БД с многопоточной вставкой.
     *
     * @param filePath путь к Parquet файлу
     * @param limit максимальное количество записей (0 = все)
     * @return количество загруженных записей
     */
    private static long loadFile(String filePath, int limit) {
        LogService.info("=== Loading Data from Parquet ===");
        LogService.infof("File: %s", filePath);
        LogService.infof("Limit: %s", limit == 0 ? "ALL" : String.format("%,d", limit));
        LogService.infof("Threads: %d", AppConfig.THREAD_COUNT);
        LogService.info("");

        long overallStart = System.currentTimeMillis();

        try {
            // Читаем Parquet файл
            LogService.info("Step 1: Reading Parquet file...");
            ParquetTaxiReader reader = new ParquetTaxiReader();
            List<TaxiTrip> trips = reader.readFile(filePath, limit);

            if (trips.isEmpty()) {
                LogService.info("No data to load");
                return 0;
            }

            LogService.infof("✅ Read %,d trips from Parquet", trips.size());
            LogService.info("");

            // Записываем в БД многопоточно
            LogService.infof("Step 2: Writing to %s (multithreaded)...",
                    DatabaseWriterFactory.getCurrentDatabaseName());

            if (AppConfig.THREAD_COUNT <= 1) {
                // Однопоточный режим
                insertSingleThreaded(trips);
            } else {
                // Многопоточный режим
                insertMultiThreaded(trips);
            }

            // Общая статистика
            long overallElapsed = System.currentTimeMillis() - overallStart;
            double overallRate = (trips.size() * 1000.0) / overallElapsed;

            LogService.info("");
            LogService.info("=== Overall Statistics ===");
            LogService.infof("Total records:    %,d", trips.size());
            LogService.infof("Total time:       %.1f seconds", overallElapsed / 1000.0);
            LogService.infof("Overall rate:     %.0f records/sec", overallRate);

            // Простая статистика по данным
            printDataStatistics(trips);
            return trips.size();

        } catch (Exception e) {
            LogService.errorf("Error loading file: %s", e.getMessage());
            throw new RuntimeException("Failed to load data", e);
        }
    }

    /**
     * Однопоточная вставка (когда THREAD_COUNT = 1).
     */
    private static void insertSingleThreaded(List<TaxiTrip> trips) throws Exception {
        try (DatabaseWriter writer = DatabaseWriterFactory.createWriter("Main")) {
            writer.addTrips(trips);
            writer.flush();

            LogService.info("");
            LogService.infof("✅ Inserted %,d records into %s",
                    writer.getTotalInserted(), DatabaseWriterFactory.getCurrentDatabaseName());
            LogService.infof("   %s", writer.getPerformanceStats());
        }
    }

    /**
     * Многопоточная вставка данных в БД.
     */
    private static void insertMultiThreaded(List<TaxiTrip> trips) throws Exception {
        int threadCount = AppConfig.THREAD_COUNT;
        int totalRecords = trips.size();
        int chunkSize = (totalRecords + threadCount - 1) / threadCount; // Округление вверх

        LogService.infof("Splitting %,d records into %d chunks of ~%,d records each",
                totalRecords, threadCount, chunkSize);
        LogService.info("");

        // Создать пул потоков
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<InsertResult>> futures = new ArrayList<>();

        // Разбить данные на chunks и запустить задачи
        for (int i = 0; i < threadCount; i++) {
            int startIdx = i * chunkSize;
            int endIdx = Math.min(startIdx + chunkSize, totalRecords);

            if (startIdx >= totalRecords) {
                break; // Больше нечего обрабатывать
            }

            List<TaxiTrip> chunk = trips.subList(startIdx, endIdx);
            String taskId = String.format("Task-%d", i + 1);

            LogService.infof("[%s] Starting: %,d records (indices %,d to %,d)",
                    taskId, chunk.size(), startIdx, endIdx - 1);

            InsertTask task = new InsertTask(taskId, chunk, startIdx, endIdx - 1);
            Future<InsertResult> future = executor.submit(task);
            futures.add(future);
        }

        LogService.info("");
        LogService.info("All tasks submitted, waiting for completion...");
        LogService.info("");

        // Собрать результаты
        long totalInserted = 0;
        long totalTime = 0;
        List<InsertResult> results = new ArrayList<>();

        for (Future<InsertResult> future : futures) {
            try {
                InsertResult result = future.get(); // Ждем завершения задачи
                results.add(result);
                totalInserted += result.recordsInserted;
                totalTime = Math.max(totalTime, result.elapsedMs);

                LogService.infof("[%s] ✅ Completed: %,d records in %.1f sec (%.0f rec/sec)",
                        result.taskId, result.recordsInserted,
                        result.elapsedMs / 1000.0, result.rate);

            } catch (ExecutionException e) {
                LogService.errorf("Task failed: %s", e.getCause().getMessage());
                throw new RuntimeException("Insert task failed", e.getCause());
            }
        }

        // Завершить пул потоков
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Итоговая статистика по потокам
        LogService.info("");
        LogService.info("=== Thread Performance Summary ===");
        for (InsertResult result : results) {
            LogService.infof("  %s: %,d records | %.1f sec | %.0f rec/sec",
                    result.taskId, result.recordsInserted,
                    result.elapsedMs / 1000.0, result.rate);
        }

        double avgRate = results.stream().mapToDouble(r -> r.rate).average().orElse(0);
        double overallRate = (totalInserted * 1000.0) / totalTime;

        LogService.info("");
        LogService.infof("✅ Total inserted: %,d records", totalInserted);
        LogService.infof("   Max thread time: %.1f sec", totalTime / 1000.0);
        LogService.infof("   Average thread rate: %.0f rec/sec", avgRate);
        LogService.infof("   Overall throughput: %.0f rec/sec", overallRate);
    }

    /**
     * Результат выполнения задачи вставки.
     */
    private static class InsertResult {
        final String taskId;
        final long recordsInserted;
        final long elapsedMs;
        final double rate;

        InsertResult(String taskId, long recordsInserted, long elapsedMs) {
            this.taskId = taskId;
            this.recordsInserted = recordsInserted;
            this.elapsedMs = elapsedMs;
            this.rate = (recordsInserted * 1000.0) / elapsedMs;
        }
    }

    /**
     * Задача для вставки данных в отдельном потоке.
     */
    private static class InsertTask implements Callable<InsertResult> {
        private final String taskId;
        private final List<TaxiTrip> trips;
        private final int startIdx;
        private final int endIdx;

        InsertTask(String taskId, List<TaxiTrip> trips, int startIdx, int endIdx) {
            this.taskId = taskId;
            this.trips = trips;
            this.startIdx = startIdx;
            this.endIdx = endIdx;
        }

        @Override
        public InsertResult call() throws Exception {
            long taskStart = System.currentTimeMillis();

            LogService.infof("[%s] Thread started at %s",
                    taskId, java.time.LocalDateTime.now().format(
                            java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")));

            try (DatabaseWriter writer = DatabaseWriterFactory.createWriter(taskId)) {
                // Вставить данные
                writer.addTrips(trips);
                writer.flush();

                long elapsed = System.currentTimeMillis() - taskStart;

                LogService.infof("[%s] Thread finished at %s",
                        taskId, java.time.LocalDateTime.now().format(
                                java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")));

                return new InsertResult(taskId, writer.getTotalInserted(), elapsed);

            } catch (Exception e) {
                LogService.errorf("[%s] Thread failed: %s", taskId, e.getMessage());
                throw e;
            }
        }
    }

    /**
     * Вывести простую статистику по загруженным данным.
     */
    private static void printDataStatistics(List<TaxiTrip> trips) {
        if (trips.isEmpty()) {
            return;
        }

        LogService.info("");
        LogService.info("=== Data Statistics ===");

        double totalFare = trips.stream()
                .mapToDouble(TaxiTrip::getTotalAmount)
                .sum();

        double avgFare = totalFare / trips.size();

        double totalDistance = trips.stream()
                .mapToDouble(TaxiTrip::getTripDistance)
                .sum();

        double avgDistance = totalDistance / trips.size();

        int totalPassengers = trips.stream()
                .mapToInt(TaxiTrip::getPassengerCount)
                .sum();

        LogService.infof("Total fare:       $%,.2f", totalFare);
        LogService.infof("Average fare:     $%.2f", avgFare);
        LogService.infof("Total distance:   %.2f miles", totalDistance);
        LogService.infof("Average distance: %.2f miles", avgDistance);
        LogService.infof("Total passengers: %,d", totalPassengers);
    }
}