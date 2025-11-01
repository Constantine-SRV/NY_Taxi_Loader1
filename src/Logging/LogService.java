package Logging;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Простой сервис логирования: пишет и в консоль, и в файл.
 * API и формат сообщений сохранены как в оригинале (info/infof/error/errorf/debug).
 * Никакой init(...) не требуется.
 */
public final class LogService {
    private LogService() {}

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter FILE_TS = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final String LOG_DIR = "logs";

    private static volatile BufferedWriter writer;

    // Ленивая инициализация файла (один общий файл на день).
    private static BufferedWriter getWriter() {
        BufferedWriter w = writer;
        if (w != null) return w;
        synchronized (LogService.class) {
            if (writer == null) {
                try {
                    File dir = new File(LOG_DIR);
                    if (!dir.exists()) dir.mkdirs();
                    String name = "log_" + LocalDateTime.now().format(FILE_TS) + ".txt";
                    writer = new BufferedWriter(new FileWriter(new File(dir, name), true));
                } catch (IOException e) {
                    System.err.printf("[LOG] Failed to init file logging: %s%n", e.getMessage());
                    // не падаем — продолжаем хотя бы консоль
                }
            }
            return writer;
        }
    }

    private static String ts() {
        return LocalDateTime.now().format(TIME_FMT);
    }

    private static void writeBoth(boolean isError, String formattedLine) {
        // консоль
        if (isError) System.err.print(formattedLine);
        else System.out.print(formattedLine);

        // файл
        BufferedWriter w = getWriter();
        if (w != null) {
            synchronized (LogService.class) {
                try {
                    w.write(formattedLine);
                    w.flush();
                } catch (IOException e) {
                    System.err.printf("[LOG] File logging failed: %s%n", e.getMessage());
                }
            }
        }
    }

    // ---------- публичный API (как в твоём исходнике) ----------

    public static void info(String message) {
        String line = String.format("[%s] [INFO] %s%n", ts(), message);
        writeBoth(false, line);
    }

    public static void infof(String format, Object... args) {
        info(String.format(format, args));
    }

    public static void error(String message) {
        String line = String.format("[%s] [ERROR] %s%n", ts(), message);
        writeBoth(true, line);
    }

    public static void errorf(String format, Object... args) {
        error(String.format(format, args));
    }

    public static void debug(String message) {
        String line = String.format("[%s] [DEBUG] %s%n", ts(), message);
        writeBoth(false, line);
    }

    // опционально — аккуратно закрыть при завершении (можно не вызывать)
    public static synchronized void close() {
        if (writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch (IOException ignore) {}
            writer = null;
        }
    }
}
