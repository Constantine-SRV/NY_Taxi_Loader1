package logging;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Простой сервис логирования без внешних библиотек.
 * Пока только консоль, потом можно добавить файлы.
 */
public final class LogService {

    private LogService() {}

    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public static void info(String message) {
        String timestamp = LocalDateTime.now().format(TIME_FMT);
        System.out.printf("[%s] [INFO]  %s%n", timestamp, message);
    }

    public static void infof(String format, Object... args) {
        info(String.format(format, args));
    }

    public static void error(String message) {
        String timestamp = LocalDateTime.now().format(TIME_FMT);
        System.err.printf("[%s] [ERROR] %s%n", timestamp, message);
    }

    public static void errorf(String format, Object... args) {
        error(String.format(format, args));
    }

    public static void debug(String message) {
        String timestamp = LocalDateTime.now().format(TIME_FMT);
        System.out.printf("[%s] [DEBUG] %s%n", timestamp, message);
    }
}