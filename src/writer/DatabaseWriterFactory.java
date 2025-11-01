package writer;

import config.AppConfig;
import config.DatabaseType;
import Logging.LogService;

import java.sql.SQLException;

/**
 * Фабрика для создания нужного DatabaseWriter в зависимости от типа базы данных.
 */
public class DatabaseWriterFactory {

    private DatabaseWriterFactory() {
        // Утилитный класс - не создаем экземпляры
    }

    /**
     * Создать DatabaseWriter для текущей базы данных из AppConfig.
     *
     * @param taskId идентификатор задачи для логирования
     * @return экземпляр DatabaseWriter для текущей БД
     * @throws SQLException если не удалось создать подключение
     */
    public static DatabaseWriter createWriter(String taskId) throws SQLException {
        DatabaseType dbType = AppConfig.DATABASE_TYPE;

        LogService.infof("[%s] Creating writer for database: %s", taskId, dbType.getDisplayName());

        switch (dbType) {
            case OCEANBASE:
                return new OceanBaseWriter(taskId);

            case POSTGRESQL:
                return new PostgreSQLWriter(taskId);

            case MSSQL:
                return new MSSQLWriter(taskId);

            default:
                throw new IllegalStateException("Unsupported database type: " + dbType);
        }
    }

    /**
     * Создать DatabaseWriter без указания taskId (использует "Main").
     *
     * @return экземпляр DatabaseWriter для текущей БД
     * @throws SQLException если не удалось создать подключение
     */
    public static DatabaseWriter createWriter() throws SQLException {
        return createWriter("Main");
    }

    /**
     * Получить название текущей базы данных.
     */
    public static String getCurrentDatabaseName() {
        return AppConfig.DATABASE_TYPE.getDisplayName();
    }
}