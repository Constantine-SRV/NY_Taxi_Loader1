package writer;

import model.TaxiTrip;

import java.sql.SQLException;
import java.util.List;

/**
 * Интерфейс для записи данных TaxiTrip в различные базы данных.
 * Обеспечивает единый API для OceanBase, PostgreSQL и MS SQL Server.
 */
public interface DatabaseWriter extends AutoCloseable {

    /**
     * Добавить одну поездку в batch.
     */
    void addTrip(TaxiTrip trip) throws SQLException;

    /**
     * Добавить список поездок в batch.
     */
    void addTrips(List<TaxiTrip> trips) throws SQLException;

    /**
     * Выполнить текущий batch.
     */
    void executeBatch() throws SQLException;

    /**
     * Завершить запись - выполнить оставшиеся batch.
     */
    void flush() throws SQLException;

    /**
     * Получить общее количество вставленных записей.
     */
    long getTotalInserted();

    /**
     * Получить статистику производительности.
     */
    String getPerformanceStats();

    /**
     * Получить taskId.
     */
    String getTaskId();

    /**
     * Тестовая вставка одной записи для проверки подключения.
     */
    void testInsert() throws SQLException;

    /**
     * Закрыть все ресурсы.
     */
    @Override
    void close() throws SQLException;
}