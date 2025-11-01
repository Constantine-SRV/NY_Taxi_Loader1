package config;

/**
 * Типы поддерживаемых баз данных.
 */
public enum DatabaseType {
    OCEANBASE("OceanBase"),
    POSTGRESQL("PostgreSQL"),
    MSSQL("Microsoft SQL Server");

    private final String displayName;

    DatabaseType(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String toString() {
        return displayName;
    }
}