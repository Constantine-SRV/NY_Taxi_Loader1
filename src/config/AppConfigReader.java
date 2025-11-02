package config;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

/**
 * Читает конфигурацию приложения из XML файла.
 * Все значения записываются прямо в статические поля AppConfig.
 */
public final class AppConfigReader {

    private AppConfigReader() {
    }

    public static void read(String filename) {
        try {
            File f = new File(filename);
            if (!f.exists() || !f.isFile()) {
                System.err.println("ERROR: Config file not found: " + f.getAbsolutePath());
                System.exit(1);
            }

            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(f);
            doc.getDocumentElement().normalize();
            Element root = doc.getDocumentElement();

            if (!"AppConfig".equalsIgnoreCase(root.getTagName())) {
                System.err.println("ERROR: Root element must be <AppConfig>");
                System.exit(1);
            }

            // === Основные настройки ===
            AppConfig.DATABASE_TYPE = DatabaseType.valueOf(text(root, "DATABASE_TYPE").trim().toUpperCase());

            // === OceanBase ===
            AppConfig.OCEANBASE_URL = text(root, "OCEANBASE_URL");
            AppConfig.OCEANBASE_USER = text(root, "OCEANBASE_USER");
            AppConfig.OCEANBASE_PASSWORD = text(root, "OCEANBASE_PASSWORD");

            // === PostgreSQL ===
            AppConfig.POSTGRESQL_URL = text(root, "POSTGRESQL_URL");
            AppConfig.POSTGRESQL_USER = text(root, "POSTGRESQL_USER");
            AppConfig.POSTGRESQL_PASSWORD = text(root, "POSTGRESQL_PASSWORD");

            // === MSSQL ===
            AppConfig.MSSQL_URL = text(root, "MSSQL_URL");
            AppConfig.MSSQL_USER = text(root, "MSSQL_USER");
            AppConfig.MSSQL_PASSWORD = text(root, "MSSQL_PASSWORD");

            // === MongoDB ===
            AppConfig.MONGO_USE_SSL = parseBool(text(root, "MONGO_USE_SSL"));
            AppConfig.MONGO_USERNAME = text(root, "MONGO_USERNAME");
            AppConfig.MONGO_PASSWORD = text(root, "MONGO_PASSWORD");
            AppConfig.MONGO_AUTH_SOURCE = text(root, "MONGO_AUTH_SOURCE");
            AppConfig.MONGO_HOSTS = text(root, "MONGO_HOSTS");
            AppConfig.MONGO_DATABASE = text(root, "MONGO_DATABASE");
            AppConfig.MONGO_COLLECTION = text(root, "MONGO_COLLECTION");

            // === Data source ===
            AppConfig.PARQUET_DIR = text(root, "PARQUET_DIR");
            AppConfig.FIRST_FILE = text(root, "FIRST_FILE");

            // === Пакеты/лимиты ===
            AppConfig.BATCH_SIZE = parseInt(text(root, "BATCH_SIZE"));
            AppConfig.TEST_LIMIT = parseInt(text(root, "TEST_LIMIT"));
            AppConfig.LOG_INTERVAL = parseInt(text(root, "LOG_INTERVAL"));

            // === Таблица / база ===
            AppConfig.TABLE_NAME = text(root, "TABLE_NAME");
            AppConfig.DATABASE_NAME = text(root, "DATABASE_NAME");

            // === Потоки ===
            AppConfig.THREAD_COUNT = parseInt(text(root, "THREAD_COUNT"));

            System.out.println("INFO: Configuration loaded from " + f.getAbsolutePath());
            AppConfig.printConfig();

        } catch (Exception e) {
            System.err.println("ERROR reading configuration: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    // --- helpers ---

    private static String text(Element root, String tag) {
        if (root.getElementsByTagName(tag).getLength() == 0)
            return "";
        return root.getElementsByTagName(tag).item(0).getTextContent().trim();
    }

    private static boolean parseBool(String s) {
        return "true".equalsIgnoreCase(s) || "1".equals(s) || "yes".equalsIgnoreCase(s);
    }

    private static int parseInt(String s) {
        try {
            return Integer.parseInt(s.trim());
        } catch (Exception e) {
            return 0;
        }
    }
}
