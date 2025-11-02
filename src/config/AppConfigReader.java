package config;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

/**
 * Читает XML-конфиг и заливает значения в AppConfig.ACTIVE + статические поля AppConfig.
 * Если файла нет или структура не подходит — бросаем Exception.
 */
public final class AppConfigReader {

    private AppConfigReader() {}

    public static AppConfig read(String filename) throws Exception {
        File f = new File(filename);
        if (!f.exists() || !f.isFile()) {
            System.err.println("ERROR: Config file not found: " + f.getAbsolutePath());
            throw new IllegalArgumentException("Config file not found: " + f.getAbsolutePath());
        }

        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(f);
        doc.getDocumentElement().normalize();
        Element root = doc.getDocumentElement();
        if (!"AppConfig".equals(root.getTagName())) {
            System.err.println("ERROR: Root element must be <AppConfig>");
            throw new IllegalArgumentException("Root element must be <AppConfig>");
        }

        AppConfig cfg = new AppConfig();

        // === DB type ===
        String dbType = text(root, "DATABASE_TYPE");
        if (dbType == null || dbType.isEmpty()) {
            throw new IllegalArgumentException("DATABASE_TYPE is required");
        }
        cfg.DATABASE_TYPE = DatabaseType.valueOf(dbType.trim().toUpperCase());

        // === OceanBase ===
        cfg.OCEANBASE_URL      = req(root, "OCEANBASE_URL");
        cfg.OCEANBASE_USER     = req(root, "OCEANBASE_USER");
        cfg.OCEANBASE_PASSWORD = req(root, "OCEANBASE_PASSWORD");

        // === PostgreSQL ===
        cfg.POSTGRESQL_URL      = req(root, "POSTGRESQL_URL");
        cfg.POSTGRESQL_USER     = req(root, "POSTGRESQL_USER");
        cfg.POSTGRESQL_PASSWORD = req(root, "POSTGRESQL_PASSWORD");

        // === MSSQL ===
        cfg.MSSQL_URL      = req(root, "MSSQL_URL");
        cfg.MSSQL_USER     = req(root, "MSSQL_USER");
        cfg.MSSQL_PASSWORD = req(root, "MSSQL_PASSWORD");

        // === MongoDB ===
        cfg.MONGO_USE_SSL    = parseBool(req(root, "MONGO_USE_SSL"));
        cfg.MONGO_USERNAME   = req(root, "MONGO_USERNAME");
        cfg.MONGO_PASSWORD   = req(root, "MONGO_PASSWORD");
        cfg.MONGO_AUTH_SOURCE= req(root, "MONGO_AUTH_SOURCE");
        cfg.MONGO_HOSTS      = splitList(req(root, "MONGO_HOSTS")); // comma/semicolon/space separated
        cfg.MONGO_DATABASE   = req(root, "MONGO_DATABASE");
        cfg.MONGO_COLLECTION = req(root, "MONGO_COLLECTION");

        // === Data source ===
        cfg.PARQUET_DIR  = req(root, "PARQUET_DIR");
        cfg.FIRST_FILE   = req(root, "FIRST_FILE");

        // === Batch/limits/logs ===
        cfg.BATCH_SIZE   = parseInt(req(root, "BATCH_SIZE"));
        cfg.TEST_LIMIT   = parseInt(req(root, "TEST_LIMIT"));
        cfg.LOG_INTERVAL = parseInt(req(root, "LOG_INTERVAL"));

        // === Table / DB name ===
        cfg.TABLE_NAME    = req(root, "TABLE_NAME");
        cfg.DATABASE_NAME = req(root, "DATABASE_NAME");

        // === Threads ===
        cfg.THREAD_COUNT  = parseInt(req(root, "THREAD_COUNT"));

        // Сгенерировать *URL_FULL и применить как активную конфигурацию + в статические поля (для обратной совместимости)
        cfg.applyToStatics();

        // Сохранить активную ссылку
        AppConfig.ACTIVE = cfg;

        return cfg;
    }

    // --- helpers ---

    private static String req(Element root, String tag) {
        String v = text(root, tag);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required tag <" + tag + ">");
        }
        return v.trim();
    }

    private static String text(Element root, String tag) {
        if (root.getElementsByTagName(tag).getLength() == 0) return null;
        return root.getElementsByTagName(tag).item(0).getTextContent();
    }

    private static boolean parseBool(String s) {
        return "true".equalsIgnoreCase(s) || "1".equals(s) || "yes".equalsIgnoreCase(s);
    }

    private static int parseInt(String s) {
        try { return Integer.parseInt(s.trim()); }
        catch (Exception e) { throw new IllegalArgumentException("Bad integer value: " + s); }
    }

    private static java.util.List<String> splitList(String s) {
        String[] parts = s.split("[,;\\s]+");
        java.util.List<String> out = new java.util.ArrayList<>();
        for (String p : parts) {
            if (!p.isBlank()) out.add(p.trim());
        }
        return out;
    }
}
