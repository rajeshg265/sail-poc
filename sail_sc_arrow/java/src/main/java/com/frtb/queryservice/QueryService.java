package com.frtb.queryservice;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
// Arrow + SparkConnect gRPC (sc_arrow engine — zero-copy Arrow IPC without JDBC deserialization)
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.spark.connect.proto.ExecutePlanResponse;
import org.apache.spark.connect.proto.Plan;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.connect.proto.SQL;
import org.apache.spark.sql.connect.client.SparkConnectClient;
import org.apache.spark.sql.util.CloseableIterator;

/**
 * FRTB – distributed query service.
 *
 * Usage:
 *   java -cp target/classes:lib/postgresql.jar com.frtb.queryservice.QueryService [--mode sail|sharding|server] [--port 8090]
 *
 * server mode (default when invoked from the Flask UI):
 *   Embedded HTTP server on --port (default 8090).
 *   POST /query   {"sql":"...", "mode":"sail|sharding"} → JSON array of rows
 *   POST /explain {"sql":"..."}                         → JSON array (EXPLAIN rows)
 *   GET  /status                                        → {"sail":bool, "sharding":bool}
 *   GET  /health                                        → {"status":"ok"}
 *
 * sail mode:
 *   CLI demo — HTTP POST → Flask bridge → pysail SparkConnect → PostgreSQL shards
 *              OR Arrow FlightSQL → Parquet views (SAIL_ENGINE=flightsql)
 *
 * sharding mode:
 *   CLI demo — JDBC → ShardingSphere Proxy (port 3307)
 */
public class QueryService {

    // ── Sail execution engine ────────────────────────────────────────────────
    // SAIL_ENGINE=http (default):    HTTP bridge :8080 → SparkConnect (JSON)
    // SAIL_ENGINE=flightsql:         Arrow FlightSQL :32010 → Arrow-backed views (zero-copy)
    // SAIL_ENGINE=sparkconnect:      Spark Connect JDBC → session from metadata service :7070
    // SAIL_ENGINE=sc_arrow:          Spark Connect gRPC client → Arrow IPC (zero-copy, no JDBC)
    private static volatile String sailEngine  = System.getenv("SAIL_ENGINE") != null
                                                 ? System.getenv("SAIL_ENGINE") : "sc_arrow";
    // Back-compat alias used by legacy code compiled before this change
    private static String SAIL_ENGINE() { return sailEngine; }
    private static final String SAIL_BRIDGE_URL = "http://127.0.0.1:8080/query";
    private static final String FLIGHT_HOST     = "127.0.0.1";
    private static final int    FLIGHT_PORT     = 32010;

    // ── Spark Connect JDBC (SAIL_ENGINE=sparkconnect) ────────────────────────
    // Metadata service URL — bootstrap.py exposes GET /session → {session_id, sc_host, sc_port, ready}
    private static final String SAIL_META_URL = System.getenv("SAIL_META_URL") != null
                                                ? System.getenv("SAIL_META_URL") : "http://127.0.0.1:7075";
    private static volatile String     scJdbcUrl    = null;  // built once session is resolved
    private static volatile Connection scConnection = null;  // kept open — closing it releases the pySail session

    // ── Real Java/JVM Spark HTTP bridge ─────────────────────────────────────
    private static final String SPARK_BRIDGE_URL = System.getenv("SPARK_BRIDGE_URL") != null
                                                   ? System.getenv("SPARK_BRIDGE_URL")
                                                   : "http://127.0.0.1:8095/query";

    // ── ShardingSphere Proxy JDBC ────────────────────────────────────────────
    private static final String SS_JDBC_URL  = "jdbc:postgresql://127.0.0.1:3307/frtb_sharding";
    private static final String SS_JDBC_USER = "frtb";
    // Read password from env; fall back to local dev default
    private static final String SS_JDBC_PASS =
        System.getenv("SS_JDBC_PASSWORD") != null ? System.getenv("SS_JDBC_PASSWORD") : "frtb";

    // ── Queries ──────────────────────────────────────────────────────────────

    private static final String Q_TOPN = """
        SELECT   legal_entity_group_id,
                 SUM(total)                        AS sum_total,
                 SUM(interest_rates_delta)          AS ir_delta,
                 SUM(equities_delta)                AS eq_delta,
                 SUM(foreign_exchange_delta)        AS fx_delta,
                 COUNT(*)                           AS row_count,
                 RANK() OVER (ORDER BY SUM(total) DESC) AS global_rank
        FROM     frtb_sa_capital_output
        GROUP BY legal_entity_group_id
        ORDER BY global_rank, legal_entity_group_id
        LIMIT    20
        """;

    private static final String Q_CASE_WHEN = """
        SELECT   legal_entity_group_id,
                 SUM(CASE WHEN driving_scenario = 'HIGH'   THEN total ELSE 0.0 END) AS high_total,
                 SUM(CASE WHEN driving_scenario = 'MEDIUM' THEN total ELSE 0.0 END) AS medium_total,
                 SUM(CASE WHEN driving_scenario = 'LOW'    THEN total ELSE 0.0 END) AS low_total,
                 SUM(CASE WHEN total > (SELECT AVG(total) FROM frtb_sa_capital_output)
                          THEN total ELSE 0.0 END)                                  AS above_avg_total,
                 COUNT(CASE WHEN total > (SELECT AVG(total) FROM frtb_sa_capital_output)
                            THEN 1 END)                                             AS above_avg_count
        FROM     frtb_sa_capital_output
        WHERE    cobdate BETWEEN DATE '2024-06-01' AND DATE '2026-03-31'
        GROUP BY legal_entity_group_id
        ORDER BY above_avg_total DESC
        LIMIT    20
        """;

    private static final String Q_AGGREGATIONS = """
        SELECT   correlation_scenario_id,
                 driving_scenario,
                 COUNT(*)                               AS positions,
                 SUM(total)                             AS total_capital,
                 AVG(total)                             AS avg_capital,
                 SUM(commodities_delta + commodities_vega
                     + commodities_curvature)           AS commodities_sa,
                 SUM(interest_rates_delta + interest_rates_vega
                     + interest_rates_curvature)        AS ir_sa,
                 SUM(equities_delta + equities_vega
                     + equities_curvature)              AS equities_sa,
                 SUM(foreign_exchange_delta + foreign_exchange_vega
                     + foreign_exchange_curvature)      AS fx_sa
        FROM     frtb_sa_capital_output
        GROUP BY correlation_scenario_id, driving_scenario
        ORDER BY total_capital DESC
        """;

    private static final String Q_PARTITION_PRUNE = """
        SELECT   cobdate,
                 COUNT(*)      AS positions,
                 SUM(total)    AS total_capital
        FROM     frtb_sa_capital_output
        WHERE    cobdate BETWEEN DATE '2024-01-01' AND DATE '2024-03-31'
        GROUP BY cobdate
        ORDER BY cobdate
        """;

    // ── Mode ─────────────────────────────────────────────────────────────────

    private enum Mode { SAIL, SHARDING, SERVER }

    // ── Entry point ──────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        Mode mode = Mode.SERVER;
        int  serverPort = 8092;

        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if ("--mode".equals(a)) continue;
            if ("server".equalsIgnoreCase(a))   { mode = Mode.SERVER;   }
            if ("sharding".equalsIgnoreCase(a)) { mode = Mode.SHARDING; }
            if ("sail".equalsIgnoreCase(a))     { mode = Mode.SAIL;     }
            if ("--port".equals(a) && i + 1 < args.length) {
                serverPort = Integer.parseInt(args[++i]);
            }
        }

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════╗");
        String engineLabel = switch (SAIL_ENGINE().toLowerCase()) {
            case "flightsql"    -> "FlightSQL :32010 (Arrow zero-copy)";
            case "sparkconnect" -> "SparkConnect JDBC (session from metadata :7070)";
            case "sc_arrow"     -> "SparkConnect gRPC + Arrow IPC (zero-copy, no JDBC)";
            default             -> "HTTP bridge :8080";
        };
        String modeLabel = switch (mode) {
            case SAIL     -> "SAIL [" + engineLabel + "]";
            case SHARDING -> "SHARDING (ShardingSphere Proxy 5.5.0)";
            case SERVER   -> "SERVER (HTTP " + serverPort + ") — Sail via " + engineLabel;
        };
        System.out.printf ("║  Mode: %-63s║%n", modeLabel);
        System.out.println("╚══════════════════════════════════════════════════════════════════════╝");
        System.out.println();

        switch (mode) {
            case SAIL     -> runSailMode();
            case SHARDING -> runShardingMode();
            case SERVER   -> runServerMode(serverPort);
        }
    }

    // ── HTTP Server mode ──────────────────────────────────────────────────────

    private static void runServerMode(int port) throws Exception {
        // Warm up JDBC driver
        Class.forName("org.postgresql.Driver");

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);

        server.createContext("/health", ex -> {
            sendJson(ex, 200, "{\"status\":\"ok\"}");
        });

        server.createContext("/status", ex -> {
            if (!"GET".equals(ex.getRequestMethod())) { ex.sendResponseHeaders(405, -1); return; }
            boolean sailOk = false, ssOk = false;
            switch (SAIL_ENGINE().toLowerCase()) {
                case "flightsql" -> {
                    try { sailFlightSqlJson("SELECT 1 AS n"); sailOk = true; } catch (Exception ignored) {}
                }
                case "sparkconnect", "sc_arrow" -> {
                    try {
                        HttpURLConnection hc = (HttpURLConnection)
                            new URL(SAIL_META_URL + "/session").openConnection();
                        hc.setConnectTimeout(3000); hc.setReadTimeout(3000);
                        if (hc.getResponseCode() == 200) {
                            String body = new String(hc.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
                                              .replaceAll("\\s", "");
                            sailOk = body.contains("\"ready\":true");
                        }
                    } catch (Exception ignored) {}
                }
                default -> {
                    try {
                        HttpURLConnection hc = (HttpURLConnection)
                            new URL("http://127.0.0.1:8080/health").openConnection();
                        hc.setConnectTimeout(3000); hc.setReadTimeout(3000);
                        sailOk = hc.getResponseCode() == 200;
                    } catch (Exception ignored) {}
                }
            }
            try (Connection c = DriverManager.getConnection(SS_JDBC_URL, SS_JDBC_USER, SS_JDBC_PASS)) {
                ssOk = true;
            } catch (Exception ignored) {}
            boolean sparkOk = false;
            try {
                HttpURLConnection sc = (HttpURLConnection) new URL("http://127.0.0.1:8095/health").openConnection();
                sc.setConnectTimeout(2000); sc.setReadTimeout(2000);
                sparkOk = sc.getResponseCode() == 200;
            } catch (Exception ignored) {}
            sendJson(ex, 200, "{\"sail\":" + sailOk + ",\"sharding\":" + ssOk
                    + ",\"spark\":" + sparkOk
                    + ",\"engine\":" + jsonString(SAIL_ENGINE()) + "}");
        });

        server.createContext("/engine", ex -> {
            if ("GET".equals(ex.getRequestMethod())) {
                sendJson(ex, 200, "{\"engine\":" + jsonString(SAIL_ENGINE()) + "}");
                return;
            }
            if (!"POST".equals(ex.getRequestMethod())) { ex.sendResponseHeaders(405, -1); return; }
            String body   = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String engine = extractJsonString(body, "engine");
            if (engine.isEmpty()) { sendJson(ex, 400, "{\"error\":\"missing engine\"}"); return; }
            switch (engine.toLowerCase()) {
                case "http", "flightsql", "sparkconnect", "sc_arrow" -> {
                    sailEngine = engine.toLowerCase();
                    // Do NOT close scConnection on engine switch — closing it sends releaseSession
                    // to pySail which permanently destroys the session. Leave it open so switching
                    // back to sparkconnect reuses the same live session.
                    System.out.println("[engine] switched to " + sailEngine);
                    sendJson(ex, 200, "{\"engine\":" + jsonString(sailEngine) + "}");
                }
                default -> sendJson(ex, 400, "{\"error\":\"unknown engine: " + engine + "\"}");
            }
        });

        server.createContext("/query", ex -> {
            if (!"POST".equals(ex.getRequestMethod())) { ex.sendResponseHeaders(405, -1); return; }
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String sql  = extractJsonString(body, "sql");
            String mode = extractJsonString(body, "mode");
            if (sql.isEmpty()) { sendJson(ex, 400, "{\"error\":\"missing sql\"}"); return; }
            try {
                String result = "sharding".equalsIgnoreCase(mode)
                    ? runShardingJson(sql)
                    : "spark".equalsIgnoreCase(mode)
                    ? runSparkJson(sql)
                    : runSailJson(sql);
                sendJson(ex, 200, result);
            } catch (Exception e) {
                sendJson(ex, 500, "{\"error\":" + jsonString(e.getMessage()) + "}");
            }
        });

        server.createContext("/explain", ex -> {
            if (!"POST".equals(ex.getRequestMethod())) { ex.sendResponseHeaders(405, -1); return; }
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String sql  = extractJsonString(body, "sql");
            if (sql.isEmpty()) { sendJson(ex, 400, "{\"error\":\"missing sql\"}"); return; }
            try {
                sendJson(ex, 200, runSailJson("EXPLAIN " + sql));
            } catch (Exception e) {
                sendJson(ex, 500, "{\"error\":" + jsonString(e.getMessage()) + "}");
            }
        });

        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();
        System.out.println("QueryService HTTP server listening on http://127.0.0.1:" + port);
        System.out.println("  POST /query   {\"sql\":\"...\", \"mode\":\"sail|sharding\"}");
        System.out.println("  POST /explain {\"sql\":\"...\"}");
        System.out.println("  GET  /status  → {\"sail\":bool, \"sharding\":bool}");
        System.out.println("  GET  /health  → {\"status\":\"ok\"}");
        // Block main thread
        Thread.currentThread().join();
    }

    private static void sendJson(HttpExchange ex, int status, String json) throws java.io.IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "http://127.0.0.1:5050");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    /** Run SQL via ShardingSphere JDBC and return a JSON array string. */
    private static String runShardingJson(String sql) throws Exception {
        StringBuilder json = new StringBuilder("[");
        try (Connection conn = DriverManager.getConnection(SS_JDBC_URL, SS_JDBC_USER, SS_JDBC_PASS);
             Statement  st   = conn.createStatement();
             ResultSet  rs   = st.executeQuery(sql)) {

            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            List<String> colNames = new ArrayList<>();
            for (int i = 1; i <= cols; i++) colNames.add(meta.getColumnLabel(i));

            boolean firstRow = true;
            while (rs.next()) {
                if (!firstRow) json.append(",");
                firstRow = false;
                json.append("{");
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) json.append(",");
                    json.append(jsonString(colNames.get(i - 1))).append(":");
                    json.append(toJsonValue(rs.getObject(i)));
                }
                json.append("}");
            }
        }
        json.append("]");
        return json.toString();
    }

    /** Run SQL via the real Java/JVM Spark HTTP bridge and return a JSON array string. */
    private static String runSparkJson(String sql) throws Exception {
        byte[] payload = ("{\"sql\":" + jsonString(sql) + "}").getBytes(StandardCharsets.UTF_8);
        HttpURLConnection hc = (HttpURLConnection) new URL(SPARK_BRIDGE_URL).openConnection();
        hc.setRequestMethod("POST");
        hc.setDoOutput(true);
        hc.setRequestProperty("Content-Type", "application/json");
        hc.setConnectTimeout(10000);
        hc.setReadTimeout(120000);
        try (OutputStream os = hc.getOutputStream()) { os.write(payload); }
        int code = hc.getResponseCode();
        String body = new String(
            (code == 200 ? hc.getInputStream() : hc.getErrorStream()).readAllBytes(),
            StandardCharsets.UTF_8);
        if (code != 200) throw new RuntimeException("Spark bridge HTTP " + code + ": " + body);
        if (body.contains("\"error\"")) throw new RuntimeException(extractJsonString(body, "error"));
        return body;
    }

    /** Serialize a JDBC or Arrow value to its JSON representation. */
    private static String toJsonValue(Object v) {
        if (v == null) return "null";
        if (v instanceof Boolean) return v.toString();
        if (v instanceof Double || v instanceof Float) {
            // Arrow returns Double for FLOAT8 columns; avoid Java scientific notation.
            return new java.math.BigDecimal(v.toString()).toPlainString();
        }
        if (v instanceof Number)  return v.toString();
        return jsonString(v.toString());
    }

    /**
     * Extract a JSON string field value, correctly decoding JSON escape sequences.
     * Works on simple flat JSON objects with string or non-string values.
     */
    private static String extractJsonString(String json, String field) {
        String needle = "\"" + field + "\"";
        int k = json.indexOf(needle);
        if (k < 0) return "";
        k += needle.length();
        while (k < json.length() && " \t\r\n:".indexOf(json.charAt(k)) >= 0) k++;
        if (k >= json.length() || json.charAt(k) != '"') return "";
        k++;
        StringBuilder sb = new StringBuilder();
        while (k < json.length()) {
            char c = json.charAt(k++);
            if (c == '"') break;
            if (c == '\\' && k < json.length()) {
                char esc = json.charAt(k++);
                switch (esc) {
                    case '"'  -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/'  -> sb.append('/');
                    case 'n'  -> sb.append('\n');
                    case 'r'  -> sb.append('\r');
                    case 't'  -> sb.append('\t');
                    case 'b'  -> sb.append('\b');
                    case 'f'  -> sb.append('\f');
                    default   -> sb.append(esc);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    // ── Sail query dispatcher ─────────────────────────────────────────────────

    private static String runSailJson(String sql) throws Exception {
        return switch (SAIL_ENGINE().toLowerCase()) {
            case "flightsql"    -> sailFlightSqlJson(sql);
            case "sparkconnect" -> sailSparkConnectJson(sql);
            case "sc_arrow"     -> sailScArrowJson(sql);
            default             -> sailPost(sql);
        };
    }

    /** Arrow FlightSQL path — zero-copy Arrow; no JSON serialisation on the Sail side. */
    private static String sailFlightSqlJson(String sql) throws Exception {
        try (var allocator = new cfjd.org.apache.arrow.memory.RootAllocator()) {
            var location    = cfjd.org.apache.arrow.flight.Location.forGrpcInsecure(FLIGHT_HOST, FLIGHT_PORT);
            var flightClient = cfjd.org.apache.arrow.flight.FlightClient
                                   .builder(allocator, location).build();
            var sqlClient   = new cfjd.org.apache.arrow.flight.sql.FlightSqlClient(flightClient);
            try {
                var info      = sqlClient.execute(sql);
                var json      = new StringBuilder("[");
                boolean first = true;

                for (var endpoint : info.getEndpoints()) {
                    try (var stream = flightClient.getStream(endpoint.getTicket())) {
                        while (stream.next()) {
                            var root   = stream.getRoot();
                            var fields = root.getSchema().getFields();
                            int rows   = root.getRowCount();
                            for (int r = 0; r < rows; r++) {
                                if (!first) json.append(",");
                                first = false;
                                json.append("{");
                                for (int c = 0; c < fields.size(); c++) {
                                    if (c > 0) json.append(",");
                                    String col = fields.get(c).getName();
                                    var    vec = root.getVector(c);
                                    Object val = vec.isNull(r) ? null : vec.getObject(r);
                                    json.append(jsonString(col)).append(":")
                                        .append(toJsonValue(val));
                                }
                                json.append("}");
                            }
                        }
                    }
                }
                json.append("]");
                return json.toString();
            } finally {
                sqlClient.close();
                flightClient.close();
            }
        }
    }

    // ── Sail mode ─────────────────────────────────────────────────────────────

    private static void runSailMode() throws Exception {
        switch (SAIL_ENGINE().toLowerCase()) {
            case "flightsql" -> {
                try {
                    sailFlightSqlJson("SELECT 1 AS n");
                    System.out.println("Sail FlightSQL status: OK (grpc://" + FLIGHT_HOST + ":" + FLIGHT_PORT + ")");
                } catch (Exception e) {
                    System.err.println("ERROR: Sail FlightSQL not reachable on port " + FLIGHT_PORT + ". Start sail/bootstrap.py first.");
                    System.exit(1);
                }
            }
            case "sparkconnect" -> {
                try {
                    resolveSparkConnectSession();
                    System.out.println("Sail SparkConnect status: OK (session resolved via " + SAIL_META_URL + ")");
                } catch (Exception e) {
                    System.err.println("ERROR: SparkConnect metadata not ready: " + e.getMessage() + ". Start sail/bootstrap.py first.");
                    System.exit(1);
                }
            }
            case "sc_arrow" -> {
                try {
                    getScArrowClient();
                    System.out.println("Sail sc_arrow status: OK (SparkConnect gRPC + Arrow IPC, session via " + SAIL_META_URL + ")");
                } catch (Exception e) {
                    System.err.println("ERROR: sc_arrow metadata not ready: " + e.getMessage() + ". Start sail/bootstrap.py first.");
                    System.exit(1);
                }
            }
            default -> {
                try {
                    HttpURLConnection hc = (HttpURLConnection)
                            new URL("http://127.0.0.1:8080/health").openConnection();
                    hc.setConnectTimeout(5000);
                    System.out.println("Sail bridge status: HTTP " + hc.getResponseCode());
                } catch (Exception e) {
                    System.err.println("ERROR: Sail bootstrap not running on port 8080. Start sail/bootstrap.py first.");
                    System.exit(1);
                }
            }
        }

        sailRun("TopN – Top 20 entities by total capital (cross-shard SUM)", Q_TOPN);
        sailRun("CASE/WHEN – scenario breakdown per entity+date", Q_CASE_WHEN);
        sailRun("Complex aggregations – SA components by scenario", Q_AGGREGATIONS);
        sailRun("Partition pruning – shard1-only date range", Q_PARTITION_PRUNE);
    }

    private static void sailRun(String label, String sql) throws Exception {
        printHeader(label, "SAIL [" + SAIL_ENGINE() + "]");
        long t0 = System.currentTimeMillis();
        String json = runSailJson(sql);
        long ms = System.currentTimeMillis() - t0;
        if (json.startsWith("{\"error\"")) {
            System.err.println("Query failed: " + json);
        } else {
            printJsonTable(json);
        }
        System.out.printf("Elapsed: %d ms%n%n", ms);
    }

    private static String sailPost(String sql) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(SAIL_BRIDGE_URL).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(120000);
        String body = "{\"sql\":" + jsonString(sql) + "}";
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        int status = conn.getResponseCode();
        var stream = status < 400 ? conn.getInputStream() : conn.getErrorStream();
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) sb.append(line);
        }
        return sb.toString();
    }

    // ── SparkConnect JDBC path ────────────────────────────────────────────────

    /**
     * Polls the bootstrap.py metadata service until ready==true, then builds
     * the jdbc:sc:// URL and caches it in scJdbcUrl.
     * Fails fast — caller retries on the next request (no blocking loop).
     */
    private static synchronized void resolveSparkConnectSession() throws Exception {
        if (scJdbcUrl != null) return;  // already resolved and not invalidated
        String metaUrl = SAIL_META_URL + "/session";
        try {
            HttpURLConnection hc = (HttpURLConnection) new URL(metaUrl).openConnection();
            hc.setConnectTimeout(3000);
            hc.setReadTimeout(5000);
            if (hc.getResponseCode() == 200) {
                String body       = new String(hc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                String  compact   = body.replaceAll("\\s", "");
                boolean ready     = compact.contains("\"ready\":true");
                String  sessionId = extractJsonString(body, "session_id");
                String  scHost    = extractJsonString(body, "sc_host");
                String  scPortStr = extractJsonField(body, "sc_port");
                if (ready && !sessionId.isEmpty()) {
                    String host = scHost.isEmpty() ? "127.0.0.1" : scHost;
                    String port = scPortStr.isEmpty() ? "50051" : scPortStr;
                    scJdbcUrl = "jdbc:sc://" + host + ":" + port + "/;session_id=" + sessionId;
                    System.out.println("[sparkconnect] resolved session: " + scJdbcUrl);
                    return;
                }
                throw new RuntimeException("SparkConnect session not ready yet at " + metaUrl);
            }
            throw new RuntimeException("SparkConnect metadata HTTP " + hc.getResponseCode() + " at " + metaUrl);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("SparkConnect metadata unreachable at " + metaUrl + ": " + e.getMessage(), e);
        }
    }

    /** Extract a bare (non-quoted) JSON field value — used for numeric sc_port. */
    private static String extractJsonField(String json, String field) {
        String needle = "\"" + field + "\":";
        int k = json.indexOf(needle);
        if (k < 0) return "";
        k += needle.length();
        while (k < json.length() && " \t".indexOf(json.charAt(k)) >= 0) k++;
        if (k >= json.length()) return "";
        if (json.charAt(k) == '"') return extractJsonString(json, field);
        int start = k;
        while (k < json.length() && ",} \t\r\n".indexOf(json.charAt(k)) < 0) k++;
        return json.substring(start, k).trim();
    }

    /**
     * Returns (or opens) the persistent SparkConnect JDBC connection.
     * The connection is kept open across queries — closing it calls releaseSession
     * on pySail, which destroys the session and breaks subsequent queries.
     */
    private static synchronized Connection getScConnection() throws Exception {
        if (scConnection != null) {
            try { if (!scConnection.isClosed()) return scConnection; }
            catch (Exception ignored) {}
        }
        resolveSparkConnectSession();
        Class.forName("org.apache.spark.sql.connect.client.jdbc.SparkConnectDriver");
        scConnection = DriverManager.getConnection(scJdbcUrl);
        System.out.println("[sparkconnect] connection opened to " + scJdbcUrl);
        return scConnection;
    }

    /** Invalidate both the cached connection and session URL. */
    private static synchronized void invalidateScConnection(String reason) {
        System.out.println("[sparkconnect] invalidating connection: " + reason);
        if (scConnection != null) {
            try { scConnection.close(); } catch (Exception ignored) {}
            scConnection = null;
        }
        scJdbcUrl = null;
    }

    /** SparkConnect JDBC path — reuses a persistent connection so pySail keeps the session alive. */
    private static String sailSparkConnectJson(String sql) throws Exception {
        // Retry once on session errors — bootstrap may have recovered to a new session_id
        // while our cached scJdbcUrl still points at the evicted session.
        for (int attempt = 0; attempt < 2; attempt++) {
            Connection conn = getScConnection();
            StringBuilder json = new StringBuilder("[");
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {

                ResultSetMetaData meta = rs.getMetaData();
                int cols = meta.getColumnCount();
                List<String> colNames = new ArrayList<>();
                for (int i = 1; i <= cols; i++) colNames.add(meta.getColumnLabel(i));

                boolean firstRow = true;
                while (rs.next()) {
                    if (!firstRow) json.append(",");
                    firstRow = false;
                    json.append("{");
                    for (int i = 1; i <= cols; i++) {
                        if (i > 1) json.append(",");
                        json.append(jsonString(colNames.get(i - 1))).append(":");
                        json.append(toJsonValue(rs.getObject(i)));
                    }
                    json.append("}");
                }
                json.append("]");
                return json.toString();
            } catch (Exception e) {
                String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
                if ((msg.contains("not running") || msg.contains("session") || msg.contains("closed"))
                        && attempt == 0) {
                    invalidateScConnection(e.getMessage());
                    System.out.println("[sparkconnect] session error on first attempt — retrying with fresh session");
                    continue;
                }
                throw e;
            }
        }
        throw new RuntimeException("unreachable");
    }

    // ── SparkConnect gRPC + Arrow IPC (sc_arrow — true zero-copy) ────────────

    private static volatile SparkConnectClient scArrowClient    = null;
    private static volatile String             scArrowSessionId = null;

    private static synchronized SparkConnectClient getScArrowClient() throws Exception {
        if (scArrowClient != null && scArrowSessionId != null) return scArrowClient;
        String metaUrl = SAIL_META_URL + "/session";
        try {
            HttpURLConnection hc = (HttpURLConnection) new URL(metaUrl).openConnection();
            hc.setConnectTimeout(3000); hc.setReadTimeout(5000);
            if (hc.getResponseCode() != 200)
                throw new RuntimeException("sc_arrow metadata HTTP " + hc.getResponseCode() + " at " + metaUrl);
            String body      = new String(hc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            String compact   = body.replaceAll("\\s", "");
            if (!compact.contains("\"ready\":true"))
                throw new RuntimeException("SparkConnect session not ready at " + metaUrl);
            String sessionId = extractJsonString(body, "session_id");
            String scHost    = extractJsonString(body, "sc_host");
            String scPortStr = extractJsonField(body, "sc_port");
            if (sessionId.isEmpty())
                throw new RuntimeException("No session_id from sc_arrow metadata");
            String host = scHost.isEmpty()    ? "127.0.0.1" : scHost;
            int    port = scPortStr.isEmpty() ? 50051 : Integer.parseInt(scPortStr);
            scArrowClient    = SparkConnectClient.builder().host(host).port(port).sessionId(sessionId).build();
            scArrowSessionId = sessionId;
            System.out.println("[sc_arrow] client built for session " + sessionId + " at " + host + ":" + port);
            return scArrowClient;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("sc_arrow metadata unreachable: " + e.getMessage(), e);
        }
    }

    private static synchronized void invalidateScArrowClient(String reason) {
        System.out.println("[sc_arrow] invalidating client: " + reason);
        scArrowClient    = null;
        scArrowSessionId = null;
    }

    /**
     * SparkConnect gRPC path — zero-copy Arrow IPC.
     * Uses SparkConnectClient.execute() directly, bypassing JDBC row deserialization.
     * Arrow IPC bytes stream from DataFusion → Java Arrow vectors → JSON.
     */
    private static String sailScArrowJson(String sql) throws Exception {
        for (int attempt = 0; attempt < 2; attempt++) {
            SparkConnectClient client = getScArrowClient();
            try {
                Plan plan = Plan.newBuilder()
                    .setRoot(Relation.newBuilder()
                        .setSql(SQL.newBuilder().setQuery(sql).build())
                        .build())
                    .build();

                StringBuilder json    = new StringBuilder("[");
                boolean       firstRow = true;

                CloseableIterator<ExecutePlanResponse> iter =
                    client.execute(plan, client.execute$default$2());
                try {
                    try (RootAllocator allocator = new RootAllocator()) {
                        java.util.Iterator<ExecutePlanResponse> jIter = iter.asJava();
                        while (jIter.hasNext()) {
                            ExecutePlanResponse resp = jIter.next();
                            if (!resp.hasArrowBatch()) continue;
                            org.sparkproject.connect.protobuf.ByteString data =
                                resp.getArrowBatch().getData();
                            if (data.isEmpty()) continue;
                            try (ArrowStreamReader reader =
                                    new ArrowStreamReader(data.newInput(), allocator)) {
                                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                                while (reader.loadNextBatch()) {
                                    List<FieldVector> vecs = root.getFieldVectors();
                                    List<String>      cols = new ArrayList<>();
                                    for (FieldVector v : vecs) cols.add(v.getName());
                                    int rows = root.getRowCount();
                                    for (int r = 0; r < rows; r++) {
                                        if (!firstRow) json.append(",");
                                        firstRow = false;
                                        json.append("{");
                                        for (int c = 0; c < vecs.size(); c++) {
                                            if (c > 0) json.append(",");
                                            json.append(jsonString(cols.get(c))).append(":");
                                            Object val = vecs.get(c).isNull(r)
                                                         ? null : vecs.get(c).getObject(r);
                                            json.append(toJsonValue(val));
                                        }
                                        json.append("}");
                                    }
                                }
                            }
                        }
                    }
                } finally {
                    iter.close();
                }
                json.append("]");
                return json.toString();

            } catch (Exception e) {
                String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
                if ((msg.contains("session") || msg.contains("not found") || msg.contains("closed")
                        || msg.contains("unavailable") || msg.contains("status_code=14")) && attempt == 0) {
                    invalidateScArrowClient(e.getMessage());
                    System.out.println("[sc_arrow] session error on first attempt — retrying with fresh session");
                    continue;
                }
                throw e;
            }
        }
        throw new RuntimeException("unreachable");
    }

    // ── ShardingSphere mode ────────────────────────────────────────────────────

    private static void runShardingMode() throws Exception {
        Class.forName("org.postgresql.Driver");

        try (Connection conn = DriverManager.getConnection(
                SS_JDBC_URL, SS_JDBC_USER, SS_JDBC_PASS)) {
            System.out.println("Connected to ShardingSphere Proxy at " + SS_JDBC_URL);
            ssRun(conn, "TopN – Top 20 entities by total capital (cross-shard SUM — EXPECT WRONG)", Q_TOPN);
            ssRun(conn, "CASE/WHEN – scenario breakdown per entity+date — EXPECT WRONG if cross-shard", Q_CASE_WHEN);
            ssRun(conn, "Complex aggregations – SA components by scenario", Q_AGGREGATIONS);
            ssRun(conn, "Partition pruning – shard1-only date range (cobdate route to shard1)", Q_PARTITION_PRUNE);
        } catch (SQLException e) {
            System.err.println("ERROR connecting to ShardingSphere Proxy: " + e.getMessage());
            System.err.println("  Make sure the proxy is running: infra/start_shardingsphere.sh");
        }
    }

    private static void ssRun(Connection conn, String label, String sql) {
        printHeader(label, "SHARDING");
        long t0 = System.currentTimeMillis();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            List<String> colNames = new ArrayList<>();
            for (int i = 1; i <= cols; i++) colNames.add(meta.getColumnLabel(i));

            List<Map<String, String>> rows = new ArrayList<>();
            while (rs.next()) {
                Map<String, String> row = new LinkedHashMap<>();
                for (int i = 1; i <= cols; i++) {
                    Object v = rs.getObject(i);
                    row.put(colNames.get(i - 1), v == null ? "null" : v.toString());
                }
                rows.add(row);
            }
            long ms = System.currentTimeMillis() - t0;
            printTable(rows, colNames);
            System.out.printf("Elapsed: %d ms%n%n", ms);
        } catch (SQLException e) {
            System.err.println("Query error: " + e.getMessage() + "\n");
        }
    }

    // ── Formatting ────────────────────────────────────────────────────────────

    private static void printHeader(String label, String mode) {
        System.out.println("=".repeat(72));
        System.out.printf("[%s] %s%n", mode, label);
        System.out.println("=".repeat(72));
    }

    private static void printTable(List<Map<String, String>> rows, List<String> cols) {
        if (rows.isEmpty()) { System.out.println("(0 rows)"); return; }
        int w = 18;
        StringBuilder hdr = new StringBuilder();
        for (String c : cols) {
            if (hdr.length() > 0) hdr.append(" | ");
            hdr.append(pad(c, w));
        }
        System.out.println(hdr);
        System.out.println("-".repeat(hdr.length()));
        for (Map<String, String> row : rows) {
            StringBuilder line = new StringBuilder();
            for (String c : cols) {
                if (line.length() > 0) line.append(" | ");
                String v = row.getOrDefault(c, "null");
                line.append(pad(v.length() > w ? v.substring(0, w - 1) + "…" : v, w));
            }
            System.out.println(line);
        }
        System.out.printf("(%d rows)%n", rows.size());
    }

    private static void printJsonTable(String json) {
        List<Map<String, String>> rows = parseJsonArray(json);
        if (rows.isEmpty()) { System.out.println("(0 rows)"); return; }
        List<String> cols = new ArrayList<>(rows.get(0).keySet());
        printTable(rows, cols);
    }

    private static List<Map<String, String>> parseJsonArray(String json) {
        List<Map<String, String>> result = new ArrayList<>();
        json = json.trim();
        if (!json.startsWith("[")) return result;
        int depth = 0, objStart = -1;
        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '{') { if (depth++ == 0) objStart = i; }
            else if (c == '}') {
                if (--depth == 0 && objStart >= 0) {
                    result.add(parseJsonObject(json.substring(objStart, i + 1)));
                    objStart = -1;
                }
            }
        }
        return result;
    }

    private static Map<String, String> parseJsonObject(String obj) {
        Map<String, String> map = new LinkedHashMap<>();
        obj = obj.trim();
        if (obj.startsWith("{")) obj = obj.substring(1, obj.length() - 1).trim();
        int i = 0;
        while (i < obj.length()) {
            while (i < obj.length() && (obj.charAt(i) == ',' || obj.charAt(i) == ' ')) i++;
            if (i >= obj.length()) break;
            String key = null, value = null;
            if (obj.charAt(i) == '"') {
                int end = obj.indexOf('"', i + 1);
                key = obj.substring(i + 1, end);
                i = end + 1;
            } else break;
            while (i < obj.length() && (obj.charAt(i) == ':' || obj.charAt(i) == ' ')) i++;
            if (i < obj.length() && obj.charAt(i) == '"') {
                int end = i + 1;
                while (end < obj.length() && !(obj.charAt(end) == '"' && obj.charAt(end - 1) != '\\')) end++;
                value = obj.substring(i + 1, end);
                i = end + 1;
            } else {
                int end = i;
                while (end < obj.length() && obj.charAt(end) != ',' && obj.charAt(end) != '}') end++;
                value = obj.substring(i, end).trim();
                i = end;
            }
            if (key != null) map.put(key, value);
        }
        return map;
    }

    private static String pad(String s, int w) {
        if (s.length() >= w) return s;
        return s + " ".repeat(w - s.length());
    }

    private static String jsonString(String s) {
        return "\"" + s.replace("\\", "\\\\")
                       .replace("\"", "\\\"")
                       .replace("\n", "\\n")
                       .replace("\r", "\\r")
                       .replace("\t", "\\t") + "\"";
    }
}
