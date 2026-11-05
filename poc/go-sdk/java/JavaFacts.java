import com.codingful.tandem.core.BucketHash;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.jdbc.JdbcOutboxRepository;

import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * POC harness: prints what the Java reference writer does on the inputs the Go POC checks, as JSON
 * the Go tests read (java-facts.json). Run with the tandem-core, tandem-jdbc and pgjdbc jars on the
 * classpath: java -cp ... JavaFacts.java <out.json> <jdbc url>
 */
public class JavaFacts {

    public static void main(String[] args) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Object> inputs = readInputs();
        out.put("buckets", buckets((List<String>) inputs.get("ids"), (List<Integer>) inputs.get("counts")));
        out.put("whitespace", whitespace());
        out.put("uri", uris((List<String>) inputs.get("uris")));
        out.put("sortedIds", new ArrayList<>(new TreeSet<>((List<String>) inputs.get("sortIds"))));
        out.put("lengths", lengths((List<String>) inputs.get("lengthIds")));
        out.put("rows", rows(args[1]));
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(Path.of(args[0])))) {
            w.print(Json.write(out));
        }
    }

    /** The shared inputs live in inputs.json beside this file, written by hand, read by both sides. */
    @SuppressWarnings("unchecked")
    static Map<String, Object> readInputs() throws Exception {
        return (Map<String, Object>) Json.read(Files.readString(Path.of("inputs.json")));
    }

    static List<Object> buckets(List<String> ids, List<Integer> counts) {
        List<Object> result = new ArrayList<>();
        for (String id : ids) {
            for (int count : counts) {
                result.add(Map.of("id", id, "count", count, "bucket", BucketHash.bucketFor(id, count)));
            }
        }
        return result;
    }

    /** Every code point Character.isWhitespace accepts: what String.isBlank treats as blank. */
    static List<Integer> whitespace() {
        List<Integer> cps = new ArrayList<>();
        for (int cp = 0; cp <= Character.MAX_CODE_POINT; cp++) {
            if (Character.isWhitespace(cp)) {
                cps.add(cp);
            }
        }
        return cps;
    }

    static List<Object> uris(List<String> candidates) {
        List<Object> result = new ArrayList<>();
        for (String s : candidates) {
            boolean ok;
            try {
                new URI(s);
                ok = true;
            } catch (Exception e) {
                ok = false;
            }
            result.add(Map.of("s", s, "ok", ok));
        }
        return result;
    }

    /** Whether the Java message model accepts each id as aggregateId and as aggregateType. */
    static List<Object> lengths(List<String> ids) {
        List<Object> result = new ArrayList<>();
        for (String id : ids) {
            result.add(Map.of("id", id,
                    "aggregateId", accepts(() -> base().aggregateId(id).aggregateType("T").build()),
                    "aggregateType", accepts(() -> base().aggregateId("a").aggregateType(id).build()),
                    "type", accepts(() -> base().aggregateId("a").aggregateType("T").type(id).build()),
                    "utf16", id.length(), "codePoints", id.codePointCount(0, id.length())));
        }
        return result;
    }

    static OutboxMessage.Builder base() {
        return OutboxMessage.builder().unsequenced().payload("{}".getBytes(StandardCharsets.UTF_8));
    }

    static boolean accepts(Runnable r) {
        try {
            r.run();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Inserts each row case through JdbcOutboxRepository inside a transaction, reads the stored row
     * back, and rolls back, so Java and Go can write the same cases to the same database.
     */
    static List<Object> rows(String url) throws Exception {
        List<Object> result = new ArrayList<>();
        for (RowCase c : RowCase.ALL) {
            try (Connection conn = DriverManager.getConnection(url)) {
                conn.setAutoCommit(false);
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("name", c.name);
                try {
                    new JdbcOutboxRepository(sameConnection(conn), c.bucketCount).insert(c.message.build());
                    entry.put("row", readRow(conn, c.aggregateId));
                } catch (Exception e) {
                    entry.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
                    Throwable cause = e.getCause();
                    if (cause instanceof SQLException sql) {
                        entry.put("sqlState", sql.getSQLState());
                    }
                }
                conn.rollback();
                result.add(entry);
            }
        }
        return result;
    }

    static Map<String, Object> readRow(Connection conn, String aggregateId) throws SQLException {
        try (var ps = conn.prepareStatement("SELECT aggregate_id, aggregate_type, type, bucket, seq, seq_source, "
                + "payload::text, headers::text, correlation_id, octet_length(correlation_id) AS cid_bytes, "
                + "char_length(correlation_id) AS cid_chars FROM tandem_outbox WHERE aggregate_id = ?")) {
            ps.setString(1, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    Object v = rs.getObject(i);
                    row.put(rs.getMetaData().getColumnLabel(i), v == null ? null : v.toString());
                }
                return row;
            }
        }
    }

    /** A DataSource handing out one connection whose close() does nothing: the caller's transaction. */
    static DataSource sameConnection(Connection conn) {
        Connection shared = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class}, (p, m, a) -> m.getName().equals("close") ? null : m.invoke(conn, a));
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (p, m, a) -> switch (m.getName()) {
                    case "getConnection" -> shared;
                    case "getParentLogger" -> Logger.getGlobal();
                    default -> null;
                });
    }

    record RowCase(String name, String aggregateId, int bucketCount, OutboxMessage.Builder message) {
        static final String EMOJI = "😀";
        static final List<RowCase> ALL = List.of(
                c("application", "poc-app", 256, b -> b.seq(7)),
                c("managed", "poc-managed", 256, OutboxMessage.Builder::managedSeq),
                c("none", "poc-none", 256, OutboxMessage.Builder::unsequenced),
                c("bucket-100-top-bit", "order-7", 100, OutboxMessage.Builder::unsequenced),
                c("headers-and-type", "poc-headers", 256, b -> b.unsequenced().type("OrderPlaced")
                        .contentType("application/json").header("content-type", "text/plain")
                        .header("dataschema", "https://example.com/s.json").header("x-html", "<a&b>")),
                c("correlation-ascii-300", "poc-cid-ascii", 256,
                        b -> b.unsequenced().header("correlation-id", "c".repeat(300))),
                c("correlation-emoji-at-255", "poc-cid-emoji", 256,
                        b -> b.unsequenced().header("correlation-id", "c".repeat(254) + EMOJI + "tail")),
                c("correlation-emoji-only", "poc-cid-emoji-only", 256,
                        b -> b.unsequenced().header("correlation-id", EMOJI.repeat(200))),
                c("payload-not-json", "poc-bad-json", 256, b -> b.unsequenced().payload("{nope".getBytes())),
                c("payload-unicode", "poc-unicode", 256, b -> b.unsequenced()
                        .payload("{\"s\":\"café \\u00e9 <&>\",\"n\":1.0e2,\"k\":{\"b\":1,\"a\":2}}".getBytes(StandardCharsets.UTF_8))),
                c("aggregate-type-128-emoji", "poc-type-emoji", 256,
                        b -> b.unsequenced().aggregateType(EMOJI.repeat(128))));

        static RowCase c(String name, String id, int count, java.util.function.Consumer<OutboxMessage.Builder> f) {
            OutboxMessage.Builder b = OutboxMessage.builder().aggregateId(id).aggregateType("PocAggregate")
                    .payload("{\"k\":1}".getBytes(StandardCharsets.UTF_8));
            f.accept(b);
            return new RowCase(name, id, count, b);
        }
    }

    /** Minimal JSON reader/writer, so the harness needs no library. */
    static final class Json {
        static String write(Object o) {
            StringBuilder sb = new StringBuilder();
            write(o, sb);
            return sb.toString();
        }

        static void write(Object o, StringBuilder sb) {
            if (o == null) {
                sb.append("null");
            } else if (o instanceof String s) {
                sb.append('"');
                for (char ch : s.toCharArray()) {
                    if (ch == '"' || ch == '\\') {
                        sb.append('\\').append(ch);
                    } else if (ch < 0x20 || Character.isSurrogate(ch)) {
                        sb.append(String.format("\\u%04x", (int) ch));
                    } else {
                        sb.append(ch);
                    }
                }
                sb.append('"');
            } else if (o instanceof Number || o instanceof Boolean) {
                sb.append(o);
            } else if (o instanceof Map<?, ?> m) {
                sb.append('{');
                boolean first = true;
                for (var e : m.entrySet()) {
                    if (!first) sb.append(',');
                    first = false;
                    write(e.getKey().toString(), sb);
                    sb.append(':');
                    write(e.getValue(), sb);
                }
                sb.append('}');
            } else if (o instanceof List<?> l) {
                sb.append('[');
                for (int i = 0; i < l.size(); i++) {
                    if (i > 0) sb.append(',');
                    write(l.get(i), sb);
                }
                sb.append(']');
            } else {
                throw new IllegalArgumentException(o.getClass().toString());
            }
        }

        static Object read(String s) {
            int[] pos = {0};
            return value(s, pos);
        }

        static Object value(String s, int[] p) {
            ws(s, p);
            char c = s.charAt(p[0]);
            if (c == '{') {
                Map<String, Object> m = new LinkedHashMap<>();
                p[0]++;
                ws(s, p);
                if (s.charAt(p[0]) == '}') { p[0]++; return m; }
                while (true) {
                    String k = (String) value(s, p);
                    ws(s, p); p[0]++;
                    m.put(k, value(s, p));
                    ws(s, p);
                    if (s.charAt(p[0]++) == '}') return m;
                }
            }
            if (c == '[') {
                List<Object> l = new ArrayList<>();
                p[0]++;
                ws(s, p);
                if (s.charAt(p[0]) == ']') { p[0]++; return l; }
                while (true) {
                    l.add(value(s, p));
                    ws(s, p);
                    if (s.charAt(p[0]++) == ']') return l;
                }
            }
            if (c == '"') {
                StringBuilder sb = new StringBuilder();
                p[0]++;
                while (true) {
                    char ch = s.charAt(p[0]++);
                    if (ch == '"') return sb.toString();
                    if (ch == '\\') {
                        char e = s.charAt(p[0]++);
                        switch (e) {
                            case 'u' -> { sb.append((char) Integer.parseInt(s.substring(p[0], p[0] + 4), 16)); p[0] += 4; }
                            case 'n' -> sb.append('\n');
                            case 't' -> sb.append('\t');
                            case 'r' -> sb.append('\r');
                            case 'b' -> sb.append('\b');
                            case 'f' -> sb.append('\f');
                            default -> sb.append(e);
                        }
                    } else {
                        sb.append(ch);
                    }
                }
            }
            int start = p[0];
            while (p[0] < s.length() && "-+.0123456789eEtruefalsn".indexOf(s.charAt(p[0])) >= 0) p[0]++;
            String tok = s.substring(start, p[0]);
            if (tok.equals("true")) return true;
            if (tok.equals("false")) return false;
            if (tok.equals("null")) return null;
            return Integer.parseInt(tok);
        }

        static void ws(String s, int[] p) {
            while (p[0] < s.length() && Character.isWhitespace(s.charAt(p[0]))) p[0]++;
        }
    }
}
