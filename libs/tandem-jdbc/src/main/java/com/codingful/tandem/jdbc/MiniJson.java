package com.codingful.tandem.jdbc;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A tiny, dependency-free JSON codec for the {@code headers} column only — a flat object of
 * string→string (LLD-jdbc §2). The write-side must not force a JSON library on the client (§1.3), and
 * {@code headers} is the only structured value the adapter itself serializes (the {@code payload} is
 * passed through as raw JSON text the client already produced).
 *
 * <p>The reader is deliberately <b>tolerant</b> (compatibility, AGENTS): it keeps string values,
 * reads a {@code null} value as an absent header, and preserves any other JSON value, scalar, array or
 * object, as its raw JSON text rather than failing — so a header written by a future/other writer never
 * breaks this reader. Absent
 * rather than {@code null} because a header map holds no null values: one would fail the row's mapping,
 * and with it the whole claim batch the row came in.
 */
final class MiniJson {

    private MiniJson() {
    }

    /** Serialize a flat string→string map to a JSON object. */
    static String writeObject(Map<String, String> map) {
        StringBuilder sb = new StringBuilder(16 + map.size() * 16);
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            writeString(sb, e.getKey());
            sb.append(':');
            if (e.getValue() == null) {
                sb.append("null");
            } else {
                writeString(sb, e.getValue());
            }
        }
        sb.append('}');
        return sb.toString();
    }

    // Byte-for-byte identical to PayloadJson.quote in tandem-admin (different module, so it can't
    // share this method) — keep them in sync if you touch either. Not worth a shared published type
    // for 18 lines of a closed, unchanging algorithm (RFC 8259 string escaping).
    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    /** Parse a flat JSON object into a string→string map. Returns an empty map for {@code null}/blank. */
    static Map<String, String> parseObject(String json) {
        Map<String, String> out = new LinkedHashMap<>();
        if (json == null) {
            return out;
        }
        int i = skipWs(json, 0);
        if (i >= json.length() || json.charAt(i) != '{') {
            return out;   // tolerate non-objects / empty
        }
        i++;
        i = skipWs(json, i);
        if (i < json.length() && json.charAt(i) == '}') {
            return out;
        }
        while (i < json.length()) {
            i = skipWs(json, i);
            if (charAt(json, i) != '"') {
                throw new IllegalArgumentException("expected a string key at index " + i + " in: " + json);
            }
            StringBuilder key = new StringBuilder();
            i = readString(json, i, key);
            i = skipWs(json, i);
            expect(json, i, ':');
            i = skipWs(json, i + 1);
            String value;
            if (charAt(json, i) == '"') {
                StringBuilder val = new StringBuilder();
                i = readString(json, i, val);
                value = val.toString();
            } else if (json.startsWith("null", i)) {
                value = null;   // absent: not put below
                i += 4;
            } else if (charAt(json, i) == '[' || charAt(json, i) == '{') {
                int start = i;
                i = skipNested(json, i);
                value = json.substring(start, i);   // tolerant: keep the raw array/object text
            } else {
                int start = i;
                while (i < json.length() && json.charAt(i) != ',' && json.charAt(i) != '}') {
                    i++;
                }
                value = json.substring(start, i).trim();   // tolerant: keep the raw scalar
            }
            if (value != null) {
                out.put(key.toString(), value);
            }
            i = skipWs(json, i);
            char c = charAt(json, i);
            if (c == ',') {
                i++;
                continue;
            }
            if (c == '}') {
                break;
            }
            throw new IllegalArgumentException("expected ',' or '}' at index " + i + " in: " + json);
        }
        return out;
    }

    private static int readString(String json, int i, StringBuilder out) {
        // json.charAt(i) == '"'
        i++;
        while (i < json.length()) {
            char c = json.charAt(i++);
            if (c == '"') {
                return i;
            }
            if (c == '\\') {
                if (i >= json.length()) {
                    throw new IllegalArgumentException("dangling escape at end of: " + json);
                }
                char esc = json.charAt(i++);
                switch (esc) {
                    case '"' -> out.append('"');
                    case '\\' -> out.append('\\');
                    case '/' -> out.append('/');
                    case 'n' -> out.append('\n');
                    case 'r' -> out.append('\r');
                    case 't' -> out.append('\t');
                    case 'b' -> out.append('\b');
                    case 'f' -> out.append('\f');
                    case 'u' -> {
                        if (i + 4 > json.length()) {
                            throw new IllegalArgumentException("truncated \\u escape in: " + json);
                        }
                        out.append((char) Integer.parseInt(json.substring(i, i + 4), 16));
                        i += 4;
                    }
                    default -> throw new IllegalArgumentException("bad escape \\" + esc + " in: " + json);
                }
            } else {
                out.append(c);
            }
        }
        throw new IllegalArgumentException("unterminated string in: " + json);
    }

    /**
     * The index just past the array or object opening at {@code i}, found by balancing brackets outside
     * strings: a bracket or comma inside a string value belongs to the value, not to the structure.
     */
    private static int skipNested(String json, int i) {
        int depth = 0;
        while (i < json.length()) {
            char c = json.charAt(i);
            if (c == '"') {
                i = readString(json, i, new StringBuilder());
                continue;
            }
            if (c == '[' || c == '{') {
                depth++;
            } else if (c == ']' || c == '}') {
                depth--;
                if (depth == 0) {
                    return i + 1;
                }
            }
            i++;
        }
        throw new IllegalArgumentException("unterminated array or object in: " + json);
    }

    /** {@code charAt} with a bounds check, so truncated input fails like any other malformed JSON. */
    private static char charAt(String json, int i) {
        if (i >= json.length()) {
            throw new IllegalArgumentException("unexpected end of input in: " + json);
        }
        return json.charAt(i);
    }

    private static int skipWs(String json, int i) {
        while (i < json.length() && Character.isWhitespace(json.charAt(i))) {
            i++;
        }
        return i;
    }

    private static void expect(String json, int i, char c) {
        if (i >= json.length() || json.charAt(i) != c) {
            throw new IllegalArgumentException("expected '" + c + "' at index " + i + " in: " + json);
        }
    }
}
