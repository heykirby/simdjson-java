package org.simdjson;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;
import lombok.RequiredArgsConstructor;

public class SimdJsonParser2 {

    @Data
    @RequiredArgsConstructor
    static class JsonNode {
        private long version = 0;
        private boolean isLeaf = false;
        private final String name;
        private String value = null;
        private JsonNode parent = null;
        private Map<String, JsonNode> children = new HashMap<>();
        private int start = -1;
        private int end = -1;
    }

    private final SimdJsonParser parser;
    private BitIndexes bitIndexes;
    private final JsonNode root = new JsonNode(null);
    private final JsonNode[] row;
    private final String[] result;
    private final String[] emptyResult;
    private JsonNode ptr;
    private byte[] buffer;
    private final int targetParseNum;
    private long currentVersion = 0;
    // pruning, when alreadyProcessedCols == NUM
    private long alreadyProcessedCols = 0;

    public SimdJsonParser2(String... args) {
        parser = new SimdJsonParser();
        targetParseNum = args.length;
        row = new JsonNode[targetParseNum];
        result = new String[targetParseNum];
        emptyResult = new String[targetParseNum];
        for (int i = 0; i < args.length; i++) {
            emptyResult[i] = null;
        }
        for (int i = 0; i < targetParseNum; i++) {
            JsonNode cur = root;
            String[] paths = args[i].split("\\.");
            for (int j = 0; j < paths.length; j++) {
                if (!cur.getChildren().containsKey(paths[j])) {
                    JsonNode child = new JsonNode(paths[j]);
                    cur.getChildren().put(paths[j], child);
                    child.setParent(cur);
                }
                cur = cur.getChildren().get(paths[j]);
            }
            cur.setLeaf(true);
            row[i] = cur;
        }

    }

    public String[] parse(byte[] buffer, int len) {
        this.bitIndexes = parser.buildBitIndex(buffer, len);
        if (buffer == null || buffer.length == 0) {
            return emptyResult;
        }
        this.alreadyProcessedCols = 0;
        this.currentVersion++;
        this.ptr = root;
        this.buffer = buffer;

        switch (buffer[bitIndexes.peek()]) {
            case '{' -> {
                parseMap();
            }
            case '[' -> {
                parseList();
            }
            default -> {
                throw new RuntimeException("invalid json format");
            }
        }
        return getResult();
    }

    private void parseElement(String fieldName) {
        if (fieldName == null) {
            int start = bitIndexes.advance();
            int realEnd = bitIndexes.advance();
            while (realEnd > start) {
                if (buffer[--realEnd] == '"') {
                    break;
                }
            }
            fieldName = new String(buffer, start + 1, realEnd - start - 1);
        }
        if (!ptr.getChildren().containsKey(fieldName)) {
            skip(false);
            return;
        }
        ptr = ptr.getChildren().get(fieldName);
        switch (buffer[bitIndexes.peek()]) {
            case '{' -> {
                parseMap();
            }
            case '[' -> {
                parseList();
            }
            default -> {
                ptr.setValue(skip(true));
                ptr.setVersion(currentVersion);
                ++alreadyProcessedCols;
            }
        }
        ptr = ptr.getParent();
    }

    private void parseMap() {
        if (ptr.getChildren() == null) {
            ptr.setValue(skip(true));
            ptr.setVersion(currentVersion);
            ++alreadyProcessedCols;
            return;
        }
        ptr.setStart(bitIndexes.peek());
        bitIndexes.advance();
        while (bitIndexes.hasNext() && buffer[bitIndexes.peek()] != '}' && alreadyProcessedCols < targetParseNum) {
            parseElement(null);
            if (buffer[bitIndexes.peek()] == ',') {
                bitIndexes.advance();
            }
        }
        ptr.setEnd(bitIndexes.peek());
        if (ptr.isLeaf()) {
            ptr.setValue(new String(buffer, ptr.getStart(), ptr.getEnd() - ptr.getStart() + 1));
            ptr.setVersion(currentVersion);
            ++alreadyProcessedCols;
        }
        bitIndexes.advance();
    }

    private void parseList() {
        if (ptr.getChildren() == null) {
            ptr.setValue(skip(true));
            ptr.setVersion(currentVersion);
            ++alreadyProcessedCols;
            return;
        }
        ptr.setStart(bitIndexes.peek());
        bitIndexes.advance();
        int i = 0;
        while (bitIndexes.hasNext() && buffer[bitIndexes.peek()] != ']' && alreadyProcessedCols < targetParseNum) {
            parseElement("" + i);
            if (buffer[bitIndexes.peek()] == ',') {
                bitIndexes.advance();
            }
            i++;
        }
        ptr.setEnd(bitIndexes.peek());
        if (ptr.isLeaf()) {
            ptr.setValue(new String(buffer, ptr.getStart(), ptr.getEnd() - ptr.getStart() + 1));
            ptr.setVersion(currentVersion);
            ++alreadyProcessedCols;
        }
        bitIndexes.advance();
    }

    private String skip(boolean retainValue) {
        int i = 0;
        int start = retainValue ? bitIndexes.peek() : 0;
        switch (buffer[bitIndexes.peek()]) {
            case '{' -> {
                i++;
                while (i > 0) {
                    bitIndexes.advance();
                    if (buffer[bitIndexes.peek()] == '{') {
                        i++;
                    } else if (buffer[bitIndexes.peek()] == '}') {
                        i--;
                    }
                }
                int end = bitIndexes.peek();
                bitIndexes.advance();
                return retainValue ? new String(buffer, start, end - start + 1) : null;
            }
            case '[' -> {
                i++;
                while (i > 0) {
                    bitIndexes.advance();
                    if (buffer[bitIndexes.peek()] == '[') {
                        i++;
                    } else if (buffer[bitIndexes.peek()] == ']') {
                        i--;
                    }
                }
                int end = bitIndexes.peek();
                bitIndexes.advance();
                return retainValue ? new String(buffer, start, end - start + 1) : null;
            }
            case '"' -> {
                bitIndexes.advance();
                int realEnd = bitIndexes.peek();
                while (realEnd > start) {
                    if (buffer[--realEnd] == '"') {
                        break;
                    }
                }
                return retainValue ? new String(buffer, start + 1, realEnd - start - 1) : null;
            }
            default -> {
                bitIndexes.advance();
                int realEnd = bitIndexes.peek();
                while (realEnd >= start) {
                    --realEnd;
                    if (buffer[realEnd] >= '0' && buffer[realEnd] <= '9') {
                        break;
                    }
                }
                return retainValue ? new String(buffer, start, realEnd - start + 1) : null;
            }
        }
    }

    private String[] getResult() {
        for (int i = 0; i < targetParseNum; i++) {
            if (row[i].getVersion() < currentVersion) {
                result[i] = null;
                continue;
            }
            result[i] = row[i].getValue();
        }
        return result;
    }
}
