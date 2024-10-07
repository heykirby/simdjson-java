package org.simdjson;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class Parse2VsJacksonBenchMark {
    @Param({"/twitter.json"})
    String fileName;
    private byte[] buffer;
    private final SimdJsonParser2 parser = new SimdJsonParser2("statuses.0.metadata", "metadata.0.created_at", "metadata.0.id",
            "statuses.1.metadata", "metadata.1.created_at", "metadata.1.id");
    private final ObjectMapper MAPPER = new ObjectMapper();

    @Setup(Level.Trial)
    public void setup() throws IOException {
        try (InputStream is = ParseBenchmark.class.getResourceAsStream(fileName)) {
            assert is != null;
            buffer = is.readAllBytes();
        }
    }

    @Benchmark
    public void parseBySimdJson() {
        String[] result = parser.parse(buffer, buffer.length);
    }

    @Benchmark
    public void parseByJackson() throws Exception {
        ArrayNode arrayNode = (ArrayNode) MAPPER.readTree(buffer).path("statuses");
        String[] result = new String[6];
        result[0] = arrayNode.get(0).path("metadata").toString();
        result[1] = arrayNode.get(0).path("created_at").toString();
        result[2] = arrayNode.get(0).path("id").toString();
        result[3] = arrayNode.get(1).path("metadata").toString();
        result[4] = arrayNode.get(1).path("created_at").toString();
        result[5] = arrayNode.get(1).path("id").toString();
    }
}
