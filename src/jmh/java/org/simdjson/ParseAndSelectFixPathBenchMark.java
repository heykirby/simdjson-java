package org.simdjson;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.checkerframework.checker.units.qual.A;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ParseAndSelectFixPathBenchMark {
    @Param({"/twitter.json"})
    String fileName;
    private byte[] buffer;
    private final SimdJsonParser parser = new SimdJsonParser();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SimdJsonParser2 parser2 = new SimdJsonParser2(
            "statuses.0.user.default_profile", "statuses.0.user.screen_name",
            "statuses.1.user.default_profile", "statuses.1.user.screen_name",
            "statuses.2.user.default_profile", "statuses.2.user.screen_name",
            "statuses.3.user.default_profile", "statuses.3.user.screen_name",
            "statuses.4.user.default_profile", "statuses.4.user.screen_name",
            "statuses.5.user.default_profile", "statuses.5.user.screen_name",
            "statuses.6.user.default_profile", "statuses.6.user.screen_name",
            "statuses.7.user.default_profile", "statuses.7.user.screen_name",
            "statuses.8.user.default_profile", "statuses.8.user.screen_name",
            "statuses.9.user.default_profile", "statuses.9.user.screen_name");

    @Benchmark
    public String[] parseMultiValuesForFixPaths_SimdJson() {
        JsonValue jsonValue = parser.parse(buffer, buffer.length);
        String[] result = new String[20];
        Iterator<JsonValue> tweets = jsonValue.get("statuses").arrayIterator();
        int i = 0;
        while (tweets.hasNext() && i++ < 10) {
            JsonValue tweet = tweets.next();
            result[i] = tweet.get("user").get("default_profile").asString();
            result[i + 1] = tweet.get("user").get("screen_name").asString();
        }
        return result;
    }

    @Benchmark
    public String[] parseMultiValuesForFixPaths_SimdJson2() {
        return parser2.parse(buffer, buffer.length);
    }

    @Benchmark
    public String[] parseMultiValuesForFixPaths_Jackson() throws IOException {
        JsonNode jacksonJsonNode = objectMapper.readTree(buffer);
        String[] result = new String[20];
        ArrayNode tweets = (ArrayNode) jacksonJsonNode.get("statuses");
        for (int i = 0; i < 10; i++) {
            result[i] = tweets.get(i).path("user").path("default_profile").textValue();
            result[i + 1] = tweets.get(i).path("user").path("screen_name").textValue();
        }
        return result;
    }
}
