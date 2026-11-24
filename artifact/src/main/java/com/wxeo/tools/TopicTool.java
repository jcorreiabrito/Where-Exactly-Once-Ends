package com.wxeo.tools;

import com.wxeo.config.Args;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Topic lifecycle helper for the runner (fresh-state protocol, docs/CONTRACTS.md §10) and the
 * native-stack smoke tests. Uses the Java AdminClient from the shipped jar, so the runner does not
 * depend on the Kafka distribution's tool classes (whose package names changed across 3.x).
 *
 * <pre>
 * java -cp artifact/target/wxeo-service.jar com.wxeo.tools.TopicTool \
 *   --action=create|delete|describe --bootstrap=127.0.0.1:9091 --topic=wxeo.src.&lt;run_id&gt; \
 *   [--partitions=3] [--replication-factor=3] [--min-insync=2] [--retention-ms=3600000]
 * </pre>
 *
 * <p>Prints one compact JSON line; exit code 0 on success.</p>
 */
public final class TopicTool {

    public static void main(String[] argv) {
        Args a = Args.parse(argv);
        String action = a.get("action", "describe");
        String bootstrap = a.get("bootstrap", "127.0.0.1:9091");
        String topic = a.get("topic", null);
        StringBuilder json = new StringBuilder("{\"schema\":\"wxeo.topic.v1\"");
        int exit = 0;
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("default.api.timeout.ms", "30000");
        props.put("request.timeout.ms", "20000");
        try (Admin admin = Admin.create(props)) {
            switch (action) {
                case "create" -> {
                    int partitions = a.getInt("partitions", 6);
                    short rf = (short) a.getInt("replication-factor", 3);
                    NewTopic nt = new NewTopic(topic, partitions, rf);
                    String minIsr = a.get("min-insync", "2");
                    nt.configs(java.util.Map.of("min.insync.replicas", minIsr));
                    admin.createTopics(List.of(nt)).all().get(60, TimeUnit.SECONDS);
                    json.append(",\"action\":\"create\",\"topic\":\"").append(topic)
                            .append("\",\"partitions\":").append(partitions)
                            .append(",\"replication_factor\":").append(rf)
                            .append(",\"min_insync_replicas\":").append(minIsr);
                }
                case "delete" -> {
                    admin.deleteTopics(List.of(topic)).all().get(60, TimeUnit.SECONDS);
                    json.append(",\"action\":\"delete\",\"topic\":\"").append(topic).append('"');
                }
                case "describe" -> {
                    TopicDescription d = admin.describeTopics(List.of(topic)).allTopicNames()
                            .get(30, TimeUnit.SECONDS).get(topic);
                    json.append(",\"action\":\"describe\",\"topic\":\"").append(topic)
                            .append("\",\"partitions\":[");
                    boolean first = true;
                    for (TopicPartitionInfo p : d.partitions()) {
                        if (!first) {
                            json.append(',');
                        }
                        first = false;
                        json.append("{\"partition\":").append(p.partition())
                                .append(",\"leader\":").append(p.leader() == null ? -1 : p.leader().id())
                                .append(",\"replicas\":").append(p.replicas().size())
                                .append(",\"isr\":").append(p.isr().size()).append('}');
                    }
                    json.append(']');
                }
                default -> throw new IllegalArgumentException("unknown --action: " + action);
            }
            json.append(",\"ok\":true}");
            System.out.println(json);
        } catch (Exception e) {
            json.append(",\"ok\":false,\"error\":\"")
                    .append(e.getClass().getSimpleName()).append(": ")
                    .append(e.getMessage() == null ? "" : e.getMessage().replace("\"", "'"))
                    .append("\"}");
            System.out.println(json);
            exit = 1;
        }
        System.exit(exit);
    }

    private TopicTool() {
    }
}
