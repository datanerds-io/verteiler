package io.datanerds.verteiler.it_test;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.api.FixedPortTestUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public abstract class EmbeddedKafkaTest {

    protected static String zkConnect;
    protected static String kafkaConnect;

    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static ZkUtils zkUtils;
    private static ZkConnection zkConnection;
    private static KafkaServer kafkaServer;

    private static List<KafkaServer> servers = new ArrayList<>();

    public static void setUp() {
        // setup Zookeeper
        zkServer = new EmbeddedZookeeper();
        zkConnect = "127.0.0.1:" + zkServer.port();
        zkConnection = new ZkConnection(zkConnect);
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = new ZkUtils(zkClient, zkConnection, false);

        // setup Broker
        Properties props = FixedPortTestUtils.createBrokerConfigs(1, zkConnect, true, false).apply(0);
        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        servers.add(kafkaServer);
        kafkaConnect = (String) kafkaServer.config().props().get("listeners");
    }

    public static void tearDown() throws InterruptedException {
        zkClient.close();
        zkConnection.close();
        kafkaServer.shutdown();
        zkServer.shutdown();
        servers.clear();
    }

    protected void createTopic(String topic) {
        AdminUtils.createTopic(zkUtils, topic, 20, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0,
                10000);
    }
}
