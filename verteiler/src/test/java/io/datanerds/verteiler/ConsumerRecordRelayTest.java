package io.datanerds.verteiler;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerRecordRelayTest {

    @Mock
    private Consumer<Integer, String> consumer;

    @Mock
    private BlockingQueueConsumer<Integer, String> blockingQueueConsumer;

    private ConsumerRecord<Integer, String> record = new ConsumerRecord<>("testTopic", 1, 42, 1234, "SomeValue");

    @Test
    public void relayAndCommitOffset() throws Exception {
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> map = new HashMap<>();
        map.put(new TopicPartition("testTopic", 1), Arrays.asList(record));
        ConsumerRecords<Integer, String> records = new ConsumerRecords<>(map);

        when(consumer.poll(anyLong())).thenReturn(records);

        ConsumerRecordRelay<Integer, String> relay = new ConsumerRecordRelay<>(consumer, blockingQueueConsumer);
        relay.setOffset(record);
        new Thread(relay).start();

        verify(blockingQueueConsumer, timeout(1000).atLeastOnce()).relay(eq(record));
        verify(consumer, timeout(1000).atLeastOnce()).commitAsync(any(), any());
        relay.stop();
    }

    @Test
    public void relayWithException() throws Exception {
        when(consumer.poll(anyLong())).thenThrow(RuntimeException.class);

        ConsumerRecordRelay<Integer, String> relay = new ConsumerRecordRelay<>(consumer, blockingQueueConsumer);
        new Thread(relay).start();
        verify(blockingQueueConsumer, never()).relay(record);
        verify(consumer, times(1)).poll(anyLong());
        relay.stop();
    }

}