/*
 * Copyright 2019-2023 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.batch.item.kafka;

import java.time.Duration;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import org.springframework.batch.item.ExecutionContext;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockConstruction;

/**
 * @author Mathieu Ouellet
 * @author Mahmoud Ben Hassine
 */
class KafkaItemReaderTests {

	@Test
	void testValidation() {
		Exception exception = assertThrows(IllegalArgumentException.class,
				() -> new KafkaItemReader<>(null, "topic", 0));
		assertEquals("Consumer properties must not be null", exception.getMessage());

		exception = assertThrows(IllegalArgumentException.class,
				() -> new KafkaItemReader<>(new Properties(), "topic", 0));
		assertEquals("bootstrap.servers property must be provided", exception.getMessage());

		Properties consumerProperties = new Properties();
		consumerProperties.put("bootstrap.servers", "mockServer");
		exception = assertThrows(IllegalArgumentException.class,
				() -> new KafkaItemReader<>(consumerProperties, "topic", 0));
		assertEquals("group.id property must be provided", exception.getMessage());

		consumerProperties.put("group.id", "1");
		exception = assertThrows(IllegalArgumentException.class,
				() -> new KafkaItemReader<>(consumerProperties, "topic", 0));
		assertEquals("key.deserializer property must be provided", exception.getMessage());

		consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
		exception = assertThrows(IllegalArgumentException.class,
				() -> new KafkaItemReader<>(consumerProperties, "topic", 0));
		assertEquals("value.deserializer property must be provided", exception.getMessage());

		consumerProperties.put("value.deserializer", StringDeserializer.class.getName());
		exception = assertThrows(IllegalArgumentException.class,
				() -> new KafkaItemReader<>(consumerProperties, "", 0));
		assertEquals("Topic name must not be null or empty", exception.getMessage());

		exception = assertThrows(Exception.class, () -> new KafkaItemReader<>(consumerProperties, "topic"));
		assertEquals("At least one partition must be provided", exception.getMessage());

		KafkaItemReader<String, String> reader = new KafkaItemReader<>(consumerProperties, "topic", 0);

		exception = assertThrows(IllegalArgumentException.class, () -> reader.setPollTimeout(null));
		assertEquals("pollTimeout must not be null", exception.getMessage());

		exception = assertThrows(IllegalArgumentException.class, () -> reader.setPollTimeout(Duration.ZERO));
		assertEquals("pollTimeout must not be zero", exception.getMessage());

		exception = assertThrows(IllegalArgumentException.class, () -> reader.setPollTimeout(Duration.ofSeconds(-1)));
		assertEquals("pollTimeout must not be negative", exception.getMessage());
	}

	@Test
	void testExecutionContextSerializationWithJackson() throws Exception {
		Properties consumerProperties = new Properties();
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "mockServer");
		consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		KafkaItemReader<String, String> reader = new KafkaItemReader<>(consumerProperties, "testTopic", 0, 1);
		reader.setName("kafkaItemReader");

		// Simulate how Jackson would serialize/deserialize the offset data
		ExecutionContext executionContext = new ExecutionContext();
		List<Map<String, Object>> offsets = new ArrayList<>();
		
		Map<String, Object> offset1 = new HashMap<>();
		offset1.put("topic", "testTopic");
		offset1.put("partition", 0);
		offset1.put("offset", 100L);
		offsets.add(offset1);
		
		Map<String, Object> offset2 = new HashMap<>();
		offset2.put("topic", "testTopic");
		offset2.put("partition", 1);
		offset2.put("offset", 200L);
		offsets.add(offset2);

		// Simulate Jackson serialization/deserialization
		ObjectMapper objectMapper = new ObjectMapper();
		String serialized = objectMapper.writeValueAsString(offsets);
		List<Map<String, Object>> deserializedOffsets = objectMapper.readValue(serialized, List.class);

		executionContext.put("kafkaItemReader.topic.partition.offsets", deserializedOffsets);

		try (MockedConstruction<org.apache.kafka.clients.consumer.KafkaConsumer> mockedConstruction = mockConstruction(
				org.apache.kafka.clients.consumer.KafkaConsumer.class)) {

			reader.open(executionContext);

			ExecutionContext newContext = new ExecutionContext();
			reader.update(newContext);

			List<Map<String, Object>> savedOffsets = (List<Map<String, Object>>) newContext.get("kafkaItemReader.topic.partition.offsets");
			assertNotNull(savedOffsets);
			assertEquals(2, savedOffsets.size());
			
			boolean foundPartition0 = false;
			boolean foundPartition1 = false;
			for (Map<String, Object> offsetEntry : savedOffsets) {
				String topic = (String) offsetEntry.get("topic");
				Integer partition = (Integer) offsetEntry.get("partition");
				Long offset = (Long) offsetEntry.get("offset");
				
				assertEquals("testTopic", topic);
				assertNotNull(offset);
				
				if (partition == 0) {
					foundPartition0 = true;
					assertEquals(101L, offset);  // restored offset + 1
				} else if (partition == 1) {
					foundPartition1 = true;
					assertEquals(201L, offset);  // restored offset + 1
				}
			}
			
			assertTrue(foundPartition0);
			assertTrue(foundPartition1);
		}
	}

	@Test
	void testExecutionContextWithStringKeys() throws Exception {
		Properties consumerProperties = new Properties();
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "mockServer");
		consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		KafkaItemReader<String, String> reader = new KafkaItemReader<>(consumerProperties, "testTopic", 0, 1);
		reader.setName("kafkaItemReader");

		// Create ExecutionContext with list of maps (as it would be after Jackson
		// deserialization)
		ExecutionContext executionContext = new ExecutionContext();
		List<Map<String, Object>> storedOffsets = new ArrayList<>();
		
		Map<String, Object> offset1 = new HashMap<>();
		offset1.put("topic", "testTopic");
		offset1.put("partition", 0);
		offset1.put("offset", 100L);
		storedOffsets.add(offset1);
		
		Map<String, Object> offset2 = new HashMap<>();
		offset2.put("topic", "testTopic");
		offset2.put("partition", 1);
		offset2.put("offset", 200L);
		storedOffsets.add(offset2);
		
		executionContext.put("kafkaItemReader.topic.partition.offsets", storedOffsets);

		try (MockedConstruction<org.apache.kafka.clients.consumer.KafkaConsumer> mockedConstruction = mockConstruction(
				org.apache.kafka.clients.consumer.KafkaConsumer.class)) {

			reader.open(executionContext);

			// Verify that offsets are saved correctly
			ExecutionContext newContext = new ExecutionContext();
			reader.update(newContext);

			List<Map<String, Object>> savedOffsets = (List<Map<String, Object>>) newContext.get("kafkaItemReader.topic.partition.offsets");
			assertNotNull(savedOffsets);
			assertEquals(2, savedOffsets.size());
		}
	}
}
