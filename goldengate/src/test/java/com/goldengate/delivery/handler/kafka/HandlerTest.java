package com.goldengate.delivery.handler.kafka;

public class HandlerTest {
	public static void main(String[] args) throws Exception {
		KafkaProducerWrapper producer;
		producer = new KafkaProducerWrapper("kafka.properties");

		try {
			String input = "My test: This is a test string to make sure the encyption works. Need to make it longer. Very long. Just to make sure!";
		//	ProducerRecordWrapper event = new ProducerRecordWrapper(
			//		"test_table2", input.getBytes());
		//	producer.send(event);

		} catch (Exception e1) {
			// logger.error("Unable to process operation.", e1);
		}
	}
}
