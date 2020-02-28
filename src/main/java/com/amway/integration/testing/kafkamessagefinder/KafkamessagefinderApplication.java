package com.amway.integration.testing.kafkamessagefinder;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.PartitionInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
public class KafkamessagefinderApplication implements ApplicationRunner
{
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkamessagefinderApplication.class);
	
	@Override
	public void run(ApplicationArguments args) throws Exception 
	{
		LOGGER.info("Application started with command-line arguments: {}", Arrays.toString(args.getSourceArgs()));
		KafkaConsumer<String, String> consumer = null;
		try
		{
			// read args
			String configfile = null;
			String topic = null;
			int count = 1;
			int partitionID = -1;
			long offset = -1L;
			List<String> countArg = args.getOptionValues("count");
			if(countArg != null && countArg.size() == 1)
			{
				count = Integer.parseInt(countArg.get(0));
			} else {
				LOGGER.info("--count assumed as 1");
			}
			List<String> correlationIds = args.getOptionValues("correlation");
			if(correlationIds != null || correlationIds.size() > 1)
			{
				String correlationId = correlationIds.get(0);
				LOGGER.info("attempting to parse log correlation ID: " + correlationId);
				Matcher m = Pattern.compile("^([^/]+)/([^/]+)/([^/]+)/([^/]+)$").matcher(correlationId);
				if(!m.matches())
				{
					LOGGER.error("Unable to parse correlationId '" + correlationId + "'");
					return;
				}
				configfile = String.format("config/%s.properties", m.group(1));
				topic = m.group(2);
				partitionID = Integer.parseInt(m.group(3));
				offset = Long.parseLong(m.group(4));
			} else {
				boolean isValid = true;
				List<String> topicArg = args.getOptionValues("topic");
				if(topicArg == null || topicArg.size() != 1)
				{
					LOGGER.error("just one --topic is required");
					isValid = false; 
				} else {
					topic = topicArg.get(0);
				}
				
				List<String> partitionArg = args.getOptionValues("partition");
				if(partitionArg == null || partitionArg.size() != 1)
				{
					LOGGER.error("just one --partition is required");
					isValid = false; 
				} else {
					partitionID = Integer.parseInt(partitionArg.get(0));
				}
				
				List<String> offsetArgs = args.getOptionValues("offset");
				if(offsetArgs == null || offsetArgs.size() != 1)
				{
					LOGGER.error("just one --offset is required");
					isValid = false; 
				} else {
					offset = Long.parseLong(offsetArgs.get(0));
				}
				
				List<String> configfileArg = args.getOptionValues("config");
				if(configfileArg == null || configfileArg.size() != 1)
				{
					LOGGER.error("just one --config is required");
					isValid = false; 
				} else {
					configfile = configfileArg.get(0);
				}
				
				if(!isValid)
				{	return; }
			}
			// setup consumer
			LOGGER.info("Connecting to broker");
			consumer = createConsumer(configfile);
			// list partitions
			LOGGER.info("listing partitions");
			List<PartitionInfo> partitions = consumer.partitionsFor(topic);
			if(partitions == null)
			{
				LOGGER.error("No partitions found for the topic '" + topic + "'");
				return;
			}
			PartitionInfo part = null;
			for(Iterator<PartitionInfo> i = partitions.listIterator(); i.hasNext();)
			{
				PartitionInfo pi = i.next();
				LOGGER.info("Found partition " + pi.partition());
				if(partitionID == pi.partition())
				{	part = pi; break; }
			}
			if(part == null)
			{
				LOGGER.error("Unable to find requested partition!");
				return;
			}
			// read messages
			TopicPartition topicPart = new TopicPartition(topic, partitionID);
			consumer.assign(Collections.singletonList(topicPart));
			consumer.poll(30000L);
			consumer.seek(topicPart, offset);
			ConsumerRecords<String,String> records = consumer.poll(30000L);
			if(!records.isEmpty())
			{
				LOGGER.error("found " + records.count() + " records");
				int printed = 0;
				PRINT_RECORDS:for(Iterator<ConsumerRecord<String,String>> i = records.iterator(); i.hasNext();)
				{
					if(printed >= count)
					{	break PRINT_RECORDS; }
					ConsumerRecord<String,String> rec = i.next();
					LOGGER.info("***** Found headers record for topic " + rec.topic() + ", partition " + rec.partition() + ", offset " + rec.offset() + " with key '" + rec.key() + "' at " + DateTimeFormatter.ISO_DATE_TIME.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(rec.timestamp​()),ZoneId.systemDefault())) + " *****");
					for(Iterator<Header> ih = rec.headers().iterator(); ih.hasNext();)
					{
						Header header = ih.next();
						LOGGER.info(String.format("'%s' -> '%s'", header.key(), new String(header.value(), StandardCharsets.UTF_8)));
					}
					LOGGER.info("===== Found data record for topic " + rec.topic() + ", partition " + rec.partition() + ", offset " + rec.offset() + " with key '" + rec.key() + "' ========");
					LOGGER.info(rec.value());
					printed++;
				}
			} else {
				LOGGER.error("No records found at topic/offset");
			}
		} catch(Exception e) {
			LOGGER.error("Consume failed: " + e.getMessage(), e);
		} finally {
			if(consumer != null)
			{
				try { consumer.unsubscribe(); } catch(Exception e) { }
				try { consumer.close(); } catch(Exception e) { }
			}
		}
	}
	
	
	private KafkaConsumer<String, String> createConsumer(String propfile)
		throws Exception
	{
		
		//Class.forName("org.apache.kafka.common.security.plain.PlainLoginModule");
		Properties kafkaSessionConfig = new Properties();
		kafkaSessionConfig.load(new FileInputStream(propfile));
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaSessionConfig);
		return consumer;
  }
	
	public static void main(String[] args) 
	{
		//Thread.currentThread().setContextClassLoader(null);
		SpringApplication.run(KafkamessagefinderApplication.class, args);
	}

}
