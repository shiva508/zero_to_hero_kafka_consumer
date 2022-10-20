package com.pool.configuration;

import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.pool.basic.producer.model.PageView;

@Configuration
public class ConsumerConfiguraton {

	@Bean
	public Function<KStream<String, PageView>, KStream<String, Long>> counter() {
		return psv->psv
				
				//.filter((s,pageview)->pageview.duriation()>100)
				.map((s,pageview)->new KeyValue<>(pageview.page(),0L))
				.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
				.count(Materialized.as("pcmv"))
				.toStream();
				
	}
	@Bean
	public Consumer<KTable<String, Long>> loggerone(){
		return counts->
			   counts.toStream()
			   .foreach((key, value)->System.out.println("Page:"+key+",count:"+value));
	}

}
