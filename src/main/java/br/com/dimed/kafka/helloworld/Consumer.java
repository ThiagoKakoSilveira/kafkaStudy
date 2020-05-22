package br.com.dimed.kafka.helloworld;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {

        Logger log = LoggerFactory.getLogger(Consumer.class.getName());

        String servidorKafka = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topico = "primeiro_topico";

        //Criando as propriedades essenciais
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidorKafka);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Criando o Consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Inscrever um Consumer em um tópico

        consumer.subscribe(Collections.singleton(topico));
//        consumer.subscribe(Arrays.asList(topico)); Na maioria das vezes se usa esse apenas se usa o de cima para
//        garantir q apenas uma instância do tópico
        //votacao de dados

        while (true){
            ConsumerRecords<String, String> registros = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> registro : registros) {
                log.info("Chave: " + registro.key() + ", Valor: " + registro.value());
                log.info("Partição: "+ registro.partition() + ", Offset: "+registro.offset());
            }

        }
    }
}
