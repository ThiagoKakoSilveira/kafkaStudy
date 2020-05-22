package br.com.dimed.kafka.helloworld;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        String servidorKafka = "127.0.0.1:9092";
        // Criando as 3 propriedades essenciais

        // Jeito feio de fazer

//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.server", servidorKafka);
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Jeito bonito

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidorKafka);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // criando os produdores

        KafkaProducer<String, String> produtor = new KafkaProducer<String, String>(properties);

        //Criado um registro
        ProducerRecord<String, String> registro =
                new ProducerRecord<String, String>("primeiro_topico", "Hello World!");

        // enviando as msgs

        // envio assíncrono
        produtor.send(registro);

        // descarrega os dados
        produtor.flush();

        // descarrega os dados e fecha o produtor
        produtor.close();
    }
}
