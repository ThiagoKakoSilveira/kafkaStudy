package br.com.dimed.kafka.helloworld;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerProcurarEAtribuir {

    public static void main(String[] args) {

        Logger log = LoggerFactory.getLogger(ConsumerProcurarEAtribuir.class.getName());

        String servidorKafka = "127.0.0.1:9092";
        String topico = "primeiro_topico";

        //Criando as propriedades essenciais
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidorKafka);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Criando o Consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //buscar e atriubuir são na maioria das vezes usados para reproduzir um dado ou buscar uma msg específica
                                            //|
                                            //V

        //attribuição
        TopicPartition particaoParaLerDe = new TopicPartition(topico, 1);
        Long offsetParaLerDe = 5L;
        consumer.assign(Arrays.asList(particaoParaLerDe));

        //procurar

        consumer.seek(particaoParaLerDe, offsetParaLerDe);

        int numeroDeMsgParaLer = 5;
        boolean continuarLendo = true;
        int numeroDeMsgLidas = 0;

        //votacao de dados
        while (continuarLendo){
            ConsumerRecords<String, String> registros = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> registro : registros) {
                numeroDeMsgLidas += 1;
                log.info("Chave: " + registro.key() + ", Valor: " + registro.value());
                log.info("Partição: "+ registro.partition() + ", Offset: "+registro.offset());
                if (numeroDeMsgLidas >= numeroDeMsgParaLer){
                    continuarLendo = false;
                    break;
                }
            }
        }
        log.info("Fechando a aplicação");
    }
}
