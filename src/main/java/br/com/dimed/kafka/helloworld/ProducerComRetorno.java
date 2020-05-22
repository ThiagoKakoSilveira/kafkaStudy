package br.com.dimed.kafka.helloworld;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerComRetorno {

    public static void main(String[] args) {
        String servidorKafka = "127.0.0.1:9092";

        final Logger log = LoggerFactory.getLogger(ProducerComRetorno.class);
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

        // enviando as msgs

        // envio assíncrono
        for (int i=0; i<10; i++){
            //Criado um registro
            ProducerRecord<String, String> registro =
                    new ProducerRecord<String, String>("primeiro_topico", "Hello World "+Integer.toString(i));

            produtor.send(registro, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Esse bloco será executado sempre q enviar ou falhar no envio da msg!
                    if (e == null){
                        log.info("Metadados recebidos no callback: \n" +
                                "Tópicos: "+recordMetadata.topic() + "\n" +
                                "Partições: "+recordMetadata.partition() + "\n" +
                                "Offsets: "+recordMetadata.offset()  + "\n" +
                                "DataHora: "+recordMetadata.timestamp() + "\n");
                    }else{
                        log.error("Erro no envio do produtor", e);
                    }
                }
            });
        }

        // descarrega os dados
        produtor.flush();

        // descarrega os dados e fecha o produtor
        produtor.close();
    }
}
