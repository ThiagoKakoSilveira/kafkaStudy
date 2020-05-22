package br.com.dimed.kafka.helloworld;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerComChaves {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String servidorKafka = "127.0.0.1:9092";

        final Logger log = LoggerFactory.getLogger(ProducerComChaves.class);
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

        // envio assíncrono
        for (int i=0; i<10; i++){

            String topico = "primeiro_topico";
            String msg = "Hello World "+Integer.toString(i);
            String chave = "id_"+Integer.toString(i);
            //Criado um registro
            ProducerRecord<String, String> registro =
                    new ProducerRecord<String, String>(topico, chave, msg);

            log.info("chave: "+chave);

            //id_0 foi para partição 1
            //id_0 foi para partição 0
            //id_0 foi para partição 2
            //id_0 foi para partição 0
            //id_0 foi para partição 2
            //id_0 foi para partição 2
            //id_0 foi para partição 0
            //id_0 foi para partição 2
            //id_0 foi para partição 1
            //id_0 foi para partição 2

        //Em tópicos com 3 partições sempre q enviar 10 msg com esses id's assim ficará distribuodo entre as partições

            // enviando as msgs
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
            }).get();// trava o envio, tornando-o síncrono - mas jamais faça isso em produção!
        }

        // descarrega os dados
        produtor.flush();

        // descarrega os dados e fecha o produtor
        produtor.close();
    }
}
