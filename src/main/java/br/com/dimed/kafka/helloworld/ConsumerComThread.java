package br.com.dimed.kafka.helloworld;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerComThread {

    public static void main(String[] args) {
        new ConsumerComThread().run();
    }

    private ConsumerComThread() {
    }

    private void run(){
        Logger log = LoggerFactory.getLogger(ConsumerComThread.class.getName());

        String servidorKafka = "127.0.0.1:9092";
        String groupId = "quinta-aplicacao";
        String topico = "primeiro_topico";

        CountDownLatch latch = new CountDownLatch(1);

        log.info("Criando o consumidor com thread");
        Runnable meuConsumidor = new ConsumerExecutavel(latch, topico, servidorKafka, groupId);

        Thread minhaThread = new Thread(meuConsumidor);
        minhaThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Pegando o desligamento da execução!");
            ((ConsumerExecutavel) meuConsumidor).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("A aplicação foi desligada!");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Aplicação interrompida!", e);
        } finally {
            log.info("Fechando a aplicação!");
        }

    }

    public class ConsumerExecutavel implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Properties properties;
        private Logger log = LoggerFactory.getLogger(ConsumerExecutavel.class.getName());

        public ConsumerExecutavel(CountDownLatch contRegressivo, String topico, String servidorKafka, String groupId) {
            this.latch = contRegressivo;

            this.properties = entregaPropriedade(topico, servidorKafka, groupId);

            this.consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singleton(topico));
        }

        @Override
        public void run() {
            try {
                while (true){
                    ConsumerRecords<String, String> registros = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> registro : registros) {
                        log.info("Chave: " + registro.key() + ", Valor: " + registro.value());
                        log.info("Partição: "+ registro.partition() + ", Offset: "+registro.offset());
                    }

                }
            } catch (WakeupException e){
                log.info("Mensagem recebida desligando!");
            } finally {
                consumer.close();

                latch.countDown();
            }
        }

        public void shutdown(){
            consumer.wakeup();
        }

        private Properties entregaPropriedade(String topico, String servidorKafka, String groupId){
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidorKafka);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return properties;
        }
    }
}
