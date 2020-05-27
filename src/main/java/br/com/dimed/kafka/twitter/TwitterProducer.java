package br.com.dimed.kafka.twitter;

import br.com.dimed.kafka.helloworld.Consumer;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger log = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private String consumerKey = "IQMDm4zuV7i0sxDdtoLOSCkpi";
    private String consumerSecret = "3zWmB9D95RZO87jkeRrHoC7knTO9Zrz5s8TbyIx1rFqXh6gYDK";
    private String token = "1264872882695438341-8yvYdXErloSbJaEFaU3dml7NpiHOZp";
    private String secret = "vY8ccSvwM2NLeusVVCvYIMQI98MVEoYFnvucj6mE62AWz";

    List<String> terms = Lists.newArrayList("Escruntcha Fire   ");

    private String servidorKafka = "127.0.0.1:9092";

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){

        log.info("Configurando...");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        //BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        //criar um cliente do twitter
        Client client = criarClienteTwitter(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        //criar um produtor do kafka
        KafkaProducer<String, String> producer = criarProdutorKafka();

        // adicionar um gancho de desligamento
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            log.info("Parando a aplicação...");
            log.info("Derrubando o cliente do twitter...");
            client.stop();
            log.info("Fechando o produtor do kafka");
            producer.close();
            log.info("Feito!");
        }));

        //loop para enviar tweets para o kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null){
                log.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            log.error("Deu ruim", e);
                        }
                    }
                });
            }
        }
        log.info("O aplicativo fechou!");
    }

    private KafkaProducer<String, String> criarProdutorKafka() {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidorKafka);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Criando um produtor seguro q guarda em mais de um nó e garante a entrega seja por tentativas forçadas
        // seja pelo fluxo normal.
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); //Importante para evitar duplicação
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));//Em um kafka > 0.11
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");//Em um kafka < 0.11 usar 1
        //Essas duas configuração de cima já é padrão para os servidores kafka > 0.11

        // Configurações de alto rendimento de taxa de transferência, compactação, envio em lotes e tamanho do lote
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "30");
        //Aqui defino o tamanho que por padrão é 16k e aqui transformo para 32k
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }

    public Client criarClienteTwitter(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //        List<Long> followings = Lists.newArrayList(1234L, 566788L);

        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                //.eventMessageQueue(eventQueue); //optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }
}
