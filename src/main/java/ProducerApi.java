import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;

public class ProducerApi {

    private static final String REMOTE_HOST = "113.198.103.137";
    private static final String USERNAME = "hydrak";
    private static final String PASSWORD = "fpemdnemzpdl123$";
    private static final int REMOTE_PORT = 10022;
    private static final int SESSION_TIMEOUT = 100000;
    private static final int CHANNEL_TIMEOUT = 100000;

    private static final String KAFKA_SERVER = "yh01:9092";
    private static final String TOPIC_NAME = "iu01";

    private static final String COMMAND = "tail -f /work/logs/modules/web/hydrak.log";

    public static void main(String[] args) {

        ProducerApi producerApi = new ProducerApi();
        producerApi.executeCommand(COMMAND);

    }

    private void executeCommand(String executeCommand) {
        //  lib 이름이 jsch 여서 명명함
        JSch jsch = null;
        //  sftp 연결 Session
        Session jschSession = null;
        //  jsch lib에서 실행 객체
        ChannelExec channelExec = null;
        //  sftp 에서 결과 를 받아 올 Stream
        InputStream inputStream = null;

        //  카프카 Properties
        Properties kafkaProperties = null; // kafka properties
        //  카프카 Producer
        KafkaProducer<String, String> producer = null;

        try {
            //  카프카 Producer
            kafkaProperties = new Properties();

            kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
            kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            producer = new KafkaProducer<>(kafkaProperties);

            //  sftp 연결
            jsch = new JSch();
            jschSession = jsch.getSession(USERNAME, REMOTE_HOST, REMOTE_PORT);
            jschSession.setConfig("StrictHostKeyChecking", "no");
            jschSession.setPassword(PASSWORD);
            jschSession.connect(SESSION_TIMEOUT);

            //  Command 실행
            channelExec = (ChannelExec) jschSession.openChannel("exec");
            channelExec.setCommand(executeCommand);

            //  결과 가져오기 Using Stream
            channelExec.setErrStream(System.err);
            inputStream = channelExec.getInputStream();
            channelExec.connect(CHANNEL_TIMEOUT);

            byte[] tmp = new byte[1024];
            while (true) {

                while (inputStream.available() > 0) {
                    int i = inputStream.read(tmp, 0, 1024);
                    if (i < 0) break;

                    //region Producer send

                    String result = new String(tmp, 0, i);

                    System.out.println(result);
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, result);

                    try {
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                // some exception
                                System.out.println("------------------------------------ exception ------------------------------------");
                            }
                        });

                    } catch (Exception e) {
                        System.out.println("Kafka Producer Send Exception");
                        e.printStackTrace();
                        // exception
                    } finally {
                        producer.flush();
                    }

                    //endregion
                }

                if (channelExec.isClosed()) {
                    if (inputStream.available() > 0) continue;
                    break;
                }

                try {
                    Thread.sleep(500);
                } catch (Exception e) {
                }
            }

            channelExec.disconnect();

        } catch (JSchException | IOException e) {
            e.printStackTrace();
        } finally {
            if (jschSession != null) {
                jschSession.disconnect();
            }
        }
    }

}