
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
    private final String TOPIC_NAME = "iu01";
    //private final String FIN_MESSAGE = "exit";

    public static void main(String[] args) {
        String command = "tail -f /work/logs/modules/web/hydrak.log";
        ProducerApi producerApi = new ProducerApi();
        producerApi.executeCommand(command);

    }

    private void executeCommand(String executeCommand) {
        //  lib 이름이 jsch 여서 명명함
        Session jschSession = null;

        try {

            Properties properties = new Properties();

            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "yh01:9092");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


            JSch jsch = new JSch();
            jschSession = jsch.getSession(USERNAME, REMOTE_HOST, REMOTE_PORT);
            jschSession.setConfig("StrictHostKeyChecking", "no");
            jschSession.setPassword(PASSWORD);
            jschSession.connect(SESSION_TIMEOUT);

            ChannelExec channelExec = (ChannelExec) jschSession.openChannel("exec");

            channelExec.setCommand(executeCommand);
            channelExec.setErrStream(System.err);
            InputStream inputStream = channelExec.getInputStream();
            channelExec.connect(CHANNEL_TIMEOUT);
            byte[] tmp = new byte[1024];

            while (true) {

                while (inputStream.available() > 0) {
                    int i = inputStream.read(tmp, 0, 1024);
                    if (i < 0) break;

                    System.out.println(new String(tmp, 0, i));

                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, new String(tmp, 0, i));

                    try {
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                // some exception
                            }
                        });

                    } catch (Exception e) {
                        // exception
                    } finally {
                        producer.flush();
                    }
                    //System.out.println(new String(tmp, 0, i));

                }

                if (channelExec.isClosed()) {
                    if (inputStream.available() > 0) continue;
                    break;
                }

                try {
                    Thread.sleep(50);
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