package com.ll.exam.kafka_3_2.app.kafka.consumer;

import com.ll.exam.kafka_3_2.util.Ut;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@EnableKafka
@Service
public class EmailConsumer {
    @KafkaListener(
            topics = "sendEmail__joinComplete", groupId = "group-01")
    public void listenWith___sendEmail__joinComplete(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {

        Map<String, Object> payloadJson = Ut.json.toMap(payload);

        String email = (String)payloadJson.get("email");
        String msg = (String)payloadJson.get("msg");

        log.debug("서버2 : 토픽 sendEmail__joinComplete 메세지 수신");
        log.debug("email : {}", email);
        log.debug("msg : {}", msg);

        log.debug("실제 이메일 발송");

        Ut.kafka.send(
                "sendEmail__joinCompleteDone",
                Ut.mapOf(
                        "email", email,
                        "msg", msg
                ),
                result -> {
                    ProducerRecord producerRecord = result.getProducerRecord();

                    log.debug("서버2 : 토픽 sendEmail__joinCompleteDone 메세지 송신");

                    log.debug("producerRecord.key() : {}", producerRecord.key());
                    log.debug("producerRecord.value() : {}", producerRecord.value());
                },
                Throwable::printStackTrace
        );
    }
}
