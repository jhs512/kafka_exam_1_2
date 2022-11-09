package com.ll.exam.kafka_3_2.util;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ll.exam.kafka_3_2.app.AppConfig;
import lombok.SneakyThrows;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class Ut {

    public static class kafka {
        public interface OnSuccess<K, V> {
            void onSuccess(SendResult<K, V> result);
        }

        public interface OnFailure {
            void onFailure(Throwable ex);
        }

        public static void send(String topicName, Map<String, Object> data, OnSuccess onSuccess, OnFailure onFailure) {
            KafkaTemplate<String, String> kafkaTemplate = getKafkaTemplate();

            kafkaTemplate.send(
                    topicName,
                    json.toStr(data)
            ).addCallback(new ListenableFutureCallback<>() {

                @Override
                public void onSuccess(SendResult<String, String> result) {
                    onSuccess.onSuccess(result);
                }

                @Override
                public void onFailure(Throwable ex) {
                    onFailure.onFailure(ex);
                }
            });
        }
    }

    @SneakyThrows
    public static void sleep(int millis) {
        Thread.sleep(millis);
    }

    public static class date {
        public static int getEndDayOf(int year, int month) {
            String yearMonth = year + "-" + "%02d".formatted(month);

            return getEndDayOf(yearMonth);
        }

        public static int getEndDayOf(String yearMonth) {
            LocalDate convertedDate = LocalDate.parse(yearMonth + "-01", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            convertedDate = convertedDate.withDayOfMonth(
                    convertedDate.getMonth().length(convertedDate.isLeapYear()));

            return convertedDate.getDayOfMonth();
        }

        public static LocalDateTime parse(String pattern, String dateText) {
            return LocalDateTime.parse(dateText, DateTimeFormatter.ofPattern(pattern));
        }

        public static LocalDateTime parse(String dateText) {
            return parse("yyyy-MM-dd HH:mm:ss.SSSSSS", dateText);
        }
    }

    private static ObjectMapper getObjectMapper() {
        return (ObjectMapper) AppConfig.getContext().getBean("objectMapper");
    }

    private static <K, V> KafkaTemplate<K, V> getKafkaTemplate() {
        return (KafkaTemplate<K, V>) AppConfig.getContext().getBean("kafkaTemplate");
    }

    public static class json {

        public static String toStr(Map<String, Object> map) {
            try {
                return getObjectMapper().writeValueAsString(map);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }
        }

        public static Map<String, Object> toMap(String jsonStr) {
            try {
                return getObjectMapper().readValue(jsonStr, LinkedHashMap.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    public static <K, V> Map<K, V> mapOf(Object... args) {
        Map<K, V> map = new LinkedHashMap<>();

        int size = args.length / 2;

        for (int i = 0; i < size; i++) {
            int keyIndex = i * 2;
            int valueIndex = keyIndex + 1;

            K key = (K) args[keyIndex];
            V value = (V) args[valueIndex];

            map.put(key, value);
        }

        return map;
    }
}
