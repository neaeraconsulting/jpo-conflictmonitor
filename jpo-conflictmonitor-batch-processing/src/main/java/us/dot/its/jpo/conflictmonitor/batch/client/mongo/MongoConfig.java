package us.dot.its.jpo.conflictmonitor.batch.client.mongo;

import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Slf4j
@Configuration
public class MongoConfig {

    @Bean
    public MongoCustomConversions customConversions() {
        List<Converter<?,?>> converters = new ArrayList<>();
        converters.add(new ZonedDateTimeWriteConverter());
        converters.add(new ZonedDateTimeReadConverter());
        converters.add(new ZonedDateTimeStringReadConverter());
        converters.add(new ZonedDateTimeStringWriteConverter());
        converters.add(new DateStringWriteConverter());
        converters.add(new DateStringReadConverter());
        return new MongoCustomConversions(converters);
    }

    @ReadingConverter
    public static class ZonedDateTimeReadConverter implements Converter<Date, ZonedDateTime> {
        @Override
        public ZonedDateTime convert(@NonNull Date date) {
            Instant instant = date.toInstant();
            return instant.atZone(ZoneOffset.UTC);
        }
    }

    @WritingConverter
    public static class ZonedDateTimeWriteConverter implements Converter<ZonedDateTime, Date> {
        @Override
        public Date convert(@NonNull ZonedDateTime zonedDateTime) {
            return Date.from(zonedDateTime.toInstant());
        }
    }

    public static final DateTimeFormatter DATE_TIME_FORMATTER
            = DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneOffset.UTC);


    @ReadingConverter
    public static class ZonedDateTimeStringReadConverter implements Converter<String, ZonedDateTime> {
        @Override
        public ZonedDateTime convert(@NonNull String dateString) {
            return ZonedDateTime.parse(dateString, DATE_TIME_FORMATTER);
        }
    }

    @WritingConverter
    public static class ZonedDateTimeStringWriteConverter implements Converter<ZonedDateTime, String> {
        @Override
        public String convert(@NonNull ZonedDateTime zdt) {
            return DATE_TIME_FORMATTER.format(zdt);
        }
    }

    @ReadingConverter
    public static class DateStringReadConverter implements Converter<String, Date> {
        @Override
        public Date convert(@NonNull String dateString) {
            return Date.from(ZonedDateTime.parse(dateString, DATE_TIME_FORMATTER).toInstant());
        }
    }

    @WritingConverter
    public static class DateStringWriteConverter implements Converter<Date, String> {
        @Override
        public String convert(@NonNull Date date) {
            return date.toInstant().atZone(ZoneOffset.UTC).format(DATE_TIME_FORMATTER);
        }
    }


}
