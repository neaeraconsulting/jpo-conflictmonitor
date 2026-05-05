package us.dot.its.jpo.conflictmonitor.batch.client.mongo;

import org.jspecify.annotations.NonNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Configuration
public class MongoConfig {

//    @Primary
//    @Bean
//    public MongoTemplate customMongoTemplate(MongoDatabaseFactory factory, MappingMongoConverter converter) {
//        converter.setCustomConversions(customConversions());
//        return new MongoTemplate(factory, converter);
//    }

    @Bean
    public MongoCustomConversions customConversions() {
        List<Converter<?,?>> converters = new ArrayList<>();
        converters.add(new ZonedDateTimeWriteConverter());
        converters.add(new ZonedDateTimeReadConverter());
        converters.add(new ZonedDateTimeStringReadConverter());
        return new MongoCustomConversions(converters);
    }

    @ReadingConverter
    public static class ZonedDateTimeReadConverter implements Converter<Date, ZonedDateTime> {
        @Override
        public ZonedDateTime convert(Date date) {
            return date.toInstant().atZone(ZoneOffset.UTC);
        }
    }

    @WritingConverter
    public static class ZonedDateTimeWriteConverter implements Converter<ZonedDateTime, Date> {
        @Override
        public Date convert(ZonedDateTime zonedDateTime) {
            return Date.from(zonedDateTime.toInstant());
        }
    }

    @ReadingConverter
    public static class ZonedDateTimeStringReadConverter implements Converter<String, ZonedDateTime> {
        @Override
        public ZonedDateTime convert(@NonNull String dateString) {
            return ZonedDateTime.parse(dateString, DateTimeFormatter.ISO_DATE_TIME);
        }
    }

    @WritingConverter
    public static class ZonedDateTimeStringWriteConverter implements Converter<ZonedDateTime, String> {
        @Override
        public String convert(@NonNull ZonedDateTime zdt) {
            return DateTimeFormatter.ISO_DATE_TIME.format(zdt);
        }
    }
}
