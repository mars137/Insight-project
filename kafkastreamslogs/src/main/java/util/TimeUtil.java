package util;

/**
 * Created by hadoop-user on 6/12/17.
 */
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author mars137
 */
public class TimeUtil {

    public static LocalDateTime fromMillis(long millis) {
        return Instant.ofEpochMilli(millis)
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
    }

    public static String millisFormat(long millis, DateTimeFormatter formatter) {
        return fromMillis(millis).format(formatter);
    }

    public static String millisFormat(long millis) {
        return fromMillis(millis).format(DateTimeFormatter.ISO_LOCAL_TIME);
    }
}