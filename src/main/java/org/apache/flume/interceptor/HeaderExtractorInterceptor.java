package org.apache.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HeaderExtractorInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(HeaderExtractorInterceptor.class);
    public static final String TAG_REGEX = "%\\{([\\w\\.-]+)\\}";
    public static final Pattern tagPattern = Pattern.compile(TAG_REGEX);

    private final boolean preserveExisting;
    private final String key;
    private final String value;

    public HeaderExtractorInterceptor(boolean preserveExisting, String key, String value) {
        this.preserveExisting = preserveExisting;
        this.key = key;
        this.value = value;
    }

    @Override
    public void initialize() {
        // no-op
    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();

        if (preserveExisting && headers.containsKey(key)) {
            return event;
        }

        Matcher matcher = tagPattern.matcher(value);

        String replacement = value;
        while (matcher.find()) {
            if (matcher.group(1) != null) {
                String header = headers.get(matcher.group(1));
                if (header == null) header = "";
                replacement = replacement.replace(matcher.group(0), header);
            }
        }

        headers.put(key, replacement);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {
        // no-op
    }

    public static class Builder implements Interceptor.Builder {

        private boolean preserveExisting;
        private String key;
        private String value;

        @Override
        public void configure(Context context) {
            preserveExisting = context.getBoolean(HeaderExtractorInterceptor.Constants.PRESERVE, HeaderExtractorInterceptor.Constants.PRESERVE_DEFAULT);
            key = context.getString(HeaderExtractorInterceptor.Constants.KEY, HeaderExtractorInterceptor.Constants.KEY_DEFAULT);
            value = context.getString(HeaderExtractorInterceptor.Constants.VALUE, HeaderExtractorInterceptor.Constants.VALUE_DEFAULT);
        }

        @Override
        public Interceptor build() {
            logger.info(String.format(
                    "Creating HeaderExtractorInterceptor: preserveExisting=%s,key=%s,value=%s",
                    preserveExisting, key, value));
            return new HeaderExtractorInterceptor(preserveExisting, key, value);
        }

    }

    public static class Constants {
        public static final String KEY = "key";
        public static final String KEY_DEFAULT = "key";

        public static final String VALUE = "value";
        public static final String VALUE_DEFAULT = "value";

        public static final String PRESERVE = "preserveExisting";
        public static final boolean PRESERVE_DEFAULT = true;
    }
}
