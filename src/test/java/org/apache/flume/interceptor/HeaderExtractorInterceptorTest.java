package org.apache.flume.interceptor;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class HeaderExtractorInterceptorTest {

    @Test
    public void extractorTest() throws InstantiationException, IllegalAccessException {
        /* event prepare */
        Map<String, String> headers = new HashMap<>(1);
        headers.put("collectionName", "XXJBizInterfaceLog");
        Event event = EventBuilder.withBody("test", Charsets.UTF_8, headers);

        /* interceptor prepare */
        Interceptor.Builder builder = HeaderExtractorInterceptor.Builder.class.newInstance();
        Context ctx = new Context();
        ctx.put(HeaderExtractorInterceptor.Constants.KEY, "topic");
        ctx.put(HeaderExtractorInterceptor.Constants.VALUE, "odeon_test_%{collectionName}_topic");
        builder.configure(ctx);
        Interceptor interceptor = builder.build();

        /* test */
        Event interceptedEvent = interceptor.intercept(event);

        assertEquals("odeon_test_XXJBizInterfaceLog_topic", interceptedEvent.getHeaders().get("topic"));
    }

    @Test
    public void preserveTest() throws IllegalAccessException, InstantiationException {
        /* event prepare */
        Map<String, String> headers = new HashMap<>(1);
        headers.put("topic", "exist");
        headers.put("collectionName", "XXJBizInterfaceLog");
        Event event = EventBuilder.withBody("test", Charsets.UTF_8, headers);

        /* interceptor prepare */
        Interceptor.Builder builder = HeaderExtractorInterceptor.Builder.class.newInstance();
        Context ctx = new Context();
        ctx.put(HeaderExtractorInterceptor.Constants.PRESERVE, "true");
        ctx.put(HeaderExtractorInterceptor.Constants.KEY, "topic");
        ctx.put(HeaderExtractorInterceptor.Constants.VALUE, "odeon_test_%{collectionName}_topic");
        builder.configure(ctx);
        Interceptor interceptor = builder.build();

        /* test */
        Event interceptedEvent = interceptor.intercept(event);

        assertEquals("exist", interceptedEvent.getHeaders().get("topic"));
    }
}