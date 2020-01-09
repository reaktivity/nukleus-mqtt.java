package org.reaktivity.nukleus.mqtt.internal;

import java.util.regex.Pattern;

public class MqttUtility
{
    public static final Pattern TOPIC_FILTER_REGEX = Pattern.compile("[/]?(([^/#+]*|\\+)/)*(#|\\+|[^/#+]*)");

    public static boolean validTopicFilter(String topicFilter)
    {
        return TOPIC_FILTER_REGEX.matcher(topicFilter).matches();
    }

    private MqttUtility()
    {
        // Utility
    }
}
