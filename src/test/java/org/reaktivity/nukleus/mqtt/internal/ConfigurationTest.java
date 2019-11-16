package org.reaktivity.nukleus.mqtt.internal;

import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.PUBLISH_TIMEOUT;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.PUBLISH_TIMEOUT_NAME;

import org.junit.Test;

public class ConfigurationTest
{
    @Test
    public void shouldVerifyConstants() throws Exception
    {
        assertEquals(PUBLISH_TIMEOUT.name(), PUBLISH_TIMEOUT_NAME);
    }
}
