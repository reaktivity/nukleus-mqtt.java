/**
 * Copyright 2016-2020 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.mqtt.internal;

import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.CLIENT_ID;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.CONNECT_TIMEOUT;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.MAXIMUM_QOS;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.NO_LOCAL;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.PUBLISH_TIMEOUT;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.RETAIN_AVAILABLE;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.SESSION_EXPIRY_GRACE_PERIOD;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.SESSION_EXPIRY_INTERVAL;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.SHARED_SUBSCRIPTION_AVAILABLE;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.SUBSCRIPTION_IDENTIFIERS_AVAILABLE;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.TOPIC_ALIAS_MAXIMUM;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.WILDCARD_SUBSCRIPTION_AVAILABLE;

import org.junit.Test;

public class MqttConfigurationTest
{
    static final String PUBLISH_TIMEOUT_NAME = "nukleus.mqtt.publish.timeout";
    static final String CONNECT_TIMEOUT_NAME = "nukleus.mqtt.connect.timeout";
    static final String SESSION_EXPIRY_INTERVAL_NAME = "nukleus.mqtt.session.expiry.interval";
    static final String MAXIMUM_QOS_NAME = "nukleus.mqtt.maximum.qos";
    static final String RETAIN_AVAILABLE_NAME = "nukleus.mqtt.retain.available";
    static final String TOPIC_ALIAS_MAXIMUM_NAME = "nukleus.mqtt.topic.alias.maximum";
    static final String WILDCARD_SUBSCRIPTION_AVAILABLE_NAME = "nukleus.mqtt.wildcard.subscription.available";
    static final String SUBSCRIPTION_IDENTIFIERS_AVAILABLE_NAME = "nukleus.mqtt.subscription.identifiers.available";
    static final String SHARED_SUBSCRIPTION_AVAILABLE_NAME = "nukleus.mqtt.shared.subscription.available";
    static final String NO_LOCAL_NAME = "nukleus.mqtt.no.local";
    static final String SESSION_EXPIRY_GRACE_PERIOD_NAME = "nukleus.mqtt.session.expiry.grace.period";
    static final String CLIENT_ID_NAME = "nukleus.mqtt.client.id";

    @Test
    public void shouldVerifyConstants() throws Exception
    {
        assertEquals(PUBLISH_TIMEOUT.name(), PUBLISH_TIMEOUT_NAME);
        assertEquals(CONNECT_TIMEOUT.name(), CONNECT_TIMEOUT_NAME);
        assertEquals(SESSION_EXPIRY_INTERVAL.name(), SESSION_EXPIRY_INTERVAL_NAME);
        assertEquals(MAXIMUM_QOS.name(), MAXIMUM_QOS_NAME);
        assertEquals(RETAIN_AVAILABLE.name(), RETAIN_AVAILABLE_NAME);
        assertEquals(TOPIC_ALIAS_MAXIMUM.name(), TOPIC_ALIAS_MAXIMUM_NAME);
        assertEquals(WILDCARD_SUBSCRIPTION_AVAILABLE.name(), WILDCARD_SUBSCRIPTION_AVAILABLE_NAME);
        assertEquals(SUBSCRIPTION_IDENTIFIERS_AVAILABLE.name(), SUBSCRIPTION_IDENTIFIERS_AVAILABLE_NAME);
        assertEquals(SHARED_SUBSCRIPTION_AVAILABLE.name(), SHARED_SUBSCRIPTION_AVAILABLE_NAME);
        assertEquals(NO_LOCAL.name(), NO_LOCAL_NAME);
        assertEquals(SESSION_EXPIRY_GRACE_PERIOD.name(), SESSION_EXPIRY_GRACE_PERIOD_NAME);
        assertEquals(CLIENT_ID.name(), CLIENT_ID_NAME);
    }
}
