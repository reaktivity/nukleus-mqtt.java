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
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.CLIENT_ID_NAME;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.CONNECT_TIMEOUT;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.CONNECT_TIMEOUT_NAME;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.PUBLISH_TIMEOUT;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.PUBLISH_TIMEOUT_NAME;

import org.junit.Test;

public class ConfigurationTest
{
    @Test
    public void shouldVerifyConstants() throws Exception
    {
        assertEquals(CLIENT_ID.name(), CLIENT_ID_NAME);
        assertEquals(PUBLISH_TIMEOUT.name(), PUBLISH_TIMEOUT_NAME);
        assertEquals(CONNECT_TIMEOUT.name(), CONNECT_TIMEOUT_NAME);
    }
}
