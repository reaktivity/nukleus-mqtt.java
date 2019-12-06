/**
 * Copyright 2016-2019 The Reaktivity Project
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
package org.reaktivity.nukleus.mqtt.internal.stream;

public class MqttSettings
{
    private static final int DEFAULT_ENABLE_PUSH = 1;
    private static final int DEFAULT_MAX_CONCURRENT_STREAMS = Integer.MAX_VALUE;
    private static final int DEFAULT_INITIAL_WINDOW_SIZE = 65_535;

    public int enablePush = DEFAULT_ENABLE_PUSH;
    public int maxConcurrentStreams = DEFAULT_MAX_CONCURRENT_STREAMS;
    public int initialWindowSize = DEFAULT_INITIAL_WINDOW_SIZE;

    public MqttSettings(
        int maxConcurrentStreams,
        int initialWindowSize)
    {
        this.maxConcurrentStreams = maxConcurrentStreams;
        this.initialWindowSize = initialWindowSize;
    }

    public MqttSettings()
    {
    }

    public void apply(
        MqttSettings settings)
    {
        this.maxConcurrentStreams = settings.maxConcurrentStreams;
        this.initialWindowSize = settings.initialWindowSize;
    }

    private boolean enablePushIsValid()
    {
        return enablePush == 0 || enablePush == 1;
    }

    private boolean initialWindowSizeIsValid()
    {
        return initialWindowSize >= 0;
    }
}
