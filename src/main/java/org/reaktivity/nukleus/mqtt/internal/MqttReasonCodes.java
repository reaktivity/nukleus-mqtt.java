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

public final class MqttReasonCodes
{
    public static final byte SUCCESS = 0x00;

    public static final byte NORMAL_DISCONNECT = 0x00;

    public static final byte GRANTED_QOS_1 = 0x00;
    public static final byte GRANTED_QOS_2 = 0x01;
    public static final byte GRANTED_QOS_3 = 0x02;

    public static final byte DISCONNECT_WITH_WILL_MESSAGE = 0x04;

    public static final byte NO_MATCHING_SUBSCRIBERS = 0x10;
    public static final byte NO_SUBSCRIPTION_EXISTED = 0x11;

    public static final byte CONTINUE_AUTHENTICATION = 0x18;
    public static final byte REAUTHENTICATE = 0x19;

    public static final byte UNSPECIFIED_ERROR = -0x80;
    public static final byte MALFORMED_PACKET = -0x7F;
    public static final byte PROTOCOL_ERROR = -0x7E;
    public static final byte IMPLEMENTATION_SPECIFIC_ERROR = -0x7D;
    public static final byte UNSUPPORTED_PROTOCOL_VERSION = -0x7C;
    public static final byte CLIENT_IDENTIFIER_NOT_VALID = -0x7B;
    public static final byte BAD_USER_NAME_OR_PASSWORD = -0x7A;
    public static final byte NOT_AUTHORIZED = -0x79;
    public static final byte SERVER_UNAVAILABLE = -0x78;
    public static final byte SERVER_BUSY = -0x77;
    public static final byte BANNED = -0x76;
    public static final byte SERVER_SHUTTING_DOWN = -0x75;
    public static final byte BAD_AUTHENTICATION_METHOD = -0x74;

    public static final byte KEEP_ALIVE_TIMEOUT = -0x73;
    public static final byte SESSION_TAKEN_OVER = -0x72;

    public static final byte TOPIC_FILTER_INVALID = -0x71;
    public static final byte TOPIC_NAME_INVALID = -0x70;

    public static final byte PACKET_IDENTIFIER_IN_USE = -0x6F;
    public static final byte PACKET_IDENTIFIER_NOT_FOUND = -0x6E;
    public static final byte RECEIVE_MAXIMUM_EXCEEDED = -0x6D;
    public static final byte TOPIC_ALIAS_INVALID = -0x6C;

    private MqttReasonCodes()
    {
        // Utility
    }
}
