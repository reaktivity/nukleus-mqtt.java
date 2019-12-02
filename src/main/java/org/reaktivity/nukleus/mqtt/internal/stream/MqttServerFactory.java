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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.TimeUnit.of;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.mqtt.internal.MqttReasonCodes.MALFORMED_PACKET;
import static org.reaktivity.nukleus.mqtt.internal.MqttReasonCodes.NORMAL_DISCONNECT;
import static org.reaktivity.nukleus.mqtt.internal.MqttReasonCodes.PROTOCOL_ERROR;
import static org.reaktivity.nukleus.mqtt.internal.MqttReasonCodes.SUCCESS;
import static org.reaktivity.nukleus.mqtt.internal.MqttReasonCodes.UNSUPPORTED_PROTOCOL_VERSION;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.SignalingExecutor;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.mqtt.internal.MqttConfiguration;
import org.reaktivity.nukleus.mqtt.internal.MqttNukleus;
import org.reaktivity.nukleus.mqtt.internal.types.Flyweight;
import org.reaktivity.nukleus.mqtt.internal.types.MqttPayloadFormat;
import org.reaktivity.nukleus.mqtt.internal.types.MqttRole;
import org.reaktivity.nukleus.mqtt.internal.types.OctetsFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttConnackFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttConnectFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttDisconnectFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPacketFixedHeaderFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPacketType;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPingReqFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPingRespFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPropertyFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPublishFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttSubackFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttSubscribeFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttSubscriptionFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttTopicFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttUnsubackFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttUnsubscribeFW;
import org.reaktivity.nukleus.mqtt.internal.types.control.MqttRouteExFW;
import org.reaktivity.nukleus.mqtt.internal.types.control.RouteFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.DataFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.EndFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.MqttBeginExFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.MqttDataExFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.MqttEndExFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class MqttServerFactory implements StreamFactory
{
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(new UnsafeBuffer(new byte[0]), 0, 0);

    private static final int CONNECT_FIXED_HEADER = 0b0000_0000;
    private static final int SUBSCRIBE_FIXED_HEADER = 0b1000_0010;
    private static final int UNSUBSCRIBE_FIXED_HEADER = 0b1010_0010;
    private static final int DISCONNECT_FIXED_HEADER = 0b1110_0000;

    private static final int PUBLISH_TIMEOUT_SIGNAL = 1;

    private final RouteFW routeRO = new RouteFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();
    private final SignalFW signalRO = new SignalFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final SignalFW.Builder signalRW = new SignalFW.Builder();

    private final MqttBeginExFW mqttBeginExRO = new MqttBeginExFW();
    private final MqttDataExFW mqttDataExRO = new MqttDataExFW();

    private final MqttBeginExFW.Builder mqttBeginExRW = new MqttBeginExFW.Builder();
    private final MqttDataExFW.Builder mqttDataExRW = new MqttDataExFW.Builder();
    private final MqttEndExFW.Builder mqttEndExRW = new MqttEndExFW.Builder();

    private final MqttPacketFixedHeaderFW mqttPacketFixedHeaderRO = new MqttPacketFixedHeaderFW();
    private final MqttConnectFW mqttConnectRO = new MqttConnectFW();
    private final MqttConnackFW mqttConnackRO = new MqttConnackFW();
    private final MqttPingReqFW mqttPingReqRO = new MqttPingReqFW();
    private final MqttPingRespFW mqttPingRespRO = new MqttPingRespFW();
    private final MqttDisconnectFW mqttDisconnectRO = new MqttDisconnectFW();
    private final MqttSubscribeFW mqttSubscribeRO = new MqttSubscribeFW();
    private final MqttSubackFW mqttSubackRO = new MqttSubackFW();
    private final MqttUnsubscribeFW mqttUnsubscribeRO = new MqttUnsubscribeFW();
    private final MqttUnsubackFW mqttUnsubackRO = new MqttUnsubackFW();
    private final MqttPublishFW mqttPublishRO = new MqttPublishFW();
    private final MqttSubscriptionFW mqttSubscriptionRO = new MqttSubscriptionFW();
    private final MqttTopicFW mqttTopicRO = new MqttTopicFW();
    private final MqttRouteExFW routeExRO = new MqttRouteExFW();
    private final MqttPropertyFW mqttPropertyRO = new MqttPropertyFW();

    private final OctetsFW.Builder octetsRW = new OctetsFW.Builder();

    private final MqttPacketFixedHeaderFW.Builder mqttPacketFixedHeaderRW = new MqttPacketFixedHeaderFW.Builder();
    private final MqttConnectFW.Builder mqttConnectRW = new MqttConnectFW.Builder();
    private final MqttConnackFW.Builder mqttConnackRW = new MqttConnackFW.Builder();
    private final MqttPingReqFW.Builder mqttPingReqRW = new MqttPingReqFW.Builder();
    private final MqttDisconnectFW.Builder mqttDisconnectRW = new MqttDisconnectFW.Builder();
    private final MqttPingRespFW.Builder mqttPingRespRW = new MqttPingRespFW.Builder();
    private final MqttSubscribeFW.Builder mqttSubscribeRW = new MqttSubscribeFW.Builder();
    private final MqttSubackFW.Builder mqttSubackRW = new MqttSubackFW.Builder();
    private final MqttUnsubscribeFW.Builder mqttUnsubscribeRW = new MqttUnsubscribeFW.Builder();
    private final MqttUnsubackFW.Builder mqttUnsubackRW = new MqttUnsubackFW.Builder();
    private final MqttPublishFW.Builder mqttPublishRW = new MqttPublishFW.Builder();

    private final SignalingExecutor executor;

    private final MqttConfiguration config;
    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer extBuffer;
    private final MutableDirectBuffer dataExtBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final LongSupplier supplyBudgetId;

    private final long publishTimeout;

    private final Long2ObjectHashMap<MqttServer.MqttServerStream> correlations;
    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);
    private final int mqttTypeId;

    private final BufferPool bufferPool;

    private final MqttServerDecoder decodePacketType = this::decodePacketType;
    private final MqttServerDecoder decodeConnect = this::decodeConnect;
    private final MqttServerDecoder decodePublish = this::decodePublish;
    private final MqttServerDecoder decodeSubscribe = this::decodeSubscribe;
    private final MqttServerDecoder decodeUnsubscribe = this::decodeUnsubscribe;
    private final MqttServerDecoder decodePingreq = this::decodePingreq;
    private final MqttServerDecoder decodeDisconnect = this::decodeDisconnect;
    private final MqttServerDecoder decodeIgnoreAll = this::decodeIgnoreAll;
    private final MqttServerDecoder decodeUnknownType = this::decodeUnknownType;

    private final Map<MqttPacketType, MqttServerDecoder> decodersByPacketType;

    {
        final Map<MqttPacketType, MqttServerDecoder> decodersByPacketType = new EnumMap<>(MqttPacketType.class);
        decodersByPacketType.put(MqttPacketType.CONNECT, decodeConnect);
        decodersByPacketType.put(MqttPacketType.PUBLISH, decodePublish);
        // decodersByPacketType.put(MqttPacketType.PUBREC, decodePubrec);
        // decodersByPacketType.put(MqttPacketType.PUBREL, decodePubrel);
        // decodersByPacketType.put(MqttPacketType.PUBCOMP, decodePubcomp);
        decodersByPacketType.put(MqttPacketType.SUBSCRIBE, decodeSubscribe);
        decodersByPacketType.put(MqttPacketType.UNSUBSCRIBE, decodeUnsubscribe);
        decodersByPacketType.put(MqttPacketType.PINGREQ, decodePingreq);
        decodersByPacketType.put(MqttPacketType.DISCONNECT, decodeDisconnect);
        // decodersByPacketType.put(MqttPacketType.AUTH, decodeAuth);
        this.decodersByPacketType = decodersByPacketType;
    }

    public MqttServerFactory(
        MqttConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyBudgetId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        SignalingExecutor executor)
    {
        this.config = config;
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.extBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.dataExtBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.bufferPool = bufferPool;
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyBudgetId = requireNonNull(supplyBudgetId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.correlations = new Long2ObjectHashMap<>();
        this.mqttTypeId = supplyTypeId.applyAsInt(MqttNukleus.NAME);
        this.executor = executor;
        this.publishTimeout = config.getPublishTimeout();
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long streamId = begin.streamId();

        MessageConsumer newStream = null;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newInitialStream(begin, throttle);
        }
        else
        {
            newStream = newReplyStream(begin, throttle);
        }
        return newStream;
    }

    private MessageConsumer newInitialStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long routeId = begin.routeId();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            /*
            final RouteFW route = routeRO.wrap(b, o, o + l);
            final OctetsFW routeEx = route.extension();


            if (routeEx.sizeof() != 0)
            {
                TODO
            }
            */
            return true;
        };

        final RouteFW route = router.resolve(routeId, begin.authorization(), filter, wrapRoute);
        MessageConsumer newStream = null;


        if (route != null)
        {
            final long initialId = begin.streamId();
            final long affinity = begin.affinity();
            final long replyId = supplyReplyId.applyAsLong(initialId);
            final long budgetId = supplyBudgetId.getAsLong();

            final MqttServer connection = new MqttServer(sender, routeId, initialId, replyId, budgetId, affinity);
            newStream = connection::onNetwork;
        }
        return newStream;
    }

    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long replyId = begin.streamId();
        final MqttServer.MqttServerStream reply = correlations.remove(replyId);

        MessageConsumer newStream = null;
        if (reply != null)
        {
            newStream = reply::onApplicationReply;
        }
        return newStream;
    }

    private RouteFW resolveTarget(
        long routeId,
        long authorization,
        String topicFilter)
    {
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            final OctetsFW ext = route.extension();
            if (ext.sizeof() > 0)
            {
                final MqttRouteExFW routeEx = ext.get(routeExRO::wrap);
                final String topicEx = routeEx.topic().asString();
                return topicEx.equals(topicFilter);
            }
            return true;
        };
        return router.resolve(routeId, authorization, filter, wrapRoute);
    }

    private int topicKey(
        String topicFilter)
    {
        return System.identityHashCode(topicFilter.intern());
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long replyId,
        long traceId,
        long authorization,
        long affinity,
        Flyweight extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                  .routeId(routeId)
                                  .streamId(replyId)
                                  .traceId(traceId)
                                  .authorization(authorization)
                                  .affinity(affinity)
                                  .extension(extension.buffer(), extension.offset(), extension.sizeof())
                                  .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doData(
        MessageConsumer receiver,
        long routeId,
        long replyId,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int index,
        int length,
        Flyweight extension)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                .routeId(routeId)
                                .streamId(replyId)
                                .traceId(traceId)
                                .authorization(authorization)
                                .budgetId(budgetId)
                                .reserved(reserved)
                                .payload(buffer, index, length)
                                .extension(extension.buffer(), extension.offset(), extension.sizeof())
                                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private void doEnd(
        MessageConsumer receiver,
        long routeId,
        long replyId,
        long traceId,
        long authorization,
        Flyweight extension)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                              .routeId(routeId)
                              .streamId(replyId)
                              .traceId(traceId)
                              .authorization(authorization)
                              .extension(extension.buffer(), extension.offset(), extension.sizeof())
                              .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doAbort(
        MessageConsumer receiver,
        long routeId,
        long replyId,
        long traceId,
        long authorization,
        Flyweight extension)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                  .routeId(routeId)
                                  .streamId(replyId)
                                  .traceId(traceId)
                                  .authorization(authorization)
                                  .extension(extension.buffer(), extension.offset(), extension.sizeof())
                                  .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doWindow(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long budgetId,
        int initialPadding,
        int initialCredit)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                    .routeId(routeId)
                                    .streamId(streamId)
                                    .traceId(traceId)
                                    .budgetId(budgetId)
                                    .credit(initialCredit)
                                    .padding(initialPadding)
                                    .build();

        receiver.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        Flyweight extension)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity()).routeId(routeId)
                                  .streamId(streamId)
                                  .traceId(traceId)
                                  .authorization(authorization)
                                  .extension(extension.buffer(), extension.offset(), extension.sizeof())
                                  .build();

        receiver.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doSignal(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        final SignalFW signal = signalRW.wrap(writeBuffer, 0, writeBuffer.capacity()).routeId(routeId)
                                    .streamId(streamId)
                                    .traceId(traceId)
                                    .build();

        receiver.accept(signal.typeId(), signal.buffer(), signal.offset(), signal.sizeof());
    }

    private int decodePacketType(
        MqttServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        final MqttPacketFixedHeaderFW packet = mqttPacketFixedHeaderRO.tryWrap(buffer, offset, limit);

        if (packet != null)
        {
            final int length = packet.remainingLength();
            final MqttPacketType packetType = MqttPacketType.valueOf(packet.typeAndFlags() >> 4);
            final MqttServerDecoder decoder = decodersByPacketType.getOrDefault(packetType, decodeUnknownType);

            if (limit - packet.limit() >= length)
            {
                server.decoder = decoder;
            }
        }

        return offset;
    }

    private int decodeConnect(
        MqttServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        MqttConnectFW mqttConnect = mqttConnectRO.tryWrap(buffer, offset, limit);

        int progress = offset;
        int reasonCode = SUCCESS;
        if (mqttConnect == null)
        {
            reasonCode = PROTOCOL_ERROR;
        }
        else if ((mqttConnect.flags() & 0b0000_0001) != CONNECT_FIXED_HEADER)
        {
            reasonCode = MALFORMED_PACKET;
        }
        else if (!"MQTT".equals(mqttConnect.protocolName().asString()) || mqttConnect.protocolVersion() != 5)
        {
            reasonCode = UNSUPPORTED_PROTOCOL_VERSION;
        }

        if (reasonCode == 0)
        {
            server.onDecodeConnect(traceId, authorization, mqttConnect);
            server.decoder = decodePacketType;
            progress = mqttConnect.limit();
        }
        else
        {
            server.onDecodeError(traceId, authorization, reasonCode);
            server.decoder = decodeIgnoreAll;
        }

        return progress;
    }

    private int decodePublish(
        MqttServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        final MqttPublishFW publish = mqttPublishRO.tryWrap(buffer, offset, limit);

        int progress = offset;
        int reasonCode = SUCCESS;
        if (publish == null || publish.topicName().asString() == null)
        {
            reasonCode = PROTOCOL_ERROR;
        }

        if (reasonCode == 0)
        {
            server.onDecodePublish(traceId, authorization, publish);
            server.decoder = decodePacketType;
            progress = publish.limit();
        }
        else
        {
            server.onDecodeError(traceId, authorization, reasonCode);
            server.decoder = decodeIgnoreAll;
        }

        return progress;
    }

    private int decodeSubscribe(
        MqttServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        final MqttSubscribeFW subscribe = mqttSubscribeRO.tryWrap(buffer, offset, limit);

        int progress = offset;
        int reasonCode = SUCCESS;
        if (subscribe == null)
        {
            reasonCode = PROTOCOL_ERROR;
        }
        else if ((subscribe.typeAndFlags() & 0b1111_1111) != SUBSCRIBE_FIXED_HEADER)
        {
            reasonCode = MALFORMED_PACKET;
        }

        if (reasonCode == 0)
        {
            server.onDecodeSubscribe(traceId, authorization, subscribe);
            server.decoder = decodePacketType;
            progress = subscribe.limit();
        }
        else
        {
            server.onDecodeError(traceId, authorization, reasonCode);
            server.decoder = decodeIgnoreAll;
        }

        return progress;
    }

    private int decodeUnsubscribe(
        MqttServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        final MqttUnsubscribeFW unsubscribe = mqttUnsubscribeRO.tryWrap(buffer, offset, limit);

        int progress = offset;
        int reasonCode = SUCCESS;
        if (unsubscribe == null)
        {
            reasonCode = PROTOCOL_ERROR;
        }
        else if ((unsubscribe.typeAndFlags() & 0b1111_1111) != UNSUBSCRIBE_FIXED_HEADER)
        {
            reasonCode = MALFORMED_PACKET;
        }

        if (reasonCode == 0)
        {
            server.onDecodeUnsubscribe(traceId, authorization, unsubscribe);
            server.decoder = decodePacketType;
            progress = unsubscribe.limit();
        }
        else
        {
            server.onDecodeError(traceId, authorization, reasonCode);
            server.decoder = decodeIgnoreAll;
        }

        return progress;
    }

    private int decodePingreq(
        MqttServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        final MqttPingReqFW ping = mqttPingReqRO.tryWrap(buffer, offset, limit);
        int progress = offset;
        if (ping == null)
        {
            server.onDecodeError(traceId, authorization, PROTOCOL_ERROR);
            server.decoder = decodeIgnoreAll;
        }
        else
        {
            server.onDecodePingReq(traceId, authorization, ping);
            server.decoder = decodePacketType;
            progress = ping.limit();
        }

        return progress;
    }

    private int decodeDisconnect(
        MqttServer server,
        final long traceId,
        final long authorization,
        final long budgetId,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        final MqttDisconnectFW disconnect = mqttDisconnectRO.tryWrap(buffer, offset, limit);

        int progress = offset;
        int reasonCode = NORMAL_DISCONNECT;
        if (disconnect == null)
        {
            reasonCode = PROTOCOL_ERROR;
        }
        else if ((disconnect.typeAndFlags() & 0b1111_1111) != DISCONNECT_FIXED_HEADER)
        {
            reasonCode = MALFORMED_PACKET;
        }

        if (reasonCode == 0)
        {
            server.onDecodeDisconnect(traceId, authorization, disconnect);
            server.decoder = decodePacketType;
            progress = disconnect.limit();
        }
        else
        {
            server.onDecodeError(traceId, authorization, reasonCode);
            server.decoder = decodeIgnoreAll;
        }

        return progress;
    }

    private int decodeIgnoreAll(
        MqttServer server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        return limit;
    }

    private int decodeUnknownType(
        MqttServer server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        server.onDecodeError(traceId, authorization, PROTOCOL_ERROR);
        server.decoder = decodeIgnoreAll;
        return limit;
    }

    @FunctionalInterface
    private interface MqttServerDecoder
    {
        int decode(
            MqttServer server,
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit);
    }

    private final class MqttServer
    {
        private final MessageConsumer network;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long affinity;
        private final long budgetId;

        private final Int2ObjectHashMap<MqttSubscribeStream> subscribers;
        private final Int2ObjectHashMap<MqttPublishStream> publishers;
        private final Map<String, MutableInteger> activeSubscribers;
        private final Map<String, MutableInteger> activePublishers;
        private final Int2ObjectHashMap<Subscription> subscriptionsByPacketId;

        private int initialBudget;
        private int initialPadding;
        private int replyPadding;
        private int replyBudget;

        private int decodeSlot = NO_SLOT;
        private int decodeSlotLimit;

        private int encodeSlot = NO_SLOT;
        private int encodeSlotOffset;
        private long encodeSlotTraceId;
        private int encodeSlotMaxLimit = Integer.MAX_VALUE;

        private MqttServerDecoder decoder;

        private int keepAlive;
        private boolean connected;

        private MqttServer(
            MessageConsumer network,
            long routeId,
            long initialId,
            long replyId,
            long affinity,
            long budgetId)
        {
            this.network = network;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = replyId;
            this.affinity = affinity;
            this.budgetId = budgetId;
            this.decoder = decodePacketType;
            this.subscribers = new Int2ObjectHashMap<>();
            this.publishers = new Int2ObjectHashMap<>();
            this.activeSubscribers = new HashMap<>();
            this.activePublishers = new HashMap<>();
            this.subscriptionsByPacketId = new Int2ObjectHashMap<>();
        }

        private void onNetwork(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onNetworkBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onNetworkData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onNetworkEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onNetworkAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onNetworkWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onNetworkReset(reset);
                break;
            case SignalFW.TYPE_ID:
                final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                onNetworkSignal(signal);
                break;
            default:
                break;
            }
        }

        private void onNetworkBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            final long authorization = begin.authorization();
            doNetworkBegin(traceId, authorization);
        }

        private void onNetworkData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final long authorization = data.authorization();

            initialBudget -= data.reserved();

            if (initialBudget < 0)
            {
                doNetworkReset(supplyTraceId.getAsLong(), authorization);
            }
            else
            {
                final long streamId = data.streamId();
                final long budgetId = data.budgetId();
                final OctetsFW payload = data.payload();

                DirectBuffer buffer = payload.buffer();
                int offset = payload.offset();
                int limit = payload.limit();

                if (decodeSlot != NO_SLOT)
                {
                    final MutableDirectBuffer slotBuffer = bufferPool.buffer(decodeSlot);
                    slotBuffer.putBytes(decodeSlotLimit, buffer, offset, limit - offset);
                    decodeSlotLimit += limit - offset;
                    buffer = slotBuffer;
                    offset = 0;
                    limit = decodeSlotLimit;
                }

                decodeNetwork(traceId, authorization, budgetId, buffer, offset, limit);
            }
        }

        private void onNetworkEnd(
            EndFW end)
        {
            final long authorization = end.authorization();
            if (decodeSlot == NO_SLOT)
            {
                final long traceId = end.traceId();

                cleanupDecodeSlotIfNecessary();

                doNetworkEnd(traceId, authorization);
            }
            decoder = decodeIgnoreAll;
        }

        private void onNetworkAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();
            doNetworkAbort(traceId, authorization);
        }

        private void onNetworkWindow(
            WindowFW window)
        {
            final int replyCredit = window.credit();

            replyBudget += replyCredit;
            replyPadding += window.padding();

            final int initialCredit = bufferPool.slotCapacity() - initialBudget;

            doNetworkWindow(supplyTraceId.getAsLong(), initialCredit);
        }

        private void onNetworkReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();
            doNetworkReset(traceId, authorization);
        }

        private void onNetworkSignal(
            SignalFW signal)
        {
            final long traceId = signal.traceId();
            doNetworkSignal(traceId);
        }

        private void onDecodeConnect(
            long traceId,
            long authorization,
            MqttConnectFW packet)
        {
            int reasonCode = SUCCESS;
            if (connected)
            {
                reasonCode = PROTOCOL_ERROR;
            }

            doEncodeConnack(traceId, authorization, reasonCode);

            if (reasonCode == 0)
            {
                connected = true;
                keepAlive = packet.keepAlive();
            }
            else
            {
                doNetworkEnd(traceId, authorization);
            }
        }

        private void onDecodePublish(
            long traceId,
            long authorization,
            MqttPublishFW publish)
        {
            final String topicName = publish.topicName().asString();

            String info = "info";

            final MqttDataExFW dataEx = mqttDataExRW
                                            .wrap(dataExtBuffer, 0, dataExtBuffer.capacity())
                                            .typeId(mqttTypeId)
                                            .topic(topicName)
                                            .expiryInterval(15)
                                            .contentType("message")
                                            .format(f -> f.set(MqttPayloadFormat.TEXT))
                                            .responseTopic(topicName)
                                            .correlationInfo(c -> c.bytes(b -> b.set(info.getBytes(UTF_8))))
                                            .build();

            OctetsFW payload = publish.payload();

            final RouteFW route = resolveTarget(routeId, authorization, topicName);
            if (route != null)
            {
                final long newRouteId = route.correlationId();
                final long newInitialId = supplyInitialId.applyAsLong(newRouteId);
                final long newReplyId = supplyReplyId.applyAsLong(newInitialId);
                final MessageConsumer newTarget = router.supplyReceiver(newInitialId);

                final int topicKey = topicKey(topicName);
                MqttServer.MqttPublishStream publishStream = publishers.computeIfAbsent(topicKey, s ->
                {
                    final MqttPublishStream newPublishStream = new MqttPublishStream(newTarget,
                                                                     newRouteId, newInitialId, newReplyId, 0, topicName);
                    newPublishStream.doApplicationBegin(traceId, authorization, affinity);
                    return newPublishStream;
                });
                publishStream.doApplicationData(traceId, authorization, payload, dataEx);
                correlations.put(newReplyId, publishStream);    // TODO: do we need to clean up correlations onAbort()?

                // doEncodePublish(traceId, authorization, topicName, );
            }
        }

        private void onDecodeSubscribe(
            long traceId,
            long authorization,
            MqttSubscribeFW subscribe)
        {
            final OctetsFW topicFilters = subscribe.topicFilters();
            final DirectBuffer buffer = topicFilters.buffer();
            final int limit = topicFilters.limit();
            final int offset = topicFilters.offset();
            final int packetId = subscribe.packetId();
            int subscriptionId = 0;
            int unrouteableMask = 0;

            Subscription subscription = new Subscription();
            subscriptionsByPacketId.put(packetId, subscription);
            MqttSubscriptionFW mqttSubscription;
            OctetsFW properties = subscribe.properties();
            final int propertiesOffset = properties.offset();
            final int propertiesLimit = properties.limit();

            MqttPropertyFW mqttProperty;

            for (int progress = propertiesOffset; progress < propertiesLimit; progress = mqttProperty.limit())
            {
                mqttProperty = mqttPropertyRO.tryWrap(buffer, progress, propertiesLimit);
                switch (mqttProperty.kind())
                {
                case 0x0b:
                    subscriptionId = mqttProperty.subscriptionId();
                    break;
                }
            }

            for (int progress = offset; progress < limit; progress = mqttSubscription.limit(), subscription.ackCount++)
            {
                mqttSubscription = mqttSubscriptionRO.tryWrap(buffer, progress, limit);
                if (mqttSubscription == null)
                {
                    break;
                }
                final String topicFilter = mqttSubscription.topicFilter().asString();
                final RouteFW route = resolveTarget(routeId, authorization, topicFilter);

                if (route != null)
                {
                    final long newRouteId = route.correlationId();
                    final long newInitialId = supplyInitialId.applyAsLong(newRouteId);
                    final long newReplyId = supplyReplyId.applyAsLong(newInitialId);
                    final MessageConsumer newTarget = router.supplyReceiver(newInitialId);

                    final MqttSubscribeStream subscribeStream = new MqttSubscribeStream(newTarget,
                                                    newRouteId, newInitialId, newReplyId, packetId, topicFilter, subscription);

                    subscribeStream.doApplicationBegin(traceId, authorization, affinity, topicFilter, subscriptionId);
                    subscribeStream.doApplicationData(traceId, authorization, buffer, offset, limit);

                    correlations.put(newReplyId, subscribeStream);

                    if (topicFilter == null)
                    {
                        onDecodeError(traceId, authorization, PROTOCOL_ERROR);
                        return;
                    }

                    final int key = System.identityHashCode(topicFilter.intern());
                    subscribers.put(key, subscribeStream);
                }
                else
                {
                    unrouteableMask |= 1 << subscription.ackCount;
                }
            }

            subscription.ackMask |= unrouteableMask;
        }

        private void onDecodeUnsubscribe(
            long traceId,
            long authorization,
            MqttUnsubscribeFW unsubscribe)
        {
            final OctetsFW topicFilters = unsubscribe.topicFilters();
            final DirectBuffer buffer = topicFilters.buffer();
            final int limit = topicFilters.limit();
            final int offset = topicFilters.offset();
            final int packetId = unsubscribe.packetId();

            MqttTopicFW topic;
            for (int progress = offset; progress < limit; progress = topic.limit())
            {
                topic = mqttTopicRO.tryWrap(buffer, progress, limit);
                if (topic == null)
                {
                    break;
                }
                final int topicKey = System.identityHashCode(topic.filter().asString().intern());
                subscribers.remove(topicKey);
            }
            doEncodeUnsuback(traceId, authorization, packetId);
        }

        private void onDecodePingReq(
            long traceId,
            long authorization,
            MqttPingReqFW ping)
        {
            doEncodePingResp(traceId, authorization);
        }

        private void onDecodeDisconnect(
            long traceId,
            long authorization,
            MqttDisconnectFW disconnect)
        {
            doNetworkEnd(traceId, authorization);
        }

        private void onDecodeError(
            long traceId,
            long authorization,
            int reasonCode)
        {
            cleanupStreams(traceId, authorization);
            if (connected)
            {
                doEncodeDisconnect(traceId, authorization, reasonCode);
            }
            else
            {
                doEncodeConnack(traceId, authorization, reasonCode);
            }
            doNetworkEnd(traceId, authorization);
        }

        private void doNetworkBegin(
            long traceId,
            long authorization)
        {
            doBegin(network, routeId, replyId, traceId, authorization, affinity, EMPTY_OCTETS);
            router.setThrottle(replyId, this::onNetwork);
        }

        private void doNetworkData(
            long traceId,
            long authorization,
            long budgetId,
            Flyweight payload)
        {
            doNetworkData(traceId, authorization, budgetId, payload.buffer(), payload.offset(), payload.limit());
        }

        private void doNetworkData(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            int maxLimit = limit;

            if (encodeSlot != NO_SLOT)
            {
                final MutableDirectBuffer encodeBuffer = bufferPool.buffer(encodeSlot);
                encodeBuffer.putBytes(encodeSlotOffset, buffer, offset, limit - offset);
                encodeSlotOffset += limit - offset;
                encodeSlotTraceId = traceId;

                buffer = encodeBuffer;
                offset = 0;
                limit = Math.min(encodeSlotOffset, encodeSlotMaxLimit);
                maxLimit = encodeSlotOffset;
            }

            encodeNetwork(traceId, authorization, budgetId, buffer, offset, limit, maxLimit);
        }

        private void doNetworkEnd(
            long traceId,
            long authorization)
        {
            cleanupEncodeSlotIfNecessary();
            doEnd(network, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doNetworkAbort(
            long traceId,
            long authorization)
        {
            cleanupEncodeSlotIfNecessary();
            doAbort(network, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doNetworkWindow(
            long traceId,
            int initialCredit)
        {
            if (initialCredit > 0)
            {
                initialBudget += initialCredit;

                doWindow(network, routeId, initialId, traceId, 0L, initialPadding, initialCredit);
            }
        }

        private void doNetworkReset(
            long traceId,
            long authorization)
        {
            cleanupDecodeSlotIfNecessary();
            doReset(network, routeId, initialId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doNetworkSignal(
            long traceId)
        {
            doSignal(network, routeId, initialId, traceId);
        }

        private void doEncodePublish(
            long traceId,
            long authorization,
            String topicName,
            OctetsFW payload)
        {
            OctetsFW properties = octetsRW.wrap(writeBuffer, 0, 0)
                                          .build();

            final MqttPublishFW publish = mqttPublishRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                              .typeAndFlags(0x30)
                                              .remainingLength(0x14)
                                              .topicName(topicName)
                                              .properties(properties)
                                              .payload(payload)
                                              .build();

            doNetworkData(traceId, authorization, 0L, publish);
        }

        private void doEncodeConnack(
            long traceId,
            long authorization,
            int reasonCode)
        {
            OctetsFW properties = octetsRW.wrap(writeBuffer, 0, 0)
                                          .build();

            final MqttConnackFW connack = mqttConnackRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                              .typeAndFlags(0x20)
                                              .remainingLength(properties.sizeof() + 3)
                                              .flags(0x00)
                                              .reasonCode(reasonCode)
                                              .propertiesLength(properties.sizeof())
                                              .properties(properties)
                                              .build();

            doNetworkData(traceId, authorization, 0L, connack);
        }

        private void doEncodeSuback(
            long traceId,
            long authorization,
            int packetId,
            int ackMask,
            int successMask)
        {
            final int ackCount = Integer.bitCount(ackMask);
            final byte[] subscriptions = new byte[ackCount];
            for (int i = 0; i < ackCount; i++)
            {
                final int ackIndex = 1 << i;
                subscriptions[i] = (byte) ((successMask & ackIndex) > 0 ? 0x00 : 0x8F);
            }

            OctetsFW reasonCodes = octetsRW
                                       .wrap(writeBuffer, 0, writeBuffer.capacity())
                                       .put(subscriptions)
                                       .build();

            final MqttSubackFW suback = mqttSubackRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                            .typeAndFlags(0x90)
                                            .remainingLength(reasonCodes.sizeof() + 1)
                                            .packetId(packetId)
                                            .propertiesLength(0x00)
                                            .reasonCodes(reasonCodes)
                                            .build();

            doNetworkData(traceId, authorization, 0L, suback);
        }

        private void doEncodeUnsuback(
            long traceId,
            long authorization,
            int packetId)
        {
            OctetsFW reasonCodes = octetsRW
                                       .wrap(writeBuffer, 0, writeBuffer.capacity())
                                       .put(new byte[]{0x00})
                                       .build();

            final MqttUnsubackFW unsuback = mqttUnsubackRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                                .typeAndFlags(0xa0)
                                                .remainingLength(reasonCodes.sizeof() + 1)
                                                .packetId(packetId)
                                                .propertiesLength(0x00)
                                                .reasonCodes(reasonCodes)
                                                .build();

            doNetworkData(traceId, authorization, 0L, unsuback);
        }

        private void doEncodePingResp(
            long traceId,
            long authorization)
        {
            final MqttPingRespFW ping = mqttPingRespRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                            .typeAndFlags(0xd0)
                                            .remainingLength(0x00)
                                            .build();

            doNetworkData(traceId, authorization, 0L, ping);
        }

        private void doEncodeDisconnect(
            long traceId,
            long authorization,
            int reasonCode)
        {
            OctetsFW properties = octetsRW
                                      .wrap(writeBuffer, 0, 0)
                                      .build();

            final MqttDisconnectFW disconnect = mqttDisconnectRW
                                                    .wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                                    .typeAndFlags(0xe0)
                                                    .remainingLength((byte) properties.sizeof() + 2)
                                                    .reasonCode(reasonCode)
                                                    .propertiesLength(properties.sizeof())
                                                    .properties(properties)
                                                    .build();

            doNetworkData(traceId, authorization, 0L, disconnect);
        }

        private void encodeNetwork(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit,
            int maxLimit)
        {
            encodeNetworkData(traceId, authorization, budgetId, buffer, offset, limit, maxLimit);
        }

        private void encodeNetworkData(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit,
            int maxLimit)
        {
            final int length = Math.max(Math.min(replyBudget - replyPadding, limit - offset), 0);

            if (length > 0)
            {
                final int reserved = length + replyPadding;

                replyBudget -= reserved;

                assert replyBudget >= 0;

                doData(network, routeId, replyId, traceId, authorization, budgetId,
                       reserved, buffer, offset, length, EMPTY_OCTETS);
            }

            final int maxLength = maxLimit - offset;
            final int remaining = maxLength - length;
            if (remaining > 0)
            {
                if (encodeSlot == NO_SLOT)
                {
                    encodeSlot = bufferPool.acquire(replyId);
                }
                else
                {
                    encodeSlotMaxLimit -= length;
                    assert encodeSlotMaxLimit >= 0;
                }

                if (encodeSlot == NO_SLOT)
                {
                    cleanupNetwork(traceId, authorization);
                }
                else
                {
                    final MutableDirectBuffer encodeBuffer = bufferPool.buffer(encodeSlot);
                    encodeBuffer.putBytes(0, buffer, offset + length, remaining);
                    encodeSlotOffset = remaining;
                }
            }
            else
            {
                cleanupEncodeSlotIfNecessary();

                if (publishers.isEmpty() && subscribers.isEmpty() && decoder == decodeIgnoreAll)
                {
                    doNetworkEnd(traceId, authorization);
                }
            }
        }

        private void decodeNetwork(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            MqttServerDecoder previous = null;
            int progress = offset;
            while (progress <= limit && previous != decoder)
            {
                previous = decoder;
                progress = decoder.decode(this, traceId, authorization, budgetId, buffer, progress, limit);
            }

            if (progress < limit)
            {
                if (decodeSlot == NO_SLOT)
                {
                    decodeSlot = bufferPool.acquire(initialId);
                }

                if (decodeSlot == NO_SLOT)
                {
                    cleanupNetwork(traceId, authorization);
                }
                else
                {
                    final MutableDirectBuffer slotBuffer = bufferPool.buffer(decodeSlot);
                    decodeSlotLimit = limit - progress;
                    slotBuffer.putBytes(0, buffer, progress, decodeSlotLimit);
                }
            }
            else
            {
                cleanupDecodeSlotIfNecessary();
            }
        }

        private void cleanupNetwork(
            long traceId,
            long authorization)
        {
            doNetworkReset(traceId, authorization);
            doNetworkAbort(traceId, authorization);
            cleanupStreams(traceId, authorization);
        }

        private void cleanupStreams(
            long traceId,
            long authorization)
        {
            subscribers.values().forEach(subscriber -> subscriber.cleanup(traceId, authorization));
            publishers.values().forEach(publisher -> publisher.cleanup(traceId, authorization));
        }

        private void cleanupDecodeSlotIfNecessary()
        {
            if (decodeSlot != NO_SLOT)
            {
                bufferPool.release(decodeSlot);
                decodeSlot = NO_SLOT;
                decodeSlotLimit = 0;
            }
        }

        private void cleanupEncodeSlotIfNecessary()
        {
            if (encodeSlot != NO_SLOT)
            {
                bufferPool.release(encodeSlot);
                encodeSlot = NO_SLOT;
                encodeSlotOffset = 0;
                encodeSlotTraceId = 0;
            }
        }

        private final class Subscription
        {
            private int ackCount;
            private int successMask;
            private int ackMask;

            private Subscription()
            {
                this.successMask = 0;
            }

            private void onSubscribeFailed(
                long traceId,
                long authorization,
                int packetId,
                int ackIndex)
            {
                final int bit = 1 << ackIndex;
                ackMask |= bit;
                onSubscribeCompleted(traceId, authorization, packetId);
            }

            private void onSubscribeSucceeded(
                long traceId,
                long authorization,
                int packetId,
                int ackIndex)
            {
                final int bit = 1 << ackIndex;
                successMask |= bit;
                ackMask |= bit;
                onSubscribeCompleted(traceId, authorization, packetId);
            }

            private void onSubscribeCompleted(
                long traceId,
                long authorization,
                int packetId)
            {
                if (Integer.bitCount(ackMask) == ackCount)
                {
                    doEncodeSuback(traceId, authorization, packetId, ackMask, successMask);
                }
            }
        }

        private abstract class MqttServerStream
        {
            abstract void onApplicationInitial(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length);

            abstract void onApplicationReply(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length);
        }

        private final class MqttPublishStream extends MqttServerStream
        {
            private final MessageConsumer application;

            private long routeId;
            private long initialId;
            private long replyId;

            private String topicFilter;

            private int state;

            private int packetId;

            private int initialBudget;
            private int initialPadding;
            private int replyBudget;

            private int initialSlot = NO_SLOT;
            private int initialSlotOffset;
            private long initialSlotTraceId;

            private Future<?> signalFuture;

            MqttPublishStream(
                MessageConsumer application,
                long routeId,
                long initialId,
                long replyId,
                int packetId,
                String topicFilter)
            {
                this.application = application;
                this.routeId = routeId;
                this.initialId = initialId;
                this.replyId = replyId;
                this.packetId = packetId;
                this.topicFilter = topicFilter;
            }

            private void doApplicationBegin(
                long traceId,
                long authorization,
                long affinity)
            {
                assert state == 0;
                state = MqttState.openingInitial(state);

                this.signalFuture = executor.schedule(publishTimeout, SECONDS, routeId, initialId,
                                                      PUBLISH_TIMEOUT_SIGNAL);

                router.setThrottle(initialId, this::onApplicationInitial);

                final MqttBeginExFW beginEx = mqttBeginExRW
                                                  .wrap(extBuffer, 0, extBuffer.capacity())
                                                  .typeId(mqttTypeId)
                                                  .role(r -> r.set(MqttRole.SENDER))
                                                  .clientId("client")
                                                  .topic(topicFilter)
                                                  .subscriptionId(0)
                                                  .build();

                doBegin(application, routeId, initialId, traceId, authorization, affinity, beginEx);

                final MutableInteger value = activePublishers.computeIfAbsent(topicFilter, key -> new MutableInteger());
                value.value++;
            }

            // TODO - when getting application data, must also get onApplicationData
            private void doApplicationData(
                long traceId,
                long authorization,
                OctetsFW payload,
                Flyweight extension)
            {
                assert MqttState.initialOpening(state);

                refreshPublishTimeout();

                doData(application, routeId, initialId, traceId, authorization, 0L, payload.sizeof(),
                       payload.buffer(), payload.offset(), payload.sizeof(), extension);
            }

            private void doApplicationEnd(
                long traceId,
                long authorization,
                Flyweight extension)
            {
                if (!MqttState.initialOpened(state) || initialSlot != NO_SLOT)
                {
                    state = MqttState.closingInitial(state);
                }
                else
                {
                    flushApplicationEnd(traceId, authorization, extension);
                }
            }

            private void doApplicationAbort(
                long traceId,
                long authorization,
                Flyweight extension)
            {
                setInitialClosed();

                doAbort(application, routeId, initialId, traceId, authorization, extension);
            }

            private void doApplicationAbortIfNecessary(
                long traceId,
                long authorization)
            {
                if (!MqttState.initialClosed(state))
                {
                    doApplicationAbort(traceId, authorization, EMPTY_OCTETS);
                }
            }

            private void flushApplicationEnd(
                long traceId,
                long authorization,
                Flyweight extension)
            {
                setInitialClosed();
                doEnd(application, routeId, initialId, traceId, authorization, extension);
            }

            private void setInitialClosed()
            {
                assert !MqttState.initialClosed(state);

                state = MqttState.closeInitial(state);
                cleanupInitialSlotIfNecessary();

                if (MqttState.closed(state))
                {
                    publishers.remove(topicKey(topicFilter));
                    final MutableInteger count = activePublishers.get(topicFilter);

                    assert count != null;

                    count.value--;

                    assert count.value >= 0;

                    if (count.value == 0)
                    {
                        activePublishers.remove(topicFilter);
                    }
                }
            }

            @Override
            public void onApplicationInitial(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                switch (msgTypeId)
                {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    onApplicationWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    onApplicationReset(reset);
                    break;
                case SignalFW.TYPE_ID:
                    final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                    onApplicationSignal(signal);
                    break;
                }
            }

            private void onApplicationWindow(
                WindowFW window)
            {
            }

            private void onApplicationReset(
                ResetFW reset)
            {
            }


            private void onApplicationSignal(
                SignalFW signal)
            {
                final long signalId = signal.signalId();

                switch ((int) signalId)
                {
                case PUBLISH_TIMEOUT_SIGNAL:
                    onPublishTimeoutSignal(signal);
                    break;
                default:
                    break;
                }
            }

            private void onPublishTimeoutSignal(
                SignalFW signal)
            {
                final long traceId = signal.traceId();
                final long authorization = signal.authorization();

                final MqttEndExFW endEx = mqttEndExRW
                                              .wrap(extBuffer, 0, extBuffer.capacity())
                                              .typeId(mqttTypeId)
                                              .build();

                doApplicationEnd(traceId, authorization, endEx);
            }

            @Override
            public void onApplicationReply(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                switch (msgTypeId)
                {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    onApplicationBegin(begin);
                    break;
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    onApplicationData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    onApplicationEnd(end);
                    break;
                }
            }

            private void onApplicationBegin(
                BeginFW begin)
            {
            }

            private void onApplicationData(
                DataFW data)
            {
                final long traceId = data.traceId();
                final OctetsFW extension = data.extension();
                final long authorization = data.authorization();

                final DirectBuffer exBuffer = extension.buffer();
                final int exOffset = extension.offset();
                final int exLength = extension.sizeof();

                final MqttDataExFW dataEx = mqttDataExRO.wrap(exBuffer, exOffset, exOffset + exLength);

                final String topicName = dataEx.topic().asString();
                doEncodePublish(traceId, authorization, topicName, data.payload());
            }

            private void onApplicationEnd(
                EndFW end)
            {
                setReplyClosed();
            }

            private void doApplicationReset(
                long traceId,
                long authorization)
            {
                setReplyClosed();

                doReset(application, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
            }

            private void doApplicationResetIfNecessary(
                long traceId,
                long authorization)
            {
                correlations.remove(replyId);

                doApplicationReset(traceId, authorization);
            }

            private void cleanupInitialSlotIfNecessary()
            {
                if (initialSlot != NO_SLOT)
                {
                    bufferPool.release(initialSlot);
                    initialSlot = NO_SLOT;
                    initialSlotOffset = 0;
                    initialSlotTraceId = 0;
                }
            }

            private void setReplyClosed()
            {
                assert !MqttState.replyClosed(state);

                state = MqttState.closeReply(state);

                if (MqttState.closed(state))
                {
                    publishers.remove(topicKey(topicFilter));
                    final MutableInteger count = activePublishers.get(topicFilter);

                    assert count != null;

                    count.value--;

                    assert count.value >= 0;

                    if (count.value == 0)
                    {
                        activePublishers.remove(topicFilter);
                    }
                }
            }

            private void cleanup(
                long traceId,
                long authorization)
            {
                doApplicationAbortIfNecessary(traceId, authorization);
                doApplicationResetIfNecessary(traceId, authorization);
                cancelTimerIfNecessary();
            }

            private void refreshPublishTimeout()
            {
                if (signalFuture != null)
                {
                    signalFuture.cancel(false);
                }

                this.signalFuture = executor.schedule(publishTimeout, SECONDS, routeId, initialId,
                                                      PUBLISH_TIMEOUT_SIGNAL);
            }

            private void cancelTimerIfNecessary()
            {
                if (signalFuture != null)
                {
                    signalFuture.cancel(true);
                    signalFuture = null;
                }
            }
        }

        private final class MqttSubscribeStream extends MqttServerStream
        {
            private final MessageConsumer application;
            private final int ackIndex;

            private long routeId;
            private long initialId;
            private long replyId;

            private String topicFilter;

            private int state;

            private Subscription subscription;
            private int packetId;

            private int initialBudget;
            private int initialPadding;
            private int replyBudget;

            private int initialSlot = NO_SLOT;
            private int initialSlotOffset;
            private long initialSlotTraceId;

            MqttSubscribeStream(
                MessageConsumer application,
                long routeId,
                long initialId,
                long replyId,
                int packetId,
                String topicFilter,
                Subscription subscription)
            {
                this.application = application;
                this.routeId = routeId;
                this.initialId = initialId;
                this.replyId = replyId;
                this.packetId = packetId;
                this.topicFilter = topicFilter;
                this.subscription = subscription;
                this.ackIndex = subscription != null ? subscription.ackCount : -1;
            }

            private void doApplicationBegin(
                long traceId,
                long authorization,
                long affinity,
                String topicFilter,
                int subscriptionId)
            {
                assert state == 0;
                state = MqttState.openingInitial(state);

                router.setThrottle(initialId, this::onApplicationInitial);

                final MqttBeginExFW beginEx = mqttBeginExRW
                                                  .wrap(extBuffer, 0, extBuffer.capacity())
                                                  .typeId(mqttTypeId)
                                                  .role(r -> r.set(MqttRole.RECEIVER))
                                                  .clientId("client")
                                                  .topic(topicFilter)
                                                  .subscriptionId(subscriptionId)
                                                  .build();

                doBegin(application, routeId, initialId, traceId, authorization, affinity, beginEx);
                final MutableInteger value = activeSubscribers.computeIfAbsent(topicFilter, key -> new MutableInteger());
                value.value++;
            }

            private void doApplicationData(
                long traceId,
                long authorization,
                DirectBuffer buffer,
                int offset,
                int limit)
            {
                assert MqttState.initialOpening(state);


                flushApplicationData(traceId, authorization, buffer, offset, limit);
                // doData(application, routeId, initialId, traceId, authorization, 0L, payload.sizeof(),
                //        payload.buffer(), payload.offset(), payload.sizeof(), extension);
            }

            private void doApplicationAbort(
                long traceId,
                long authorization,
                Flyweight extension)
            {
                setInitialClosed();

                doAbort(application, routeId, initialId, traceId, authorization, extension);
            }

            private void doApplicationAbortIfNecessary(
                long traceId,
                long authorization)
            {
                if (!MqttState.initialClosed(state))
                {
                    doApplicationAbort(traceId, authorization, EMPTY_OCTETS);
                }
            }

            private void setInitialClosed()
            {
                assert !MqttState.initialClosed(state);

                state = MqttState.closeInitial(state);
                cleanupInitialSlotIfNecessary();

                if (MqttState.closed(state))
                {
                    subscribers.remove(topicKey(topicFilter));
                    final MutableInteger count = activeSubscribers.get(topicFilter);

                    assert count != null;

                    count.value--;

                    assert count.value >= 0;

                    if (count.value == 0)
                    {
                        activeSubscribers.remove(topicFilter);
                    }
                }
            }

            private void cleanupInitialSlotIfNecessary()
            {
                if (initialSlot != NO_SLOT)
                {
                    bufferPool.release(initialSlot);
                    initialSlot = NO_SLOT;
                    initialSlotOffset = 0;
                    initialSlotTraceId = 0;
                }
            }

            @Override
            public void onApplicationInitial(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                switch (msgTypeId)
                {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    onApplicationWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    onApplicationReset(reset);
                    break;
                }
            }

            private void onApplicationWindow(
                WindowFW window)
            {
                final long traceId = window.traceId();
                final long authorization = window.authorization();
                final int credit = window.credit();
                final int padding = window.padding();

                state = MqttState.openInitial(state);

                initialBudget += credit;
                initialPadding = padding;

                subscription.onSubscribeSucceeded(traceId, authorization, packetId, ackIndex);

                // if (initialSlot != NO_SLOT)
                // {
                final MutableDirectBuffer buffer = bufferPool.buffer(initialSlot);
                final int offset = 0;
                final int limit = initialSlotOffset;

                flushApplicationData(traceId, authorization, buffer, offset, limit);
                // }

                // if (initialSlot == NO_SLOT)
                // {
                //     if (!MqttState.initialClosed(padding))
                //     {
                //         if (MqttState.initialClosing(state))
                //         {
                //             // TODO: trailers extension?
                //             flushApplicationEnd(traceId, authorization, EMPTY_OCTETS);
                //         }
                //         else
                //         {
                //             // flushRequestWindowUpdate(traceId, authorization);
                //         }
                //     }
                // }

                if (!MqttState.initialClosed(padding))
                {
                    if (MqttState.initialClosing(state))
                    {
                        flushApplicationEnd(traceId, authorization, EMPTY_OCTETS);
                    }
                }
            }

            private void onApplicationReset(
                ResetFW reset)
            {
                setInitialClosed();

                final long traceId = reset.traceId();
                final long authorization = reset.authorization();
                subscription.onSubscribeFailed(traceId, authorization, packetId, ackIndex);

                cleanup(traceId, authorization);
            }

            @Override
            public void onApplicationReply(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                switch (msgTypeId)
                {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    onApplicationBegin(begin);
                    break;
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    onApplicationData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    onApplicationEnd(end);
                    break;
                }
            }

            private void onApplicationBegin(
                BeginFW begin)
            {
                state = MqttState.openReply(state);

                final MqttBeginExFW beginEx = begin.extension().get(mqttBeginExRO::tryWrap);
            }

            private void onApplicationData(
                DataFW data)
            {
                final long traceId = data.traceId();
                final long authorization = data.authorization();
                final OctetsFW extension = data.extension();

                final MqttDataExFW dataEx = extension.get(mqttDataExRO::tryWrap);

                final String topicName = dataEx.topic().asString();
                doEncodePublish(traceId, authorization, topicName, data.payload());
            }

            private void onApplicationEnd(
                EndFW end)
            {
                setReplyClosed();
            }

            private void doApplicationReset(
                long traceId,
                long authorization)
            {
                setReplyClosed();

                doReset(application, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
            }

            private void doApplicationResetIfNecessary(
                long traceId,
                long authorization)
            {
                correlations.remove(replyId);

                if (!MqttState.replyClosed(state))
                {
                    doApplicationReset(traceId, authorization);
                }
            }

            private void flushApplicationData(
                long traceId,
                long authorization,
                DirectBuffer buffer,
                int offset,
                int limit)
            {
                final int maxLength = limit - offset;
                final int length = Math.max(Math.min(initialBudget - initialPadding, maxLength), 0);

                if (length > 0)
                {
                    final int reserved = length + initialPadding;

                    initialBudget -= reserved;

                    assert initialBudget >= 0;

                    doData(application, routeId, replyId, traceId, authorization, budgetId,
                        reserved, buffer, offset, length, EMPTY_OCTETS);
                }

                final int remaining = maxLength - length;
                if (remaining > 0)
                {
                    if (initialSlot == NO_SLOT)
                    {
                        initialSlot = bufferPool.acquire(initialId);
                    }

                    if (initialSlot == NO_SLOT)
                    {
                        // doEncodeRstStream(traceId, authorization, streamId, Http2ErrorCode.INTERNAL_ERROR);
                        // cleanup(traceId, authorization);
                    }
                    else
                    {
                        final MutableDirectBuffer requestBuffer = bufferPool.buffer(initialSlot);
                        requestBuffer.putBytes(0, buffer, offset, remaining);
                        initialSlotOffset = remaining;
                        initialSlotTraceId = traceId;
                    }
                }
                else
                {
                    cleanupInitialSlotIfNecessary();
                }
            }

            private void flushApplicationEnd(
                long traceId,
                long authorization,
                Flyweight extension)
            {
                setInitialClosed();
                doEnd(application, routeId, initialId, traceId, authorization, extension);
            }

            private void setReplyClosed()
            {
                assert !MqttState.replyClosed(state);

                state = MqttState.closeReply(state);

                if (MqttState.closed(state))
                {
                    subscribers.remove(topicKey(topicFilter));
                    final MutableInteger count = activeSubscribers.get(topicFilter);

                    assert count != null;

                    count.value--;

                    assert count.value >= 0;

                    if (count.value == 0)
                    {
                        activeSubscribers.remove(topicFilter);
                    }
                }
            }

            private void cleanup(
                long traceId,
                long authorization)
            {
                doApplicationAbortIfNecessary(traceId, authorization);
                doApplicationResetIfNecessary(traceId, authorization);
            }
        }
    }

    private static final class MqttState
    {
        private static final int INITIAL_OPENING = 0x10;
        private static final int INITIAL_OPENED = 0x20;
        private static final int INITIAL_CLOSING = 0x40;
        private static final int INITIAL_CLOSED = 0x80;
        private static final int REPLY_OPENED = 0x01;
        private static final int REPLY_CLOSING = 0x02;
        private static final int REPLY_CLOSED = 0x04;

        static int openingInitial(
            int state)
        {
            return state | INITIAL_OPENING;
        }

        static int openInitial(
            int state)
        {
            return openingInitial(state) | INITIAL_OPENED;
        }

        static int closingInitial(
            int state)
        {
            return state | INITIAL_CLOSING;
        }

        static int closeInitial(
            int state)
        {
            return closingInitial(state) | INITIAL_CLOSED;
        }

        static boolean initialOpening(
            int state)
        {
            return (state & INITIAL_OPENING) != 0;
        }

        static boolean initialOpened(
            int state)
        {
            return (state & INITIAL_OPENED) != 0;
        }

        static boolean initialClosing(
            int state)
        {
            return (state & INITIAL_CLOSING) != 0;
        }

        static boolean initialClosed(
            int state)
        {
            return (state & INITIAL_CLOSED) != 0;
        }

        static boolean closed(
            int state)
        {
            return initialClosed(state) && replyClosed(state);
        }

        static int openReply(
            int state)
        {
            return state | REPLY_OPENED;
        }

        static boolean replyOpened(
            int state)
        {
            return (state & REPLY_OPENED) != 0;
        }

        static int closingReply(
            int state)
        {
            return state | REPLY_CLOSING;
        }

        static int closeReply(
            int state)
        {
            return closingReply(state) | REPLY_CLOSED;
        }

        static boolean replyClosed(
            int state)
        {
            return (state & REPLY_CLOSED) != 0;
        }
    }
}
