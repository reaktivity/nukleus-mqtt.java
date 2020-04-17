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
package org.reaktivity.nukleus.mqtt.internal.stream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.reaktivity.nukleus.budget.BudgetCreditor.NO_CREDITOR_INDEX;
import static org.reaktivity.nukleus.budget.BudgetDebitor.NO_DEBITOR_INDEX;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.concurrent.Signaler.NO_CANCEL_ID;
import static org.reaktivity.nukleus.mqtt.internal.MqttReasonCodes.MALFORMED_PACKET;
import static org.reaktivity.nukleus.mqtt.internal.MqttReasonCodes.NORMAL_DISCONNECT;
import static org.reaktivity.nukleus.mqtt.internal.MqttReasonCodes.NO_SUBSCRIPTION_EXISTED;
import static org.reaktivity.nukleus.mqtt.internal.MqttReasonCodes.PROTOCOL_ERROR;
import static org.reaktivity.nukleus.mqtt.internal.MqttReasonCodes.SUCCESS;
import static org.reaktivity.nukleus.mqtt.internal.MqttReasonCodes.TOPIC_FILTER_INVALID;
import static org.reaktivity.nukleus.mqtt.internal.MqttReasonCodes.UNSUPPORTED_PROTOCOL_VERSION;
import static org.reaktivity.nukleus.mqtt.internal.types.MqttCapabilities.PUBLISH_AND_SUBSCRIBE;
import static org.reaktivity.nukleus.mqtt.internal.types.MqttCapabilities.PUBLISH_ONLY;
import static org.reaktivity.nukleus.mqtt.internal.types.MqttCapabilities.SUBSCRIBE_ONLY;
import static org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPropertyFW.KIND_CONTENT_TYPE;
import static org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPropertyFW.KIND_CORRELATION_DATA;
import static org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPropertyFW.KIND_EXPIRY_INTERVAL;
import static org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPropertyFW.KIND_PAYLOAD_FORMAT;
import static org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPropertyFW.KIND_RESPONSE_TOPIC;
import static org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPropertyFW.KIND_SUBSCRIPTION_ID;
import static org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPropertyFW.KIND_TOPIC_ALIAS;
import static org.reaktivity.nukleus.mqtt.internal.types.stream.DataFW.FIELD_OFFSET_PAYLOAD;

import java.util.EnumMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.budget.BudgetCreditor;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.Signaler;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.mqtt.internal.MqttConfiguration;
import org.reaktivity.nukleus.mqtt.internal.MqttNukleus;
import org.reaktivity.nukleus.mqtt.internal.MqttValidator;
import org.reaktivity.nukleus.mqtt.internal.types.Flyweight;
import org.reaktivity.nukleus.mqtt.internal.types.MqttBinaryFW;
import org.reaktivity.nukleus.mqtt.internal.types.MqttCapabilities;
import org.reaktivity.nukleus.mqtt.internal.types.MqttPayloadFormat;
import org.reaktivity.nukleus.mqtt.internal.types.OctetsFW;
import org.reaktivity.nukleus.mqtt.internal.types.String8FW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttConnackFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttConnectFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttDisconnectFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPacketHeaderFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPacketType;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPingReqFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPingRespFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPropertiesFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPropertyFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPublishFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttSubackFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttSubscribeFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttSubscribePayloadFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttUnsubackFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttUnsubackPayloadFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttUnsubscribeFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttUnsubscribePayloadFW;
import org.reaktivity.nukleus.mqtt.internal.types.control.MqttRouteExFW;
import org.reaktivity.nukleus.mqtt.internal.types.control.RouteFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.DataFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.EndFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.FlushFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.MqttBeginExFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.MqttDataExFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.MqttFlushExFW;
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

    private static final int PUBLISH_EXPIRED_SIGNAL = 1;

    private final RouteFW routeRO = new RouteFW();
    private final MqttRouteExFW mqttRouteExRO = new MqttRouteExFW();

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
    private final FlushFW.Builder flushRW = new FlushFW.Builder();

    private final MqttDataExFW mqttDataExRO = new MqttDataExFW();

    private final MqttBeginExFW.Builder mqttBeginExRW = new MqttBeginExFW.Builder();
    private final MqttDataExFW.Builder mqttDataExRW = new MqttDataExFW.Builder();
    private final MqttFlushExFW.Builder mqttFlushExRW = new MqttFlushExFW.Builder();

    private final MqttPacketHeaderFW mqttPacketHeaderRO = new MqttPacketHeaderFW();
    private final MqttConnectFW mqttConnectRO = new MqttConnectFW();
    private final MqttPublishFW mqttPublishRO = new MqttPublishFW();
    private final MqttSubscribeFW mqttSubscribeRO = new MqttSubscribeFW();
    private final MqttSubscribePayloadFW mqttSubscribePayloadRO = new MqttSubscribePayloadFW();
    private final MqttUnsubscribeFW mqttUnsubscribeRO = new MqttUnsubscribeFW();
    private final MqttUnsubscribePayloadFW mqttUnsubscribePayloadRO = new MqttUnsubscribePayloadFW();
    private final MqttPingReqFW mqttPingReqRO = new MqttPingReqFW();
    private final MqttDisconnectFW mqttDisconnectRO = new MqttDisconnectFW();

    private final OctetsFW octetsRO = new OctetsFW();
    private final OctetsFW.Builder octetsRW = new OctetsFW.Builder();
    private final MqttPropertyFW mqttPropertyRO = new MqttPropertyFW();
    private final MqttPropertyFW.Builder mqttPropertyRW = new MqttPropertyFW.Builder();

    private final MqttConnackFW.Builder mqttConnackRW = new MqttConnackFW.Builder();
    private final MqttPublishFW.Builder mqttPublishRW = new MqttPublishFW.Builder();
    private final MqttSubackFW.Builder mqttSubackRW = new MqttSubackFW.Builder();
    private final MqttUnsubackFW.Builder mqttUnsubackRW = new MqttUnsubackFW.Builder();
    private final MqttUnsubackPayloadFW.Builder mqttUnsubackPayloadRW = new MqttUnsubackPayloadFW.Builder();
    private final MqttPingRespFW.Builder mqttPingRespRW = new MqttPingRespFW.Builder();
    private final MqttDisconnectFW.Builder mqttDisconnectRW = new MqttDisconnectFW.Builder();

    private final Signaler signaler;

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer extBuffer;
    private final MutableDirectBuffer dataExtBuffer;
    private final MutableDirectBuffer payloadBuffer;
    private final MutableDirectBuffer propertyBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final LongSupplier supplyBudgetId;

    private final String clientId;
    private final long publishTimeoutMillis;

    private final MqttValidator validator;

    private final Long2ObjectHashMap<MessageConsumer> correlations;
    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);
    private final int mqttTypeId;

    private final BufferPool bufferPool;
    private final BudgetCreditor creditor;
    private final LongFunction<BudgetDebitor> supplyDebitor;

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
        BudgetCreditor creditor,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyBudgetId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        LongFunction<BudgetDebitor> supplyDebitor,
        Signaler signaler)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.extBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.dataExtBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.propertyBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.payloadBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.bufferPool = bufferPool;
        this.creditor = creditor;
        this.supplyDebitor = supplyDebitor;
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyBudgetId = requireNonNull(supplyBudgetId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.correlations = new Long2ObjectHashMap<>();
        this.mqttTypeId = supplyTypeId.applyAsInt(MqttNukleus.NAME);
        this.signaler = signaler;
        this.clientId = config.getClientId();
        this.publishTimeoutMillis = SECONDS.toMillis(config.getPublishTimeout());
        this.validator = new MqttValidator();
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
                final MqttRouteExFW mqttRouteEx = routeEx.get(routeExRO::tryWrap);
                final String routeTopic = mqttRouteEx.topic().asString();
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

            final MqttServer connection = new MqttServer(sender, routeId, initialId, replyId, affinity, budgetId);
            newStream = connection::onNetwork;
        }
        return newStream;
    }

    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long replyId = begin.streamId();

        return correlations.remove(replyId);
    }

    private RouteFW resolveTarget(
        long routeId,
        long authorization,
        String topicFilter,
        MqttCapabilities capabilities)
    {
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            final OctetsFW ext = route.extension();
            if (ext.sizeof() > 0)
            {
                final MqttRouteExFW routeEx = ext.get(mqttRouteExRO::wrap);
                final String topicEx = routeEx.topic().asString();
                final MqttCapabilities routeCapabilities = routeEx.capabilities().get();

                return topicEx.equals(topicFilter) && (capabilities == PUBLISH_AND_SUBSCRIBE ?
                                           (capabilities.value() & routeCapabilities.value()) == PUBLISH_AND_SUBSCRIBE.value() :
                                           (capabilities.value() & routeCapabilities.value()) != 0);
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
        long authorization,
        long budgetId,
        int credit,
        int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                    .routeId(routeId)
                                    .streamId(streamId)
                                    .traceId(traceId)
                                    .authorization(authorization)
                                    .budgetId(budgetId)
                                    .credit(credit)
                                    .padding(padding)
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

    private void doFlush(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        Consumer<OctetsFW.Builder> extension)
    {
        final FlushFW flush = flushRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(streamId)
                                     .traceId(traceId)
                                     .authorization(authorization)
                                     .budgetId(budgetId)
                                     .reserved(reserved)
                                     .extension(extension)
                                     .build();

        receiver.accept(flush.typeId(), flush.buffer(), flush.offset(), flush.sizeof());
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
        final MqttPacketHeaderFW packet = mqttPacketHeaderRO.tryWrap(buffer, offset, limit);

        if (packet != null)
        {
            final int length = packet.remainingLength();
            final MqttPacketType packetType = MqttPacketType.valueOf(packet.typeAndFlags() >> 4);
            final MqttServerDecoder decoder = decodersByPacketType.getOrDefault(packetType, decodeUnknownType);

            if (limit - packet.limit() >= length)
            {
                server.decodeablePacketBytes = packet.sizeof() + length;
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
        final int length = limit - offset;

        int progress = offset;

        if (length > 0)
        {
            int reasonCode = SUCCESS;

            final MqttConnectFW mqttConnect = mqttConnectRO.tryWrap(buffer, offset, limit);
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
        final int length = limit - offset;

        int progress = offset;

        decode:
        if (length >= server.decodeablePacketBytes)
        {
            int reasonCode = SUCCESS;

            final MqttPublishFW publish = mqttPublishRO.tryWrap(buffer, offset, offset + server.decodeablePacketBytes);
            if (publish == null)
            {
                reasonCode = PROTOCOL_ERROR;
            }

            final String topicName = publish.topicName().asString();
            if (topicName == null)
            {
                reasonCode = PROTOCOL_ERROR;
            }

            if (reasonCode == 0)
            {
                final int topicKey = topicKey(topicName);
                MqttServer.MqttServerStream publisher = server.streams.get(topicKey);

                if (publisher == null)
                {
                    publisher = server.resolvePublisher(traceId, authorization, topicName);
                    if (publisher == null)
                    {
                        server.decodeablePacketBytes = 0;
                        server.decoder = decodePacketType;
                        progress = publish.limit();
                        break decode;
                    }
                }

                assert publisher != null;

                final OctetsFW payload = publish.payload();
                final int payloadSize = payload.sizeof();

                boolean canPublish = MqttState.initialOpened(publisher.state);

                int reserved = payloadSize + publisher.initialPadding;
                canPublish &= reserved <= publisher.initialBudget;

                if (canPublish && publisher.debitorIndex != NO_DEBITOR_INDEX)
                {
                    final int minimum = reserved; // TODO: fragmentation
                    reserved = publisher.debitor.claim(publisher.debitorIndex, publisher.initialId, minimum, reserved);
                }

                if (canPublish && reserved != 0) // TODO: zero length messages (throttled)
                {
                    server.onDecodePublish(traceId, authorization, reserved, publish);
                    server.decodeablePacketBytes = 0;
                    server.decoder = decodePacketType;
                    progress = publish.limit();
                }
            }
            else
            {
                server.onDecodeError(traceId, authorization, reasonCode);
                server.decoder = decodeIgnoreAll;
            }
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
        final int length = limit - offset;

        int progress = offset;

        if (length > 0)
        {
            int reasonCode = SUCCESS;

            final MqttSubscribeFW subscribe = mqttSubscribeRO.tryWrap(buffer, offset, limit);
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
        final int length = limit - offset;

        int progress = offset;

        if (length > 0)
        {
            int reasonCode = SUCCESS;

            final MqttUnsubscribeFW unsubscribe = mqttUnsubscribeRO.tryWrap(buffer, offset, limit);
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
        final int length = limit - offset;

        int progress = offset;

        if (length > 0)
        {
            final MqttPingReqFW ping = mqttPingReqRO.tryWrap(buffer, offset, limit);
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
        final int length = limit - offset;

        int progress = offset;

        if (length > 0)
        {
            int reasonCode = NORMAL_DISCONNECT;

            final MqttDisconnectFW disconnect = mqttDisconnectRO.tryWrap(buffer, offset, limit);
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
        private final long replyBudgetId;

        private final Int2ObjectHashMap<MqttServerStream> streams;
        private final Int2ObjectHashMap<MutableInteger> activeStreamsByTopic;
        private final Int2ObjectHashMap<Subscription> subscriptionsByPacketId;

        private int decodeBudget;
        private int encodeBudget;
        private int encodePadding;

        private long replyBudgetIndex = NO_CREDITOR_INDEX;
        private int sharedReplyBudget;

        private int decodeSlot = NO_SLOT;
        private int decodeSlotOffset;
        private int decodeSlotReserved;

        private int encodeSlot = NO_SLOT;
        private int encodeSlotOffset;
        private long encodeSlotTraceId;

        private MqttServerDecoder decoder;
        private int decodeablePacketBytes;

        private int keepAlive;
        private boolean connected;

        private int state;

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
            this.replyBudgetId = budgetId;
            this.decoder = decodePacketType;
            this.streams = new Int2ObjectHashMap<>();
            this.activeStreamsByTopic = new Int2ObjectHashMap<>();
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
            default:
                break;
            }
        }

        private void onNetworkBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            final long authorization = begin.authorization();

            state = MqttState.openingInitial(state);

            doNetworkBegin(traceId, authorization);
            doNetworkWindow(traceId, authorization, bufferPool.slotCapacity(), 0, 0L);
        }

        private void onNetworkData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final long authorization = data.authorization();

            decodeBudget -= data.reserved();

            if (decodeBudget < 0)
            {
                doNetworkReset(supplyTraceId.getAsLong(), authorization);
            }
            else
            {
                final long budgetId = data.budgetId();
                final OctetsFW payload = data.payload();

                DirectBuffer buffer = payload.buffer();
                int offset = payload.offset();
                int limit = payload.limit();
                int reserved = data.reserved();

                if (decodeSlot != NO_SLOT)
                {
                    final MutableDirectBuffer slotBuffer = bufferPool.buffer(decodeSlot);
                    slotBuffer.putBytes(decodeSlotOffset, buffer, offset, limit - offset);
                    decodeSlotOffset += limit - offset;
                    decodeSlotReserved += reserved;

                    buffer = slotBuffer;
                    offset = 0;
                    limit = decodeSlotOffset;
                    reserved = decodeSlotReserved;
                }

                decodeNetwork(traceId, authorization, budgetId, reserved, buffer, offset, limit);

                final int initialCredit = reserved - decodeSlotReserved;
                if (initialCredit > 0)
                {
                    doNetworkWindow(traceId, authorization, initialCredit, 0, 0);
                }
            }
        }

        private void onNetworkEnd(
            EndFW end)
        {
            final long authorization = end.authorization();

            state = MqttState.closeInitial(state);

            if (decodeSlot == NO_SLOT)
            {
                final long traceId = end.traceId();

                cleanupStreams(traceId, authorization);

                doNetworkEndIfNecessary(traceId, authorization);
            }

            decoder = decodeIgnoreAll;
        }

        private void onNetworkAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();

            cleanupNetwork(traceId, authorization);
        }

        private void onNetworkWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long authorization = window.authorization();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            state = MqttState.openReply(state);

            encodeBudget += credit;
            encodePadding += padding;

            if (encodeSlot != NO_SLOT)
            {
                final MutableDirectBuffer buffer = bufferPool.buffer(encodeSlot);
                final int limit = encodeSlotOffset;

                encodeNetwork(encodeSlotTraceId, authorization, budgetId, buffer, 0, limit);
            }

            if (encodeSlot == NO_SLOT)
            {
                streams.values().forEach(s -> s.flushReplyWindow(traceId, authorization));
            }

            final int slotCapacity = bufferPool.slotCapacity();
            final int sharedReplyCredit = Math.min(slotCapacity, encodeBudget - encodeSlotOffset - sharedReplyBudget);

            if (sharedReplyCredit > 0)
            {
                final long sharedBudgetPrevious = creditor.credit(traceId, replyBudgetIndex, sharedReplyCredit);
                sharedReplyBudget += sharedReplyCredit;

                assert sharedBudgetPrevious <= slotCapacity
                    : String.format("%d <= %d, replyBudget = %d",
                    sharedBudgetPrevious, slotCapacity, encodeBudget);

                assert sharedReplyCredit <= slotCapacity
                    : String.format("%d <= %d", sharedReplyCredit, slotCapacity);
            }
        }

        private void onNetworkReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();

            cleanupNetwork(traceId, authorization);
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

        private MqttServerStream resolvePublisher(
            long traceId,
            long authorization,
            String topic)
        {
            MqttServerStream stream = null;

            final RouteFW route = resolveTarget(routeId, authorization, topic, PUBLISH_ONLY);
            if (route != null)
            {
                final long resolvedId = route.correlationId();
                final int topicKey = topicKey(topic);

                stream = streams.computeIfAbsent(topicKey, s -> new MqttServerStream(resolvedId, 0, topic));
                stream.doApplicationBeginOrFlush(traceId, authorization, affinity, topic, 0, PUBLISH_ONLY);
            }

            return stream;
        }

        private void onDecodePublish(
            long traceId,
            long authorization,
            int reserved,
            MqttPublishFW publish)
        {
            final String topic = publish.topicName().asString();
            final MqttPropertiesFW properties = publish.properties();
            final OctetsFW payload = publish.payload();

            MqttPayloadFormat payloadFormat = MqttDataExFW.Builder.DEFAULT_FORMAT;
            int expiryInterval = MqttDataExFW.Builder.DEFAULT_EXPIRY_INTERVAL;
            String contentType = null;
            String responseTopic = null;
            OctetsFW correlationData = null;

            final OctetsFW propertiesValue = properties.value();
            final DirectBuffer decodeBuffer = propertiesValue.buffer();
            final int decodeOffset = propertiesValue.offset();
            final int decodeLimit = propertiesValue.limit();

            byte decodeReasonCode = SUCCESS;

            decode:
            for (int decodeProgress = decodeOffset; decodeProgress < decodeLimit; )
            {
                final MqttPropertyFW mqttProperty = mqttPropertyRO.wrap(decodeBuffer, decodeProgress, decodeLimit);
                switch (mqttProperty.kind())
                {
                case KIND_PAYLOAD_FORMAT:
                    payloadFormat = MqttPayloadFormat.valueOf(mqttProperty.payloadFormat());
                    break;
                case KIND_EXPIRY_INTERVAL:
                    expiryInterval = mqttProperty.expiryInterval();
                    break;
                case KIND_CONTENT_TYPE:
                    contentType = mqttProperty.contentType().asString();
                    break;
                case KIND_RESPONSE_TOPIC:
                    responseTopic = mqttProperty.responseTopic().asString();
                    break;
                case KIND_CORRELATION_DATA:
                    correlationData = mqttProperty.correlationData().bytes();
                    break;
                case KIND_TOPIC_ALIAS:
                    break;
                default:
                    decodeReasonCode = MALFORMED_PACKET;
                    break decode;
                }

                decodeProgress = mqttProperty.limit();
            }

            if (decodeReasonCode != SUCCESS)
            {
                onDecodeError(traceId, authorization, decodeReasonCode);
            }
            else
            {
                final int topicKey = topicKey(topic);
                MqttServerStream stream = streams.get(topicKey);

                final MqttPayloadFormat payloadFormat0 = payloadFormat;
                final OctetsFW correlationData0 = correlationData;
                final MqttDataExFW dataEx = mqttDataExRW.wrap(dataExtBuffer, 0, dataExtBuffer.capacity())
                                                        .typeId(mqttTypeId)
                                                        .topic(topic)
                                                        .expiryInterval(expiryInterval)
                                                        .contentType(contentType)
                                                        .format(f -> f.set(payloadFormat0))
                                                        .responseTopic(responseTopic)
                                                        .correlation(c -> c.bytes(correlationData0))
                                                        .build();
                stream.doApplicationData(traceId, authorization, reserved, payload, dataEx);
            }
        }

        private void onDecodeSubscribe(
            long traceId,
            long authorization,
            MqttSubscribeFW subscribe)
        {
            final int packetId = subscribe.packetId();
            final OctetsFW decodePayload = subscribe.payload();

            final DirectBuffer decodeBuffer = decodePayload.buffer();
            final int decodeOffset = decodePayload.offset();
            final int decodeLimit = decodePayload.limit();

            int subscriptionId = 0;
            boolean containsSubscriptionId = false;
            int unrouteableMask = 0;

            Subscription subscription = new Subscription();
            subscriptionsByPacketId.put(packetId, subscription);
            MqttPropertiesFW properties = subscribe.properties();
            final OctetsFW propertiesValue = properties.value();
            final DirectBuffer propertiesBuffer = decodePayload.buffer();
            final int propertiesOffset = propertiesValue.offset();
            final int propertiesLimit = propertiesValue.limit();

            MqttPropertyFW mqttProperty;
            for (int progress = propertiesOffset; progress < propertiesLimit; progress = mqttProperty.limit())
            {
                mqttProperty = mqttPropertyRO.tryWrap(propertiesBuffer, progress, propertiesLimit);
                switch (mqttProperty.kind())
                {
                case KIND_SUBSCRIPTION_ID:
                    subscriptionId = mqttProperty.subscriptionId().value();
                    containsSubscriptionId = true;
                    break;
                }
            }

            if (containsSubscriptionId && subscriptionId == 0)
            {
                onDecodeError(traceId, authorization, PROTOCOL_ERROR);
            }
            else
            {
                if (containsSubscriptionId)
                {
                    subscription.id = subscriptionId;
                }

                for (int decodeProgress = decodeOffset; decodeProgress < decodeLimit; subscription.ackCount++)
                {
                    final MqttSubscribePayloadFW mqttSubscribePayload =
                            mqttSubscribePayloadRO.tryWrap(decodeBuffer, decodeProgress, decodeLimit);
                    if (mqttSubscribePayload == null)
                    {
                        break;
                    }
                    decodeProgress = mqttSubscribePayload.limit();

                    final String filter = mqttSubscribePayload.filter().asString();
                    if (filter == null)
                    {
                        onDecodeError(traceId, authorization, PROTOCOL_ERROR);
                        break;
                    }

                    final boolean validTopicFilter = validator.isTopicFilterValid(filter);
                    if (!validTopicFilter)
                    {
                        onDecodeError(traceId, authorization, PROTOCOL_ERROR);
                        break;
                    }

                    final RouteFW route = resolveTarget(routeId, authorization, filter, SUBSCRIBE_ONLY);
                    if (route != null)
                    {
                        final long resolvedId = route.correlationId();

                        final int topicKey = topicKey(filter);

                        MqttServerStream stream = streams.computeIfAbsent(topicKey, s ->
                                                    new MqttServerStream(resolvedId, packetId, filter));
                        stream.onApplicationSubscribe(subscription);
                        stream.doApplicationBeginOrFlush(traceId, authorization, affinity,
                                filter, subscriptionId, SUBSCRIBE_ONLY);
                    }
                    else
                    {
                        unrouteableMask |= 1 << subscription.ackCount;
                    }
                }

                subscription.ackMask |= unrouteableMask;
            }
        }

        private void onDecodeUnsubscribe(
            long traceId,
            long authorization,
            MqttUnsubscribeFW unsubscribe)
        {
            final int packetId = unsubscribe.packetId();
            final OctetsFW decodePayload = unsubscribe.payload();
            final DirectBuffer decodeBuffer = decodePayload.buffer();
            final int decodeLimit = decodePayload.limit();
            final int offset = decodePayload.offset();

            final MutableDirectBuffer encodeBuffer = payloadBuffer;
            final int encodeOffset = 0;
            final int encodeLimit = payloadBuffer.capacity();

            int encodeProgress = encodeOffset;

            int decodeReasonCode = SUCCESS;
            for (int decodeProgress = offset; decodeProgress < decodeLimit; )
            {
                final MqttUnsubscribePayloadFW mqttUnsubscribePayload =
                        mqttUnsubscribePayloadRO.tryWrap(decodeBuffer, decodeProgress, decodeLimit);
                if (mqttUnsubscribePayload == null)
                {
                    decodeReasonCode = PROTOCOL_ERROR;
                    break;
                }

                final String topic = mqttUnsubscribePayload.filter().asString();
                if (topic == null)
                {
                    decodeReasonCode = PROTOCOL_ERROR;
                    break;
                }

                final int topicKey = topicKey(topic);
                final MqttServerStream stream = streams.get(topicKey);

                int encodeReasonCode = NO_SUBSCRIPTION_EXISTED;
                if (stream != null)
                {
                    encodeReasonCode = SUCCESS;
                    stream.doApplicationFlushOrEnd(traceId, authorization, SUBSCRIBE_ONLY);
                }

                final MqttUnsubackPayloadFW mqttUnsubackPayload =
                        mqttUnsubackPayloadRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                        .reasonCode(encodeReasonCode)
                        .build();
                encodeProgress = mqttUnsubackPayload.limit();

                decodeProgress = mqttUnsubscribePayload.limit();
            }

            if (decodeReasonCode != SUCCESS)
            {
                onDecodeError(traceId, authorization, decodeReasonCode);
            }
            else
            {
                final OctetsFW encodePayload = octetsRO.wrap(encodeBuffer, encodeOffset, encodeProgress);
                doEncodeUnsuback(traceId, authorization, packetId, encodePayload);
            }
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
            state = MqttState.openingReply(state);

            doBegin(network, routeId, replyId, traceId, authorization, affinity, EMPTY_OCTETS);
            router.setThrottle(replyId, this::onNetwork);

            assert replyBudgetIndex == NO_CREDITOR_INDEX;
            this.replyBudgetIndex = creditor.acquire(replyBudgetId);
        }

        private void doNetworkData(
            long traceId,
            long authorization,
            long budgetId,
            Flyweight flyweight)
        {
            doNetworkData(traceId, authorization, budgetId, flyweight.buffer(), flyweight.offset(), flyweight.limit());
        }

        private void doNetworkData(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            if (encodeSlot != NO_SLOT)
            {
                final MutableDirectBuffer encodeBuffer = bufferPool.buffer(encodeSlot);
                encodeBuffer.putBytes(encodeSlotOffset, buffer, offset, limit - offset);
                encodeSlotOffset += limit - offset;
                encodeSlotTraceId = traceId;

                buffer = encodeBuffer;
                offset = 0;
                limit = encodeSlotOffset;
            }

            encodeNetwork(traceId, authorization, budgetId, buffer, offset, limit);
        }

        private void doNetworkEndIfNecessary(
            long traceId,
            long authorization)
        {
            if (!MqttState.replyClosed(state))
            {
                doNetworkEnd(traceId, authorization);
            }
        }

        private void doNetworkEnd(
            long traceId,
            long authorization)
        {
            state = MqttState.closeReply(state);

            cleanupBudgetCreditorIfNecessary();
            cleanupEncodeSlotIfNecessary();

            doEnd(network, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doNetworkAbortIfNecessary(
            long traceId,
            long authorization)
        {
            if (!MqttState.replyClosed(state))
            {
                doNetworkAbort(traceId, authorization);
            }
        }

        private void doNetworkAbort(
            long traceId,
            long authorization)
        {
            state = MqttState.closeReply(state);

            cleanupBudgetCreditorIfNecessary();
            cleanupEncodeSlotIfNecessary();

            doAbort(network, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doNetworkResetIfNecessary(
            long traceId,
            long authorization)
        {
            if (!MqttState.initialClosed(state))
            {
                doNetworkReset(traceId, authorization);
            }
        }

        private void doNetworkReset(
            long traceId,
            long authorization)
        {
            state = MqttState.closeInitial(state);

            cleanupDecodeSlotIfNecessary();

            doReset(network, routeId, initialId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doNetworkWindow(
            long traceId,
            long authorization,
            int credit,
            int padding,
            long budgetId)
        {
            assert credit > 0;

            state = MqttState.openInitial(state);

            decodeBudget += credit;
            doWindow(network, routeId, initialId, traceId, authorization, budgetId, credit, padding);
        }

        private void doEncodePublish(
            long traceId,
            long authorization,
            int flags,
            int subscriptionId,
            String topic,
            OctetsFW payload,
            OctetsFW extension)
        {
            if ((flags & 0x02) != 0)
            {
                final MqttDataExFW dataEx = extension.get(mqttDataExRO::tryWrap);
                final int payloadSize = payload.sizeof();
                final int deferred = dataEx.deferred();
                final int expiryInterval = dataEx.expiryInterval();
                final String8FW contentType = dataEx.contentType();
                final String8FW responseTopic = dataEx.responseTopic();
                final MqttBinaryFW correlation = dataEx.correlation();

                String topicName = dataEx.topic().asString();

                if (topicName == null)
                {
                    topicName = topic;
                }

                final int topicNameLength = topicName != null ? topicName.length() : 0;

                int propertiesSize = 0;

                if (subscriptionId > 0)
                {
                    mqttPropertyRW.wrap(propertyBuffer, propertiesSize, propertyBuffer.capacity())
                        .subscriptionId(v -> v.set(subscriptionId));
                    propertiesSize = mqttPropertyRW.limit();
                }

                if (expiryInterval != -1)
                {
                    mqttPropertyRW.wrap(propertyBuffer, propertiesSize, propertyBuffer.capacity())
                        .expiryInterval(expiryInterval)
                        .build();
                    propertiesSize = mqttPropertyRW.limit();
                }

                if (contentType.value() != null)
                {
                    mqttPropertyRW.wrap(propertyBuffer, propertiesSize, propertyBuffer.capacity())
                        .contentType(contentType.asString())
                        .build();
                    propertiesSize = mqttPropertyRW.limit();
                }

                // TODO: optional format
                mqttPropertyRW.wrap(propertyBuffer, propertiesSize, propertyBuffer.capacity())
                    .payloadFormat((byte) dataEx.format().get().ordinal())
                    .build();
                propertiesSize = mqttPropertyRW.limit();

                if (responseTopic.value() != null)
                {
                    mqttPropertyRW.wrap(propertyBuffer, propertiesSize, propertyBuffer.capacity())
                                  .responseTopic(responseTopic.asString())
                                  .build();
                    propertiesSize = mqttPropertyRW.limit();
                }

                if (correlation.length() != -1)
                {
                    mqttPropertyRW.wrap(propertyBuffer, propertiesSize, propertyBuffer.capacity())
                                  .correlationData(a -> a.bytes(correlation.bytes()))
                                  .build();
                    propertiesSize = mqttPropertyRW.limit();
                }

                final int propertiesSize0 = propertiesSize;
                final MqttPublishFW publish =
                        mqttPublishRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                     .typeAndFlags(0x30)
                                     .remainingLength(3 + topicNameLength + propertiesSize + payloadSize + deferred)
                                     .topicName(topicName)
                                     .properties(p -> p.length(propertiesSize0)
                                                       .value(propertyBuffer, 0, propertiesSize0))
                                     .payload(payload)
                                     .build();

                doNetworkData(traceId, authorization, 0L, publish);
            }
            else
            {
                doNetworkData(traceId, authorization, 0L, payload);
            }
        }

        private void doEncodeConnack(
            long traceId,
            long authorization,
            int reasonCode)
        {
            final MqttConnackFW connack =
                    mqttConnackRW.wrap(writeBuffer, FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                       .typeAndFlags(0x20)
                                       .remainingLength(3)
                                       .flags(0x00)
                                       .reasonCode(reasonCode & 0xff)
                                       .properties(p -> p.length(0).value(EMPTY_OCTETS))
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
                subscriptions[i] = (successMask & ackIndex) != 0 ? SUCCESS : TOPIC_FILTER_INVALID;
            }

            OctetsFW reasonCodes = octetsRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                           .put(subscriptions)
                                           .build();

            final MqttSubackFW suback = mqttSubackRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                            .typeAndFlags(0x90)
                                            .remainingLength(3 + reasonCodes.sizeof())
                                            .packetId(packetId)
                                            .properties(p -> p.length(0).value(EMPTY_OCTETS))
                                            .payload(reasonCodes)
                                            .build();

            doNetworkData(traceId, authorization, 0L, suback);
        }

        private void doEncodeUnsuback(
            long traceId,
            long authorization,
            int packetId,
            OctetsFW payload)
        {
            final MutableDirectBuffer encodeBuffer = writeBuffer;
            final int encodeOffset = FIELD_OFFSET_PAYLOAD;
            final int encodeLimit = writeBuffer.capacity();

            int encodeProgress = encodeOffset;

            final MqttUnsubackFW unsuback =
                    mqttUnsubackRW.wrap(encodeBuffer, encodeProgress, encodeLimit)
                                        .typeAndFlags(0xa0)
                                        .remainingLength(3 + payload.sizeof())
                                        .packetId(packetId)
                                        .properties(p -> p.length(0).value(EMPTY_OCTETS))
                                        .payload(payload)
                                        .build();
            encodeProgress = unsuback.limit();

            final int sizeofPayload = payload.sizeof();
            encodeBuffer.putBytes(encodeProgress, payload.buffer(), payload.offset(), sizeofPayload);
            encodeProgress += sizeofPayload;

            doNetworkData(traceId, authorization, 0L, encodeBuffer, encodeOffset, encodeProgress);
        }

        private void doEncodePingResp(
            long traceId,
            long authorization)
        {
            final MqttPingRespFW pingResp =
                    mqttPingRespRW.wrap(writeBuffer, FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                 .typeAndFlags(0xd0)
                                 .remainingLength(0x00)
                                 .build();

            doNetworkData(traceId, authorization, 0L, pingResp);
        }

        private void doEncodeDisconnect(
            long traceId,
            long authorization,
            int reasonCode)
        {
            final MqttDisconnectFW disconnect =
                    mqttDisconnectRW.wrap(writeBuffer, FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                                    .typeAndFlags(0xe0)
                                    .remainingLength(2)
                                    .reasonCode(reasonCode & 0xff)
                                    .properties(p -> p.length(0).value(EMPTY_OCTETS))
                                    .build();

            doNetworkData(traceId, authorization, 0L, disconnect);
        }

        private void encodeNetwork(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            final int length = Math.max(Math.min(encodeBudget - encodePadding, limit - offset), 0);

            if (length > 0)
            {
                final int reserved = length + encodePadding;

                encodeBudget -= reserved;

                assert encodeBudget >= 0;

                doData(network, routeId, replyId, traceId, authorization, budgetId,
                       reserved, buffer, offset, length, EMPTY_OCTETS);
            }

            final int maxLength = limit - offset;
            final int remaining = maxLength - length;
            if (remaining > 0)
            {
                if (encodeSlot == NO_SLOT)
                {
                    encodeSlot = bufferPool.acquire(replyId);
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

                if (streams.isEmpty() && decoder == decodeIgnoreAll)
                {
                    doNetworkEnd(traceId, authorization);
                }
            }
        }

        private void decodeNetworkIfNecessary(
            long traceId)
        {
            if (decodeSlot != NO_SLOT)
            {
                final long authorization = 0L; // TODO;
                final long budgetId = 0L; // TODO

                final DirectBuffer buffer = bufferPool.buffer(decodeSlot);
                final int offset = 0;
                final int limit = decodeSlotOffset;
                final int reserved = decodeSlotReserved;

                decodeNetwork(traceId, authorization, budgetId, reserved, buffer, offset, limit);

                final int initialCredit = reserved - decodeSlotReserved;
                if (initialCredit > 0)
                {
                    doNetworkWindow(traceId, authorization, initialCredit, 0, 0);
                }
            }
        }

        private void decodeNetwork(
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
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
                    slotBuffer.putBytes(0, buffer, progress, limit - progress);
                    decodeSlotOffset = limit - progress;
                    decodeSlotReserved = (int)((long) reserved * (limit - progress) / (limit - offset));
                }
            }
            else
            {
                cleanupDecodeSlotIfNecessary();

                if (MqttState.initialClosed(state))
                {
                    cleanupStreams(traceId, authorization);
                    doNetworkEndIfNecessary(traceId, authorization);
                }
            }
        }

        private void cleanupNetwork(
            long traceId,
            long authorization)
        {
            cleanupStreams(traceId, authorization);

            doNetworkResetIfNecessary(traceId, authorization);
            doNetworkAbortIfNecessary(traceId, authorization);
        }

        private void cleanupStreams(
            long traceId,
            long authorization)
        {
            streams.values().forEach(s -> s.cleanup(traceId, authorization));
        }

        private void cleanupBudgetCreditorIfNecessary()
        {
            if (replyBudgetIndex != NO_CREDITOR_INDEX)
            {
                creditor.release(replyBudgetIndex);
                replyBudgetIndex = NO_CREDITOR_INDEX;
            }
        }

        private void cleanupDecodeSlotIfNecessary()
        {
            if (decodeSlot != NO_SLOT)
            {
                bufferPool.release(decodeSlot);
                decodeSlot = NO_SLOT;
                decodeSlotOffset = 0;
                decodeSlotReserved = 0;
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
            private int id = 0;
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

        private boolean hasPublishCapability(
            int capabilities)
        {
            return (capabilities & PUBLISH_ONLY.value()) != 0;
        }

        private boolean hasSubscribeCapability(
            int capabilities)
        {
            return (capabilities & SUBSCRIBE_ONLY.value()) != 0;
        }

        private class MqttServerStream
        {
            private final MessageConsumer application;

            private long routeId;
            private long initialId;
            private long replyId;
            private long budgetId;

            private BudgetDebitor debitor;
            private long debitorIndex = NO_DEBITOR_INDEX;

            private int initialBudget;
            private int initialPadding;
            private int replyBudget;

            private String topicFilter;

            private Subscription subscription;
            private int subackIndex;
            private int packetId;

            private int state;
            private int capabilities;

            private long publishExpiresId = NO_CANCEL_ID;
            private long publishExpiresAt;

            MqttServerStream(
                long routeId,
                int packetId,
                String topicFilter)
            {
                this.routeId = routeId;
                this.initialId = supplyInitialId.applyAsLong(routeId);
                this.replyId = supplyReplyId.applyAsLong(initialId);
                this.application = router.supplyReceiver(initialId);
                this.packetId = packetId;
                this.topicFilter = topicFilter;
            }

            private void onApplicationSubscribe(
                Subscription subscription)
            {
                this.subscription = subscription;
                this.subackIndex = subscription != null ? subscription.ackCount : -1;
            }

            private void doApplicationBeginOrFlush(
                long traceId,
                long authorization,
                long affinity,
                String topicFilter,
                int subscriptionId,
                MqttCapabilities capability)
            {
                final int newCapabilities = capabilities | capability.value();
                if (!MqttState.initialOpening(state))
                {
                    this.capabilities = newCapabilities;
                    doApplicationBegin(traceId, authorization, affinity, topicFilter, subscriptionId);
                }
                else if (newCapabilities != capabilities)
                {
                    this.capabilities = newCapabilities;
                    doApplicationFlush(traceId, authorization, 0);
                }
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

                final MqttBeginExFW beginEx = mqttBeginExRW.wrap(extBuffer, 0, extBuffer.capacity())
                                                           .typeId(mqttTypeId)
                                                           .capabilities(r -> r.set(MqttCapabilities.valueOf(capabilities)))
                                                           .clientId(clientId)
                                                           .topic(topicFilter)
                                                           .subscriptionId(subscriptionId)
                                                           .build();

                router.setThrottle(initialId, this::onApplicationInitial);
                correlations.put(replyId, this::onApplicationReply);
                doBegin(application, routeId, initialId, traceId, authorization, affinity, beginEx);

                final int topicKey = topicKey(topicFilter);
                final MutableInteger activeStreams = activeStreamsByTopic.computeIfAbsent(topicKey, key -> new MutableInteger());
                activeStreams.value++;

                if (hasPublishCapability(capabilities))
                {
                    doSignalPublishExpirationIfNecessary();
                }
            }

            private void doApplicationData(
                long traceId,
                long authorization,
                int reserved,
                OctetsFW payload,
                Flyweight extension)
            {
                assert MqttState.initialOpening(state);

                assert hasPublishCapability(this.capabilities);

                final DirectBuffer buffer = payload.buffer();
                final int offset = payload.offset();
                final int limit = payload.limit();
                final int length = limit - offset;
                assert reserved >= length + initialPadding;

                initialBudget -= reserved;

                assert initialBudget >= 0;

                doData(application, routeId, initialId, traceId, authorization, budgetId,
                    reserved, buffer, offset, length, extension);

                doSignalPublishExpirationIfNecessary();
            }

            private void doApplicationFlushOrEnd(
                long traceId,
                long authorization,
                MqttCapabilities capability)
            {
                final int newCapabilities = capabilities & ~capability.value();
                if (newCapabilities == 0)
                {
                    this.capabilities = newCapabilities;
                    if (!MqttState.initialOpened(state))
                    {
                        state = MqttState.closingInitial(state);
                    }
                    else
                    {
                        doApplicationEnd(traceId, authorization, EMPTY_OCTETS);
                    }
                }
                else if (newCapabilities != capabilities)
                {
                    this.capabilities = newCapabilities;
                    doApplicationFlush(traceId, authorization, 0);
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

            private void doApplicationFlush(
                long traceId,
                long authorization,
                int reserved)
            {
                replyBudget -= reserved;

                assert replyBudget >= 0;

                doFlush(application, routeId, initialId, traceId, authorization, 0L, reserved,
                    ex -> ex.set((b, o, l) -> mqttFlushExRW.wrap(b, o, l)
                                                           .typeId(mqttTypeId)
                                                           .capabilities(c -> c.set(MqttCapabilities.valueOf(capabilities)))
                                                           .build()
                                                           .sizeof()));
            }

            private void setInitialClosed()
            {
                assert !MqttState.initialClosed(state);

                state = MqttState.closeInitial(state);

                if (debitorIndex != NO_DEBITOR_INDEX)
                {
                    debitor.release(debitorIndex, initialId);
                    debitorIndex = NO_DEBITOR_INDEX;
                }

                if (MqttState.closed(state))
                {
                    capabilities = 0;
                    final int topicKey = topicKey(topicFilter);
                    streams.remove(topicKey);

                    final MutableInteger activeStreams = activeStreamsByTopic.get(topicKey);

                    assert activeStreams != null;

                    activeStreams.value--;

                    assert activeStreams.value >= 0;

                    if (activeStreams.value == 0)
                    {
                        activeStreamsByTopic.remove(topicKey);
                    }
                }
            }

            private void onApplicationInitial(
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
                final long traceId = window.traceId();
                final long authorization = window.authorization();
                final long budgetId = window.budgetId();
                final int credit = window.credit();
                final int padding = window.padding();

                if (!MqttState.initialOpened(state) &&
                    hasSubscribeCapability(capabilities))
                {
                    subscription.onSubscribeSucceeded(traceId, authorization, packetId, subackIndex);
                }

                this.state = MqttState.openInitial(state);
                this.budgetId = budgetId;
                this.initialBudget += credit;
                this.initialPadding = padding;

                if (budgetId != 0L && debitorIndex == NO_DEBITOR_INDEX)
                {
                    debitor = supplyDebitor.apply(budgetId);
                    debitorIndex = debitor.acquire(budgetId, initialId, MqttServer.this::decodeNetworkIfNecessary);
                }

                if (MqttState.initialClosing(state) &&
                    !MqttState.initialClosed(state))
                {
                    doApplicationEnd(traceId, authorization, EMPTY_OCTETS);
                }

                decodeNetworkIfNecessary(traceId);
            }

            private void onApplicationReset(
                ResetFW reset)
            {
                setInitialClosed();

                final long traceId = reset.traceId();
                final long authorization = reset.authorization();

                if (!MqttState.initialOpened(state) &&
                    hasSubscribeCapability(capabilities))
                {
                    subscription.onSubscribeFailed(traceId, authorization, packetId, subackIndex);
                }

                decodeNetworkIfNecessary(traceId);
                cleanup(traceId, authorization);
            }

            private void onApplicationSignal(
                SignalFW signal)
            {
                final int signalId = signal.signalId();

                switch (signalId)
                {
                case PUBLISH_EXPIRED_SIGNAL:
                    onPublishExpiredSignal(signal);
                    break;
                default:
                    break;
                }
            }

            private void onPublishExpiredSignal(
                SignalFW signal)
            {
                final long traceId = signal.traceId();
                final long authorization = signal.authorization();

                final long now = System.currentTimeMillis();
                if (now >= publishExpiresAt)
                {
                    doApplicationFlushOrEnd(traceId, authorization, PUBLISH_ONLY);
                }
                else
                {
                    publishExpiresId = NO_CANCEL_ID;
                    doSignalPublishExpirationIfNecessary();
                }
            }

            private boolean isReplyOpen()
            {
                return MqttState.replyOpened(state);
            }

            private void onApplicationReply(
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
                case AbortFW.TYPE_ID:
                    final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                    onApplicationAbort(abort);
                    break;
                }
            }

            private void onApplicationBegin(
                BeginFW begin)
            {
                state = MqttState.openReply(state);

                final long traceId = begin.traceId();
                final long authorization = begin.authorization();

                flushReplyWindow(traceId, authorization);
            }

            private void onApplicationData(
                DataFW data)
            {
                final long traceId = data.traceId();
                final int reserved = data.reserved();
                final long authorization = data.authorization();
                final int flags = data.flags();
                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();

                replyBudget -= reserved;
                sharedReplyBudget -= reserved;

                if (replyBudget < 0)
                {
                    doApplicationReset(traceId, authorization);
                    doNetworkAbort(traceId, authorization);
                }

                doEncodePublish(traceId, authorization, flags, subscription.id, topicFilter, payload, extension);
            }

            private void onApplicationEnd(
                EndFW end)
            {
                setReplyClosed();
            }

            private void onApplicationAbort(
                AbortFW abort)
            {
                setReplyClosed();

                final long traceId = abort.traceId();
                final long authorization = abort.authorization();

                cleanupCorrelationIfNecessary();
                cleanup(traceId, authorization);
            }

            private void doApplicationEnd(
                long traceId,
                long authorization,
                Flyweight extension)
            {
                setInitialClosed();
                capabilities = 0;
                streams.remove(topicKey(topicFilter));

                doEnd(application, routeId, initialId, traceId, authorization, extension);
            }

            private void flushReplyWindow(
                long traceId,
                long authorization)
            {
                if (isReplyOpen())
                {
                    final int replyCredit = encodeBudget - encodeSlotOffset - replyBudget;

                    if (replyCredit > 0)
                    {
                        replyBudget += replyCredit;

                        doWindow(application, routeId, replyId, traceId, authorization,
                            replyBudgetId, replyCredit, 0);
                    }
                }
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

            private void setReplyClosed()
            {
                assert !MqttState.replyClosed(state);

                state = MqttState.closeReply(state);

                if (MqttState.closed(state))
                {
                    capabilities = 0;
                    final int topicKey = topicKey(topicFilter);
                    streams.remove(topicKey);
                    final MutableInteger count = activeStreamsByTopic.get(topicKey);

                    assert count != null;

                    count.value--;

                    assert count.value >= 0;

                    if (count.value == 0)
                    {
                        activeStreamsByTopic.remove(topicKey);
                    }
                }
            }

            private void cleanup(
                long traceId,
                long authorization)
            {
                doApplicationAbortIfNecessary(traceId, authorization);
                doApplicationResetIfNecessary(traceId, authorization);
                doCancelPublishExpirationIfNecessary();
            }

            private void doSignalPublishExpirationIfNecessary()
            {
                publishExpiresAt = System.currentTimeMillis() + publishTimeoutMillis;

                if (publishExpiresId == NO_CANCEL_ID)
                {
                    publishExpiresId = signaler.signalAt(publishExpiresAt, routeId, initialId, PUBLISH_EXPIRED_SIGNAL);
                }
            }

            private boolean cleanupCorrelationIfNecessary()
            {
                final MessageConsumer correlated = correlations.remove(replyId);
                if (correlated != null)
                {
                    router.clearThrottle(replyId);
                }

                return correlated != null;
            }

            private void doCancelPublishExpirationIfNecessary()
            {
                if (publishExpiresId != NO_CANCEL_ID)
                {
                    signaler.cancel(publishExpiresId);
                    publishExpiresId = NO_CANCEL_ID;
                }
            }
        }
    }
}
