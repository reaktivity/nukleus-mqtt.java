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
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
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

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer extBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;

    private final Long2ObjectHashMap<MqttServer.MqttServerStream> correlations;
    private final MessageFunction<RouteFW> wrapRoute = this::wrapRoute;
    private final int mqttTypeId;

    private final BufferPool bufferPool;

    private final MqttServerDecoder decodePacketType = this::decodePacketType;
    private final MqttServerDecoder decodeConnect = this::decodeConnect;
    private final MqttServerDecoder decodePublish = this::decodePublish;
    private final MqttServerDecoder decodeSubscribe = this::decodeSubscribe;
    private final MqttServerDecoder decodeUnsubscribe = this::decodeUnsubscribe;
    private final MqttServerDecoder decodePingreq = this::decodePingreq;
    private final MqttServerDecoder decodeDisconnect = this::decodeDisconnect;
    private final MqttServerDecoder decodeIgnoreOne = this::decodeIgnoreOne;
    private final MqttServerDecoder decodeIgnoreAll = this::decodeIgnoreAll;


    private final EnumMap<MqttPacketType, MqttServerDecoder> decodersByPacketType;
    {
        final EnumMap<MqttPacketType, MqttServerDecoder> decodersByPacketType = new EnumMap<>(MqttPacketType.class);
        decodersByPacketType.put(MqttPacketType.CONNECT, decodeConnect);
        // decodersByPacketType.put(MqttPacketType.CONNACK, decodeConnack);
        decodersByPacketType.put(MqttPacketType.PUBLISH, decodePublish);
        // decodersByPacketType.put(MqttPacketType.PUBACK, decodePuback);
        // decodersByPacketType.put(MqttPacketType.PUBREC, decodePubrec);
        // decodersByPacketType.put(MqttPacketType.PUBREL, decodePubrel);
        // decodersByPacketType.put(MqttPacketType.PUBCOMP, decodePubcomp);
        decodersByPacketType.put(MqttPacketType.SUBSCRIBE, decodeSubscribe);
        // decodersByPacketType.put(MqttPacketType.SUBACK, decodeSuback);
        decodersByPacketType.put(MqttPacketType.UNSUBSCRIBE, decodeUnsubscribe);
        // decodersByPacketType.put(MqttPacketType.UNSUBACK, decodeUnsuback);
        decodersByPacketType.put(MqttPacketType.PINGREQ, decodePingreq);
        // decodersByPacketType.put(MqttPacketType.PINGRESP, decodePingresp);
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
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.extBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.bufferPool = bufferPool;
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.correlations = new Long2ObjectHashMap<>();
        this.mqttTypeId = supplyTypeId.applyAsInt(MqttNukleus.NAME);
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
        final long initialId = begin.streamId();
        final long affinity = begin.affinity();
        final long replyId = supplyReplyId.applyAsLong(initialId);

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

        final RouteFW route = router.resolve(routeId, begin.authorization(), filter, this::wrapRoute);
        MessageConsumer newStream = null;


        if (route != null)
        {
            final MqttServer connection = new MqttServer(sender, routeId, initialId, replyId, affinity);
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
            newStream = reply::onApplication;
        }
        return newStream;
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return routeRO.wrap(buffer, index, index + length);
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
        return router.resolve(routeId, authorization, filter, this::wrapRoute);
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long replyId,
        long traceId,
        long affinity,
        Flyweight extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(replyId)
                                     .traceId(traceId)
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
        long traceId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .routeId(routeId)
                               .streamId(replyId)
                               .traceId(traceId)
                               .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doAbort(
        MessageConsumer receiver,
        long routeId,
        long replyId,
        long traceId,
        long authorization)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(replyId)
                                     .traceId(traceId)
                                     .authorization(authorization)
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
        long authorization)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity()).routeId(routeId)
                                     .streamId(streamId)
                                     .traceId(traceId)
                                     .authorization(authorization)
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
            final MqttServerDecoder decoder = decodersByPacketType.getOrDefault(packetType, decodeIgnoreOne);

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
        MqttConnectFW mqttConnect = mqttConnectRO.wrap(buffer, offset, limit);
        server.onMqttConnect(authorization, mqttConnect);
        return mqttConnect.limit();
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
        final MqttPublishFW publish = mqttPublishRO.wrap(buffer, offset, limit);
        server.onMqttPublish(authorization, publish);
        server.decoder = decodePacketType;
        return publish.limit();
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
        final MqttSubscribeFW subscribe = mqttSubscribeRO.wrap(buffer, offset, limit);
        server.onMqttSubscribe(authorization, subscribe);
        server.decoder = decodePacketType;
        return subscribe.limit();
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
        final MqttUnsubscribeFW unsubscribe = mqttUnsubscribeRO.wrap(buffer, offset, limit);
        server.onMqttUnsubscribe(authorization, unsubscribe);
        server.decoder = decodePacketType;
        return unsubscribe.limit();
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
        final MqttPingReqFW ping = mqttPingReqRO.wrap(buffer, offset, limit);
        server.onMqttPingReq(authorization, ping);
        server.decoder = decodePacketType;
        return ping.limit();
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
        final MqttDisconnectFW disconnect = mqttDisconnectRO.wrap(buffer, offset, limit);
        server.onMqttDisconnect(disconnect);
        server.decoder = decodePacketType;
        server.doNetworkEnd(traceId);
        return disconnect.limit();
    }

    private int decodeIgnoreOne(
        MqttServer server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final MqttPacketFixedHeaderFW packet = mqttPacketFixedHeaderRO.tryWrap(buffer, offset, limit);
        final int progress = packet.limit();

        server.decoder = decodePacketType;
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
        private long affinity;

        private final Map<String, MqttSubscribeStream> subscribers;
        private final Map<String, MqttPublishStream> publishers;
        private final Int2ObjectHashMap<Subscription> subscriptionsByPacketId;
        private long authorization;

        private int initialBudget;
        private int initialPadding;
        private int replyBudget;
        private int replyPadding;

        private long decodeTraceId;
        private MqttServerDecoder decoder;
        private int decodeSlot = NO_SLOT;
        private int decodeSlotLimit;
        private boolean receivedConnect;

        private MqttServer(
            MessageConsumer network,
            long routeId,
            long initialId,
            long replyId,
            long affinity)
        {
            this.network = network;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = replyId;
            this.affinity = affinity;
            this.decoder = decodePacketType;
            this.subscribers = new HashMap<>();
            this.publishers = new HashMap<>();
            this.subscriptionsByPacketId = new Int2ObjectHashMap<>();
            this.receivedConnect = false;
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
            // affinity = begin.affinity();
            // TODO - begin.traceID() vs supplyTradeId.getAsLong()
            doNetworkBegin(supplyTraceId.getAsLong(), affinity);
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
                this.decodeTraceId = traceId;
                this.authorization = authorization;
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
            if (decodeSlot == NO_SLOT)
            {
                final long traceId = end.traceId();

                cleanupDecodeSlotIfNecessary();
                // cleanupSubscribersIfNecessary();

                doNetworkEnd(traceId);
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

        private void onMqttConnect(
            long authorization,
            MqttConnectFW packet)
        {
            final String protocolName = packet.protocolName().asString();

            int reasonCode = 0x00;
            if (receivedConnect)
            {
                reasonCode = 0x82; // protocol error
            }
            else if (!"MQTT".equals(protocolName) || packet.protocolVersion() != 5)
            {
                reasonCode = 0x84; // unsupported protocol version
            }
            else if ((packet.flags() & 0b0000_0001) != 0b0000_0000)
            {
                reasonCode = 0x81; // malformed packet
            }

            if (reasonCode == 0)
            {
                doMqttConnack(authorization, reasonCode);
                decoder = decodePacketType;
            }
            else
            {
                doMqttConnack(authorization, reasonCode);
                doNetworkEnd(decodeTraceId);
            }
        }

        private void onMqttPingReq(
            long authorization,
            MqttPingReqFW ping)
        {
            doMqttPingResp(authorization);
        }

        private void onMqttSubscribe(
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

            if (limit == 0)
            {
                doMqttDisconnect(authorization, 0x82);
                doNetworkEnd(decodeTraceId);
            }
            else
            {
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

                        // TODO - initially assuming only one topicFilter per subscribe. will need to be able to support
                        //        multiple topic filters in the future
                        final MqttSubscribeStream subscribeStream = new MqttSubscribeStream(newTarget,
                            newRouteId, newInitialId, newReplyId, packetId, topicFilter, subscription);

                        subscribeStream.doMqttBeginEx(decodeTraceId, affinity, topicFilter, subscriptionId);

                        correlations.put(newReplyId, subscribeStream);

                        subscribers.put(topicFilter, subscribeStream);
                    }
                    else
                    {
                        unrouteableMask |= 1 << subscription.ackCount;
                    }
                }
            }
            subscription.ackMask |= unrouteableMask;
        }

        private void onMqttUnsubscribe(
            long authorization,
            MqttUnsubscribeFW unsubscribe)
        {
            final OctetsFW topicFilters = unsubscribe.topicFilters();
            final DirectBuffer buffer = topicFilters.buffer();
            final int limit = topicFilters.limit();
            final int offset = topicFilters.offset();

            int topics = 0;
            MqttTopicFW topic;
            for (int progress = offset; progress < limit; progress = topic.limit())
            {
                topic = mqttTopicRO.tryWrap(buffer, progress, limit);
                if (topic == null)
                {
                    break;
                }
                topics++;
            }

            doMqttUnsuback(authorization, topics);
        }

        private void onMqttPublish(
            long authorization,
            MqttPublishFW publish)
        {
            final String topicName = publish.topicName().asString();

            String info = "info";

            final MqttDataExFW dataEx = mqttDataExRW
                .wrap(extBuffer, 0, extBuffer.capacity())
                .typeId(mqttTypeId)
                .topic(topicName)
                .expiryInterval(15)
                .contentType("message")
                .format(f -> f.set(MqttPayloadFormat.TEXT))
                .responseTopic(topicName)
                .correlationInfo(c -> c.bytes(b -> b.set(info.getBytes(UTF_8))))
                .build();

            OctetsFW payload = publish.payload();

            MqttPublishStream publishStream = publishers.get(topicName);

            final RouteFW route = resolveTarget(routeId, authorization, topicName);
            if (route != null)
            {
                final long newRouteId = route.correlationId();
                final long newInitialId = supplyInitialId.applyAsLong(newRouteId);
                final long newReplyId = supplyReplyId.applyAsLong(newInitialId);
                final MessageConsumer newTarget = router.supplyReceiver(newInitialId);
                if (publishStream != null)
                {
                    publishStream.doMqttDataEx(decodeTraceId, authorization, payload, dataEx);
                    correlations.put(newReplyId, publishStream);    // TODO: do we need to clean up correlations onAbort()?
                }
                else
                {
                    final MqttPublishStream newPublishStream = new MqttPublishStream(newTarget,
                        newRouteId, newInitialId, newReplyId, 0);

                    newPublishStream.doApplicationBegin(decodeTraceId, affinity);

                    newPublishStream.doMqttDataEx(decodeTraceId, authorization, payload, dataEx);

                    correlations.put(newReplyId, newPublishStream);
                    publishers.put(topicName, newPublishStream);
                }
            }
        }

        private void onMqttDisconnect(
            MqttDisconnectFW disconnect)
        {
            /* process reason code */
        }

        private void doNetworkBegin(
            long traceId,
            long affinity)
        {
            doBegin(network, routeId, replyId, traceId, affinity, EMPTY_OCTETS);
            router.setThrottle(replyId, this::onNetwork);
        }

        private void doNetworkData(
            long authorization,
            long budgetId,
            Flyweight payload)
        {
            final int reserved = payload.sizeof() + replyPadding;
            doData(network, routeId, replyId, supplyTraceId.getAsLong(), authorization, budgetId, reserved,
                    payload.buffer(), payload.offset(), payload.sizeof(), EMPTY_OCTETS);
        }

        private void doNetworkEnd(
            long traceId)
        {
            doEnd(network, routeId, replyId, traceId);
        }

        private void doNetworkAbort(
            long traceId,
            long authorization)
        {
            doAbort(network, routeId, replyId, traceId, authorization);
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
            doReset(network, routeId, initialId, traceId, authorization);
        }

        private void doNetworkSignal(
            long traceId)
        {
            doSignal(network, routeId, initialId, traceId);
        }

        private void doMqttPublish(
            long authorization,
            String topicName,
            OctetsFW payload)
        {
            OctetsFW properties = octetsRW
                    .wrap(writeBuffer, 0, 0)
                    .build();

            final MqttPublishFW publish = mqttPublishRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                    .typeAndFlags(0x30)
                    .remainingLength(0x14)
                    .topicName(topicName)
                    .properties(properties)
                    .payload(payload)
                    .build();

            doNetworkData(authorization, 0L, publish);
        }

        private void doMqttConnack(
            long authorization,
            int reasonCode)
        {
            OctetsFW properties = octetsRW
                .wrap(writeBuffer, 0, 0)
                .build();

            final MqttConnackFW connack = mqttConnackRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .typeAndFlags(0x20)
                .remainingLength(properties.sizeof() + 3)
                .flags(0x00)
                .reasonCode(reasonCode)
                .propertiesLength(properties.sizeof())
                .properties(properties)
                .build();

            receivedConnect = true;

            doNetworkData(authorization, 0L, connack);
        }

        private void doMqttPingResp(
            long authorization)
        {
            final MqttPingRespFW ping = mqttPingRespRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .typeAndFlags(0xd0)
                .remainingLength(0x00)
                .build();

            doNetworkData(authorization, 0L, ping);
        }

        private void doMqttSuback(
            long authorization,
            int ackMask,
            int successMask,
            int packetId)
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

            doNetworkData(authorization, 0L, suback);
        }

        private void doMqttUnsuback(
            long authorization,
            int subscriptions)
        {
            OctetsFW reasonCodes = octetsRW
                .wrap(writeBuffer, 0, writeBuffer.capacity())
                .put(new byte[] {0x00})
                .build();

            final MqttUnsubackFW unsuback = mqttUnsubackRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .typeAndFlags(0xa0)
                .remainingLength(reasonCodes.sizeof() + 1)
                .propertiesLength(0x00)
                .reasonCodes(reasonCodes)
                .build();

            doNetworkData(authorization, 0L, unsuback);
        }

        private void doMqttDisconnect(
            long authorization,
            int reasonCode)
        {
            OctetsFW properties = octetsRW
                .wrap(writeBuffer, 0, 0)
                .build();

            final MqttDisconnectFW disconnect = mqttDisconnectRW
                .wrap(writeBuffer,  DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .typeAndFlags(0xe0)
                .remainingLength((byte) properties.sizeof() + 2)
                .reasonCode(reasonCode)
                .propertiesLength(properties.sizeof())
                .properties(properties)
                .build();

            doNetworkData(authorization, 0L, disconnect);
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
                // final MqttPacketFixedHeaderFW packet = mqttPacketFixedHeaderRO.tryWrap(buffer, progress, limit);
                //
                // if (packet == null || packet.limit() + packet.remainingLength() > limit)
                // {
                //     break;
                // }
                //
                // final MqttPacketType packetType = MqttPacketType.valueOf(packet.typeAndFlags() >> 4);
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

            subscribers.values().forEach(MqttSubscribeStream::cleanup);
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

        // private void cleanupSubscribersIfNecessary()
        // {
        //     if (!subscribers.isEmpty())
        //     {
        //         subscribers.values().forEach(MqttSubscribeStream::cleanup);
        //     }
        // }

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
                long authorization,
                int packetId,
                int ackIndex)
            {
                final int bit = 1 << ackIndex;
                ackMask |= bit;
                onSubscribeCompleted(authorization, packetId);
            }

            private void onSubscribeSucceeded(
                int packetId,
                int ackIndex)
            {
                final int bit = 1 << ackIndex;
                successMask |= bit;
                ackMask |= bit;
                onSubscribeCompleted(authorization, packetId);
            }

            private void onSubscribeCompleted(
                long authorization,
                int packetId)
            {
                if (Integer.bitCount(ackMask) == ackCount)
                {
                    doMqttSuback(authorization, ackMask, successMask, packetId);
                }
            }
        }

        private abstract class MqttServerStream
        {
            abstract void onApplication(
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
            private int packetId;

            MqttPublishStream(
                MessageConsumer application,
                long routeId,
                long initialId,
                long replyId,
                int packetId)
            {
                this.application = application;
                this.routeId = routeId;
                this.initialId = initialId;
                this.replyId = replyId;
                this.packetId = packetId;
            }

            @Override
            public void onApplication(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                switch (msgTypeId)
                {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    onBegin(begin);
                    break;
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    onData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    onEnd(end);
                    break;
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    onWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    onReset(reset);
                    break;
                }
            }

            private void onBegin(
                BeginFW begin)
            {
            }

            private void onData(
                DataFW data)
            {
                final OctetsFW extension = data.extension();
                final long authorization = data.authorization();

                final DirectBuffer exBuffer = extension.buffer();
                final int exOffset = extension.offset();
                final int exLength = extension.sizeof();

                final MqttDataExFW dataEx = mqttDataExRO.wrap(exBuffer, exOffset, exOffset + exLength);

                final String topicName = dataEx.topic().asString();
                doMqttPublish(authorization, topicName, data.payload());
            }

            private void onEnd(
                EndFW end)
            {
            }

            private void onWindow(
                WindowFW window)
            {
            }

            private void onReset(
                ResetFW reset)
            {
            }

            private void doApplicationBegin(
                long traceId,
                long affinity)
            {
                doBegin(application, routeId, initialId, traceId, affinity, EMPTY_OCTETS);
            }

            private void doMqttDataEx(
                long traceId,
                long authorization,
                OctetsFW payload,
                Flyweight extension)
            {
                doData(application, routeId, initialId, traceId, authorization, 0L, payload.sizeof(),
                        payload.buffer(), payload.offset(), payload.sizeof(), extension);
            }
        }

        private final class MqttSubscribeStream extends MqttServerStream
        {
            private final MessageConsumer application;
            private final int ackIndex;
            private long routeId;
            private long initialId;
            private long replyId;
            private Subscription subscription;
            private int packetId;
            private String topicFilter;

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
                this.subscription = subscription;
                this.ackIndex = subscription != null ? subscription.ackCount : -1;
                this.topicFilter = topicFilter;
            }

            @Override
            public void onApplication(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                switch (msgTypeId)
                {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    onBegin(begin);
                    break;
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    onData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    onEnd(end);
                    break;
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    onWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    onReset(reset);
                    break;
                }
            }

            private void onBegin(
                BeginFW begin)
            {
            }

            private void onData(
                DataFW data)
            {
                final OctetsFW extension = data.extension();
                final long authorization = data.authorization();

                final DirectBuffer exBuffer = extension.buffer();
                final int exOffset = extension.offset();
                final int exLength = extension.sizeof();

                final MqttDataExFW dataEx = mqttDataExRO.wrap(exBuffer, exOffset, exOffset + exLength);

                final String topicName = dataEx.topic().asString();
                doMqttPublish(authorization, topicName, data.payload());
            }

            private void onEnd(
                EndFW end)
            {
            }

            private void onWindow(
                WindowFW window)
            {
                subscription.onSubscribeSucceeded(packetId, ackIndex);
            }

            private void onReset(
                ResetFW reset)
            {
                final long authorization = reset.authorization();
                subscription.onSubscribeFailed(authorization, packetId, ackIndex);
            }

            private void doApplicationBegin(
                long traceId,
                long affinity)
            {
                doBegin(application, routeId, initialId, traceId, affinity, EMPTY_OCTETS);
            }

            private void doMqttBeginEx(
                long traceId,
                long affinity,
                String topicFilter,
                int subscriptionId)
            {
                router.setThrottle(initialId, this::onApplication);

                final MqttBeginExFW beginEx = mqttBeginExRW
                        .wrap(extBuffer, 0, extBuffer.capacity())
                        .typeId(mqttTypeId)
                        .role(r -> r.set(MqttRole.RECEIVER))
                        .clientId("client")
                        .topic(topicFilter)
                        .subscriptionId(subscriptionId)
                        .build();

                doBegin(application, routeId, initialId, traceId, affinity, beginEx);
            }

            private void doMqttDataEx(
                long traceId,
                long authorization,
                OctetsFW payload,
                Flyweight extension)
            {
                doData(application, routeId, initialId, traceId, authorization, 0L, payload.sizeof(),
                        payload.buffer(), payload.offset(), payload.sizeof(), extension);
            }

            private void cleanup()
            {
                subscribers.remove(topicFilter);
            }
        }
    }
}
