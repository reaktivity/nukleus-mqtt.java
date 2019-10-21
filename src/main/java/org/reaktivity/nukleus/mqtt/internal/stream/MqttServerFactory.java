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

    private final MqttServerDecoder decodeConnectPacket;
    private final MqttServerDecoder decodeSession;

    private final Long2ObjectHashMap<MqttServer.MqttServerStream> correlations;
    private final MessageFunction<RouteFW> wrapRoute;
    private final int mqttTypeId;

    private final BufferPool bufferPool;

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
        this.wrapRoute = this::wrapRoute;
        this.decodeConnectPacket = this::decodeConnectPacket;
        this.decodeSession = this::decodeSession;
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
            final MqttServer connection = new MqttServer(sender, routeId, initialId, replyId);
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
        long traceId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(replyId)
                                     .traceId(traceId)
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
        long traceId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity()).routeId(routeId)
                                     .streamId(streamId)
                                     .traceId(traceId)
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

    private int decodeConnectPacket(
        MqttServer server,
        final long traceId,
        final long authorization,
        final MqttPacketType packetType,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        int progress = offset;
        switch (packetType)
        {
        case CONNECT:
            MqttConnectFW mqttConnect = mqttConnectRO.wrap(buffer, offset, limit);
            server.onMqttConnect(authorization, mqttConnect);
            progress = mqttConnect.limit();
            break;
        default:
            server.doNetworkEnd(traceId);
            break;
        }
        return progress;
    }

    private int decodeSession(
        MqttServer server,
        final long traceId,
        final long authorization,
        final MqttPacketType packetType,
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        int progress = 0;

        switch (packetType)
        {
        case PINGREQ:
            final MqttPingReqFW ping = mqttPingReqRO.wrap(buffer, offset, limit);
            server.onMqttPingReq(authorization, ping);
            progress = ping.limit();
            break;
        case SUBSCRIBE:
            final MqttSubscribeFW subscribe = mqttSubscribeRO.wrap(buffer, offset, limit);
            server.onMqttSubscribe(authorization, subscribe);
            progress = subscribe.limit();
            break;
        case UNSUBSCRIBE:
            final MqttUnsubscribeFW unsubscribe = mqttUnsubscribeRO.wrap(buffer, offset, limit);
            server.onMqttUnsubscribe(authorization, unsubscribe);
            progress = unsubscribe.limit();
            break;
        case DISCONNECT:
            final MqttDisconnectFW disconnect = mqttDisconnectRO.wrap(buffer, offset, limit);
            server.onMqttDisconnect(disconnect);
            server.doNetworkEnd(traceId);
            progress = disconnect.limit();
            break;
        case PUBLISH:
            final MqttPublishFW publish = mqttPublishRO.wrap(buffer, offset, limit);
            server.onMqttPublish(authorization, publish);
            progress = publish.limit();
            break;
        default:
            server.doNetworkReset(traceId);
            break;
        }

        return progress;
    }

    @FunctionalInterface
    private interface MqttServerDecoder
    {
        int decode(
            MqttServer server,
            long traceId,
            long authorization,
            MqttPacketType packetType,
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
        private int slotIndex = NO_SLOT;
        private int slotLimit;

        private MqttServer(
            MessageConsumer network,
            long routeId,
            long initialId,
            long replyId)
        {
            this.network = network;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = replyId;
            this.decoder = decodeConnectPacket;
            this.subscribers = new HashMap<>();
            this.publishers = new HashMap<>();
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
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onReset(reset);
                break;
            case SignalFW.TYPE_ID:
                final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                onSignal(signal);
                break;
            default:
                break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            affinity = begin.affinity();
            doNetworkBegin(supplyTraceId.getAsLong(), affinity);
        }

        private void onData(
            DataFW data)
        {
            initialBudget -= data.reserved();

            if (initialBudget < 0)
            {
                doNetworkReset(supplyTraceId.getAsLong());
            }
            else
            {
                final OctetsFW payload = data.payload();
                final long streamId = data.streamId();
                decodeTraceId = data.traceId();
                this.authorization = data.authorization();

                DirectBuffer buffer = payload.buffer();
                int offset = payload.offset();
                int limit = payload.limit();

                if (slotIndex != NO_SLOT)
                {
                    final MutableDirectBuffer slotBuffer = bufferPool.buffer(slotIndex);
                    slotBuffer.putBytes(slotLimit, buffer, offset, limit - offset);
                    slotLimit += limit - offset;
                    buffer = slotBuffer;
                    offset = 0;
                    limit = slotLimit;
                }

                while (offset < limit)
                {
                    final MqttPacketFixedHeaderFW packet = mqttPacketFixedHeaderRO.tryWrap(buffer, offset, limit);

                    if (packet == null || packet.limit() + packet.remainingLength() > limit)
                    {
                        break;
                    }

                    final MqttPacketType packetType = MqttPacketType.valueOf(packet.typeAndFlags() >> 4);
                    offset += decoder.decode(this, authorization, decodeTraceId, packetType, buffer, offset, limit);
                }

                if (offset < limit)
                {
                    if (slotIndex == NO_SLOT)
                    {
                        slotIndex = bufferPool.acquire(streamId);
                    }
                    final MutableDirectBuffer slotBuffer = bufferPool.buffer(slotIndex);
                    slotLimit = limit - offset;
                    slotBuffer.putBytes(0, buffer, offset, slotLimit);
                }
                else if (slotIndex != NO_SLOT)
                {
                    bufferPool.release(slotIndex);
                    slotIndex = NO_SLOT;
                    slotLimit = 0;
                }
            }
        }

        private void onEnd(
            EndFW end)
        {
            final long traceId = end.traceId();
            doNetworkEnd(traceId);
        }

        private void onAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();
            doNetworkAbort(traceId);
        }

        private void onWindow(
            WindowFW window)
        {
            final int replyCredit = window.credit();

            replyBudget += replyCredit;
            replyPadding += window.padding();

            final int initialCredit = bufferPool.slotCapacity() - initialBudget;

            doNetworkWindow(supplyTraceId.getAsLong(), initialCredit);
        }

        private void onReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            doNetworkReset(traceId);
        }

        private void onSignal(
            SignalFW signal)
        {
            final long traceId = signal.traceId();
            doNetworkSignal(traceId);
        }

        private void onMqttConnect(
            long authorization,
            MqttConnectFW packet)
        {
            int reasonCode = 0x00;
            // TODO: Must process multiple CONNECT from client as protocol errors and close network connection: 0x82

            final String protocolName = packet.protocolName().asString();
            if (!"MQTT".equals(protocolName) || packet.protocolVersion() != 5)
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
                this.decoder = decodeSession;
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

                        final MqttSubscribeStream subscribeStream = new MqttSubscribeStream(newTarget,
                            newRouteId, newInitialId, newReplyId, packetId, subscription);

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
                    final MqttPublishStream serverStream = new MqttPublishStream(newTarget,
                        newRouteId, newInitialId, newReplyId, 0);

                    serverStream.doApplicationBegin(decodeTraceId, affinity);

                    serverStream.doMqttDataEx(decodeTraceId, authorization, payload, dataEx);

                    correlations.put(newReplyId, serverStream);
                    publishers.put(topicName, serverStream);
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
            long traceId)
        {
            doAbort(network, routeId, replyId, traceId);
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
            long traceId)
        {
            doReset(network, routeId, initialId, traceId);
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

            MqttSubscribeStream(
                MessageConsumer application,
                long routeId,
                long initialId,
                long replyId,
                int packetId,
                Subscription subscription)
            {
                this.application = application;
                this.routeId = routeId;
                this.initialId = initialId;
                this.replyId = replyId;
                this.packetId = packetId;
                this.subscription = subscription;
                this.ackIndex = subscription != null ? subscription.ackCount : -1;
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
        }
    }
}
