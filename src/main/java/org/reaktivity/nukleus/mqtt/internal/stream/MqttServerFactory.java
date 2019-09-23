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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
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

    private final Long2ObjectHashMap<MqttServerStream> correlations;
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
        final MqttServerStream reply = correlations.remove(replyId);

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

    private final class MqttServer
    {
        private final MessageConsumer network;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final Map<String, MqttServerStream> subscribers;
        private final Map<String, MqttServerStream> publishers;
        private List<Byte> reasonCodes;
        private long authorization;
        private int reasonCodeCount;

        private int initialBudget;
        private int initialPadding;
        private int replyBudget;
        private int replyPadding;

        private long decodeTraceId;
        private DecoderState decodeState;
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
            // TODO: reasonCodes variable, done from stream.doBegin.a,sd;kas;lf
            this.reasonCodes = new ArrayList<>();
            this.decodeState = this::decodeConnectPacket;
            this.subscribers = new HashMap<>();
            this.publishers = new HashMap<>();
            this.reasonCodeCount = 0;
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
            doBegin(supplyTraceId.getAsLong());
        }

        private void onData(
            DataFW data)
        {
            final OctetsFW payload = data.payload();
            initialBudget -= data.length() + data.padding();

            if (initialBudget < 0)
            {
                doReset(supplyTraceId.getAsLong());
            }
            else if (payload != null)
            {
                final long streamId = data.streamId();
                decodeTraceId = data.trace();
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
                    offset += decodeState.decode(packetType, buffer, offset, limit);
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
            final long traceId = end.trace();
            doEnd(traceId);
        }

        private void onAbort(
            AbortFW abort)
        {
            final long traceId = abort.trace();
            doAbort(traceId);
        }

        private void onWindow(
            WindowFW window)
        {
            final int replyCredit = window.credit();

            replyBudget += replyCredit;
            replyPadding += window.padding();

            final int initialCredit = bufferPool.slotCapacity() - initialBudget;

            System.out.printf("window.streamId(): %d\n", window.streamId());

            doWindow(supplyTraceId.getAsLong(), initialCredit);
        }

        private void onReset(
            ResetFW reset)
        {
            final long traceId = reset.trace();
            doReset(traceId);
        }

        private void onSignal(
            SignalFW signal)
        {
            final long traceId = signal.trace();
            doSignal(traceId);
        }

        private void onMqttConnect(
            MqttConnectFW packet)
        {
            int reasonCode = 0x00;

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
                doMqttConnack(reasonCode);
                this.decodeState = this::decodeSession;
            }
            else
            {
                doMqttConnack(reasonCode);
                doEnd(decodeTraceId);
            }
        }

        private void onMqttPingReq(
            MqttPingReqFW ping)
        {
            doMqttPingResp();
        }

        private void onMqttSubscribe(
            MqttSubscribeFW subscribe)
        {
            final OctetsFW topicFilters = subscribe.topicFilters();
            final DirectBuffer buffer = topicFilters.buffer();
            final int limit = topicFilters.limit();
            final int offset = topicFilters.offset();
            reasonCodeCount = 0;

            if (limit == 0)
            {
                doMqttDisconnect(0x82);
                doEnd(decodeTraceId);
            }
            else
            {
                MqttSubscriptionFW subscription;
                OctetsFW properties = subscribe.properties();
                final int propertiesOffset = properties.offset();
                final int propertiesLimit = properties.limit();
                int subscriptionId = 0;

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

                for (int progress = offset, index = 0; progress < limit; progress = subscription.limit(), index++)
                {
                    subscription = mqttSubscriptionRO.tryWrap(buffer, progress, limit);
                    if (subscription == null)
                    {
                        break;
                    }

                    // TODO - order of reason codes MATTER. first stream -> first entry into list, etc
                    //      - need to be able to map the correct reason codes to the correct streams
                    //      - defer reason code assignment to WINDOW
                    // need to map reason code -> stream in order of when they were created, not when they come in
                    // 	- ie. streams: 0, 1, 2, 3
                    // 			0 -> list[0]
                    // 			1 -> list[1]
                    // 			2 -> list[2]
                    // 			3 -> list[3]
                    // 				...
                    // 			n -> list[n]
                    //
                    // 		even if 1 sends WINDOW before 0, should sitll map to list[1], not list[0]
                    //      would streams be assigned a number to indicate their reasoncodesIndex?
                    int reasonCode = 0x8f;  // 0x8F - Topic Filter invalid
                    final String topicFilter = subscription.topicFilter().asString();
                    final RouteFW route = resolveTarget(routeId, authorization, topicFilter);

                    if (route != null)
                    {
                        final long newRouteId = route.correlationId();
                        final long newInitialId = supplyInitialId.applyAsLong(newRouteId);
                        final long newReplyId = supplyReplyId.applyAsLong(newInitialId);

                        final MessageConsumer newTarget = router.supplyReceiver(newInitialId);

                        final MqttServerStream serverStream = new MqttServerStream(this, newTarget,
                            newRouteId, newInitialId, newReplyId, index);

                        final MqttBeginExFW beginEx = mqttBeginExRW
                            .wrap(extBuffer, 0, extBuffer.capacity())
                            .typeId(mqttTypeId)
                            .role(r -> r.set(MqttRole.RECEIVER))
                            .clientId("client")
                            .topic(topicFilter)
                            .subscriptionId(subscriptionId)
                            .build();

                        serverStream.doMqttBeginEx(decodeTraceId, beginEx);

                        System.out.printf("new strim: %d\n", serverStream.initialId);
                        System.out.printf("mine: %d\n", initialId);
                        // reasonCodesIndex.put(serverStream.initialId, index);

                        correlations.put(newReplyId, serverStream);

                        // reasonCode = 0x00; // defer reason code assignment to WINDOW; isn't guaranteed success here

                        subscribers.put(topicFilter, serverStream);
                        reasonCodeCount++;
                    }

                    // reasonCodes.add((byte) reasonCode);
                }
            }
            // byte[] subackReasonCodes = new byte[reasonCodes.size()];
            // for (int i = 0; i < reasonCodes.size(); i++)
            // {
            //     subackReasonCodes[i] = reasonCodes.get(i);
            // }
            // // TODO - Next step would be to defer sending the SUBACK frame until all reason codes are collected from multiple
            // //      application streams, triggered by sending parallel application BEGIN streams in reaction to the same inbound
            // //      SUBSCRIBE frame.
            // System.out.printf("SUBACK: %s\n", Arrays.toString(subackReasonCodes));
            // doMqttSuback(subackReasonCodes);
            // System.out.println("sent all BEGIN");
            System.out.printf("reasonCodeCount: %d\n", reasonCodeCount);

            Subscription subscription = new Subscription(reasonCodeCount);
            correlations.forEach((id, stream) -> stream.subscription = subscription);
        }

        private void doMqttSuback() {
            byte[] subackReasonCodes = new byte[reasonCodes.size()];
            for (int i = 0; i < reasonCodes.size(); i++)
            {
                subackReasonCodes[i] = reasonCodes.get(i);
            }
            // TODO - Next step would be to defer sending the SUBACK frame until all reason codes are collected from multiple
            //      application streams, triggered by sending parallel application BEGIN streams in reaction to the same inbound
            //      SUBSCRIBE frame.
            System.out.printf("SUBACK: %s\n", Arrays.toString(subackReasonCodes));
            doMqttSuback(subackReasonCodes);
        }

        private void onMqttUnsubscribe(
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

            doMqttUnsuback(topics);
        }

        private void onMqttPublish(
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

            MqttServerStream publishStream = publishers.get(topicName);

            final RouteFW route = resolveTarget(routeId, authorization, topicName);
            if (route != null)
            {
                final long newRouteId = route.correlationId();
                final long newInitialId = supplyInitialId.applyAsLong(newRouteId);
                final long newReplyId = supplyReplyId.applyAsLong(newInitialId);
                final MessageConsumer newTarget = router.supplyReceiver(newInitialId);
                if (publishStream != null)
                {
                    publishStream.doMqttDataEx(decodeTraceId, payload, dataEx);
                    correlations.put(newReplyId, publishStream);    // TODO: do we need to clean up correlations onAbort()?
                }
                else
                {
                    final MqttServerStream serverStream = new MqttServerStream(this, newTarget,
                        newRouteId, newInitialId, newReplyId, -1);

                    serverStream.doBegin(decodeTraceId);

                    serverStream.doMqttDataEx(decodeTraceId, payload, dataEx);

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

        private void doBegin(
            long traceId)
        {
            final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(replyId)
                .trace(traceId)
                .build();

            System.out.printf("A:KJF:KAFKJALFJAF : %d\n", begin.streamId());
            network.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
            router.setThrottle(replyId, this::onNetwork);
        }

        // TODO: can change to Flyweight payload?
        private void doData(
            Flyweight payload)
        {
            final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(replyId)
                .trace(supplyTraceId.getAsLong())
                .groupId(0)
                .padding(replyPadding)
                .payload(payload.buffer(), payload.offset(), payload.sizeof())
                .build();

            network.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
        }

        private void doEnd(
            long traceId)
        {
            final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(replyId)
                .trace(traceId)
                .build();

            network.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
        }

        private void doAbort(
            long traceId)
        {
            final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(replyId)
                .trace(traceId)
                .build();

            network.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
        }

        private void doWindow(
            long traceId,
            int initialCredit)
        {
            if (initialCredit > 0)
            {
                initialBudget += initialCredit;

                final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(initialId)
                    .trace(traceId)
                    .credit(initialCredit)
                    .padding(initialPadding)
                    .groupId(0)
                    .build();

                network.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
            }
        }

        private void doReset(
            long traceId)
        {
            final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity()).routeId(routeId)
                .streamId(initialId)
                .trace(traceId)
                .build();

            network.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
        }

        private void doSignal(
            long traceId)
        {
            final SignalFW signal = signalRW.wrap(writeBuffer, 0, writeBuffer.capacity()).routeId(routeId)
                .streamId(initialId)
                .trace(traceId)
                .build();

            network.accept(signal.typeId(), signal.buffer(), signal.offset(), signal.sizeof());
        }

        private void doMqttPublish(
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

            final DirectBuffer publishBuffer = publish.buffer();
            final int publishOffset = publish.offset();
            final int publishLength = publish.sizeof();

            final DataFW dataFW = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(replyId)
                    .trace(supplyTraceId.getAsLong())
                    .groupId(0)
                    .padding(0)
                    .payload(publishBuffer, publishOffset, publishLength)
                    .build();

            this.network.accept(dataFW.typeId(), dataFW.buffer(), dataFW.offset(), dataFW.sizeof());
        }

        private void doMqttConnack(
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

            doData(connack);
        }

        private void doMqttPingResp()
        {
            final MqttPingRespFW ping = mqttPingRespRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .typeAndFlags(0xd0)
                .remainingLength(0x00)
                .build();

            doData(ping);
        }

        private void doMqttSuback(
            byte[] subscriptions)
        {
            OctetsFW reasonCodes = octetsRW
                .wrap(writeBuffer, 0, writeBuffer.capacity())
                .put(subscriptions)
                .build();

            final MqttSubackFW suback = mqttSubackRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .typeAndFlags(0x90)
                .remainingLength(reasonCodes.sizeof() + 1)
                .propertiesLength(0x00)
                .reasonCodes(reasonCodes)
                .build();

            doData(suback);
        }

        private void doMqttUnsuback(
            int subsriptions)
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

            doData(unsuback);
        }

        private void doMqttDisconnect(
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

            doData(disconnect);
        }

        private int decodeConnectPacket(
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
                onMqttConnect(mqttConnect);
                progress = mqttConnect.limit();
                break;
            default:
                doEnd(decodeTraceId);
                break;
            }
            return progress;
        }

        private int decodeSession(
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
                onMqttPingReq(ping);
                progress = ping.limit();
                break;
            case SUBSCRIBE:
                final MqttSubscribeFW subscribe = mqttSubscribeRO.wrap(buffer, offset, limit);
                onMqttSubscribe(subscribe);
                progress = subscribe.limit();
                break;
            case UNSUBSCRIBE:
                final MqttUnsubscribeFW unsubscribe = mqttUnsubscribeRO.wrap(buffer, offset, limit);
                onMqttUnsubscribe(unsubscribe);
                progress = unsubscribe.limit();
                break;
            case DISCONNECT:
                final MqttDisconnectFW disconnect = mqttDisconnectRO.wrap(buffer, offset, limit);
                onMqttDisconnect(disconnect);
                doEnd(decodeTraceId);
                progress = disconnect.limit();
                break;
            case PUBLISH:
                final MqttPublishFW publish = mqttPublishRO.wrap(buffer, offset, limit);
                onMqttPublish(publish);
                progress = publish.limit();
                break;
            default:
                doReset(decodeTraceId);
                break;
            }

            return progress;
        }
    }

    private final class Subscription
    {
        private final int fullReasonCodesMask;
        private final List<Byte> reasonCodes;

        private int reasonCodesMask;

        private Subscription(
            int reasonCodesCount)
        {
            this.reasonCodes = new ArrayList<>(reasonCodesCount);
            this.fullReasonCodesMask = 1 << (reasonCodesCount - 1);
        }

        private void setReasonCode(
            int index,
            Consumer<byte[]> doSuback)
        {
            final int bit = 1 << index;
            reasonCodes.add(index, (byte) 0x00);
            reasonCodesMask |= bit;
            if ((reasonCodesMask & fullReasonCodesMask) == fullReasonCodesMask)
            {
                doMqttSuback(doSuback);
            }
        }

        private void doMqttSuback(
            Consumer<byte[]> doSuback)
        {
            byte[] subackReasonCodes = new byte[reasonCodes.size()];
            for (int i = 0; i < reasonCodes.size(); i++)
            {
                subackReasonCodes[i] = reasonCodes.get(i);
            }

            System.out.printf("SUBACK: %s\n", Arrays.toString(subackReasonCodes));
            doSuback.accept(subackReasonCodes);
        }
    }

    private final class MqttServerStream
    {
        private final MqttServer server;
        private final MessageConsumer application;
        private final int reasonCodesIndex;
        private long routeId;
        private long initialId;
        private long replyId;
        private Subscription subscription;

        MqttServerStream(
            MqttServer server,
            MessageConsumer application,
            long routeId,
            long initialId,
            long replyId,
            int reasonCodesIndex)
        {
            this.server = server;
            this.application = application;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = replyId;
            this.reasonCodesIndex = reasonCodesIndex;
        }

        private void onApplication(
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
            System.out.printf("stream - begin: %d\n", begin.streamId());
            System.out.printf("stream - mine: %d\n", initialId);
            server.doWindow(supplyTraceId.getAsLong(), bufferPool.slotCapacity());

            subscription.setReasonCode(reasonCodesIndex, this.server::doMqttSuback);
            // need serverStream window to do work
            // if (reasonCodesIndex > -1) {
            //     reasonCodes.add(reasonCodesIndex, (byte) 0x00);
            //     reasonCodeCount--;
            //     System.out.printf("reasonCodes: %s\n", reasonCodes);
            //     if (reasonCodeCount == 0) {
            //         System.out.println("doing suback");
            //         doMqttSuback();
            //     }
            // }
        }

        private void onData(
            DataFW data)
        {
            final OctetsFW extension = data.extension();

            final DirectBuffer exBuffer = extension.buffer();
            final int exOffset = extension.offset();
            final int exLength = extension.sizeof();

            final MqttDataExFW dataEx = mqttDataExRO.wrap(exBuffer, exOffset, exOffset + exLength);

            final String topicName = dataEx.topic().asString();
            this.server.doMqttPublish(topicName, data.payload());
        }

        private void onEnd(
            EndFW end)
        {
        }

        private void onWindow(
            WindowFW window)
        {
            System.out.println("Server Stream WINDOW");
        }

        private void onReset(
            ResetFW reset)
        {
        }

        private void doBegin(
            long traceId)
        {
            final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(initialId)
                .trace(traceId)
                .build();

            application.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
        }

        private void doMqttBeginEx(
            long traceId,
            Flyweight extension)
        {
            final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(initialId)
                .trace(traceId)
                .extension(extension.buffer(), extension.offset(), extension.sizeof())
                .build();

            application.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
        }

        private void doMqttDataEx(
            long traceId,
            OctetsFW payload,
            Flyweight extension)
        {
            final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(initialId)
                .trace(traceId)
                .groupId(0)
                .padding(0)
                .payload(payload)
                .extension(extension.buffer(), extension.offset(), extension.sizeof())
                .build();

            application.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
        }
    }

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(MqttPacketType packetType, DirectBuffer buffer, int offset, int limit);
    }
}
