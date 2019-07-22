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

import static java.util.Objects.requireNonNull;

import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttConnackFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttConnectFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttDisconnectFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPacketFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPingReqFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPingRespFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttPublishFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttSubackFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttSubscribeFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttUnsubackFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttUnsubscribeFW;
import org.reaktivity.nukleus.mqtt.internal.types.codec.MqttTopicFW;

import org.reaktivity.nukleus.mqtt.internal.types.control.RouteFW;

import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.mqtt.internal.MqttConfiguration;
import org.reaktivity.nukleus.mqtt.internal.MqttNukleus;
import org.reaktivity.nukleus.mqtt.internal.types.OctetsFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.DataFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.EndFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.SignalFW;

import org.reaktivity.nukleus.mqtt.internal.types.stream.MqttBeginExFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.MqttDataExFW;
import org.reaktivity.nukleus.mqtt.internal.types.stream.MqttEndExFW;

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

    private final MqttBeginExFW.Builder mqttBeginExRW = new MqttBeginExFW.Builder();
    private final MqttDataExFW.Builder mqttDataExRW = new MqttDataExFW.Builder();
    private final MqttEndExFW.Builder mqttEndExRW = new MqttEndExFW.Builder();

    private final MqttPacketFW mqttPacketRO = new MqttPacketFW();
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
    private final MqttTopicFW mqttTopicRO = new MqttTopicFW();

    private final OctetsFW.Builder octetsRW = new OctetsFW.Builder();

    private final MqttPacketFW.Builder mqttPacketRW = new MqttPacketFW.Builder();
    private final MqttConnectFW.Builder mqttConnectRW = new MqttConnectFW.Builder();
    private final MqttConnackFW.Builder mqttConnackRW = new MqttConnackFW.Builder();
    private final MqttPingReqFW.Builder mqttPingReqRW = new MqttPingReqFW.Builder();
    private final MqttDisconnectFW.Builder mqttDisconnectRW = new MqttDisconnectFW.Builder();
    private final MqttPingRespFW.Builder mqttPingRespRW = new MqttPingRespFW.Builder();
    private final MqttSubscribeFW.Builder mqttSubscribeRW = new MqttSubscribeFW.Builder();
    private final MqttSubackFW.Builder mqttSubackRW = new MqttSubackFW.Builder();
    private final MqttUnsubscribeFW.Builder mqttUnsubscribeRW = new MqttUnsubscribeFW.Builder();
    private final MqttUnsubackFW.Builder mqttUnsubackRW = new MqttUnsubackFW.Builder();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;

    private final Long2ObjectHashMap<MqttServer> correlations;
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
            correlations.put(replyId, connection);
            newStream = connection::onNetwork;
        }
        return newStream;
    }

    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long replyId = begin.streamId();
        final MqttServer connect = correlations.remove(replyId);

        MessageConsumer newStream = null;
        if (connect != null)
        {
            newStream = connect::onNetwork;
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

    private final class MqttServer
    {
        private final MessageConsumer network;
        private final long routeId;
        private final long initialId;
        private final long replyId;

        private int initialBudget;
        private int initialPadding;
        private int replyBudget;
        private int replyPadding;

        private long decodeTraceId;
        private DecoderState decodeState;
        private int bufferSlot = BufferPool.NO_SLOT;
        private int bufferSlotOffset;

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
            this.decodeState = this::decodeConnectPacket;
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
                decodeTraceId = data.trace();
                DirectBuffer buffer = payload.buffer();

                int offset = payload.offset();
                int length = payload.sizeof();

                decodeState.decode(buffer, offset, length);
            }
        }

        private void cleanBufferSlot()
        {
            bufferPool.release(bufferSlot);
            bufferSlot = BufferPool.NO_SLOT;
        }

        private boolean noBufferSlot()
        {
            return  bufferSlot == BufferPool.NO_SLOT;
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
            DirectBuffer buffer = subscribe.topics().buffer();
            int offset = subscribe.topics().offset();
            int length = subscribe.topics().sizeof();

            int topicsLength = 0;

            while (length > 0)
            {
                MqttTopicFW topic = mqttTopicRO
                    .tryWrap(buffer, offset, offset + length);

                offset += topic.sizeof();
                length -= topic.sizeof();

                topicsLength ++;
            }

            doMqttSuback(topicsLength);
        }

        private void onMqttUnsubscribe(
            MqttUnsubscribeFW unsubscribe)
        {
            int topics = unsubscribe == null ? 0 : unsubscribe.topicFilters().sizeof();

            doMqttUnsuback(topics);
        }

        private void onMqttPublish(
            MqttPublishFW publish)
        {
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
            network.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
            router.setThrottle(replyId, this::onNetwork);
        }

        private void doData(
            DirectBuffer buffer,
            int offset,
            int sizeOf)
        {
            final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(replyId)
                .trace(supplyTraceId.getAsLong())
                .groupId(0)
                .padding(replyPadding)
                .payload(buffer, offset, sizeOf)
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

        private void doMqttConnack(
            int reasonCode)
        {
            OctetsFW properties = octetsRW
                .wrap(writeBuffer, 0, writeBuffer.capacity())
                .build();

            final MqttConnackFW connack = mqttConnackRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .remainingLength(properties.sizeof() + 3)
                .flags(0x00)
                .reasonCode(reasonCode)
                .propertiesLength(properties.sizeof())
                .properties(properties)
                .build();

            doData(connack.buffer(), connack.offset(), connack.sizeof());
        }

        private void doMqttPingResp()
        {
            final MqttPingRespFW ping = mqttPingRespRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .build();

            doData(ping.buffer(), ping.offset(), ping.sizeof());
        }

        private void doMqttSuback(
            int subscriptions)
        {
            OctetsFW reasonCodes = octetsRW
                .wrap(writeBuffer, 0, writeBuffer.capacity())
                .put(new byte[] {0x00})
                .build();

            final MqttSubackFW suback = mqttSubackRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .remainingLength(reasonCodes.sizeof() + 1)
                .propertiesLength(0x00)
                .reasonCodes(reasonCodes)
                .build();

            doData(suback.buffer(), suback.offset(), suback.sizeof());
        }

        private void doMqttUnsuback(
            int subsriptions)
        {
            OctetsFW reasonCodes = octetsRW
                .wrap(writeBuffer, 0, writeBuffer.capacity())
                .put(new byte[] {0x00})
                .build();

            final MqttUnsubackFW unsuback = mqttUnsubackRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .remainingLength(reasonCodes.sizeof() + 1)
                .propertiesLength(0x00)
                .reasonCodes(reasonCodes)
                .build();

            doData(unsuback.buffer(), unsuback.offset(), unsuback.sizeof());
        }

        private void doMqttDisconnect(
            int reasonCode)
        {
            OctetsFW properties = octetsRW
                .wrap(writeBuffer, 0, writeBuffer.capacity())
                .build();

            final MqttDisconnectFW disconnect = mqttDisconnectRW
                .wrap(writeBuffer,  DataFW.FIELD_OFFSET_PAYLOAD, writeBuffer.capacity())
                .remainingLength((byte) properties.sizeof() + 1)
                .reasonCode(reasonCode)
                .properties(properties)
                .build();

            doData(disconnect.buffer(), disconnect.offset(), disconnect.sizeof());
        }

        private boolean setReservedSubscriptionOptionsBits(
            int options)
        {
            return ((options >> 8) & 1) != 0
                && ((options >> 7) & 1) != 0;
        }

        private int decodeConnectPacket(
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            final MqttPacketFW mqttPacket = mqttPacketRO.tryWrap(buffer, offset, offset + length);
            final int packetType = mqttPacket == null ? 0 : mqttPacket.packetType();

            if (packetType == 0x10)
            {
                MqttConnectFW mqttConnect = mqttConnectRO.tryWrap(buffer, offset, offset + length);
                onMqttConnect(mqttConnect);
                return mqttConnect == null ? 0 : mqttConnect.sizeof();
            }
            else
            {
                this.decodeState = this::decodeEnd;
            }

            return mqttPacket == null ? 0 : mqttPacket.sizeof();
        }

        private int decodeSession(
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            int consumed = 0;

            final MqttPacketFW mqttPacket = mqttPacketRO.tryWrap(buffer, offset, offset + length);
            final int packetType = mqttPacket == null ? 0 : mqttPacket.packetType();

            switch (packetType)
            {
                case 0xc0:
                    final MqttPingReqFW ping = mqttPingReqRO.tryWrap(buffer, offset, offset + length);
                    onMqttPingReq(ping);
                    break;
                case 0x82:
                    final MqttSubscribeFW subscribe = mqttSubscribeRO.tryWrap(buffer, offset, offset + length);
                    onMqttSubscribe(subscribe);
                    break;
                case 0xa2:
                    final MqttUnsubscribeFW unsubscribe = mqttUnsubscribeRO.tryWrap(buffer, offset, offset + length);
                    onMqttUnsubscribe(unsubscribe);
                    break;
                case 0xe0:
                    final MqttDisconnectFW disconnect = mqttDisconnectRO.tryWrap(buffer, offset, offset + length);
                    onMqttDisconnect(disconnect);
                    doEnd(decodeTraceId);
                    break;
                case 0x30:
                    final MqttPublishFW publish = mqttPublishRO.tryWrap(buffer, offset, offset + length);
                    onMqttPublish(publish);
                    break;
                default:
                    doReset(decodeTraceId);
                    break;
            }

            return consumed;
        }

        private int decodeEnd(
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            int consumed = 0;

            doMqttDisconnect(128);
            doEnd(decodeTraceId);

            return consumed;
        }
    }

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int length);
    }
}
