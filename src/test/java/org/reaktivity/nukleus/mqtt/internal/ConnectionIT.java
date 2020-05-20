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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.CLIENT_ID_NAME;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.PUBLISH_TIMEOUT;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configure;

public class ConnectionIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/mqtt/control/route")
        .addScriptRoot("routeExt", "org/reaktivity/specification/nukleus/mqtt/control/route.ext")
        .addScriptRoot("client", "org/reaktivity/specification/mqtt")
        .addScriptRoot("server", "org/reaktivity/specification/nukleus/mqtt/streams");

    private final TestRule timeout = new DisableOnDebug(new Timeout(20, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .nukleus("mqtt"::equals)
        .configure(PUBLISH_TIMEOUT, 5L)
        .configure(ReaktorConfiguration.REAKTOR_DRAIN_ON_CLOSE, false)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/successful/client"})
    public void shouldExchangeConnectAndConnackPackets() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/ping/client"})
    public void shouldExchangeConnectionPacketsThenPingPackets() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/disconnect/client"})
    public void shouldExchangeConnectionPacketsThenDisconnect() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/unsubscribe/client",
        "${server}/subscribe.with.exact.topic.filter/server"})
    public void shouldExchangeConnectionPacketsThenUnsubscribeAfterSubscribe() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/unsubscribe/aggregated.topic.filters.both.exact/client",
        "${server}/subscribe.with.aggregated.topic.filters.both.exact/server"})
    public void shouldUnsubscribeFromTwoTopicsBothExactOneUnsubackPacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.one.message/client",
        "${server}/publish.one.message/server"})
    public void shouldPublishOneMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/publish.only/server/controller",
        "${client}/publish.one.message/client",
        "${server}/publish.one.message/server"})
    public void shouldPublishOneMessageWithRouteExtension() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.one.message.with.topic.alias/client",
        "${server}/publish.one.message.with.topic.alias/server"})
    public void shouldPublishOneMessageWithTopicAlias() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.multiple.messages/client",
        "${server}/publish.multiple.messages/server"})
    public void shouldPublishMultipleMessages() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.multiple.messages.with.delay/client",
        "${server}/publish.multiple.messages.with.delay/server"})
    public void shouldPublishMultipleMessagesWithDelay() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("PUBLISHED_MESSAGE_TWO");
        Thread.sleep(6000);
        k3po.notifyBarrier("PUBLISH_MESSAGE_THREE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.one.message/client",
        "${server}/subscribe.one.message/server"})
    public void shouldReceivePublishAfterSendingSubscribe() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/subscribe.only/server/controller",
        "${client}/subscribe.one.message/client",
        "${server}/subscribe.one.message/server"})
    public void shouldReceivePublishAfterSendingSubscribeWtihRouteExtension() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.one.message.then.publish.message/client",
        "${server}/subscribe.one.message.then.publish.message/server"})
    public void shouldSubscribeOneMessageThenPublishMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.one.message.with.null.payload/client",
        "${server}/subscribe.one.message.with.null.payload/server"})
    public void shouldReceivePublishWithNullPayloadAfterSendingSubscribe() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.fails.then.publish.message/client",
        "${server}/subscribe.fails.then.publish.message/server"})
    public void shouldFailSubscribeThenPublishMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.message.and.subscribe.correlated.message/client",
        "${server}/publish.message.and.subscribe.correlated.message/server"})
    public void shouldReceiveCorrelatedPublishAfterSendingSubscribe() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.one.message.with.invalid.subscription.id/client"})
    public void shouldReceivePublishWithInvalidSubscriptionIdAfterSendingSubscribe() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/invalid.protocol.version/client"})
    public void shouldRejectInvalidMqttProtocolVersion() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/invalid.flags/client"})
    public void shouldRejectMalformedConnectPacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/invalid.fixed.header.flags/client"})
    public void shouldRejectMalformedSubscribePacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/invalid.topic.filter/client"})
    public void shouldRejectSubscribePacketWithInvalidTopicFilter() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/unsubscribe/invalid.fixed.header.flags/client",
        "${server}/subscribe.with.exact.topic.filter/server"})
    public void shouldExchangeConnectionPacketsThenRejectMalformedUnsubscribePacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/disconnect/invalid.fixed.header.flags/client"})
    public void shouldRejectMalformedDisconnectPacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/reject.second.connect/client"})
    public void shouldRejectSecondConnectPacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/successful.fragmented/client"})
    public void shouldProcessFragmentedConnectPacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/single.topic.filter.exact/client",
        "${server}/subscribe.with.exact.topic.filter/server"})
    public void shouldSubscribeToOneExactTopic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/single.topic.filter.wildcard/client",
        "${server}/subscribe.with.wildcard.topic.filter/server"})
    public void shouldSubscribeToWildcardTopic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/aggregated.topic.filters.both.exact/client",
        "${server}/subscribe.with.aggregated.topic.filters.both.exact/server"})
    public void shouldSubscribeWithTwoTopicsBothExactOneSubscribePacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/isolated.topic.filters.both.exact/client",
        "${server}/subscribe.with.isolated.topic.filters.both.exact/server"})
    public void shouldSubscribeWithTwoTopicsBothExactTwoSubscribePackets() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/aggregated.topic.filters.both.wildcard/client",
        "${server}/subscribe.with.aggregated.topic.filters.both.wildcard/server"})
    public void shouldSubscribeWithTwoTopicsBothWildcardOneSubscribePacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/isolated.topic.filters.both.wildcard/client",
        "${server}/subscribe.with.isolated.topic.filters.both.wildcard/server"})
    public void shouldSubscribeWithTwoTopicsBothWildcardTwoSubscribePackets() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/aggregated.topic.filters.exact.and.wildcard/client",
        "${server}/subscribe.with.aggregated.topic.filters.exact.and.wildcard/server"})
    public void shouldSubscribeWithTwoTopicsOneExactOneSubscribePacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/isolated.topic.filters.exact.and.wildcard/client",
        "${server}/subscribe.with.isolated.topic.filters.exact.and.wildcard/server"})
    public void shouldSubscribeWithTwoTopicsOneExactTwoSubscribePackets() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/publish.only/server/controller",
        "${client}/topic.not.routed/client"})
    public void shouldRejectPublishWithTopicNotRouted() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/publish.only/server/controller",
        "${client}/publish.rejected/client",
        "${server}/publish.rejected/server"})
    public void shouldRejectPublish() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.sent.close/client",
        "${server}/client.sent.abort/server"})
    public void shouldReceiveClientSentClose() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.sent.abort/client",
        "${server}/client.sent.abort/server"})
    public void shouldReceiveClientSentAbort() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.sent.reset/client",
        "${server}/client.sent.abort/server"})
    public void shouldReceiveClientSentReset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.with.user.property/client",
        "${server}/publish.with.user.property/server"})
    @Configure(name = CLIENT_ID_NAME, value = "755452d5-e2ef-4113-b9c6-2f53de96fd76")
    public void shouldPublishWithUserProperty() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.with.user.properties.distinct/client",
        "${server}/publish.with.user.properties.distinct/server"})
    @Configure(name = CLIENT_ID_NAME, value = "755452d5-e2ef-4113-b9c6-2f53de96fd76")
    public void shouldPublishWithDistinctUserProperties() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.with.user.properties.repeated/client",
        "${server}/publish.with.user.properties.repeated/server"})
    @Configure(name = CLIENT_ID_NAME, value = "755452d5-e2ef-4113-b9c6-2f53de96fd76")
    public void shouldPublishWithRepeatedUserProperties() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/disconnect.after.keep.alive.timeout/client"})
    public void shouldDisconnectClientAfterKeepAliveTimeout() throws Exception
    {
        k3po.start();
        Thread.sleep(15000);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/keep.alive.with.pingreq/client",
        "${server}/subscribe.with.exact.topic.filter/server"})
    public void shouldKeepAliveWithPingreq() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/timeout.before.connect/client"})
    public void shouldTimeoutBeforeConnect() throws Exception
    {
        k3po.finish();
    }
}
