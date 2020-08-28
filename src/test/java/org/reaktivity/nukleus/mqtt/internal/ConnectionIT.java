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
import static org.reaktivity.nukleus.mqtt.internal.ConfigurationTest.CLIENT_ID_NAME;
import static org.reaktivity.nukleus.mqtt.internal.ConfigurationTest.SHARED_SUBSCRIPTION_AVAILABLE_NAME;
import static org.reaktivity.nukleus.mqtt.internal.ConfigurationTest.WILDCARD_SUBSCRIPTION_AVAILABLE_NAME;
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
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldExchangeConnectAndConnackPackets() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/server.assigned.client.id/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldExchangeConnectAndConnackPacketsWithServerAssignedClientId() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/reject.missing.client.id/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldRejectMissingClientId() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/ping/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldExchangeConnectionPacketsThenPingPackets() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/disconnect/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldExchangeConnectionPacketsThenDisconnect() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/unsubscribe/client",
        "${server}/subscribe.with.exact.topic.filter/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldExchangeConnectionPacketsThenUnsubscribeAfterSubscribe() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/unsubscribe/aggregated.topic.filters.both.exact/client",
        "${server}/subscribe.with.aggregated.topic.filters.both.exact/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldUnsubscribeFromTwoTopicsBothExactOneUnsubackPacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.one.message/client",
        "${server}/publish.one.message/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldPublishOneMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/publish.only/server/controller",
        "${client}/publish.one.message/client",
        "${server}/publish.one.message/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldPublishOneMessageWithRouteExtension() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.retained/client",
        "${server}/publish.retained/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldPublishRetainedMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.message.with.topic.alias/client",
        "${server}/publish.message.with.topic.alias/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldPublishMessageWithTopicAlias() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.multiple.messages/client",
        "${server}/publish.multiple.messages/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldPublishMultipleMessages() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.multiple.messages.with.delay/client",
        "${server}/publish.multiple.messages.with.delay/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
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
        "${client}/publish.messages.with.topic.alias.distinct/client",
        "${server}/publish.messages.with.topic.alias.distinct/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldPublishMessagesWithTopicAliasDistinct() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.messages.with.topic.alias.repeated/client",
        "${server}/publish.messages.with.topic.alias.repeated/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldPublishMessagesWithTopicAliasRepeated() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.messages.with.topic.alias.replaced/client",
        "${server}/publish.messages.with.topic.alias.replaced/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldPublishMessagesWithTopicAliasReplaced() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.one.message/client",
        "${server}/subscribe.one.message/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldReceivePublishAfterSendingSubscribe() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/subscribe.only/server/controller",
        "${client}/subscribe.one.message/client",
        "${server}/subscribe.one.message/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldReceivePublishAfterSendingSubscribeWithRouteExtension() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.one.message.then.publish.message/client",
        "${server}/subscribe.one.message.then.publish.message/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldSubscribeOneMessageThenPublishMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.one.message.with.null.payload/client",
        "${server}/subscribe.one.message.with.null.payload/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldReceivePublishWithNullPayloadAfterSendingSubscribe() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.fails.then.publish.message/client",
        "${server}/subscribe.fails.then.publish.message/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldFailSubscribeThenPublishMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.message.and.subscribe.correlated.message/client",
        "${server}/publish.message.and.subscribe.correlated.message/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldReceiveCorrelatedPublishAfterSendingSubscribe() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.retained/client",
        "${server}/subscribe.retained/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldSubscribeRetainedMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.one.message.with.invalid.subscription.id/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldReceivePublishWithInvalidSubscriptionIdAfterSendingSubscribe() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/invalid.protocol.version/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldRejectInvalidMqttProtocolVersion() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/invalid.flags/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldRejectMalformedConnectPacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/invalid.authentication.method/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldRejectBadAuthenticationMethod() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/invalid.fixed.header.flags/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldRejectMalformedSubscribePacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/invalid.topic.filter/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldRejectSubscribePacketWithInvalidTopicFilter() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/unsubscribe/invalid.fixed.header.flags/client",
        "${server}/subscribe.with.exact.topic.filter/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldExchangeConnectionPacketsThenRejectMalformedUnsubscribePacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/disconnect/invalid.fixed.header.flags/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldRejectMalformedDisconnectPacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/reject.second.connect/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldRejectSecondConnectPacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/successful.fragmented/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldProcessFragmentedConnectPacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/single.topic.filter.exact/client",
        "${server}/subscribe.with.exact.topic.filter/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldSubscribeToOneExactTopic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/single.topic.filter.wildcard/client",
        "${server}/subscribe.with.wildcard.topic.filter/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldSubscribeToWildcardTopic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/aggregated.topic.filters.both.exact/client",
        "${server}/subscribe.with.aggregated.topic.filters.both.exact/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldSubscribeWithTwoTopicsBothExactOneSubscribePacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/isolated.topic.filters.both.exact/client",
        "${server}/subscribe.with.isolated.topic.filters.both.exact/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldSubscribeWithTwoTopicsBothExactTwoSubscribePackets() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/aggregated.topic.filters.both.wildcard/client",
        "${server}/subscribe.with.aggregated.topic.filters.both.wildcard/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldSubscribeWithTwoTopicsBothWildcardOneSubscribePacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/isolated.topic.filters.both.wildcard/client",
        "${server}/subscribe.with.isolated.topic.filters.both.wildcard/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldSubscribeWithTwoTopicsBothWildcardTwoSubscribePackets() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/aggregated.topic.filters.exact.and.wildcard/client",
        "${server}/subscribe.with.aggregated.topic.filters.exact.and.wildcard/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldSubscribeWithTwoTopicsOneExactOneSubscribePacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/isolated.topic.filters.exact.and.wildcard/client",
        "${server}/subscribe.with.isolated.topic.filters.exact.and.wildcard/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldSubscribeWithTwoTopicsOneExactTwoSubscribePackets() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/publish.only/server/controller",
        "${client}/topic.not.routed/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldRejectPublishWithTopicNotRouted() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/publish.only/server/controller",
        "${client}/publish.rejected/client",
        "${server}/publish.rejected/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldRejectPublish() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/publish.only/server/controller",
        "${client}/reject.publish.when.topic.alias.exceeds.maximum/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldRejectPublishWHenTopicAliasExceedsMaximum() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/publish.only/server/controller",
        "${client}/reject.connect.when.topic.alias.maximum.repeated/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldRejectConnectWhenTopicAliasMaximumRepeated() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/publish.only/server/controller",
        "${client}/reject.publish.when.topic.alias.repeated/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldRejectPublishWithMultipleTopicAliases() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.sent.close/client",
        "${server}/client.sent.abort/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldReceiveClientSentClose() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.sent.abort/client",
        "${server}/client.sent.abort/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldReceiveClientSentAbort() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.sent.reset/client",
        "${server}/client.sent.abort/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
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
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
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
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
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
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldPublishWithRepeatedUserProperties() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/disconnect.after.keep.alive.timeout/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
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
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
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

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.retain.as.published/client",
        "${server}/subscribe.retain.as.published/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldSubscribeRetainAsPublished() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.ignore.retain.as.published/client",
        "${server}/subscribe.ignore.retain.as.published/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldSubscribeIgnoreRetainAsPublished() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.empty.retained.message/client",
        "${server}/publish.empty.retained.message/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldPublishEmptyRetainedMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.empty.message/client",
        "${server}/publish.empty.message/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    public void shouldPublishEmptyMessage() throws Exception
    {
        k3po.finish();
    }
}
