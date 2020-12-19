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
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.PUBLISH_TIMEOUT;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfigurationTest.CLIENT_ID_NAME;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfigurationTest.MAXIMUM_QOS_NAME;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfigurationTest.NO_LOCAL_NAME;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfigurationTest.RETAIN_AVAILABLE_NAME;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfigurationTest.SESSION_EXPIRY_INTERVAL_NAME;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfigurationTest.SHARED_SUBSCRIPTION_AVAILABLE_NAME;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfigurationTest.SUBSCRIPTION_IDENTIFIERS_AVAILABLE_NAME;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfigurationTest.TOPIC_ALIAS_MAXIMUM_NAME;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfigurationTest.WILDCARD_SUBSCRIPTION_AVAILABLE_NAME;
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = TOPIC_ALIAS_MAXIMUM_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = TOPIC_ALIAS_MAXIMUM_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = TOPIC_ALIAS_MAXIMUM_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = TOPIC_ALIAS_MAXIMUM_NAME, value = "1")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = TOPIC_ALIAS_MAXIMUM_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldReceiveClientSentReset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.with.user.property/client",
        "${server}/publish.with.user.property/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldPublishWithUserProperty() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.with.user.properties.distinct/client",
        "${server}/publish.with.user.properties.distinct/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldPublishWithDistinctUserProperties() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.with.user.properties.repeated/client",
        "${server}/publish.with.user.properties.repeated/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldKeepAliveWithPingreq() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/timeout.before.connect/client"})
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
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
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldPublishEmptyMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/maximum.qos.0/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldConnectWithMaximumQos0() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/retain.unavailable/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = RETAIN_AVAILABLE_NAME, value = "false")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldConnectWithNoRetain() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/wildcard.subscriptions.unavailable/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "false")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldConnectWithNoWildcardSubscriptions() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/subscription.identifiers.unavailable/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SUBSCRIPTION_IDENTIFIERS_AVAILABLE_NAME, value = "false")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldConnectWithNoSubscriptionIdentifiers() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/shared.subscriptions.unavailable/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldConnectWithNoSharedSubscriptions() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/reject.username/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldRejectConnectWithUsername() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/reject.password/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldRejectConnectWithPassword() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/reject.invalid.will.qos/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldRejectInvalidWillQos() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/reject.will.qos.1.without.will.flag/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldRejectWillQos1WithoutWillFlag() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/reject.will.qos.2.without.will.flag/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldRejectWillQos2WithoutWillFlag() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect/reject.will.retain.without.will.flag/client"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldRejectWillRetainWithoutWillFlag() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/reject.no.local/client"})
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    @Configure(name = NO_LOCAL_NAME, value = "false")
    public void shouldRejectSubscribeWithNoLocal() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/session/server/controller",
        "${client}/connect/will.message.with.abrupt.disconnect/client",
        "${server}/publish.session.data/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldPublishWillMessageAfterAbruptClientDisconnect() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/session/server/controller",
        "${client}/connect/will.message.with.normal.disconnect/client",
        "${server}/unpublished.will.message/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldIgnoreWillMessageAfterNormalClientDisconnect() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.then.subscribe.one.message/client",
        "${server}/publish.then.subscribe.one.message/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldPublishThenSubscribeOneMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.then.publish.no.local/client",
        "${server}/subscribe.then.publish.no.local/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldSubscribeThenPublishNoLocal() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish.then.subscribe.no.local/client",
        "${server}/publish.then.subscribe.no.local/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldPublishThenSubscribeNoLocal() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe.to.will.topic/client",
        "${server}/subscribe.to.will.topic/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldSubscribeToWillTopic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connect.with.will.message.then.publish.one.message/client",
        "${server}/connect.with.will.message.then.publish.one.message/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldConnectWithWillMessageThenPublishOneMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/messages.aggregated.topic.filters.both.exact/client",
        "${server}/subscribe.messages.with.aggregated.topic.filters.both.exact/server"})
    @Configure(name = WILDCARD_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = SHARED_SUBSCRIPTION_AVAILABLE_NAME, value = "true")
    @Configure(name = MAXIMUM_QOS_NAME, value = "2")
    @Configure(name = SESSION_EXPIRY_INTERVAL_NAME, value = "0")
    public void shouldSubscribeMessagesWithTwoTopicsBothExactOneSubscribePacket() throws Exception
    {
        k3po.finish();
    }
}
