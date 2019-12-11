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

package org.reaktivity.nukleus.mqtt.internal;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.mqtt.internal.MqttConfiguration.PUBLISH_TIMEOUT;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ConnectionIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/mqtt/control/route")
        .addScriptRoot("client", "org/reaktivity/specification/mqtt")
        .addScriptRoot("server", "org/reaktivity/specification/nukleus/mqtt/streams");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .nukleus("mqtt"::equals)
        .configure(PUBLISH_TIMEOUT, 5L)
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
        "${server}/connect.as.receiver.with.exact.topic.filter/server"})
    public void shouldExchangeConnectionPacketsThenUnsubscribeAfterSubscribe() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish/send.one.message/client",
        "${server}/send.one.message/server"})
    public void shouldPublishOneMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish/send.multiple.messages/client",
        "${server}/send.multiple.messages/server"})
    public void shouldPublishMultipleMessages() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish/send.multiple.messages.with.delay/client",
        "${server}/send.multiple.messages.with.delay/server"})
    public void shouldPublishMultipleMessagesWithDelay() throws Exception
    {
        k3po.start();
        Thread.sleep(6000);
        k3po.notifyBarrier("PUBLISH_CLOSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/publish/receive.one.message/client",
        "${server}/receive.one.message/server"})
    public void shouldReceivePublishAfterSendingSubscribe() throws Exception
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
        "${client}/unsubscribe/invalid.fixed.header.flags/client",
        "${server}/connect.as.receiver.with.exact.topic.filter/server"})
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
        "${server}/connect.as.receiver.with.exact.topic.filter/server"})
    public void shouldSubscribeToOneExactTopic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/single.topic.filter.wildcard/client",
        "${server}/connect.as.receiver.with.wildcard.topic.filter/server"})
    public void shouldSubscribeToWildcardTopic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/aggregated.topic.filters.both.exact/client",
        "${server}/connect.as.receiver.with.aggregated.topic.filters.both.exact/server"})
    public void shouldSubscribeWithTwoTopicsBothExactOneSubscribePacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/isolated.topic.filters.both.exact/client",
        "${server}/connect.as.receiver.with.isolated.topic.filters.both.exact/server"})
    public void shouldSubscribeWithTwoTopicsBothExactTwoSubscribePackets() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/aggregated.topic.filters.both.wildcard/client",
        "${server}/connect.as.receiver.with.aggregated.topic.filters.both.wildcard/server"})
    public void shouldSubscribeWithTwoTopicsBothWildcardOneSubscribePacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/isolated.topic.filters.both.wildcard/client",
        "${server}/connect.as.receiver.with.isolated.topic.filters.both.wildcard/server"})
    public void shouldSubscribeWithTwoTopicsBothWildcardTwoSubscribePackets() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/aggregated.topic.filters.exact.and.wildcard/client",
        "${server}/connect.as.receiver.with.aggregated.topic.filters.exact.and.wildcard/server"})
    public void shouldSubscribeWithTwoTopicsOneExactOneSubscribePacket() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/subscribe/isolated.topic.filters.exact.and.wildcard/client",
        "${server}/connect.as.receiver.with.isolated.topic.filters.exact.and.wildcard/server"})
    public void shouldSubscribeWithTwoTopicsOneExactTwoSubscribePackets() throws Exception
    {
        k3po.finish();
    }
}
