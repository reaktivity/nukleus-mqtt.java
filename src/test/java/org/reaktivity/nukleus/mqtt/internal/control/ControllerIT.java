/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.nukleus.mqtt.internal.control;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.route.RouteKind.CLIENT;
import static org.reaktivity.nukleus.route.RouteKind.SERVER;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.mqtt.internal.MqttController;
import org.reaktivity.reaktor.test.ReaktorRule;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class ControllerIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/mqtt/control/route")
        .addScriptRoot("routeExt", "org/reaktivity/specification/nukleus/mqtt/control/route.ext")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/mqtt/control/unroute")
        .addScriptRoot("freeze", "org/reaktivity/specification/nukleus/control/freeze");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .controller("mqtt"::equals);

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(reaktor);

    private final Gson gson = new Gson();

    @Test
    @Specification({
        "${route}/server/nukleus"
    })
    public void shouldRouteServer() throws Exception
    {
        k3po.start();

        reaktor.controller(MqttController.class)
                .route(SERVER, "mqtt#0", "target#0")
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/server/nukleus"
    })
    public void shouldRouteServerWithExt() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("topic", "sensor/one");

        reaktor.controller(MqttController.class)
                .route(SERVER, "mqtt#0", "target#0", gson.toJson(extension))
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/nukleus",
        "${unroute}/server/nukleus"
    })
    public void shouldUnrouteServer() throws Exception
    {
        k3po.start();

        long routeId = reaktor.controller(MqttController.class)
                .route(SERVER, "mqtt#0", "target#0")
                .get();

        k3po.notifyBarrier("ROUTED_SERVER");

        reaktor.controller(MqttController.class)
                .unroute(routeId)
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/server/nukleus",
        "${unroute}/server/nukleus"
    })
    public void shouldUnrouteServerWithExt() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("topic", "sensor/one");

        long routeId = reaktor.controller(MqttController.class)
                .route(SERVER, "mqtt#0", "target#0", gson.toJson(extension))
                .get();

        k3po.notifyBarrier("ROUTED_SERVER");

        reaktor.controller(MqttController.class)
                .unroute(routeId)
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/nukleus"
    })
    public void shouldRouteClient() throws Exception
    {
        k3po.start();

        reaktor.controller(MqttController.class)
                .route(CLIENT, "mqtt#0", "target#0")
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/client/nukleus"
    })
    public void shouldRouteClientExt() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("topic", "sensor/one");

        reaktor.controller(MqttController.class)
                .route(CLIENT, "mqtt#0", "target#0", gson.toJson(extension))
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/nukleus",
        "${unroute}/client/nukleus"
    })
    public void shouldUnrouteClient() throws Exception
    {
        k3po.start();

        long routeId = reaktor.controller(MqttController.class)
                .route(CLIENT, "mqtt#0", "target#0")
                .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(MqttController.class)
                .unroute(routeId)
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${routeExt}/client/nukleus",
        "${unroute}/client/nukleus"
    })
    public void shouldUnrouteClientWithExt() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("topic", "sensor/one");

        long routeId = reaktor.controller(MqttController.class)
                .route(CLIENT, "mqtt#0", "target#0", gson.toJson(extension))
                .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(MqttController.class)
                .unroute(routeId)
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${freeze}/nukleus"
    })
    @ScriptProperty("nameF00N \"mqtt\"")
    public void shouldFreeze() throws Exception
    {
        k3po.start();

        reaktor.controller(MqttController.class)
               .freeze()
               .get();

        k3po.finish();
    }
}
