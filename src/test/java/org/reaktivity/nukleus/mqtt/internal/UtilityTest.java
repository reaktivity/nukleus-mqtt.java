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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.reaktivity.nukleus.mqtt.internal.MqttUtility.validTopicFilter;

import org.junit.Test;

public class UtilityTest
{
    @Test
    public void testValidTopicFilterRegex()
    {
        final String[] validTopicFilters = new String[]{"#", "/#", "+/+/+/#", "+", "+/", "+/#", "//", "/", "topic/name",
            "/topic/name/", "/topic//name", "topic//name", "/topic/+/name", "/topic/name/#", "topic//+/name/+/#"};
        checkValidityOfTopicFilters(validTopicFilters, true);
    }

    @Test
    public void testInvalidTopicFilterRegex()
    {
        final String[] invalidTopicFilters = new String[]{"#/", "##", "/##", "##/", "#+", "++", "++/", "/++", "#/topic/name",
            "#topic", "+topic", "/top+ic/name/", "/topic/#/name", "topic//name+", "/#topic/+/name", "/topic/na#me/#",
            "topic//++/name/+#/#", "topic//+/name/+/+#"};
        checkValidityOfTopicFilters(invalidTopicFilters, false);
    }

    private static void checkValidityOfTopicFilters(String[] filters, boolean checkForValid)
    {
        if (checkForValid)
        {
            for (String s : filters)
            {
                assertTrue(String.format("\"%s\" was an invalid filter when it's supposed to be valid.", s), validTopicFilter(s));
            }
        }
        else
        {
            for (String s : filters)
            {
                assertFalse(String.format("\"%s\" was a valid filter when it's supposed to be invalid.", s), validTopicFilter(s));
            }
        }
    }
}
