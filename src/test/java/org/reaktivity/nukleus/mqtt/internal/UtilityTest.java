package org.reaktivity.nukleus.mqtt.internal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.reaktivity.nukleus.mqtt.internal.MqttUtility.validTopicFilter;

import org.junit.Test;

public class UtilityTest
{
    @Test
    public void TestValidTopicFilterRegex()
    {
        final String[] validTopicFilters = new String[]{"#", "/#", "+/+/+/#", "+", "+/", "+/#", "//", "/", "topic/name",
            "/topic/name/", "/topic//name", "topic//name", "/topic/+/name", "/topic/name/#", "topic//+/name/+/#"};
        checkValidityOfTopicFilters(validTopicFilters, true);
    }

    @Test
    public void TestInvalidTopicFilterRegex()
    {
        final String[] invalidTopicFilters = new String[]{"#/", "##", "/##", "##/", "#+", "++", "++/", "/++", "#/topic/name",
            "#topic", "+topic", "/top+ic/name/", "/topic/#/name", "topic//name+", "/#topic/+/name", "/topic/na#me/#",
            "topic//++/name/+#/#", "topic//+/name/+/+#"};
        checkValidityOfTopicFilters(invalidTopicFilters, false);
    }

    private static void checkValidityOfTopicFilters(String[] filters, boolean checkForValid) {
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
