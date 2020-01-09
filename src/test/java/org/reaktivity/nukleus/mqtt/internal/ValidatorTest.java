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

import org.junit.Test;

public class ValidatorTest
{
    private final MqttValidator validator = new MqttValidator();

    @Test
    public void shouldValidateIsolatedMultiLevelWildcard()
    {
        final String topicFilter = "#";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateMultiLevelWildcardAtEnd()
    {
        final String topicFilter = "/#";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateIsolatedSingleLevelWildcard()
    {
        final String topicFilter = "+";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateSingleLevelWildcardBeforeTrailingLevelSeparator()
    {
        final String topicFilter = "+/";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateSingleLevelWildcardAfterLeadingLevelSeparator()
    {
        final String topicFilter = "/+";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateMultiLevelWildcardAfterSingleWildcards()
    {
        final String topicFilter = "+/+/#";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateIsolatedLevelSeparator()
    {
        final String topicFilter = "/";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateAdjacentLevelSeparator()
    {
        final String topicFilter = "//";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateMultipleTopicNamesFilter()
    {
        final String topicFilter = "topic/name";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateMultipleTopicNamesAfterLeadingLevelSeparator()
    {
        final String topicFilter = "/topic/name";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateMultipleTopicNamesBeforeTrailingLevelSeparator()
    {
        final String topicFilter = "topic/name/";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateMultipleTopicNamesBetweenLeadingAndTrailingLevelSeparators()
    {
        final String topicFilter = "/topic/name/";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateMultipleTopicNamesWithAdjacentLevelSeparators()
    {
        final String topicFilter = "topic//name";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateMultipleTopicNamesWithLeadingAndAdjacentLevelSeparators()
    {
        final String topicFilter = "/topic//name";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateMultipleTopicNamesWithTrailingAndAdjacentLevelSeparators()
    {
        final String topicFilter = "topic//name/";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateMultipleTopicNamesWithSingleLevelWildcard()
    {
        final String topicFilter = "topic/+/name/";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateMultipleTopicNamesWithMultiLevelWildcard()
    {
        final String topicFilter = "topic/name/+";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateMultipleTopicNamesWithSingleAndMultiLevelWildcard()
    {
        final String topicFilter = "topic/+/name/#";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldValidateMultipleTopicNamesWithSingleAndMultiLevelWildcardAndAdjacentLevelSeparators()
    {
        final String topicFilter = "topic/+//name/#";

        checkForValidTopicFilter(topicFilter);
    }

    @Test
    public void shouldNotValidateLeadingMultiLevelWildcard()
    {
        final String topicFilter = "#/";

        checkForInvalidTopicFilter(topicFilter);
    }

    @Test
    public void shouldNotValidateInteriorMultiLevelWildcard()
    {
        final String topicFilter = "topic/#/name";

        checkForInvalidTopicFilter(topicFilter);
    }

    @Test
    public void shouldNotValidateAdjacentMultiLevelWildcard()
    {
        final String topicFilter = "##";

        checkForInvalidTopicFilter(topicFilter);
    }

    @Test
    public void shouldNotValidateAdjacentMultiLevelWildcardWithTrailingLevelSeparator()
    {
        final String topicFilter = "##/";

        checkForInvalidTopicFilter(topicFilter);
    }

    @Test
    public void shouldNotValidateAdjacentMultiLevelWildcardWithLeadingLevelSeparator()
    {
        final String topicFilter = "/##";

        checkForInvalidTopicFilter(topicFilter);
    }

    @Test
    public void shouldNotValidateAdjacentMultiAndSingleLevelWildcards()
    {
        final String topicFilter = "#+";

        checkForInvalidTopicFilter(topicFilter);
    }

    @Test
    public void shouldNotValidateAdjacentSingleLevelWildcards()
    {
        final String topicFilter = "++";

        checkForInvalidTopicFilter(topicFilter);
    }

    @Test
    public void shouldNotValidateAdjacentSingleLevelWildcardsWithTrailingLevelSeparator()
    {
        final String topicFilter = "++/";

        checkForInvalidTopicFilter(topicFilter);
    }

    @Test
    public void shouldNotValidateAdjacentSingleLevelWildcardsWithLeadingLevelSeparator()
    {
        final String topicFilter = "/++";

        checkForInvalidTopicFilter(topicFilter);
    }

    @Test
    public void shouldNotValidateLeadingMultiLevelWildcardsWithMultipleTopicNames()
    {
        final String topicFilter = "#/topic/name";

        checkForInvalidTopicFilter(topicFilter);
    }

    @Test
    public void shouldNotValidateSingleLevelWildcardsCombinedWithTopicName()
    {
        final String topicFilter = "+topic/name";

        checkForInvalidTopicFilter(topicFilter);
    }

    @Test
    public void shouldNotValidateMultiLevelWildcardsCombinedWithTopicName()
    {
        final String topicFilter = "#topic/name";

        checkForInvalidTopicFilter(topicFilter);
    }

    @Test
    public void shouldNotValidateMultiLevelWildcardsCombinedWithTopicNameWithValidSingleLevelWildcard()
    {
        final String topicFilter = "#topic/+/name";

        checkForInvalidTopicFilter(topicFilter);
    }


    @Test
    public void shouldNotValidateSingleLevelWildcardsCombinedWithTopicNameWithValidMultiLevelWildcard()
    {
        final String topicFilter = "+topic/name/#";

        checkForInvalidTopicFilter(topicFilter);
    }

    private void checkForValidTopicFilter(
        String topicFilter)
    {
        assertTrue(String.format("\"%s\" was an invalid filter when it's supposed to be valid.", topicFilter),
            validator.isTopicFilterValid(topicFilter));
    }

    private void checkForInvalidTopicFilter(
        String topicFilter)
    {
        assertFalse(String.format("\"%s\" was a valid filter when it's supposed to be invalid.", topicFilter),
            validator.isTopicFilterValid(topicFilter));
    }
}
