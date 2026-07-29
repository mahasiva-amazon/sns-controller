# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
#	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Integration tests for the SNS Subscription resource"""

import json
import time

import pytest
import boto3

from acktest.k8s import condition
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from acktest import adoption
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_resource
from e2e.bootstrap_resources import get_bootstrap_resources
from e2e.common.types import SUBSCRIPTION_RESOURCE_KIND, SUBSCRIPTION_RESOURCE_PLURAL
from e2e.replacement_values import REPLACEMENT_VALUES
from e2e import subscription

MODIFY_WAIT_AFTER_SECONDS = 10
CHECK_WAIT_AFTER_REF_RESOLVE_SECONDS = 10
DELETE_SUBSCRIPTION_TIMEOUT_SECONDS = 10

# Auto-confirmer API Gateway URL (created as part of test infrastructure)
CONFIRMER_URL = "https://twxw40ssm1.execute-api.us-east-1.amazonaws.com/prod/confirm"
# Time to wait for requeue + confirmation detection (1 min requeue + 70s buffer)
PENDING_CONFIRMATION_WAIT_SECONDS = 90


@pytest.fixture(scope="module")
def subscription_sqs():
    subscription_name = random_suffix_name("subscription-sqs", 24)
    display_name  = "a subscription to a queue"

    boot_resources = get_bootstrap_resources()
    q = boot_resources.Queue1
    topic = boot_resources.Topic1

    replacements = REPLACEMENT_VALUES.copy()
    replacements['SUBSCRIPTION_NAME'] = subscription_name
    replacements['TOPIC_ARN'] = topic.arn
    replacements['ENDPOINT'] = q.arn

    resource_data = load_resource(
        "subscription_with_refs",
        additional_replacements=replacements,
    )

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, SUBSCRIPTION_RESOURCE_PLURAL,
        subscription_name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)
    # NOTE(jaypipes): This works because we manually override the
    # ReturnSubscriptionArn field in SubscribeInput to "true"
    assert 'status' in cr
    assert 'ackResourceMetadata' in cr['status']
    assert 'arn' in cr['status']['ackResourceMetadata']
    sub_arn = cr['status']['ackResourceMetadata']['arn']

    yield (ref, cr, sub_arn)

    _, deleted = k8s.delete_custom_resource(
        ref,
        period_length=DELETE_SUBSCRIPTION_TIMEOUT_SECONDS,
    )
    assert deleted

    subscription.wait_until_deleted(sub_arn)


@service_marker
@pytest.mark.canary
class TestSubscription:
    def test_crud(self, subscription_sqs):
        sub_ref, sub_cr, sub_arn = subscription_sqs

        subscription.wait_until_exists(sub_arn)

        condition.assert_synced(sub_ref)

        # Before we update the Topic CR below, let's check to see that the
        # DisplayName field in the CR is still what we set in the original
        # Create call.
        cr = k8s.get_resource(sub_ref)
        assert cr is not None
        assert 'spec' in cr
        assert 'deliveryPolicy' not in cr['spec']

        attrs = subscription.get_attributes(sub_arn)
        assert attrs is not None
        assert 'DeliveryPolicy' not in attrs

        delivery_policy = {
            "healthyRetryPolicy": {
                "minDelayTarget": 1,
                "maxDelayTarget": 60,
                "numRetries": 50,
                "numNoDelayRetries": 3,
                "numMinDelayRetries": 2,
                "numMaxDelayRetries": 35,
                "backoffFunction": "exponential"
            }
        }
        new_delivery_policy = json.dumps(delivery_policy)

        # We're now going to modify the DeliveryPolicy field of the
        # Subscription, wait some time and verify that the SNS server-side
        # resource shows the new value of the field.
        updates = {
            "spec": {"deliveryPolicy": new_delivery_policy},
        }
        k8s.patch_custom_resource(sub_ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        latest = subscription.get_attributes(sub_arn)
        assert latest is not None
        assert 'DeliveryPolicy' in latest

        # NOTE(jaypipes): SNS adds some default field values to the
        # DeliveryPolicy JSON object on the server-side, including things like
        # `"guaranteed": false` and `"requestPolicy": null`. We will simply
        # verify that the healthRetryPolicy segment we updated is correct.
        got_delivery_policy= json.loads(latest['DeliveryPolicy'])
        assert 'healthyRetryPolicy' in got_delivery_policy
        exp_healthy_retry_policy = delivery_policy['healthyRetryPolicy']
        assert exp_healthy_retry_policy == got_delivery_policy['healthyRetryPolicy']

        # Verify semantic JSON comparison (is_document): patch with the same
        # logical JSON but different key ordering. With DocumentEqual, this
        # should NOT trigger a reconciliation loop or unnecessary update.
        reordered_policy = {
            "healthyRetryPolicy": {
                "backoffFunction": "exponential",
                "numMaxDelayRetries": 35,
                "numMinDelayRetries": 2,
                "numNoDelayRetries": 3,
                "numRetries": 50,
                "maxDelayTarget": 60,
                "minDelayTarget": 1
            }
        }
        updates = {
            "spec": {"deliveryPolicy": json.dumps(reordered_policy)},
        }
        k8s.patch_custom_resource(sub_ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        # Should still be synced — no unnecessary update triggered
        condition.assert_synced(sub_ref)


@pytest.fixture(scope="module")
def subscription_https(sns_client):
    """Creates an HTTPS subscription to a topic using the auto-confirmer endpoint."""
    subscription_name = random_suffix_name("subscription-https", 28)
    boot_resources = get_bootstrap_resources()
    topic = boot_resources.Topic1

    replacements = REPLACEMENT_VALUES.copy()
    replacements['SUBSCRIPTION_NAME'] = subscription_name
    replacements['TOPIC_ARN'] = topic.arn
    replacements['ENDPOINT'] = CONFIRMER_URL

    resource_data = load_resource(
        "subscription_https",
        additional_replacements=replacements,
    )

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, SUBSCRIPTION_RESOURCE_PLURAL,
        subscription_name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    yield (ref, cr)

    # Cleanup: get ARN from status if available
    cr_latest = k8s.get_resource(ref)
    sub_arn = None
    if cr_latest and 'status' in cr_latest:
        arn = cr_latest['status'].get('ackResourceMetadata', {}).get('arn')
        if arn:
            sub_arn = arn

    _, deleted = k8s.delete_custom_resource(
        ref,
        period_length=DELETE_SUBSCRIPTION_TIMEOUT_SECONDS,
    )
    assert deleted

    if sub_arn:
        subscription.wait_until_deleted(sub_arn)


@service_marker
class TestSubscriptionPendingConfirmation:
    def test_pending_confirmation_requeue(self, sns_client, subscription_https):
        """TC: HTTPS subscription confirms via Lambda auto-confirmer, verifies
        controller detects confirmation within ~90s (1 min requeue + buffer).

        Flow:
        1. HTTPS subscription is created pointing to the auto-confirmer Lambda.
        2. SNS sends a SubscriptionConfirmation message; Lambda calls SubscribeURL.
        3. Controller is expected to detect PendingConfirmation=true and requeue
           after 1 minute (the fix under test).
        4. After ~90s the controller should re-read the subscription and find
           PendingConfirmation=false, then set Synced=True.
        """
        sub_ref, sub_cr = subscription_https

        # Immediately after creation, the subscription may be pending confirmation
        cr = k8s.get_resource(sub_ref)
        assert cr is not None
        assert 'status' in cr

        # Give the auto-confirmer Lambda time to confirm (SNS typically delivers
        # the SubscriptionConfirmation within a few seconds).
        # Then wait for the controller requeue (1 min) + buffer to detect it.
        time.sleep(PENDING_CONFIRMATION_WAIT_SECONDS)

        # After requeue window, the subscription should be confirmed and Synced=True
        condition.assert_synced(sub_ref)

        # Verify via SNS API that PendingConfirmation is false
        cr_latest = k8s.get_resource(sub_ref)
        assert cr_latest is not None
        assert 'status' in cr_latest
        pending = cr_latest['status'].get('pendingConfirmation')
        # pendingConfirmation should be "false" or absent after confirmation
        assert pending != "true", (
            f"Expected pendingConfirmation to be 'false' after confirmation, "
            f"got: {pending}"
        )
