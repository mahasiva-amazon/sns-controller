	// If the subscription is pending confirmation (HTTP/HTTPS protocol),
	// set Synced=False so the ACK reconciler requeues to detect when the
	// endpoint owner confirms. Without this, the controller would not
	// re-examine the subscription until the next full resync (~10 hours).
	res := &resource{ko}
	if ko.Status.PendingConfirmation != nil && *ko.Status.PendingConfirmation == "true" {
		msg := "Subscription is pending confirmation from the endpoint owner"
		ackcondition.SetSynced(res, corev1.ConditionFalse, &msg, nil)
	} else {
		ackcondition.SetSynced(res, corev1.ConditionTrue, nil, nil)
	}
