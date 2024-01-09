// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

/** A Ledger event regarding a workflow identified by the {@link WorkflowEvent#getWorkflowId()}. */
public interface WorkflowEvent {

  String getWorkflowId();
}
