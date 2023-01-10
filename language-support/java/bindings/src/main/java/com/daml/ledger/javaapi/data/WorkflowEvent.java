// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

/** A Ledger event regarding a workflow identified by the {@link WorkflowEvent#getWorkflowId()}. */
public interface WorkflowEvent {

  String getWorkflowId();
}
