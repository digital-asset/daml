// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components.helpers;

/**
 * A {@link com.daml.ledger.javaapi.data.WorkflowEvent} context.
 * Can be either a {@link TransactionContext} or a {@link GetActiveContractsResponseContext}
 */
public interface CreatedContractContext {

    String getWorkflowId();
}
