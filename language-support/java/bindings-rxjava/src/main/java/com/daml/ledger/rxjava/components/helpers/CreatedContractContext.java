// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components.helpers;

/**
 * A {@link com.daml.ledger.javaapi.data.WorkflowEvent} context.
 * Can be either a {@link TransactionContext} or a {@link GetActiveContractsResponseContext}
 */
public interface CreatedContractContext {

    String getWorkflowId();
}
