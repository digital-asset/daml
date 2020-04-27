// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components.helpers;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

/**
 * A {@link com.daml.ledger.javaapi.data.GetActiveContractsResponse}
 */
public class GetActiveContractsResponseContext implements CreatedContractContext {

    private final String workflowId;

    public GetActiveContractsResponseContext(@NonNull String workflowId) {
        this.workflowId = workflowId;
    }

    @NonNull
    @Override
    public String getWorkflowId() {
        return this.workflowId;
    }

    @Override
    public String toString() {
        return "GetActiveContractsResponseContext{" +
                "workflowId='" + workflowId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetActiveContractsResponseContext that = (GetActiveContractsResponseContext) o;
        return Objects.equals(workflowId, that.workflowId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(workflowId);
    }
}
