// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components.helpers;

import com.daml.ledger.javaapi.data.Transaction;

import java.time.Instant;
import java.util.Objects;

/**
 * A {@link Transaction} without the {@link com.daml.ledger.javaapi.data.Event}s.
 */
public class TransactionContext implements CreatedContractContext {

    private final String transactionId;
    private final String commandId;
    private final String workflowId;
    private final Instant effectiveAt;
    private final String offset;

    public TransactionContext(String transactionId, String commandId, String workflowId, Instant effectiveAt, String offset) {
        this.transactionId = transactionId;
        this.commandId = commandId;
        this.workflowId = workflowId;
        this.effectiveAt = effectiveAt;
        this.offset = offset;
    }

    public static TransactionContext forTransaction(Transaction transaction) {
        String commandId = transaction.getCommandId();
        Instant effectiveAt = transaction.getEffectiveAt();
        String offset = transaction.getOffset();
        String transactionId = transaction.getTransactionId();
        String workflowId = transaction.getWorkflowId();
        return new TransactionContext(transactionId, commandId, workflowId, effectiveAt, offset);
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getCommandId() {
        return commandId;
    }

    @Override
    public String getWorkflowId() {
        return workflowId;
    }

    public Instant getEffectiveAt() {
        return effectiveAt;
    }

    public String getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "TransactionContext{" +
                "transactionId='" + transactionId + '\'' +
                ", commandId='" + commandId + '\'' +
                ", workflowId='" + workflowId + '\'' +
                ", effectiveAt=" + effectiveAt +
                ", offset='" + offset + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionContext that = (TransactionContext) o;
        return Objects.equals(transactionId, that.transactionId) &&
                Objects.equals(commandId, that.commandId) &&
                Objects.equals(workflowId, that.workflowId) &&
                Objects.equals(effectiveAt, that.effectiveAt) &&
                Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {

        return Objects.hash(transactionId, commandId, workflowId, effectiveAt, offset);
    }
}
