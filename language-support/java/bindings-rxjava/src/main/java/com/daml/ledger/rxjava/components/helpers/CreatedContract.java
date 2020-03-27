// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components.helpers;

import com.daml.ledger.javaapi.data.Identifier;
import com.daml.ledger.javaapi.data.Record;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public class CreatedContract {

    private final Identifier templateId;
    private final Record createArguments;
    private final CreatedContractContext context;

    public CreatedContract(@NonNull Identifier templateId, @NonNull Record createArguments, @NonNull CreatedContractContext context) {
        this.templateId = templateId;
        this.createArguments = createArguments;
        this.context = context;
    }

    @NonNull
    public Identifier getTemplateId() {
        return templateId;
    }

    @NonNull
    public Record getCreateArguments() {
        return createArguments;
    }

    @NonNull
    public CreatedContractContext getContext() {
        return context;
    }

    @Override
    public String toString() {
        return "CreatedContract{" +
                "templateId=" + templateId +
                ", createArguments=" + createArguments +
                ", context=" + context +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreatedContract that = (CreatedContract) o;
        return Objects.equals(templateId, that.templateId) &&
                Objects.equals(createArguments, that.createArguments) &&
                Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {

        return Objects.hash(templateId, createArguments, context);
    }
}
