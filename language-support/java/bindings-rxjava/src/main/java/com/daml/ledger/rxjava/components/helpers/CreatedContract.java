// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components.helpers;

import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Identifier;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public class CreatedContract {

  private final Identifier templateId;
  private final DamlRecord createArguments;
  private final CreatedContractContext context;

  public CreatedContract(
      @NonNull Identifier templateId,
      @NonNull DamlRecord createArguments,
      @NonNull CreatedContractContext context) {
    this.templateId = templateId;
    this.createArguments = createArguments;
    this.context = context;
  }

  @NonNull
  public Identifier getTemplateId() {
    return templateId;
  }

  @NonNull
  public DamlRecord getCreateArguments() {
    return createArguments;
  }

  @NonNull
  public CreatedContractContext getContext() {
    return context;
  }

  @Override
  public String toString() {
    return "CreatedContract{"
        + "templateId="
        + templateId
        + ", createArguments="
        + createArguments
        + ", context="
        + context
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreatedContract that = (CreatedContract) o;
    return Objects.equals(templateId, that.templateId)
        && Objects.equals(createArguments, that.createArguments)
        && Objects.equals(context, that.context);
  }

  @Override
  public int hashCode() {

    return Objects.hash(templateId, createArguments, context);
  }
}
