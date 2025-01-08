// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandsOuterClass;

import java.util.Objects;

public final class PrefetchContractKey {
  public final Identifier templateId;
  public final Value contractKey;

  public PrefetchContractKey(Identifier templateId, Value contractKey) {
    this.templateId = templateId;
    this.contractKey = contractKey;
  }

  public CommandsOuterClass.PrefetchContractKey toProto() {
    return CommandsOuterClass.PrefetchContractKey.newBuilder()
        .setTemplateId(this.templateId.toProto())
        .setContractKey(this.contractKey.toProto())
        .build();
  }

  public static PrefetchContractKey fromProto(
      CommandsOuterClass.PrefetchContractKey prefetchContractKey) {
    Identifier templateId = Identifier.fromProto(prefetchContractKey.getTemplateId());
    Value contractKey = Value.fromProto(prefetchContractKey.getContractKey());
    return new PrefetchContractKey(templateId, contractKey);
  }

  @Override
  public String toString() {
    return "PrefetchContractKey{"
        + "templateId="
        + templateId
        + ", contractKey='"
        + contractKey
        + "'}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PrefetchContractKey that = (PrefetchContractKey) o;
    return Objects.equals(templateId, that.templateId)
        && Objects.equals(contractKey, that.contractKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(templateId, contractKey);
  }
}
