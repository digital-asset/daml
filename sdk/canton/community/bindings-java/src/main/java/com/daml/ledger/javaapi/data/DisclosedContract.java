// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandsOuterClass;
import com.google.protobuf.ByteString;

import java.util.Objects;
import java.util.Optional;

public final class DisclosedContract {
  public final Identifier templateId;
  public final String contractId;
  public final ByteString createdEventBlob;
  public final Optional<String> domainId;

  /**
   * Constructor that does not require providing the domain id
   *
   * @deprecated since 3.2.0. It will be removed in a future release
   */
  @Deprecated
  public DisclosedContract(Identifier templateId, String contractId, ByteString createdEventBlob) {
    this.templateId = templateId;
    this.contractId = contractId;
    this.createdEventBlob = createdEventBlob;
    this.domainId = Optional.empty();
  }

  public DisclosedContract(
      Identifier templateId, String contractId, ByteString createdEventBlob, String domainId) {
    this.templateId = templateId;
    this.contractId = contractId;
    this.createdEventBlob = createdEventBlob;
    this.domainId = Optional.of(domainId);
  }

  public CommandsOuterClass.DisclosedContract toProto() {
    CommandsOuterClass.DisclosedContract.Builder builder =
        CommandsOuterClass.DisclosedContract.newBuilder()
            .setTemplateId(this.templateId.toProto())
            .setContractId(this.contractId)
            .setCreatedEventBlob(this.createdEventBlob);
    domainId.ifPresent(builder::setDomainId);
    return builder.build();
  }

  public static DisclosedContract fromProto(
      CommandsOuterClass.DisclosedContract disclosedContract) {
    Identifier templateId = Identifier.fromProto(disclosedContract.getTemplateId());
    String contractId = disclosedContract.getContractId();
    ByteString createdEventBlob = disclosedContract.getCreatedEventBlob();

    return Optional.of(disclosedContract.getDomainId())
        .filter(domainIdO -> !domainIdO.isEmpty())
        .map(domainId -> new DisclosedContract(templateId, contractId, createdEventBlob, domainId))
        .orElseGet(() -> new DisclosedContract(templateId, contractId, createdEventBlob));
  }

  @Override
  public String toString() {
    return "DisclosedContract{"
        + "templateId="
        + templateId
        + ", contractId='"
        + contractId
        + '\''
        + ", createdEventBlob='"
        + createdEventBlob
        + '\''
        + ", domainId="
        + domainId
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DisclosedContract that = (DisclosedContract) o;
    return Objects.equals(templateId, that.templateId)
        && Objects.equals(contractId, that.contractId)
        && Objects.equals(createdEventBlob, that.createdEventBlob)
        && Objects.equals(domainId, that.domainId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(templateId, contractId, createdEventBlob);
  }
}
