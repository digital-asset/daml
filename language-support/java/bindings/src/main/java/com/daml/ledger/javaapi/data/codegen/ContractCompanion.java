// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Identifier;
import com.daml.ledger.javaapi.data.Value;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public abstract class ContractCompanion<Ct, Id, Data> {
  public abstract Identifier getTemplateId();

  protected abstract Id newContractId(String contractId);

  protected abstract Data fromValue(DamlRecord record$);

  @Deprecated
  public abstract Ct fromIdAndRecord(String contractId, DamlRecord record$);

  public abstract Ct fromCreatedEvent(CreatedEvent event);

  public abstract static class WithoutKey<Ct, Id, Data> extends ContractCompanion<Ct, Id, Data> {
    protected abstract Ct newContract(
        Id id,
        Data data,
        Optional<String> agreementText,
        Set<String> signatories,
        Set<String> observers);

    public final Ct fromIdAndRecord(
        String contractId,
        DamlRecord record$,
        Optional<String> agreementText,
        Set<String> signatories,
        Set<String> observers) {
      Id id = newContractId(contractId);
      Data data = fromValue(record$);
      return newContract(id, data, agreementText, signatories, observers);
    }

    @Deprecated
    @Override
    public final Ct fromIdAndRecord(String contractId, DamlRecord record$) {
      return fromIdAndRecord(
          contractId, record$, Optional.empty(), Collections.emptySet(), Collections.emptySet());
    }

    @Override
    public final Ct fromCreatedEvent(CreatedEvent event) {
      return fromIdAndRecord(
          event.getContractId(),
          event.getArguments(),
          event.getAgreementText(),
          event.getSignatories(),
          event.getObservers());
    }
  }

  public abstract static class WithKey<Ct, Id, Data, Key> extends ContractCompanion<Ct, Id, Data> {
    protected abstract Ct newContract(
        Id id,
        Data data,
        Optional<String> agreementText,
        Optional<Key> key,
        Set<String> signatories,
        Set<String> observers);

    protected abstract Key keyFromValue(Value keyValue);

    public final Ct fromIdAndRecord(
        String contractId,
        DamlRecord record$,
        Optional<String> agreementText,
        Optional<Key> key,
        Set<String> signatories,
        Set<String> observers) {
      Id id = newContractId(contractId);
      Data data = fromValue(record$);
      return newContract(id, data, agreementText, key, signatories, observers);
    }

    @Deprecated
    @Override
    public final Ct fromIdAndRecord(String contractId, DamlRecord record$) {
      return fromIdAndRecord(
          contractId,
          record$,
          Optional.empty(),
          Optional.empty(),
          Collections.emptySet(),
          Collections.emptySet());
    }

    @Override
    public final Ct fromCreatedEvent(CreatedEvent event) {
      return fromIdAndRecord(
          event.getContractId(),
          event.getArguments(),
          event.getAgreementText(),
          event.getContractKey().map(this::keyFromValue),
          event.getSignatories(),
          event.getObservers());
    }
  }
}
