// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.Identifier;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ContractDecoder {
  private final Map<
          Identifier,
          ? extends ContractCompanion<? extends Contract<?, ?>, ?, ? extends DamlRecord<?>>>
      companions;

  public ContractDecoder(
      Iterable<? extends ContractCompanion<? extends Contract<?, ?>, ?, ? extends DamlRecord<?>>>
          companions) {
    this.companions =
        StreamSupport.stream(companions.spliterator(), false)
            .collect(Collectors.toMap(c -> c.TEMPLATE_ID, Function.identity()));
  }

  public Contract<?, ?> fromCreatedEvent(CreatedEvent event) throws IllegalArgumentException {
    Identifier templateId = event.getTemplateId();
    ContractCompanion<? extends Contract<?, ?>, ?, ?> companion =
        getContractCompanion(templateId)
            .orElseThrow(
                () ->
                    new IllegalArgumentException("No template found for identifier " + templateId));
    return companion.fromCreatedEvent(event);
  }

  public Optional<? extends ContractCompanion<? extends Contract<?, ?>, ?, ? extends DamlRecord<?>>>
      getContractCompanion(Identifier templateId) {
    return Optional.ofNullable(companions.get(templateId));
  }

  public Optional<Function<CreatedEvent, com.daml.ledger.javaapi.data.Contract>> getDecoder(
      Identifier templateId) {
    return getContractCompanion(templateId).map(companion -> companion::fromCreatedEvent);
  }

  public Optional<ContractCompanion.FromJson<? extends DamlRecord<?>>> getJsonDecoder(
      Identifier templateId) {
    return getContractCompanion(templateId).map(companion -> companion::fromJson);
  }
}
