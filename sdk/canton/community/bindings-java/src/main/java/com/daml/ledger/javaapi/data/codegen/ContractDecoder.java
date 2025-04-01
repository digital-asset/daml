// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.Identifier;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ContractDecoder {
  private final Map<
          Identifier,
          ? extends ContractCompanion<? extends Contract<?, ?>, ?, ? extends DamlRecord<?>>>
      companions;

  public ContractDecoder(
      Iterable<? extends ContractCompanion<? extends Contract<?, ?>, ?, ? extends DamlRecord<?>>>
          companions) {
    // Each companion should be keyed by a template id with both package id and package name.
    this.companions =
        StreamSupport.stream(companions.spliterator(), false)
            .flatMap(
                c ->
                    Stream.of(
                        Map.entry(c.TEMPLATE_ID, c),
                        Map.entry(c.TEMPLATE_ID_WITH_PACKAGE_ID.withoutPackageName(), c)))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (v1, v2) -> {
                      throw new IllegalArgumentException(
                          "Clashing templates with the same key "
                              + v1.TEMPLATE_ID_WITH_PACKAGE_ID
                              + " and "
                              + v2.TEMPLATE_ID_WITH_PACKAGE_ID);
                    }));
  }

  public Contract<?, ?> fromCreatedEvent(CreatedEvent event) throws IllegalArgumentException {
    Identifier templateId = event.getTemplateId();
    return getContractCompanion(templateId)
        .orElseThrow(
            () -> new IllegalArgumentException("No template found for identifier " + templateId))
        .fromCreatedEvent(event);
  }

  public Optional<? extends ContractCompanion<? extends Contract<?, ?>, ?, ? extends DamlRecord<?>>>
      getContractCompanion(Identifier templateId) {
    return Optional.ofNullable(
        companions.containsKey(templateId.withoutPackageId())
            ? companions.get(templateId.withoutPackageId())
            : companions.get(templateId.withoutPackageName()));
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
