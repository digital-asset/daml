// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

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
    this.companions =
        // Each companion should be keyed by TEMPLATE_ID and by TEMPLATE_ID_WITH_PACKAGE_ID,
        StreamSupport.stream(companions.spliterator(), false)
            .flatMap(
                c ->
                    Stream.of(
                        Map.entry(c.TEMPLATE_ID, c), Map.entry(c.TEMPLATE_ID_WITH_PACKAGE_ID, c)))
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (v, _v) -> v));
  }

  public Contract<?, ?> fromCreatedEvent(CreatedEvent event) throws IllegalArgumentException {
    Identifier templateId = event.getTemplateId();
    var companion = getContractCompanion(templateId);
    // If we do not recognise the template, and the event contains a package name, try looking up
    // the template id with a package name, as the ledger may know about newer versions of this
    // template, from packages that came after when this codegen was run, which we may still be
    // able to decode with the current decoder.
    if (!companion.isPresent()) {
      companion =
          event
              .getPackageName()
              .flatMap(pkgName -> getContractCompanion(withPackageName(templateId, pkgName)));
    }
    if (!companion.isPresent()) {
      throw new IllegalArgumentException("No template found for identifier " + templateId);
    }
    return companion.get().fromCreatedEvent(event);
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

  private Identifier withPackageName(Identifier templateId, String packageName) {
    return new Identifier(packageName, templateId.getModuleName(), templateId.getEntityName());
  }
}
