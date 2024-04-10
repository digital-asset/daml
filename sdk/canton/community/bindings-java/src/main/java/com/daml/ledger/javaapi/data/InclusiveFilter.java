// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TransactionFilterOuterClass;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class InclusiveFilter extends Filter {

  private Map<@NonNull Identifier, Filter.@NonNull Interface> interfaceFilters;
  private Map<@NonNull Identifier, Filter.@NonNull Template> templateFilters;

  public InclusiveFilter(
      @NonNull Map<@NonNull Identifier, Filter.@NonNull Interface> interfaceFilters,
      @NonNull Map<@NonNull Identifier, Filter.@NonNull Template> templateFilters) {
    this.interfaceFilters = interfaceFilters;
    this.templateFilters = templateFilters;
  }

  @NonNull
  public Map<@NonNull Identifier, Filter.@NonNull Interface> getInterfaceFilters() {
    return interfaceFilters;
  }

  @NonNull
  public Map<@NonNull Identifier, Filter.@NonNull Template> getTemplateFilters() {
    return templateFilters;
  }

  @SuppressWarnings("deprecation")
  @Override
  public TransactionFilterOuterClass.Filters toProto() {
    TransactionFilterOuterClass.InclusiveFilters inclusiveFilter =
        TransactionFilterOuterClass.InclusiveFilters.newBuilder()
            .addAllInterfaceFilters(
                interfaceFilters.entrySet().stream()
                    .map(idFilt -> idFilt.getValue().toProto(idFilt.getKey()))
                    .collect(Collectors.toUnmodifiableList()))
            .addAllTemplateFilters(
                templateFilters.entrySet().stream()
                    .map(
                        templateFilter ->
                            templateFilter.getValue().toProto(templateFilter.getKey()))
                    .collect(Collectors.toUnmodifiableList()))
            .build();
    return TransactionFilterOuterClass.Filters.newBuilder().setInclusive(inclusiveFilter).build();
  }

  @SuppressWarnings("deprecation")
  public static InclusiveFilter fromProto(
      TransactionFilterOuterClass.InclusiveFilters inclusiveFilters) {
    var interfaceIds =
        inclusiveFilters.getInterfaceFiltersList().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    ifFilt -> Identifier.fromProto(ifFilt.getInterfaceId()),
                    Filter.Interface::fromProto,
                    Filter.Interface::merge));
    var templateFilters =
        inclusiveFilters.getTemplateFiltersList().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    templateFilter -> Identifier.fromProto(templateFilter.getTemplateId()),
                    Filter.Template::fromProto,
                    Filter.Template::merge));
    return new InclusiveFilter(interfaceIds, templateFilters);
  }

  @Override
  public String toString() {
    return "InclusiveFilter{"
        + "interfaceFilters="
        + interfaceFilters
        + ", templateFilters="
        + templateFilters
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InclusiveFilter that = (InclusiveFilter) o;
    return Objects.equals(interfaceFilters, that.interfaceFilters)
        && Objects.equals(templateFilters, that.templateFilters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(interfaceFilters, templateFilters);
  }
}
