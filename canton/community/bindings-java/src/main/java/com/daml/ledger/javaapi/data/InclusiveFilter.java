// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TransactionFilterOuterClass;
import com.daml.ledger.api.v1.ValueOuterClass;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class InclusiveFilter extends Filter {

  private Set<Identifier> templateIds;
  private Map<@NonNull Identifier, Filter.@NonNull Interface> interfaceFilters;
  private Map<@NonNull Identifier, Filter.@NonNull Template> templateFilters;

  private InclusiveFilter(
      @NonNull Set<@NonNull Identifier> templateIds,
      @NonNull Map<@NonNull Identifier, Filter.@NonNull Interface> interfaceFilters,
      @NonNull Map<@NonNull Identifier, Filter.@NonNull Template> templateFilters) {
    this.templateIds = templateIds;
    this.interfaceFilters = interfaceFilters;
    this.templateFilters = templateFilters;
  }

  public InclusiveFilter(
      @NonNull Map<@NonNull Identifier, Filter.@NonNull Interface> interfaceFilters,
      @NonNull Map<@NonNull Identifier, Filter.@NonNull Template> templateFilters) {
    this(Collections.emptySet(), interfaceFilters, templateFilters);
  }

  public static InclusiveFilter ofTemplateIds(@NonNull Set<@NonNull Identifier> templateIds) {
    return new InclusiveFilter(
        Collections.emptyMap(),
        templateIds.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(), tId -> Template.HIDE_CREATED_EVENT_BLOB)));
  }

  @NonNull
  public Set<@NonNull Identifier> getTemplateIds() {
    return templateIds;
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
    ArrayList<ValueOuterClass.Identifier> templateIds = new ArrayList<>(this.templateIds.size());
    for (Identifier identifier : this.templateIds) {
      templateIds.add(identifier.toProto());
    }
    TransactionFilterOuterClass.InclusiveFilters inclusiveFilter =
        TransactionFilterOuterClass.InclusiveFilters.newBuilder()
            .addAllTemplateIds(templateIds)
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
    HashSet<Identifier> templateIds = new HashSet<>(inclusiveFilters.getTemplateIdsCount());
    for (ValueOuterClass.Identifier templateId : inclusiveFilters.getTemplateIdsList()) {
      templateIds.add(Identifier.fromProto(templateId));
    }
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
    return new InclusiveFilter(templateIds, interfaceIds, templateFilters);
  }

  @Override
  public String toString() {
    return "InclusiveFilter{"
        + "templateIds="
        + templateIds
        + ", interfaceFilters="
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
    return Objects.equals(templateIds, that.templateIds)
        && Objects.equals(interfaceFilters, that.interfaceFilters)
        && Objects.equals(templateFilters, that.templateFilters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(templateIds, interfaceFilters, templateFilters);
  }
}
