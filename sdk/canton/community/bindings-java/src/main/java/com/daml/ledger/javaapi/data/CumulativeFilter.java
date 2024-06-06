// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TransactionFilterOuterClass;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.checkerframework.checker.nullness.qual.NonNull;

public final class CumulativeFilter extends Filter {

  private Map<@NonNull Identifier, Filter.@NonNull Interface> interfaceFilters;
  private Map<@NonNull Identifier, Filter.@NonNull Template> templateFilters;
  private Optional<Filter.@NonNull Wildcard> wildcardFilter;

  public CumulativeFilter(
      @NonNull Map<@NonNull Identifier, Filter.@NonNull Interface> interfaceFilters,
      @NonNull Map<@NonNull Identifier, Filter.@NonNull Template> templateFilters,
      Optional<Filter.@NonNull Wildcard> wildcardFilter) {
    this.interfaceFilters = interfaceFilters;
    this.templateFilters = templateFilters;
    this.wildcardFilter = wildcardFilter;
  }

  @NonNull
  public Map<@NonNull Identifier, Filter.@NonNull Interface> getInterfaceFilters() {
    return interfaceFilters;
  }

  @NonNull
  public Map<@NonNull Identifier, Filter.@NonNull Template> getTemplateFilters() {
    return templateFilters;
  }

  @NonNull
  public Optional<Filter.@NonNull Wildcard> getWildcardFilter() {
    return wildcardFilter;
  }

  @SuppressWarnings("deprecation")
  @Override
  public TransactionFilterOuterClass.Filters toProto() {
    Iterable<TransactionFilterOuterClass.InterfaceFilter> ifaces =
        interfaceFilters.entrySet().stream()
            .map(idFilt -> idFilt.getValue().toProto(idFilt.getKey()))
            .collect(Collectors.toUnmodifiableList());

    Iterable<TransactionFilterOuterClass.TemplateFilter> templates =
        templateFilters.entrySet().stream()
            .map(templateFilter -> templateFilter.getValue().toProto(templateFilter.getKey()))
            .collect(Collectors.toUnmodifiableList());

    Iterable<TransactionFilterOuterClass.WildcardFilter> wildcard =
        wildcardFilter
            .map(w -> Collections.singletonList(w.toProto()))
            .orElse(Collections.emptyList());

    Stream<TransactionFilterOuterClass.CumulativeFilter> cumulativeIfaces =
        StreamSupport.stream(ifaces.spliterator(), false)
            .map(
                ifaceF ->
                    TransactionFilterOuterClass.CumulativeFilter.newBuilder()
                        .setInterfaceFilter(ifaceF)
                        .build());

    Stream<TransactionFilterOuterClass.CumulativeFilter> cumulativeTemplates =
        StreamSupport.stream(templates.spliterator(), false)
            .map(
                tempF ->
                    TransactionFilterOuterClass.CumulativeFilter.newBuilder()
                        .setTemplateFilter(tempF)
                        .build());

    Stream<TransactionFilterOuterClass.CumulativeFilter> cumulativeWildcard =
        StreamSupport.stream(wildcard.spliterator(), false)
            .map(
                wildF ->
                    TransactionFilterOuterClass.CumulativeFilter.newBuilder()
                        .setWildcardFilter(wildF)
                        .build());

    Iterable<TransactionFilterOuterClass.CumulativeFilter> cumulativeFilters =
        Stream.concat(Stream.concat(cumulativeIfaces, cumulativeTemplates), cumulativeWildcard)
            .collect(Collectors.toUnmodifiableList());

    return TransactionFilterOuterClass.Filters.newBuilder()
        .addAllCumulative(cumulativeFilters)
        .build();
  }

  @SuppressWarnings("deprecation")
  public static CumulativeFilter fromProto(
      Iterable<TransactionFilterOuterClass.CumulativeFilter> cumulativeFilters) {

    Stream<TransactionFilterOuterClass.InterfaceFilter> intrefaceStream =
        StreamSupport.stream(cumulativeFilters.spliterator(), false)
            .filter(f -> f.hasInterfaceFilter())
            .map(f -> f.getInterfaceFilter());
    var interfaceIds =
        intrefaceStream.collect(
            Collectors.toUnmodifiableMap(
                ifFilt -> Identifier.fromProto(ifFilt.getInterfaceId()),
                Filter.Interface::fromProto,
                Filter.Interface::merge));

    Stream<TransactionFilterOuterClass.TemplateFilter> templateStream =
        StreamSupport.stream(cumulativeFilters.spliterator(), false)
            .filter(f -> f.hasTemplateFilter())
            .map(f -> f.getTemplateFilter());
    var templateFilters =
        templateStream.collect(
            Collectors.toUnmodifiableMap(
                templateFilter -> Identifier.fromProto(templateFilter.getTemplateId()),
                Filter.Template::fromProto,
                Filter.Template::merge));

    var wildcardFilter =
        StreamSupport.stream(cumulativeFilters.spliterator(), false)
            .filter(f -> f.hasWildcardFilter())
            .map(f -> f.getWildcardFilter())
            .map(Filter.Wildcard::fromProto)
            .reduce(Filter.Wildcard::merge);

    return new CumulativeFilter(interfaceIds, templateFilters, wildcardFilter);
  }

  @Override
  public String toString() {
    return "CumulativeFilter{"
        + "interfaceFilters="
        + interfaceFilters
        + ", templateFilters="
        + templateFilters
        + ", wildcardFilter="
        + wildcardFilter
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CumulativeFilter that = (CumulativeFilter) o;
    return Objects.equals(interfaceFilters, that.interfaceFilters)
        && Objects.equals(templateFilters, that.templateFilters)
        && Objects.equals(wildcardFilter, that.wildcardFilter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(interfaceFilters, templateFilters, wildcardFilter);
  }
}
