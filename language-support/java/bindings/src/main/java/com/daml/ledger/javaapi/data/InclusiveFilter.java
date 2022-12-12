// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TransactionFilterOuterClass;
import com.daml.ledger.api.v1.ValueOuterClass;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class InclusiveFilter extends Filter {

  private Set<Identifier> templateIds;
  private Map<@NonNull Identifier, Filter.@NonNull Interface> interfaceIds;

  /**
   * @deprecated Use {@link #ofTemplateIds} instead; {@code templateIds} must not include interface
   *     IDs. Since Daml 2.4.0
   */
  @Deprecated
  public InclusiveFilter(@NonNull Set<@NonNull Identifier> templateIds) {
    this(templateIds, Collections.emptyMap());
  }

  public InclusiveFilter(
      @NonNull Set<@NonNull Identifier> templateIds,
      @NonNull Map<@NonNull Identifier, Filter.@NonNull Interface> interfaceIds) {
    this.templateIds = templateIds;
    this.interfaceIds = interfaceIds;
  }

  public static InclusiveFilter ofTemplateIds(@NonNull Set<@NonNull Identifier> templateIds) {
    return new InclusiveFilter(templateIds, Collections.emptyMap());
  }

  @NonNull
  public Set<@NonNull Identifier> getTemplateIds() {
    return templateIds;
  }

  @NonNull
  public Map<@NonNull Identifier, Filter.@NonNull Interface> getInterfaceIds() {
    return interfaceIds;
  }

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
                interfaceIds.entrySet().stream()
                    .map(idFilt -> idFilt.getValue().toProto(idFilt.getKey()))
                    .collect(Collectors.toUnmodifiableList()))
            .build();
    return TransactionFilterOuterClass.Filters.newBuilder().setInclusive(inclusiveFilter).build();
  }

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
    return new InclusiveFilter(templateIds, interfaceIds);
  }

  @Override
  public String toString() {
    return "InclusiveFilter{"
        + "templateIds="
        + templateIds
        + ", interfaceIds="
        + interfaceIds
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InclusiveFilter that = (InclusiveFilter) o;
    return Objects.equals(templateIds, that.templateIds)
        && Objects.equals(interfaceIds, that.interfaceIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(templateIds, interfaceIds);
  }
}
