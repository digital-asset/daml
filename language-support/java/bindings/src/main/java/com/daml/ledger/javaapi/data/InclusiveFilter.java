// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.TransactionFilterOuterClass;
import com.digitalasset.ledger.api.v1.ValueOuterClass;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class InclusiveFilter extends Filter {

    private Set<Identifier> templateIds;

    public InclusiveFilter(Set<Identifier> templateIds) {
        this.templateIds = templateIds;
    }

    public Set<Identifier> getTemplateIds() {
        return templateIds;
    }

    @Override
    public TransactionFilterOuterClass.Filters toProto() {
        ArrayList<ValueOuterClass.Identifier> templateIds = new ArrayList<>(this.templateIds.size());
        for (Identifier identifier : this.templateIds) {
            templateIds.add(identifier.toProto());
        }
        TransactionFilterOuterClass.InclusiveFilters inclusiveFilter =
                TransactionFilterOuterClass.InclusiveFilters.newBuilder()
                    .addAllTemplateIds(templateIds).build();
        return TransactionFilterOuterClass.Filters.newBuilder()
                .setInclusive(inclusiveFilter).build();
    }

    public static InclusiveFilter fromProto(TransactionFilterOuterClass.InclusiveFilters inclusiveFilters) {
        HashSet<Identifier> templateIds = new HashSet<>(inclusiveFilters.getTemplateIdsCount());
        for (ValueOuterClass.Identifier templateId : inclusiveFilters.getTemplateIdsList()) {
            templateIds.add(Identifier.fromProto(templateId));
        }
        return new InclusiveFilter(templateIds);
    }

    @Override
    public String toString() {
        return "InclusiveFilter{" +
                "templateIds=" + templateIds +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InclusiveFilter that = (InclusiveFilter) o;
        return Objects.equals(templateIds, that.templateIds);
    }

    @Override
    public int hashCode() {

        return Objects.hash(templateIds);
    }
}
