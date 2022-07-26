// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TransactionFilterOuterClass;
import java.util.Objects;

public abstract class Filter {

  public static Filter fromProto(TransactionFilterOuterClass.Filters filters) {
    if (filters.hasInclusive()) {
      return InclusiveFilter.fromProto(filters.getInclusive());
    } else {
      return NoFilter.instance;
    }
  }

  public abstract TransactionFilterOuterClass.Filters toProto();

  public static final class Interface {
    public final boolean includeInterfaceViews;

    public Interface(boolean includeInterfaceViews) {
      this.includeInterfaceViews = includeInterfaceViews;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Interface that = (Interface) o;
      return includeInterfaceViews == that.includeInterfaceViews;
    }

    @Override
    public int hashCode() {
      return Objects.hash(includeInterfaceViews);
    }

    @Override
    public String toString() {
      return "Interface{" + "includeInterfaceViews=" + includeInterfaceViews + '}';
    }
  }
}
