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

  // TODO #14537 this is just one bool now.  Is it likely to change?  If not, inline.
  public static final class Interface {
    public final boolean includeInterfaceView;

    public Interface(boolean includeInterfaceView) {
      this.includeInterfaceView = includeInterfaceView;
    }

    public TransactionFilterOuterClass.InterfaceFilter toProto(Identifier interfaceId) {
      return TransactionFilterOuterClass.InterfaceFilter.newBuilder()
          .setInterfaceId(interfaceId.toProto())
          .setIncludeInterfaceView(includeInterfaceView)
          .build();
    }

    static Interface fromProto(TransactionFilterOuterClass.InterfaceFilter proto) {
      return new Interface(proto.getIncludeInterfaceView());
    }

    Interface merge(Interface other) {
      return new Interface(includeInterfaceView || other.includeInterfaceView);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Interface that = (Interface) o;
      return includeInterfaceView == that.includeInterfaceView;
    }

    @Override
    public int hashCode() {
      return Objects.hash(includeInterfaceView);
    }

    @Override
    public String toString() {
      return "Interface{" + "includeInterfaceView=" + includeInterfaceView + '}';
    }
  }
}
