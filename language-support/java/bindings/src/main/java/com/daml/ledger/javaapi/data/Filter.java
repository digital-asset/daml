// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TransactionFilterOuterClass;

public abstract class Filter {

  public static Filter fromProto(TransactionFilterOuterClass.Filters filters) {
    if (filters.hasInclusive()) {
      return InclusiveFilter.fromProto(filters.getInclusive());
    } else {
      return NoFilter.instance;
    }
  }

  public abstract TransactionFilterOuterClass.Filters toProto();

  /**
   * Settings for including an interface in {@link InclusiveFilter}. There are two values: {@link
   * #INCLUDE_VIEW} and {@link #HIDE_VIEW}.
   */
  public static final class Interface {
    public final boolean includeInterfaceView;
    // add equals and hashCode if adding more fields

    public static final Interface INCLUDE_VIEW = new Interface(true);
    public static final Interface HIDE_VIEW = new Interface(false);

    private Interface(boolean includeInterfaceView) {
      this.includeInterfaceView = includeInterfaceView;
    }

    private static Interface includeInterfaceView(boolean includeInterfaceView) {
      return includeInterfaceView ? INCLUDE_VIEW : HIDE_VIEW;
    }

    public TransactionFilterOuterClass.InterfaceFilter toProto(Identifier interfaceId) {
      return TransactionFilterOuterClass.InterfaceFilter.newBuilder()
          .setInterfaceId(interfaceId.toProto())
          .setIncludeInterfaceView(includeInterfaceView)
          .build();
    }

    static Interface fromProto(TransactionFilterOuterClass.InterfaceFilter proto) {
      return includeInterfaceView(proto.getIncludeInterfaceView());
    }

    Interface merge(Interface other) {
      return includeInterfaceView(includeInterfaceView || other.includeInterfaceView);
    }

    @Override
    public String toString() {
      return "Filter.Interface{" + "includeInterfaceView=" + includeInterfaceView + '}';
    }
  }
}
