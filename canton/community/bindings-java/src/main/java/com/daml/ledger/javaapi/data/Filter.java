// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

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
   * Settings for including an interface in {@link InclusiveFilter}. There are four possible values:
   * {@link #HIDE_VIEW_HIDE_CREATED_EVENT_BLOB} and {@link #INCLUDE_VIEW_HIDE_CREATED_EVENT_BLOB}
   * and {@link #HIDE_VIEW_INCLUDE_CREATED_EVENT_BLOB} and {@link
   * #INCLUDE_VIEW_INCLUDE_CREATED_EVENT_BLOB}.
   */
  public static enum Interface {
    HIDE_VIEW_HIDE_CREATED_EVENT_BLOB(false, false),
    INCLUDE_VIEW_HIDE_CREATED_EVENT_BLOB(true, false),
    HIDE_VIEW_INCLUDE_CREATED_EVENT_BLOB(false, true),
    INCLUDE_VIEW_INCLUDE_CREATED_EVENT_BLOB(true, true);

    public final boolean includeInterfaceView;
    public final boolean includeCreatedEventBlob;

    Interface(boolean includeInterfaceView, boolean includeCreatedEventBlob) {
      this.includeInterfaceView = includeInterfaceView;
      this.includeCreatedEventBlob = includeCreatedEventBlob;
    }

    private static Interface includeInterfaceView(
        boolean includeInterfaceView, boolean includeCreatedEventBlob) {
      if (!includeInterfaceView && !includeCreatedEventBlob)
        return HIDE_VIEW_HIDE_CREATED_EVENT_BLOB;
      else if (includeInterfaceView && !includeCreatedEventBlob)
        return INCLUDE_VIEW_HIDE_CREATED_EVENT_BLOB;
      else if (!includeInterfaceView) return HIDE_VIEW_INCLUDE_CREATED_EVENT_BLOB;
      else return INCLUDE_VIEW_INCLUDE_CREATED_EVENT_BLOB;
    }

    public TransactionFilterOuterClass.InterfaceFilter toProto(Identifier interfaceId) {
      return TransactionFilterOuterClass.InterfaceFilter.newBuilder()
          .setInterfaceId(interfaceId.toProto())
          .setIncludeInterfaceView(includeInterfaceView)
          .build();
    }

    static Interface fromProto(TransactionFilterOuterClass.InterfaceFilter proto) {
      return includeInterfaceView(
          proto.getIncludeInterfaceView(), proto.getIncludeCreatedEventBlob());
    }

    Interface merge(Interface other) {
      return includeInterfaceView(
          includeInterfaceView || other.includeInterfaceView,
          includeCreatedEventBlob || other.includeCreatedEventBlob);
    }
  }

  public static enum Template {
    INCLUDE_CREATED_EVENT_BLOB(true),
    HIDE_CREATED_EVENT_BLOB(false);
    public final boolean includeCreatedEventBlob;

    Template(boolean includeCreatedEventBlob) {
      this.includeCreatedEventBlob = includeCreatedEventBlob;
    }

    private static Template includeCreatedEventBlob(boolean includeCreatedEventBlob) {
      return includeCreatedEventBlob ? INCLUDE_CREATED_EVENT_BLOB : HIDE_CREATED_EVENT_BLOB;
    }

    public TransactionFilterOuterClass.TemplateFilter toProto(Identifier templateId) {
      return TransactionFilterOuterClass.TemplateFilter.newBuilder()
          .setTemplateId(templateId.toProto())
          .setIncludeCreatedEventBlob(includeCreatedEventBlob)
          .build();
    }

    static Template fromProto(TransactionFilterOuterClass.TemplateFilter proto) {
      return includeCreatedEventBlob(proto.getIncludeCreatedEventBlob());
    }

    Template merge(Template other) {
      return includeCreatedEventBlob(includeCreatedEventBlob || other.includeCreatedEventBlob);
    }
  }
}
