// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  /**
   * Settings for including an interface in {@link InclusiveFilter}. There are four possible values:
   * {@link #HIDE_VIEW_HIDE_PAYLOAD} and {@link #INCLUDE_VIEW_HIDE_PAYLOAD} and {@link
   * #HIDE_VIEW_INCLUDE_PAYLOAD} and {@link #INCLUDE_VIEW_INCLUDE_PAYLOAD}.
   */
  public static final class Interface {
    public final boolean includeInterfaceView;
    public final boolean includeCreateEventPayload;
    // add equals and hashCode if adding more fields

    public static final Interface HIDE_VIEW_HIDE_PAYLOAD = new Interface(false, false);
    public static final Interface INCLUDE_VIEW_HIDE_PAYLOAD = new Interface(true, false);
    public static final Interface HIDE_VIEW_INCLUDE_PAYLOAD = new Interface(false, true);
    public static final Interface INCLUDE_VIEW_INCLUDE_PAYLOAD = new Interface(true, true);

    private Interface(boolean includeInterfaceView, boolean includeCreateEventPayload) {
      this.includeInterfaceView = includeInterfaceView;
      this.includeCreateEventPayload = includeCreateEventPayload;
    }

    private static Interface includeInterfaceView(
        boolean includeInterfaceView, boolean includeCreateEventPayload) {
      if (!includeInterfaceView && !includeCreateEventPayload) return HIDE_VIEW_HIDE_PAYLOAD;
      else if (includeInterfaceView && !includeCreateEventPayload) return INCLUDE_VIEW_HIDE_PAYLOAD;
      else if (!includeInterfaceView) return HIDE_VIEW_INCLUDE_PAYLOAD;
      else return INCLUDE_VIEW_INCLUDE_PAYLOAD;
    }

    public TransactionFilterOuterClass.InterfaceFilter toProto(Identifier interfaceId) {
      return TransactionFilterOuterClass.InterfaceFilter.newBuilder()
          .setInterfaceId(interfaceId.toProto())
          .setIncludeInterfaceView(includeInterfaceView)
          .build();
    }

    static Interface fromProto(TransactionFilterOuterClass.InterfaceFilter proto) {
      return includeInterfaceView(
          proto.getIncludeInterfaceView(), proto.getIncludeCreateEventPayload());
    }

    Interface merge(Interface other) {
      return includeInterfaceView(
          includeInterfaceView || other.includeInterfaceView,
          includeCreateEventPayload || other.includeCreateEventPayload);
    }

    @Override
    public String toString() {
      return "Filter.Interface{"
          + "includeInterfaceView="
          + includeInterfaceView
          + ", includeCreateEventPayload="
          + includeCreateEventPayload
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Filter.Interface that = (Filter.Interface) o;
      return Objects.equals(includeInterfaceView, that.includeInterfaceView)
          && Objects.equals(includeCreateEventPayload, that.includeCreateEventPayload);
    }

    @Override
    public int hashCode() {
      return Objects.hash(includeInterfaceView, includeCreateEventPayload);
    }
  }

  public static final class Template {
    public final boolean includeCreateEventPayload;

    public static final Template INCLUDE_PAYLOAD = new Template(true);
    public static final Template HIDE_PAYLOAD = new Template(false);

    private Template(boolean includeCreateEventPayload) {
      this.includeCreateEventPayload = includeCreateEventPayload;
    }

    private static Template includeCreateEventPayload(boolean includeCreateEventPayload) {
      return includeCreateEventPayload ? INCLUDE_PAYLOAD : HIDE_PAYLOAD;
    }

    public TransactionFilterOuterClass.TemplateFilter toProto(Identifier templateId) {
      return TransactionFilterOuterClass.TemplateFilter.newBuilder()
          .setTemplateId(templateId.toProto())
          .setIncludeCreateEventPayload(includeCreateEventPayload)
          .build();
    }

    static Template fromProto(TransactionFilterOuterClass.TemplateFilter proto) {
      return includeCreateEventPayload(proto.getIncludeCreateEventPayload());
    }

    Template merge(Template other) {
      return includeCreateEventPayload(
          includeCreateEventPayload || other.includeCreateEventPayload);
    }

    @Override
    public String toString() {
      return "Filter.Template{" + "includeCreateEventPayload=" + includeCreateEventPayload + '}';
    }
  }
}
