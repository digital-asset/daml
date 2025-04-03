// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ReassignmentOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public interface ReassignmentEvent {
  default ReassignmentOuterClass.ReassignmentEvent toProtoEvent() {
    ReassignmentOuterClass.ReassignmentEvent.Builder eventBuilder =
        ReassignmentOuterClass.ReassignmentEvent.newBuilder();
    if (this instanceof UnassignedEvent) {
      UnassignedEvent event = (UnassignedEvent) this;
      eventBuilder.setUnassigned(event.toProto());
    } else if (this instanceof AssignedEvent) {
      AssignedEvent event = (AssignedEvent) this;
      eventBuilder.setAssigned(event.toProto());
    } else {
      throw new RuntimeException(
          "this should be UnassignedEvent or AssignedEvent, found " + this.toString());
    }
    return eventBuilder.build();
  }

  static ReassignmentEvent fromProtoEvent(ReassignmentOuterClass.ReassignmentEvent event) {
    if (event.hasUnassigned()) {
      return UnassignedEvent.fromProto(event.getUnassigned());
    } else if (event.hasAssigned()) {
      return AssignedEvent.fromProto(event.getAssigned());
    } else {
      throw new UnsupportedEventTypeException(event.toString());
    }
  }
}
