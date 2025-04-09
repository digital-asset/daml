// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TopologyTransactionOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This interface represents events in {@link TopologyTransaction}s.
 *
 * @see ParticipantAuthorizationAdded
 * @see ParticipantAuthorizationChanged
 * @see ParticipantAuthorizationRevoked
 * @see TopologyTransaction
 */
public interface TopologyEvent {

  @NonNull
  String getParticipantId();

  default TopologyTransactionOuterClass.TopologyEvent toProtoEvent() {
    TopologyTransactionOuterClass.TopologyEvent.Builder eventBuilder =
        TopologyTransactionOuterClass.TopologyEvent.newBuilder();
    if (this instanceof ParticipantAuthorizationChanged) {
      ParticipantAuthorizationChanged event = (ParticipantAuthorizationChanged) this;
      eventBuilder.setParticipantAuthorizationChanged(event.toProto());
    } else if (this instanceof ParticipantAuthorizationAdded) {
      ParticipantAuthorizationAdded event = (ParticipantAuthorizationAdded) this;
      eventBuilder.setParticipantAuthorizationAdded(event.toProto());
    } else if (this instanceof ParticipantAuthorizationRevoked) {
      ParticipantAuthorizationRevoked event = (ParticipantAuthorizationRevoked) this;
      eventBuilder.setParticipantAuthorizationRevoked(event.toProto());
    } else {
      throw new RuntimeException(
          "this should be ParticipantAuthorizationChanged or ParticipantAuthorizationRevoked, found "
              + this.toString());
    }
    return eventBuilder.build();
  }

  static TopologyEvent fromProtoEvent(TopologyTransactionOuterClass.TopologyEvent event) {
    if (event.hasParticipantAuthorizationChanged()) {
      return ParticipantAuthorizationChanged.fromProto(event.getParticipantAuthorizationChanged());
    } else if (event.hasParticipantAuthorizationAdded()) {
      return ParticipantAuthorizationAdded.fromProto(event.getParticipantAuthorizationAdded());
    } else if (event.hasParticipantAuthorizationRevoked()) {
      return ParticipantAuthorizationRevoked.fromProto(event.getParticipantAuthorizationRevoked());
    } else {
      throw new UnsupportedEventTypeException(event.toString());
    }
  }
}
