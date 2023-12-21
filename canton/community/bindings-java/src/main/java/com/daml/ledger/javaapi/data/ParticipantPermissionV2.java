// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;

// TODO (i15873) Eliminate V2 suffix
public abstract class ParticipantPermissionV2 {

  public static final class Submission extends ParticipantPermissionV2 {
    static Submission instance = new Submission();

    private Submission() {}

    public static Submission getInstance() {
      return instance;
    }

    @Override
    public String toString() {
      return "ParticipantPermission.Submission";
    }
  }

  public static final class Confirmation extends ParticipantPermissionV2 {
    static Confirmation instance = new Confirmation();

    private Confirmation() {}

    public static Confirmation getInstance() {
      return instance;
    }

    @Override
    public String toString() {
      return "ParticipantPermission.Confirmation";
    }
  }

  public static final class Observation extends ParticipantPermissionV2 {
    static Observation instance = new Observation();

    private Observation() {}

    public static Observation getInstance() {
      return instance;
    }

    @Override
    public String toString() {
      return "ParticipantPermission.Observation";
    }
  }

  public static ParticipantPermissionV2 fromProto(
      StateServiceOuterClass.ParticipantPermission permission) {
    switch (permission.getNumber()) {
      case StateServiceOuterClass.ParticipantPermission.Submission_VALUE:
        return Submission.instance;
      case StateServiceOuterClass.ParticipantPermission.Confirmation_VALUE:
        return Confirmation.instance;
      case StateServiceOuterClass.ParticipantPermission.Observation_VALUE:
        return Observation.instance;
      default:
        throw new ParticipantPermissionUnrecognized(permission);
    }
  }

  public final StateServiceOuterClass.ParticipantPermission toProto() {
    if (this instanceof Submission) {
      return StateServiceOuterClass.ParticipantPermission.Submission;
    } else if (this instanceof Confirmation) {
      return StateServiceOuterClass.ParticipantPermission.Confirmation;
    } else if (this instanceof Observation) {
      return StateServiceOuterClass.ParticipantPermission.Observation;
    } else {
      throw new ParticipantPermissionUnknown(this);
    }
  }
}

class ParticipantPermissionUnrecognized extends RuntimeException {
  public ParticipantPermissionUnrecognized(
      StateServiceOuterClass.ParticipantPermission permission) {
    super("Participant permission unrecognized " + permission.toString());
  }
}

class ParticipantPermissionUnknown extends RuntimeException {
  public ParticipantPermissionUnknown(ParticipantPermissionV2 permission) {
    super("Participant permission unknown " + permission.toString());
  }
}
