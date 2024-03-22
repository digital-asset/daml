// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;

public abstract class ParticipantPermission {

  public static final class Submission extends ParticipantPermission {
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

  public static final class Confirmation extends ParticipantPermission {
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

  public static final class Observation extends ParticipantPermission {
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

  public static ParticipantPermission fromProto(
      StateServiceOuterClass.ParticipantPermission permission) {
    switch (permission.getNumber()) {
      case StateServiceOuterClass.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION_VALUE:
        return Submission.instance;
      case StateServiceOuterClass.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION_VALUE:
        return Confirmation.instance;
      case StateServiceOuterClass.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION_VALUE:
        return Observation.instance;
      default:
        throw new ParticipantPermissionUnrecognized(permission);
    }
  }

  public final StateServiceOuterClass.ParticipantPermission toProto() {
    if (this instanceof Submission) {
      return StateServiceOuterClass.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION;
    } else if (this instanceof Confirmation) {
      return StateServiceOuterClass.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION;
    } else if (this instanceof Observation) {
      return StateServiceOuterClass.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION;
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
  public ParticipantPermissionUnknown(ParticipantPermission permission) {
    super("Participant permission unknown " + permission.toString());
  }
}
