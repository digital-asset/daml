// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.{ErrorClass, ErrorGroup}

object CantonErrorGroups {

  private implicit val errorClass: ErrorClass = ErrorClass.root()

  abstract class ConfigErrorGroup extends ErrorGroup()

  abstract class CommandErrorGroup extends ErrorGroup()

  abstract class HandshakeErrorGroup extends ErrorGroup

  abstract class ProtoDeserializationErrorGroup extends ErrorGroup

  abstract class SequencerErrorGroup extends ErrorGroup()

  abstract class SequencerSubscriptionErrorGroup extends ErrorGroup()

  abstract class MediatorErrorGroup extends ErrorGroup()

  abstract class GrpcErrorGroup extends ErrorGroup()

  object ParticipantErrorGroup extends ErrorGroup() {

    abstract class DomainConnectivityErrorGroup extends ErrorGroup()
    abstract class SyncServiceErrorGroup extends ErrorGroup()
    abstract class PackageServiceErrorGroup extends ErrorGroup()
    abstract class PruningServiceErrorGroup extends ErrorGroup()
    abstract class RepairServiceErrorGroup extends ErrorGroup()

    object TransactionErrorGroup extends ErrorGroup() {
      // Errors emitted by Ledger Api server
      abstract class LedgerApiErrorGroup extends ErrorGroup()
      // TransactionInjectErrors are initial injection errors resulting from the canton sync service
      abstract class InjectionErrorGroup extends ErrorGroup()
      // TransactionRoutingErrors are routing errors resulting from the domain router
      abstract class RoutingErrorGroup extends ErrorGroup()
      // TransactionSubmissionErrors are routing errors resulting from the transaction processor
      abstract class SubmissionErrorGroup extends ErrorGroup()
      // local rejections made by participants during transaction processing
      abstract class LocalRejectionGroup extends ErrorGroup()
    }

    // replicated participant errors
    abstract class ReplicationErrorGroup extends ErrorGroup()

    abstract class AcsCommitmentErrorGroup extends ErrorGroup()

    abstract class AdminWorkflowServicesErrorGroup extends ErrorGroup()

  }

  object TopologyManagementErrorGroup extends ErrorGroup() {
    abstract class TopologyManagerErrorGroup extends ErrorGroup()
    abstract class TopologyDispatchingErrorGroup extends ErrorGroup()
  }

  abstract class StorageErrorGroup extends ErrorGroup()

  abstract class ClockErrorGroup extends ErrorGroup() {}

}
