// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset

import com.digitalasset.canton.data.{Counter, CounterCompanion}
import com.digitalasset.canton.serialization.DeterministicEncoding.encodeLong
import com.digitalasset.daml.lf.command.ReplayCommand
import com.digitalasset.daml.lf.data.{IdString, Ref, Time}
import com.digitalasset.daml.lf.transaction.{ContractStateMachine, Versioned}
import com.digitalasset.daml.lf.value.Value
import com.google.protobuf.ByteString
import scalapb.GeneratedMessage

package object canton {

  // Lf type for other ledger scalars, e.g. application, command and workflow id
  // LfLedgerString has a length limit of 255 characters and contains alphanumeric characters and an itemized set of
  // separators including _, :, - and even spaces
  type LfLedgerString = Ref.LedgerString
  val LfLedgerString: Ref.LedgerString.type = Ref.LedgerString

  // A party identifier representation in LF. See [[com.digitalasset.canton.topology.PartyId]] for the party identifier
  // in Canton.
  type LfPartyId = Ref.Party
  val LfPartyId: Ref.Party.type = Ref.Party

  // Ledger participant id
  type LedgerParticipantId = Ref.ParticipantId
  val LedgerParticipantId: Ref.ParticipantId.type = Ref.ParticipantId

  // Ledger submission id
  type LedgerSubmissionId = Ref.SubmissionId
  val LedgerSubmissionId: Ref.SubmissionId.type = Ref.SubmissionId

  // Ledger command id
  type LedgerCommandId = Ref.CommandId
  val LedgerCommandId: Ref.CommandId.type = Ref.CommandId

  // Ledger user id
  type LedgerUserId = Ref.UserId
  val LedgerUserId: Ref.UserId.type = Ref.UserId

  // Ledger transaction id
  type LedgerTransactionId = Ref.TransactionId
  val LedgerTransactionId: Ref.TransactionId.type = Ref.TransactionId

  // Command Id
  type LfCommandId = Ref.CommandId
  val LfCommandId: Ref.CommandId.type = Ref.CommandId

  // Submission Id
  type LfSubmissionId = Ref.SubmissionId
  val LfSubmissionId: Ref.SubmissionId.type = Ref.SubmissionId

  // Exercise choice name
  type LfChoiceName = Ref.ChoiceName
  val LfChoiceName: Ref.ChoiceName.type = Ref.ChoiceName

  type LfPackageId = Ref.PackageId
  val LfPackageId: Ref.PackageId.type = Ref.PackageId

  type LfPackageName = Ref.PackageName
  val LfPackageName: Ref.PackageName.type = Ref.PackageName

  type LfPackageRef = Ref.PackageRef
  val LfPackageRef: Ref.PackageRef.type = Ref.PackageRef

  type LfPackageVersion = Ref.PackageVersion
  val LfPackageVersion: Ref.PackageVersion.type = Ref.PackageVersion

  type LfInterfaceId = Ref.TypeConId
  val LfInterfaceId: Ref.TypeConId.type = Ref.TypeConId

  // Timestamp used by lf and sync api
  type LfTimestamp = Time.Timestamp
  val LfTimestamp: Time.Timestamp.type = Time.Timestamp

  type LfValue = Value
  val LfValue: Value.type = Value

  type LfVersioned[T] = Versioned[T]
  val LfVersioned: Versioned.type = Versioned

  // Lf commands for use by lf engine.reinterpret
  type LfCommand = ReplayCommand
  val LfCommand: ReplayCommand.type = ReplayCommand

  type LfCreateCommand = LfCommand.Create
  val LfCreateCommand: LfCommand.Create.type = LfCommand.Create

  type LfExerciseCommand = LfCommand.Exercise
  val LfExerciseCommand: LfCommand.Exercise.type = LfCommand.Exercise

  type LfExerciseByKeyCommand = LfCommand.ExerciseByKey
  val LfExerciseByKeyCommand: LfCommand.ExerciseByKey.type = LfCommand.ExerciseByKey

  type LfFetchCommand = LfCommand.Fetch
  val LfFetchCommand: LfCommand.Fetch.type = LfCommand.Fetch

  type LfFetchByKeyCommand = LfCommand.FetchByKey
  val LfFetchByKeyCommand: LfCommand.FetchByKey.type = LfCommand.FetchByKey

  type LfLookupByKeyCommand = LfCommand.LookupByKey
  val LfLookupByKeyCommand: LfCommand.LookupByKey.type = LfCommand.LookupByKey

  type LfWorkflowId = Ref.WorkflowId
  val LfWorkflowId: Ref.WorkflowId.type = Ref.WorkflowId

  type LfKeyResolver = ContractStateMachine.KeyResolver

  /** The counter assigned by the sequencer to messages sent to the participant. The counter is
    * specific to every participant.
    */
  type SequencerCounterDiscriminator
  type SequencerCounter = Counter[SequencerCounterDiscriminator]

  val SequencerCounter = new CounterCompanion[SequencerCounterDiscriminator] {}

  /** The counter assigned by the transaction processor to confirmation and reassignment requests.
    */
  type RequestCounterDiscriminator
  type RequestCounter = Counter[RequestCounterDiscriminator]

  object RequestCounter extends CounterCompanion[RequestCounterDiscriminator]

  /** The counter assigned to a contract to count the number of its reassignments */
  type ReassignmentDiscriminator
  type ReassignmentCounter = Counter[ReassignmentDiscriminator]

  object ReassignmentCounter extends CounterCompanion[ReassignmentDiscriminator] {
    def encodeDeterministically(reassignmentCounter: ReassignmentCounter): ByteString = encodeLong(
      reassignmentCounter.unwrap
    )
  }

  /** The counter assigned to a contract to track different repair changes. The counter is relative
    * to a sequenced request timestamp.
    */
  type RepairCounterDiscriminator
  type RepairCounter = Counter[RepairCounterDiscriminator]

  object RepairCounter extends CounterCompanion[RepairCounterDiscriminator]

  /** Wrap a method call with this method to document that the caller is sure that the callee's
    * preconditions are met.
    */
  def checked[A](x: => A): A = x

  /** We should not call `toByteString` directly on a proto message. Rather, we should use the
    * versioning tooling which ensures that the correct version of the proto message is used (based
    * on the protocol version). However, in some cases (e.g., when we are sure that the message is
    * not versioned), we can invoke this method directly.
    */
  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def checkedToByteString(proto: GeneratedMessage): ByteString = proto.toByteString

  implicit class RichGeneratedMessage(val message: GeneratedMessage) extends AnyVal {
    @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
    def checkedToByteString: ByteString = message.toByteString
  }

  implicit val lfPartyOrdering: Ordering[LfPartyId] =
    IdString.`Party order instance`.toScalaOrdering

  /** Use this type when scalac struggles to infer `Nothing` due to it being treated specially.
    *
    * see https://www.reddit.com/r/scala/comments/73791p/nothings_twin_brother_the_better_one/
    */
  type Uninhabited <: Nothing
}
