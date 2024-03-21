// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset

import com.daml.lf.command.ReplayCommand
import com.daml.lf.data.{IdString, Ref, Time}
import com.daml.lf.transaction.{ContractStateMachine, Versioned}
import com.daml.lf.value.Value
import com.digitalasset.canton.data.{Counter, CounterCompanion}
import com.digitalasset.canton.serialization.DeterministicEncoding.encodeLong
import com.google.protobuf.ByteString

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

  // Ledger application id
  type LedgerApplicationId = Ref.ApplicationId
  val LedgerApplicationId: Ref.ApplicationId.type = Ref.ApplicationId

  // Ledger transaction id
  type LedgerTransactionId = Ref.TransactionId
  val LedgerTransactionId: Ref.TransactionId.type = Ref.TransactionId

  // Exercise choice name
  type LfChoiceName = Ref.ChoiceName
  val LfChoiceName: Ref.ChoiceName.type = Ref.ChoiceName

  type LfPackageId = Ref.PackageId
  val LfPackageId: Ref.PackageId.type = Ref.PackageId

  type LfPackageName = Ref.PackageName
  val LfPackageName: Ref.PackageName.type = Ref.PackageName

  type LfPackageVersion = Ref.PackageVersion
  val LfPackageVersion: Ref.PackageVersion.type = Ref.PackageVersion

  type LfInterfaceId = Ref.TypeConName
  val LfInterfaceId: Ref.TypeConName.type = Ref.TypeConName

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

  /** The counter assigned by the sequencer to messages sent to the participant.
    * The counter is specific to every participant.
    */
  type SequencerCounterDiscriminator
  type SequencerCounter = Counter[SequencerCounterDiscriminator]

  val SequencerCounter = new CounterCompanion[SequencerCounterDiscriminator] {}

  /** The counter assigned by the transaction processor to confirmation and transfer requests. */
  type RequestCounterDiscriminator
  type RequestCounter = Counter[RequestCounterDiscriminator]

  /** The counter assigned to a contract to count the number of its transfers */
  type TransferCounterDiscriminator
  type TransferCounter = Counter[TransferCounterDiscriminator]

  object TransferCounter extends CounterCompanion[TransferCounterDiscriminator] {
    def encodeDeterministically(transferCounter: TransferCounter): ByteString = encodeLong(
      transferCounter.unwrap
    )
  }

  object RequestCounter extends CounterCompanion[RequestCounterDiscriminator]

  /** Wrap a method call with this method to document that the caller is sure that the callee's preconditions are met. */
  def checked[A](x: => A): A = x

  /** Evaluate the expression and discard the result. */
  implicit final class DiscardOps[A](private val a: A) extends AnyVal {
    @inline
    def discard[B](implicit ev: A =:= B): Unit = ()
  }

  implicit val lfPartyOrdering: Ordering[LfPartyId] =
    IdString.`Party order instance`.toScalaOrdering

  /** Use this type when scalac struggles to infer `Nothing`
    * due to it being treated specially.
    *
    * see https://www.reddit.com/r/scala/comments/73791p/nothings_twin_brother_the_better_one/
    */
  type Uninhabited <: Nothing
}
