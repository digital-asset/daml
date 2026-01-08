// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import better.files.*
import cats.implicits.*
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.admin.participant.v30.ContractState.SynchronizerState.State
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.participant.store.ActiveContractStore.{
  ActivenessChangeDetail,
  ReassignmentType,
}
import com.digitalasset.canton.protocol.{
  ContractInstance,
  LfContractId,
  ReassignmentId,
  SerializableContract,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.{IndexedStringStore, IndexedSynchronizer}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.digitalasset.canton.{ProtoDeserializationError, ReassignmentCounter}
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf.ByteString
import upickle.default.*

import java.util.Base64
import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("org.wartremover.warts.Null"))
object JsonCodecs {
  implicit val contractIdRW: ReadWriter[ContractId] =
    readwriter[String].bimap[ContractId](
      cid => cid.coid,
      s => LfContractId.assertFromString(s),
    )

  implicit val synchronizerIdRW: ReadWriter[SynchronizerId] =
    readwriter[String].bimap[SynchronizerId](
      synchronizerId => synchronizerId.toProtoPrimitive,
      s => SynchronizerId.tryFromString(s),
    )

  implicit def genContractInstanceRW: ReadWriter[ContractInstance] =
    readwriter[String].bimap[ContractInstance](
      contractInstance => Base64.getEncoder.encodeToString(contractInstance.encoded.toByteArray),
      s =>
        ContractInstance
          .decodeWithCreatedAt(ByteString.copyFrom(Base64.getDecoder.decode(s)))
          .valueOr(err =>
            throw new IllegalArgumentException(s"Invalid GenContractInstance payload: $err")
          ),
    )

  implicit val reassignmentCounterRW: ReadWriter[ReassignmentCounter] =
    readwriter[String].bimap[ReassignmentCounter](
      reassignmentCounter => reassignmentCounter.unwrap.toString,
      s => ReassignmentCounter(s.toLong),
    )

  implicit val reassignmentIdRW: ReadWriter[ReassignmentId] =
    readwriter[String].bimap[ReassignmentId](
      reassignmentId => reassignmentId.toProtoPrimitive,
      ReassignmentId.tryCreate,
    )
}

final case class CommitmentContractMetadata(
    cid: LfContractId,
    reassignmentCounter: ReassignmentCounter,
) extends HasVersionedWrapper[CommitmentContractMetadata]
    with PrettyPrinting {

  @transient override protected lazy val companionObj: CommitmentContractMetadata.type =
    CommitmentContractMetadata
  override protected def pretty: Pretty[CommitmentContractMetadata.this.type] =
    prettyOfClass(
      param("contract id", _.cid),
      param("reassignment counter", _.reassignmentCounter.v),
    )

  private def toProtoV30: v30.CommitmentContractMeta = v30.CommitmentContractMeta(
    cid.toBytes.toByteString,
    reassignmentCounter.v,
  )
}

object CommitmentContractMetadata
    extends HasVersionedMessageCompanion[
      CommitmentContractMetadata,
    ] {
  import JsonCodecs.*
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  implicit val rw: ReadWriter[CommitmentContractMetadata] = macroRW

  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> ProtoCodec(
        ProtocolVersion.v34,
        supportedProtoVersion(v30.CommitmentContractMeta)(fromProtoV30),
        _.toProtoV30,
      )
    )

  private def fromProtoV30(
      contract: v30.CommitmentContractMeta
  ): ParsingResult[CommitmentContractMetadata] =
    for {
      cid <- LfContractId
        .fromBytes(Bytes.fromByteString(contract.cid))
        .leftMap(ProtoDeserializationError.StringConversionError.apply(_, field = Some("cid")))
    } yield CommitmentContractMetadata(cid, ReassignmentCounter(contract.reassignmentCounter))

  override def name: String = "commitment contract metadata"

  def create(
      cid: LfContractId,
      reassignmentCounter: ReassignmentCounter,
  ): CommitmentContractMetadata =
    CommitmentContractMetadata(cid, reassignmentCounter)

  def compare(
      localContracts: Seq[CommitmentContractMetadata],
      remoteContracts: Seq[CommitmentContractMetadata],
  ): CompareCmtContracts = {
    // check that there are no contract id duplicates in each seq
    val localMap = localContracts.map(cmt => cmt.cid -> cmt.reassignmentCounter).toMap
    require(localMap.keys.sizeIs == localContracts.size, "Duplicate contract ids in local sequence")

    val remoteMap = remoteContracts.map(cmt => cmt.cid -> cmt.reassignmentCounter).toMap
    require(
      remoteMap.keys.sizeIs == remoteContracts.size,
      "Duplicate contract ids in remote sequence",
    )

    val (cidsInBoth, cidsOnlyLocal) = localMap.keys.partition(cid => remoteMap.contains(cid))
    val (_, cidsOnlyRemote) = remoteMap.keys.partition(cid => localMap.contains(cid))

    val (_sameContracts, diffReassignmentCounters) =
      cidsInBoth.partition(cid => localMap(cid) == remoteMap(cid))

    CompareCmtContracts(cidsOnlyLocal.toSeq, cidsOnlyRemote.toSeq, diffReassignmentCounters.toSeq)
  }
}

final case class CompareCmtContracts(
    cidsOnlyLocal: Seq[LfContractId],
    cidsOnlyRemote: Seq[LfContractId],
    differentReassignmentCounters: Seq[LfContractId],
) {

  def writeToFile(filename: String): Unit = {
    val file = File(filename)
    val jsonString = write(this, indent = 2)
    val _ = file.write(jsonString)
  }
}

object CompareCmtContracts {
  import JsonCodecs.*
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  implicit val rw: ReadWriter[CompareCmtContracts] = macroRW

  def readFromFile(filename: String): CompareCmtContracts = {
    val jsonString = File(filename).contentAsString()
    read[CompareCmtContracts](jsonString)
  }
}

final case class CommitmentInspectContract(
    cid: LfContractId,
    activeOnExpectedSynchronizer: Boolean,
    contract: Option[ContractInstance],
    state: Seq[ContractStateOnSynchronizer],
) extends HasVersionedWrapper[CommitmentInspectContract]
    with PrettyPrinting {
  @transient override protected lazy val companionObj: CommitmentInspectContract.type =
    CommitmentInspectContract

  override protected def pretty: Pretty[CommitmentInspectContract.this.type] =
    prettyOfClass(
      param("contract id", _.cid),
      param("active on expected synchronizer", _.activeOnExpectedSynchronizer),
      paramIfDefined("contract", _.contract),
      param("contract state", _.state),
    )

  private def toProtoV30: v30.CommitmentContract = v30.CommitmentContract(
    cid.toBytes.toByteString,
    activeOnExpectedSynchronizer,
    contract.map(_.encoded),
    state.map(_.toProtoV30),
  )
}

object CommitmentInspectContract extends HasVersionedMessageCompanion[CommitmentInspectContract] {
  import JsonCodecs.*
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  implicit val rw: ReadWriter[CommitmentInspectContract] = macroRW

  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> ProtoCodec(
        ProtocolVersion.v34,
        supportedProtoVersion(v30.CommitmentContract)(fromProtoV30),
        _.toProtoV30,
      )
    )

  private def fromProtoV30(
      cmtContract: v30.CommitmentContract
  ): ParsingResult[CommitmentInspectContract] =
    for {
      cid <- LfContractId
        .fromBytes(Bytes.fromByteString(cmtContract.cid))
        .leftMap(ProtoDeserializationError.StringConversionError.apply(_))
      contract <- cmtContract.contract
        .traverse(ContractInstance.decodeWithCreatedAt)
        .leftMap(e => ProtoDeserializationError.ContractDeserializationError(e))
      states <- cmtContract.states.traverse(ContractStateOnSynchronizer.fromProtoV30)
      activeOnExpectedSynchronizer = cmtContract.activeOnExpectedSynchronizer
    } yield CommitmentInspectContract(cid, activeOnExpectedSynchronizer, contract, states)

  override def name: String = "commitment inspect contract"

  def inspectContractState(
      queriedContracts: Seq[LfContractId],
      expectedSynchronizerId: SynchronizerId,
      timestamp: CantonTimestamp,
      downloadPayloads: Boolean,
      syncStateInspection: SyncStateInspection,
      indexedStringStore: IndexedStringStore,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): FutureUnlessShutdown[Seq[CommitmentInspectContract]] = {

    def synchronizerIdFromIdx(
        idx: Int
    ): FutureUnlessShutdown[SynchronizerId] =
      IndexedSynchronizer
        .fromDbIndexOT("par_active_contracts remote synchronizer index", indexedStringStore)(idx)
        .map(_.synchronizerId)
        .getOrElse(
          throw new RuntimeException(
            s"Unable to find synchronizer id for synchronizer with index $idx"
          )
        )

    def synchronizerContractStates(
        synchronizerId: SynchronizerId,
        contractChanges: Map[LfContractId, Seq[(CantonTimestamp, ActivenessChangeDetail)]],
    ): FutureUnlessShutdown[Map[LfContractId, Seq[ContractState]]] = {
      val changes = contractChanges.map { case (cid, timestampedChanges) =>
        cid -> timestampedChanges.map { case (ts, change) =>
          change match {
            case reassignment: ActivenessChangeDetail.ReassignmentChangeDetail =>
              reassignment.toReassignmentType match {
                case ReassignmentType.Unassignment =>
                  for {
                    targetSynchronizerId <- synchronizerIdFromIdx(
                      reassignment.remoteSynchronizerIdx
                    )
                  } yield {
                    val reassignmentIds = syncStateInspection.lookupReassignmentIds(
                      targetSynchronizerId,
                      synchronizerId,
                      Seq(cid),
                      Some(ts),
                    )
                    if (reassignmentIds(cid).sizeIs > 1) {
                      throw new IllegalStateException(
                        s"There should be at most one reassignment id for unassigned contract id $cid, but we have ${reassignmentIds(cid).size}"
                      )
                    }
                    ContractUnassigned(
                      targetSynchronizerId,
                      // the reassignment counter the contract had on the source synchronizer *before* the reassignment op
                      reassignment.reassignmentCounter - 1,
                      reassignmentIds(cid).headOption,
                    ): ContractState
                  }
                case ReassignmentType.Assignment =>
                  for {
                    sourceSynchronizerId <- synchronizerIdFromIdx(
                      reassignment.remoteSynchronizerIdx
                    )
                  } yield {
                    val reassignmentIds = syncStateInspection.lookupReassignmentIds(
                      synchronizerId,
                      sourceSynchronizerId,
                      Seq(cid),
                      None,
                      Some(ts),
                    )
                    if (reassignmentIds(cid).sizeIs > 1) {
                      throw new IllegalStateException(
                        s"There should be at most one reassignment id for unassigned contract id $cid, but we have ${reassignmentIds(cid).size}"
                      )
                    }
                    ContractAssigned(
                      reassignment.reassignmentCounter,
                      reassignmentIds(cid).headOption,
                    ): ContractState
                  }
              }
            case _change: ActivenessChangeDetail.HasReassignmentCounter =>
              FutureUnlessShutdown.outcomeF(
                Future.successful(ContractCreated(): ContractState)
              )
            case _ =>
              FutureUnlessShutdown.outcomeF(
                Future.successful(ContractArchived(): ContractState)
              )
          }
        }.sequence
      }

      MonadUtil
        .sequentialTraverse(changes.toSeq) { case (cid, states) =>
          states.map(cid -> _)
        }
        .map(_.toMap)
    }

    for {
      contractChanges <-
        syncStateInspection.lookupContractSynchronizers(queriedContracts.toSet)

      activeOnExpectedSynchronizer = contractChanges
        .get(expectedSynchronizerId)
        .map(
          _.toMap
            .map { case (cid1, changes1) =>
              cid1 -> changes1.filter(_._1 <= timestamp).sortBy(_._1)
            }
            .map { case (cid2, changes2) => cid2 -> changes2.lastOption }
            .filter { case (_cid3, changes3) =>
              changes3.exists(c =>
                c._2.name == ActivenessChangeDetail.create || c._2.name == ActivenessChangeDetail.assign
              )
            }
        )

      activeCidsOnExpectedSynchronizer = activeOnExpectedSynchronizer.fold(Set.empty[LfContractId])(
        _.keySet
      )

      // 1. Find on which synchronizers the queried contracts are active at the given timestamp

      //  Retrieve contract payloads if required
      payloads <-
        if (downloadPayloads)
          contractChanges
            .map { case (synchronizer, states) =>
              for {
                payloads <- syncStateInspection.findContractPayloads(
                  synchronizer,
                  states.keys.toSeq,
                )
              } yield synchronizer -> payloads
            }
            .toSeq
            .sequence
            .map(_.toMap)
        else
          FutureUnlessShutdown.pure(
            Map.empty[SynchronizerId, Map[LfContractId, ContractInstance]]
          )

      statesPerSynchronizer <-
        MonadUtil
          .sequentialTraverse(
            contractChanges.map { case (synchronizer, states) =>
              synchronizer -> synchronizerContractStates(synchronizer, states)
            }.toSeq
          ) { case (synchronizerId, states) =>
            states.map(synchronizerId -> _)
          }

      states = statesPerSynchronizer
        .flatMap { case (synchronizerId, contractStates) =>
          contractStates
            .groupBy(_._1)
            .map { case (cid, cidsToStates) =>
              cid -> cidsToStates.values
            }
            .map { case (cid, states) =>
              val payload = payloads.get(synchronizerId).traverse(_.get(cid)).flatten
              CommitmentInspectContract(
                cid,
                activeCidsOnExpectedSynchronizer.contains(cid),
                payload,
                states.flatten
                  .map(cs => ContractStateOnSynchronizer(synchronizerId, cs))
                  .toSeq,
              )
            }
        }

      unknownContracts = queriedContracts.toSet
        .diff(
          contractChanges.values.foldLeft(Set.empty[LfContractId]) { case (s, m) =>
            s ++ m.keys.toSet
          }
        )
        .map(cid =>
          CommitmentInspectContract(
            cid,
            activeOnExpectedSynchronizer = false,
            None,
            Seq {
              ContractStateOnSynchronizer(expectedSynchronizerId, ContractUnknown())
            },
          )
        )
    } yield (states ++ unknownContracts).toSeq
  }

  def writeToFile(filename: String, items: Seq[CommitmentInspectContract]): Unit = {
    val file = File(filename)
    val ndjson = items.map(p => write(p)).mkString("\n")
    val _ = file.overwrite(ndjson)
  }
}

final case class CommitmentMismatchInfo(
    // This is the reference synchronizer for contract mismatch info
    synchronizerId: SynchronizerId,
    timestamp: CantonTimestamp,
    participant: ParticipantId,
    counterParticipant: ParticipantId,
    mismatches: Seq[ContractMismatchInfo],
) extends PrettyPrinting {
  override protected def pretty: Pretty[CommitmentMismatchInfo] = prettyOfClass(
    param("synchronizer id", _.synchronizerId),
    param("synchronizer mismatch timestamp", _.timestamp),
    param("participant", _.participant),
    param("counter-participant", _.counterParticipant),
    param("mismatching contracts", _.mismatches),
  )
}

final case class ContractMismatchInfo(
    contract: SerializableContract,
    reason: MismatchReason,
) extends PrettyPrinting {
  override protected def pretty: Pretty[ContractMismatchInfo] = prettyOfClass(
    param("contract", _.contract),
    param("mismatch reason", _.reason),
  )
}

sealed trait MismatchReason extends Product with Serializable with PrettyPrinting

final case class UnknownContract(
    participantWithContract: ParticipantId,
    active: ContractActive,
    participantWithoutContract: ParticipantId,
) extends MismatchReason {
  override protected def pretty: Pretty[UnknownContract] = prettyOfClass(
    param("Contract exists on participant", _.participantWithContract),
    param("activated by", _.active),
    param("but does not exist on participant", _.participantWithoutContract),
  )
}

final case class DeactivatedContract(
    participantWithContract: ParticipantId,
    active: ContractActive,
    participantWithoutContract: ParticipantId,
    inactive: ContractInactive,
    whereActive: Option[ContractActive],
) extends MismatchReason
    with PrettyPrinting {
  override protected def pretty: Pretty[DeactivatedContract] = prettyOfClass(
    param("participantWithContract", _.participantWithContract),
    param("activated by", _.active),
    param("participantWithoutContract", _.participantWithoutContract),
    param("deactivated by", _.inactive),
    paramIfDefined("whereActive", _.whereActive),
  )
}

final case class ContractStateOnSynchronizer(
    synchronizerId: SynchronizerId,
    contractState: ContractState,
) extends HasVersionedWrapper[ContractStateOnSynchronizer]
    with PrettyPrinting {
  override def pretty: Pretty[ContractStateOnSynchronizer] = prettyOfClass(
    param("synchronizer id", _.synchronizerId),
    param("contract state", _.contractState),
  )

  @transient override protected lazy val companionObj: ContractStateOnSynchronizer.type =
    ContractStateOnSynchronizer

  def toProtoV30: v30.ContractState.SynchronizerState = v30.ContractState.SynchronizerState(
    synchronizerId.toProtoPrimitive,
    contractState match {
      case active: ContractActive =>
        active match {
          case c: ContractCreated => State.Created(c.toProtoV30)
          case c: ContractAssigned => State.Assigned(c.toProtoV30)
        }
      case inactive: ContractInactive =>
        inactive match {
          case c: ContractUnassigned => State.Unassigned(c.toProtoV30)
          case c: ContractArchived => State.Archived(c.toProtoV30)
          case c: ContractUnknown => State.Unknown(c.toProtoV30)
        }
    },
  )
}

object ContractStateOnSynchronizer
    extends HasVersionedMessageCompanion[
      ContractStateOnSynchronizer
    ] {
  import JsonCodecs.*
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  implicit val rw: ReadWriter[ContractStateOnSynchronizer] = macroRW

  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> ProtoCodec(
        ProtocolVersion.v34,
        supportedProtoVersion(v30.ContractState.SynchronizerState)(fromProtoV30),
        _.toProtoV30,
      )
    )

  def fromProtoV30(
      state: v30.ContractState.SynchronizerState
  ): ParsingResult[ContractStateOnSynchronizer] =
    for {
      synchronizerId <- SynchronizerId.fromProtoPrimitive(state.synchronizerId, "synchronizerId")
      contractState <- state.state match {
        case State.Created(value) => ContractCreated.fromProtoV30(value)
        case State.Archived(value) => ContractArchived.fromProtoV30(value)
        case State.Unassigned(value) => ContractUnassigned.fromProtoV30(value)
        case State.Assigned(value) => ContractAssigned.fromProtoV30(value)
        case State.Unknown(value) => ContractUnknown.fromProtoV30(value)
        case _ => Left(ProtoDeserializationError.FieldNotSet("state"))
      }
    } yield ContractStateOnSynchronizer(synchronizerId, contractState)

  override def name: String = "contract state on synchronizer"
}

sealed trait ContractState extends Product with Serializable with PrettyPrinting

object ContractState {
  implicit val rw: ReadWriter[ContractState] = macroRW
}

sealed trait ContractActive extends ContractState

object ContractActive {
  implicit val rw: ReadWriter[ContractActive] = macroRW
}

sealed trait ContractInactive extends ContractState

object ContractInactive {
  implicit val rw: ReadWriter[ContractInactive] = macroRW
}

final case class ContractCreated()
    extends ContractActive
    with HasVersionedWrapper[ContractCreated] {
  override protected def pretty: Pretty[ContractCreated] = prettyOfClass()

  @transient override protected lazy val companionObj: ContractCreated.type =
    ContractCreated

  def toProtoV30: v30.ContractState.Created = v30.ContractState.Created()
}

object ContractCreated
    extends HasVersionedMessageCompanion[
      ContractCreated
    ] {

  implicit val rw: ReadWriter[ContractCreated] = macroRW

  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> ProtoCodec(
        ProtocolVersion.v34,
        supportedProtoVersion(v30.ContractState.Created)(fromProtoV30),
        _.toProtoV30,
      )
    )

  def fromProtoV30(
      @unused created: v30.ContractState.Created
  ): ParsingResult[ContractCreated] = Right(ContractCreated())

  override def name: String = "contract created"
}

final case class ContractAssigned(
    reassignmentCounterTarget: ReassignmentCounter,
    // None if the assignation was changed usign the repair service, or if the transfer store has been pruned
    reassignmentId: Option[ReassignmentId],
) extends ContractActive
    with HasVersionedWrapper[ContractAssigned] {
  override protected def pretty: Pretty[ContractAssigned] = prettyOfClass(
    param("reassignment counter on target", _.reassignmentCounterTarget),
    paramIfDefined("reasignment id", _.reassignmentId),
  )

  @transient override protected lazy val companionObj: ContractAssigned.type =
    ContractAssigned

  def toProtoV30: v30.ContractState.Assigned = v30.ContractState.Assigned(
    reassignmentCounterTarget.v,
    reassignmentId match {
      case Some(rid) => Some(v30.ContractState.ReassignmentId(rid.toProtoPrimitive))
      case None => None: Option[v30.ContractState.ReassignmentId]
    },
  )
}

object ContractAssigned
    extends HasVersionedMessageCompanion[
      ContractAssigned
    ] {
  import JsonCodecs.*
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  implicit val rw: ReadWriter[ContractAssigned] = macroRW

  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> ProtoCodec(
        ProtocolVersion.v34,
        supportedProtoVersion(v30.ContractState.Assigned)(fromProtoV30),
        _.toProtoV30,
      )
    )

  def fromProtoV30(
      assigned: v30.ContractState.Assigned
  ): ParsingResult[ContractAssigned] = {
    val reassignmentCounterTarget = ReassignmentCounter(assigned.reassignmentCounterTarget)
    val reassignmentIdE = assigned.reassignmentId match {
      case Some(reassignmentId) =>
        for {
          reassignmentId <- ReassignmentId.fromProtoPrimitive(reassignmentId.id)
        } yield Some(reassignmentId)
      case None => Right(None: Option[ReassignmentId])
    }

    reassignmentIdE.map(ContractAssigned(reassignmentCounterTarget, _))
  }

  override def name: String = "contract assigned"
}

final case class ContractUnassigned(
    targetSynchronizerId: SynchronizerId,
    // the reassignment counter the contract had on the source synchronizer *before* the reassignment op
    // it represents the transfer data reassignment counter - 1
    reassignmentCounterSrc: ReassignmentCounter,
    // None if the assignation was changed usign the repair service, or if the transfer store has been pruned
    reassignmentId: Option[ReassignmentId],
) extends ContractInactive
    with HasVersionedWrapper[ContractUnassigned] {
  override protected def pretty: Pretty[ContractUnassigned] = prettyOfClass(
    param("target synchronizer id", _.targetSynchronizerId),
    param("reassignment counter on source", _.reassignmentCounterSrc),
    paramIfDefined("reassignment id", _.reassignmentId),
  )

  @transient override protected lazy val companionObj: ContractUnassigned.type =
    ContractUnassigned

  def toProtoV30: v30.ContractState.Unassigned = v30.ContractState.Unassigned(
    targetSynchronizerId.toProtoPrimitive,
    reassignmentCounterSrc.v,
    reassignmentId match {
      case Some(rid) => Some(v30.ContractState.ReassignmentId(rid.toProtoPrimitive))
      case None => None: Option[v30.ContractState.ReassignmentId]
    },
  )
}

object ContractUnassigned extends HasVersionedMessageCompanion[ContractUnassigned] {
  import JsonCodecs.*
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  implicit val rw: ReadWriter[ContractUnassigned] = macroRW

  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> ProtoCodec(
        ProtocolVersion.v34,
        supportedProtoVersion(v30.ContractState.Unassigned)(fromProtoV30),
        _.toProtoV30,
      )
    )

  def fromProtoV30(
      unassigned: v30.ContractState.Unassigned
  ): ParsingResult[ContractUnassigned] =
    for {
      targetSynchronizerId <- SynchronizerId.fromProtoPrimitive(
        unassigned.targetSynchronizerId,
        "target_synchronizer_id",
      )
      reassignmentCounterSrc = ReassignmentCounter(unassigned.reassignmentCounterSrc)
      reassignmentIdE = unassigned.reassignmentId match {
        case Some(reassignmentId) =>
          for {
            reassignmentId <- ReassignmentId.fromProtoPrimitive(reassignmentId.id)
          } yield Some(reassignmentId)
        case None => Right(None: Option[ReassignmentId])
      }

      reassignmentId <- reassignmentIdE
    } yield ContractUnassigned(
      targetSynchronizerId,
      reassignmentCounterSrc,
      reassignmentId,
    )

  override def name: String = "contract assigned"
}

final case class ContractArchived()
    extends ContractInactive
    with HasVersionedWrapper[ContractArchived] {
  override protected def pretty: Pretty[ContractArchived] = prettyOfClass()

  @transient override protected lazy val companionObj: ContractArchived.type =
    ContractArchived

  def toProtoV30: v30.ContractState.Archived = v30.ContractState.Archived()
}

object ContractArchived
    extends HasVersionedMessageCompanion[
      ContractArchived
    ] {
  implicit val rw: ReadWriter[ContractArchived] = macroRW

  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> ProtoCodec(
        ProtocolVersion.v34,
        supportedProtoVersion(v30.ContractState.Archived)(fromProtoV30),
        _.toProtoV30,
      )
    )

  def fromProtoV30(
      @unused archived: v30.ContractState.Archived
  ): ParsingResult[ContractArchived] = Right(ContractArchived())

  override def name: String = "contract archived"
}

final case class ContractUnknown(
) extends ContractInactive
    with HasVersionedWrapper[ContractUnknown] {
  override def pretty: Pretty[ContractUnknown] = prettyOfClass(
  )

  @transient override protected lazy val companionObj: ContractUnknown.type =
    ContractUnknown

  def toProtoV30: v30.ContractState.Unknown = v30.ContractState.Unknown(
  )
}

object ContractUnknown
    extends HasVersionedMessageCompanion[
      ContractUnknown
    ] {
  implicit val rw: ReadWriter[ContractUnknown] = macroRW

  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> ProtoCodec(
        ProtocolVersion.v34,
        supportedProtoVersion(v30.ContractState.Unknown)(fromProtoV30),
        _.toProtoV30,
      )
    )

  def fromProtoV30(
      @unused unknown: v30.ContractState.Unknown
  ): ParsingResult[ContractUnknown] = Right(ContractUnknown())

  override def name: String = "contract unknown"
}

object OpenCommitmentHelper {
  def writeToFile(filename: String, result: Seq[CommitmentContractMetadata]): Unit = {
    val file = File(filename)
    val ndjson = result.map(p => write(p)).mkString("\n")
    val _ = file.overwrite(ndjson)
  }

  def readFromFile(filename: String): Seq[CommitmentContractMetadata] = {
    val file = File(filename)

    val contracts: Seq[CommitmentContractMetadata] =
      file
        .lineIterator()
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(line => read[CommitmentContractMetadata](line))
        .toSeq
    contracts
  }
}
