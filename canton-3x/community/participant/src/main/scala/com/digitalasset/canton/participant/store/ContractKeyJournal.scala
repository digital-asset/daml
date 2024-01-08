// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.store.ContractKeyJournal.{
  ContractKeyJournalError,
  ContractKeyState,
  Status,
}
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.protocol.LfGlobalKey
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.Future

/** The contract key journal determines for each [[com.digitalasset.canton.protocol.LfGlobalKey]]
  * whether it is considered to be allocated. The store is organized as a journal,
  * indexed by timestamp and request counter, so that crash recovery can remove all changes
  * due to dirty request before replay starts.
  *
  * With unique contract key semantics and honest key maintainers,
  * the allocation status reflects whether there is an active contract in the active contract store for the given key.
  * However, if two or more contracts with the same key have been active,
  * this correspondence no longer needs to hold.
  * Then, the contract key journal has the authority over the allocation status of the key.
  */
trait ContractKeyJournal extends ConflictDetectionStore[LfGlobalKey, ContractKeyJournal.Status] {

  override protected def kind: String = "contract keys"

  /** Returns the latest state for the given keys.
    * The map contains only keys that are found in the store.
    */
  override def fetchStates(keys: Iterable[LfGlobalKey])(implicit
      traceContext: TraceContext
  ): Future[Map[LfGlobalKey, ContractKeyState]]

  /** Writes the given updates as a journal entry to the store with the given time of change.
    * The updates need not be written atomically and can be written partially in case of an error.
    *
    * @return Returns [[com.digitalasset.canton.participant.store.ContractKeyJournal.ContractKeyJournalError]]
    *         if a different count with the same [[com.digitalasset.canton.participant.util.TimeOfChange]]
    *         has been written for one of the keys in the `updates` map.
    */
  def addKeyStateUpdates(updates: Map[LfGlobalKey, (Status, TimeOfChange)])(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit]

  /** Deletes all journal entries whose timestamp is before or at the given timestamp.
    * This operation need not execute atomically.
    */
  override def doPrune(beforeAndIncluding: CantonTimestamp, lastPruning: Option[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): Future[Int]

  /** Deletes all journal entries whose time of change is at least `inclusive`.
    * This operation need not execute atomically.
    */
  def deleteSince(inclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit]

  /** Returns the number of stored updates for the given contract key. */
  @VisibleForTesting
  def countUpdates(key: LfGlobalKey)(implicit traceContext: TraceContext): Future[Int]
}

object ContractKeyJournal {

  sealed trait Status extends Product with Serializable with PrettyPrinting with HasPrunable {

    /** Returns whether pruning may delete a contract key in this state */
    override def prunable: Boolean

    /** The name of the status in the database columns */
    def name: String

    // lazy val so that `kind` is initialized first in the subclasses
    final lazy val toDbPrimitive: String300 =
      // The Oracle schema allows 300 characters; Postgres and H2 map this to an enum
      String300.tryCreate(name)
  }

  object Status {
    implicit val contractKeyStatusGetResult: GetResult[Status] = GetResult(r =>
      r.nextString() match {
        case Assigned.name => Assigned
        case Unassigned.name => Unassigned
        case unknown => throw new DbDeserializationException(s"Unknown key status [$unknown]")
      }
    )

    // Use `setString` because we don't want to do Java object serialization on strings and hope
    // that the DB drivers correctly deserialize them again (fails for H2).
    // Postgresql doesn't like enums serialized as VARCHAR, so the queries must wrap bindings in
    // `CAST($... as key_status)`
    implicit val contractKeyStatusSetParameter: SetParameter[Status] = (v, pp) =>
      pp >> v.toDbPrimitive
  }

  case object Assigned extends Status {
    override val name: String = "assigned"
    override def prunable: Boolean = false
    override def pretty: Pretty[Assigned.this.type] = prettyOfObject[Assigned.this.type]
  }

  case object Unassigned extends Status {
    override val name: String = "unassigned"
    override def prunable: Boolean = true
    override def pretty: Pretty[Unassigned.this.type] = prettyOfObject[Unassigned.this.type]
  }

  type ContractKeyState = StateChange[Status]
  val ContractKeyState: StateChange.type = StateChange

  trait ContractKeyJournalError extends Product with Serializable {
    def asThrowable: Throwable
  }

  final case class InconsistentKeyAllocationStatus(
      key: LfGlobalKey,
      toc: TimeOfChange,
      oldStatus: Status,
      newStatus: Status,
  ) extends ContractKeyJournalError
      with PrettyPrinting {

    override def asThrowable: Throwable = new IllegalStateException(this.toString)

    override def pretty: Pretty[InconsistentKeyAllocationStatus] = prettyOfClass(
      param("key", _.key),
      param("time", _.toc),
      param("stored status", _.oldStatus),
      param("new status", _.newStatus),
    )
  }
}
