// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

/** Errors that can occur while starting the [[SequencerWriter]].
  * In the HA Sequencer Writer it is possible due to racy assignment of writer locks that the startup may fail.
  * This is by design and starting the writer can simply be retried.
  * However other errors such as discovering incorrect configuration or a unexpected commit mode will not
  * be recoverable and the writer should propagate the failure.
  */
trait WriterStartupError {

  /** If true we can retry starting the [[SequencerWriter]].
    * Otherwise the error will be propagated.
    */
  val retryable: Boolean
}

object WriterStartupError {

  /** We don't want to run the sequencer with a database commit mode that could lead to data loss,
    * so this is optionally and by default validated upon startup.
    */
  final case class BadCommitMode(message: String) extends WriterStartupError {
    override val retryable =
      false // likely set at the database or user level, so retrying won't make a difference
  }

  /** We only support running some features with certain types of databases (locking for HA requires postgres or oracle),
    * an enterprise config validation should prevent starting a node with a bad configuration however if we reach creating
    * a Writer with this config this error will be returned.
    */
  final case class DatabaseNotSupported(message: String) extends WriterStartupError {
    override val retryable = false // config or setup error
  }

  /** We have a upper limit of how many instances can run concurrently (this is defined by how many [[resource.DbLockCounters.MAX_SEQUENCER_WRITERS_AVAILABLE]]
    * are allocated). We can not start a new sequencer instance if locks for all of these counters are allocated and
    * appear online. However we can keep retrying starting on the chance that one of these will eventually go offline.
    */
  final case class AllInstanceIndexesInUse(message: String) extends WriterStartupError {
    override val retryable = true
  }

  /** The sequencer is in the process of shutting down so cannot create a new sequencer writer.
    * If we happen to attempt to recreate the writer during a shutdown this will be logged at
    * error level but can be safely ignored.
    */
  case object WriterShuttingDown extends WriterStartupError {
    override val retryable = false
  }

  /** We failed to create an exclusive storage instance for this writer. Typically this
    * will be caused by contention caused by multiple writers trying to create a lock
    * with the same instance index. This is expected and we can just safely retry
    * as a new free instance index will be assigned.
    */
  final case class FailedToCreateExclusiveStorage(message: String) extends WriterStartupError {
    override val retryable = true
  }

  /** When creating a separate database sequencer from a snapshot given by another one, there
    * was an error consuming the snapshot to initialize the initial state.
    * If this is a database problem, it can be retried.
    * Other kinds of issues would indicate that the new sequencer does not have a fresh database,
    * which is a fundamental requirement for being able to start a new sequencer fresh from a snapshot.
    */
  final case class FailedToInitializeFromSnapshot(message: String) extends WriterStartupError {
    override val retryable = true
  }

  /** Failed to reset the watermark during the recovery
    * @param message
    */
  final case class WatermarkResetError(message: String) extends WriterStartupError {
    // only possible via concurrent modification, which is only possible via misconfiguration,
    // as in unified sequencer there should only be one writer
    override val retryable = false
  }
}
