// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.sequencing.client.transports.replay.ReplayingSendsSequencerClientTransport
import com.digitalasset.canton.time.NonNegativeFiniteDuration

import java.nio.file.Path
import scala.concurrent.{Future, Promise}

/** Configuration for where to record sequencer sends and events to.
  * @param directory Root directory for holding all recording files
  * @param filename Filename that is initially empty and updated to a name based on the member-id at runtime.
  *                 Use [[setFilename]] to ensure this can only be set once.
  */
final case class RecordingConfig(directory: Path, filename: Option[String] = None) {
  def setFilename(value: String): RecordingConfig = filename.fold(copy(filename = Some(value))) {
    existingFilename =>
      sys.error(s"Recording filename has already been set: $existingFilename")
  }

  /** Gets the full filepath and throws if the filepath has not yet been set. */
  lazy val fullFilePath: Path = filename.fold(sys.error("filename has not been set")) { filename =>
    directory.resolve(filename)
  }
}

/** Configuration for setting up a sequencer client to replay requests or received events.
  * @param recordingConfig The path to where all recorded content is stored
  * @param action What type of replay we'll be performing
  */
final case class ReplayConfig(recordingConfig: RecordingConfig, action: ReplayAction)

object ReplayConfig {
  def apply(recordingBasePath: Path, action: ReplayAction): ReplayConfig =
    ReplayConfig(RecordingConfig(recordingBasePath), action)
}

sealed trait ReplayAction

object ReplayAction {

  /** Replay events received from the sequencer */
  case object SequencerEvents extends ReplayAction

  /** Replay sends that were made to the sequencer.
    * Tests can control the [[transports.replay.ReplayingSendsSequencerClientTransport]] once constructed
    * by waiting for the `transport` future to be completed with the transport instance.
    */
  final case class SequencerSends(
      sendTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.tryOfSeconds(20),
      private val transportP: Promise[ReplayingSendsSequencerClientTransport] =
        Promise[ReplayingSendsSequencerClientTransport](),
      usePekko: Boolean = false,
  ) extends ReplayAction {

    /** Used by the transport to notify a test that the transport is ready */
    private[client] def publishTransport(transport: ReplayingSendsSequencerClientTransport): Unit =
      transportP.success(transport)

    val transport: Future[ReplayingSendsSequencerClientTransport] = transportP.future
  }
}
