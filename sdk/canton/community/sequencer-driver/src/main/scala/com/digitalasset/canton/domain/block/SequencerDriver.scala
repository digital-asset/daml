// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.TimeProvider
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString
import io.grpc.ServerServiceDefinition
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.{KillSwitch, Materializer}
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.{ExecutionContext, Future}

/** Factory for creating a [[SequencerDriver]] for a block-based sequencer,
  * including methods for dealing with configuration of the ledger driver.
  */
trait SequencerDriverFactory {

  /** The name of the ledger driver
    * Used in Canton configurations to specify the ledger driver as in `type = name`.
    * {{{
    *    sequencer {
    *      type = "foobar"
    *      config = { config specific to driver foobar }
    *    }
    * }}}
    */
  def name: String

  def version: Int

  /** The Scala type holding the driver-specific configuration */
  type ConfigType

  /** Parser for the driver-specific configuration. */
  def configParser: ConfigReader[ConfigType]

  /** Serializer for the driver-specific configuration.
    * @param confidential Indicates whether confidential information (e.g. passwords) should be written
    *                     in a shaded form without disclosing the actual plain information.
    */
  def configWriter(confidential: Boolean): ConfigWriter[ConfigType]

  /** Creates a new ledger driver instance
    *
    * @param config The driver-specific configuration.
    * @param nonStandardConfig Whether to be lax in enforcing certain configuration constraints such
    *                          as required external component versions.
    * @param timeProvider Time provider to obtain time readings from.
    *                     If [[usesTimeProvider]] returns true, must be used instead of system time
    *                     so that we can modify time in tests.
    * @param firstBlockHeight Initial block from which the driver will start serving the block subscription.
    *                         It will be valued when a sequencer is restarted or dynamically onboarded based
    *                         on the state of a running sequencer.
    *                         In the case of a newly started sequencer, it will be `None` and the driver
    *                         will start serving from whichever block it considers the beginning.
    *                         Given a specific `firstBlockHeight`, the sequence of blocks served by a driver
    *                         must be always exactly the same and the blocks must be consecutively numbered.*
    * @param domainId The `String` representation of the domain ID.
    * @param loggerFactory A logger factory through which all logging should be done.
    *                      Useful in tests as we can capture log entries and check them.
    */
  def create(
      config: ConfigType,
      nonStandardConfig: Boolean,
      timeProvider: TimeProvider,
      firstBlockHeight: Option[Long],
      domainId: String,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      materializer: Materializer,
  ): SequencerDriver

  /** Returns whether the driver produced by [[create]] will use the [[com.digitalasset.canton.time.TimeProvider]]
    * for generating timestamps on [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Send]] events.
    *
    * This information is used to prevent using the driver in an environment
    * that needs to control time, e.g., for testing.
    */
  def usesTimeProvider: Boolean
}

/** Defines methods for synchronizing data in blocks among all sequencer nodes of a domain.
  *
  * The write operations sequence and distribute different kinds of requests.
  * They can all be implemented by the same mechanism of sequencing a bytestring,
  * but are kept separately for legacy reasons: The Fabric and Ethereum drivers have separate
  * entry points or messages for the different request kinds.
  *
  * Sequenced requests are delivered in a stream of [[RawLedgerBlock]]s ordered by their block height.
  * The driver must make sure that all sequencer nodes of a domain receive the same stream of [[RawLedgerBlock]]s eventually.
  * That is, if one sequencer node receives a block `b` at block height `h`, then every other sequencer node has already
  * or will eventually receive `b` at height `h` unless the node fails permanently.
  * Each [[RawLedgerBlock]] contains [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent]]s
  * that correspond to the sequenced requests. The [[com.digitalasset.canton.tracing.TraceContext]]
  * passed to the write operations should be propagated into the corresponding
  * [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent]].
  *
  * All write operations are asynchronous: the [[scala.concurrent.Future]] may complete
  * before the request is actually sequenced. Under normal circumstances, the request should then also eventually
  * be delivered in a [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent]],
  * but there is no guarantee. A write operation may fail with an exception; in that case,
  * the request must not be sequenced. Every write operation may result in at most one corresponding
  * [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent]].
  *
  * The [[SequencerDriver]] is responsible for assigning timestamps to
  * [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Send]] events.
  * The assigned timestamps must be close to real-world time given the trust assumptions of the [[SequencerDriver]].
  * For example, assume that the clocks among all honest sequencer nodes are synchronized up to a given `skew`.
  * Let `ts0` be the local sequencer's time when an honest sequencer node calls [[SequencerDriver.send]].
  * Let `ts1` be the local sequencer's time when it receives the corresponding
  * [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Send]].
  * Then the assigned timestamp `ts` must satisfy `ts0 - skew <= ts <= ts1 + skew`.
  *
  * Several [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Send]] events may
  * have the same timestamp or go backwards, as long as they remain close to real-world time.
  */
trait SequencerDriver extends AutoCloseable {

  // Admin end points

  /** Services for administering the ledger driver.
    * These services will be exposed on the sequencer node's admin API endpoint.
    */
  def adminServices: Seq[ServerServiceDefinition]

  // Write operations

  /** Register the given member.
    * Results in a [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.AddMember]].
    */
  def registerMember(member: String)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Distribute an acknowledgement request.
    * Results in a [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Acknowledgment]].
    */
  def acknowledge(acknowledgement: ByteString)(implicit traceContext: TraceContext): Future[Unit]

  /** Send a submission request.
    * Results in a [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Send]].
    */
  def send(request: ByteString)(implicit traceContext: TraceContext): Future[Unit]

  // Read operations

  /** Delivers a stream of blocks starting with `firstBlockHeight` (if specified in the factory call)
    * or the first block serveable.
    * Block heights must be consecutive.
    *
    * If `firstBlockHeight` refers to a block whose sequencing number the sequencer node has not yet observed,
    * returns a source that will eventually serve that block when it gets created.
    *
    * Must succeed if an earlier call to `subscribe` delivered a block with height `firstBlockHeight`
    * unless the block has been pruned in between, in which case it fails
    *
    * This method will be called only once, so implementations do not have to try to create separate sources
    * on every call to this method. It is acceptable to for the driver to have one internal source and just return
    * it here.
    */
  def subscribe()(implicit
      traceContext: TraceContext
  ): Source[RawLedgerBlock, KillSwitch]

  // Operability

  def health(implicit traceContext: TraceContext): Future[SequencerDriverHealthStatus]

}

object SequencerDriver {
  val DefaultInitialBlockHeight = -1L

  // domain bootstrap will load this version of driver; bump for incompatible change
  val DriverApiVersion = 1
}

/** A block that a [[SequencerDriver]] delivers to the sequencer node.
  *
  * @param blockHeight The height of the block. Block heights must be consecutive.
  * @param events The events in the given block.
  */
final case class RawLedgerBlock(
    blockHeight: Long,
    events: Seq[Traced[RawLedgerBlock.RawBlockEvent]],
)

object RawLedgerBlock {
  sealed trait RawBlockEvent extends Product with Serializable

  object RawBlockEvent {
    final case class Send(
        request: ByteString,
        microsecondsSinceEpoch: Long,
    ) extends RawBlockEvent

    final case class AddMember(member: String) extends RawBlockEvent

    final case class Acknowledgment(acknowledgement: ByteString) extends RawBlockEvent
  }
}

final case class SequencerDriverHealthStatus(
    isActive: Boolean,
    description: Option[String],
)
