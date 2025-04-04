// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import better.files.*
import cats.syntax.either.*
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.{DbConfig, LocalNodeConfig}
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.crypto.provider.jce.JcePureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou
import com.digitalasset.canton.examples.java.iou.Amount
import com.digitalasset.canton.integration.plugins.{UseCommunityReferenceBlockSequencer, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.protocol.messages.AcsCommitment
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.sequencing.PossiblyIgnoredProtocolEvent
import com.digitalasset.canton.store.SequencedEventStore.{
  IgnoredSequencedEvent,
  LatestUpto,
  PossiblyIgnoredSequencedEvent,
}
import com.digitalasset.canton.{ProtoDeserializationError, SequencerCounter, config}
import com.google.protobuf.{ByteString, InvalidProtocolBufferException}

import java.nio.file.NoSuchFileException
import scala.jdk.CollectionConverters.*

sealed trait DumpIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .withSetup { implicit env =>
        import env.*

        // Make sure an acs commitment is created quickly. This is required for dumping.
        getInitializedSynchronizer(daName).synchronizerOwners.foreach(
          _.topology.synchronizer_parameters
            .propose_update(
              daId,
              _.update(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
            )
        )

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.health.ping(participant2)

        participants.all.dars.upload(CantonExamplesPath)
      }

  def cryptoPureApi(config: LocalNodeConfig): CryptoPureApi =
    JcePureCrypto
      .create(config.crypto, loggerFactory)
      .valueOr(err => throw new RuntimeException(s"Failed to create pure crypto api: $err"))

  "create a dump file" in { implicit env =>
    import env.*
    val dumpFile = health.dump().toFile
    // Need to convert to string to match on start and end of the filename
    val dumpFileName = dumpFile.path.getFileName.toString
    try {
      assert(dumpFile.nonEmpty)
      dumpFileName should (startWith("canton-dump") and endWith(".zip"))
    } finally {
      // clean up dump file (ensure we actually only delete a canton dump zip file)
      if (dumpFileName.startsWith("canton-dump") && dumpFileName.endsWith(".zip"))
        dumpFile.delete()
    }
  }

  "dump and load a single sequenced event" in { implicit env =>
    import env.*

    File.usingTemporaryFile(this.getClass.getSimpleName) { dumpFile =>
      val dumpFilePath = dumpFile.toString

// architecture-handbook-entry-begin: DumpLastSequencedEventToFile
      // Obtain the last event.
      val lastEvent: PossiblyIgnoredProtocolEvent =
        participant1.testing.state_inspection
          .findMessage(daName, LatestUpto(CantonTimestamp.MaxValue))
          .value
          .value

      // Dump the last event to a file.
      utils.write_to_file(lastEvent.toProtoV30, dumpFilePath)

      // Read the last event back from the file.
      val dumpedLastEventP: v30.PossiblyIgnoredSequencedEvent =
        utils.read_first_message_from_file[v30.PossiblyIgnoredSequencedEvent](
          dumpFilePath
        )

      val dumpedLastEventOrErr: Either[
        ProtoDeserializationError,
        PossiblyIgnoredProtocolEvent,
      ] =
        PossiblyIgnoredSequencedEvent
          .fromProtoV30(testedProtocolVersion, cryptoPureApi(participant1.config))(
            dumpedLastEventP
          )
// architecture-handbook-entry-end: DumpLastSequencedEventToFile

      dumpedLastEventOrErr.value shouldBe lastEvent
    }
  }

  "dump and load multiple sequenced events" in { implicit env =>
    import env.*

    File.usingTemporaryFile(this.getClass.getSimpleName) { dumpFile =>
      val dumpFilePath = dumpFile.toString

// architecture-handbook-entry-begin: DumpAllSequencedEventsToFile
      // Obtain all events.
      val allEvents: Seq[PossiblyIgnoredProtocolEvent] =
        participant1.testing.state_inspection.findMessages(daName, None, None, None).map(_.value)

      // Dump all events to a file.
      utils.write_to_file(allEvents.map(_.toProtoV30), dumpFilePath)

      // Read the dumped events back from the file.
      val dumpedEventsP: Seq[v30.PossiblyIgnoredSequencedEvent] =
        utils.read_all_messages_from_file[v30.PossiblyIgnoredSequencedEvent](
          dumpFilePath
        )

      val dumpedEventsOrErr: Seq[Either[
        ProtoDeserializationError,
        PossiblyIgnoredProtocolEvent,
      ]] =
        dumpedEventsP.map {
          PossiblyIgnoredSequencedEvent.fromProtoV30(
            testedProtocolVersion,
            cryptoPureApi(participant1.config),
          )(_)
        }
// architecture-handbook-entry-end: DumpAllSequencedEventsToFile

      dumpedEventsOrErr.map(_.value) shouldBe allEvents
    }
  }

  "dump and load ignored sequenced events" in { implicit env =>
    import env.*

    File.usingTemporaryFile(this.getClass.getSimpleName) { dumpFile =>
      val dumpFilePath = dumpFile.toString

      // Create ignored events.
      val lastEvent =
        participant1.testing.state_inspection
          .findMessage(daName, LatestUpto(CantonTimestamp.MaxValue))
          .value
          .value
      val emptyIgnoredEvent =
        IgnoredSequencedEvent(
          CantonTimestamp.Epoch.plusSeconds(97),
          SequencerCounter(33),
          None,
        )(
          nonEmptyTraceContext1
        )
      val ignoredEvents = Seq(lastEvent.asIgnoredEvent, emptyIgnoredEvent)

      // Dump all events to a file.
      utils.write_to_file(ignoredEvents.map(_.toProtoV30), dumpFilePath)

      // Read the dumped events back from the file.
      val dumpedEventsP =
        utils.read_all_messages_from_file[v30.PossiblyIgnoredSequencedEvent](dumpFilePath)

      val dumpedEventsOrErr = dumpedEventsP.map {
        PossiblyIgnoredSequencedEvent.fromProtoV30(
          testedProtocolVersion,
          cryptoPureApi(participant1.config),
        )(_)
      }

      dumpedEventsOrErr.map(_.value) shouldBe ignoredEvents
    }
  }

  "dump and load the last acs commitment" in { implicit env =>
    import env.*

    val createIou = new iou.Iou(
      participant1.adminParty.toProtoPrimitive,
      participant2.adminParty.toProtoPrimitive,
      new Amount(3.toBigDecimal, "sheep"),
      List.empty.asJava,
    ).create.commands.asScala.toSeq

    eventually() {
      participant1.ledger_api.javaapi.commands.submit(Seq(participant1.adminParty), createIou)
      participant1.commitments
        .received(
          daName,
          CantonTimestamp.MinValue.toInstant,
          CantonTimestamp.MaxValue.toInstant,
        ) should not be empty
    }

    File.usingTemporaryFile(this.getClass.getSimpleName) { dumpFile =>
      val protocolVersion = testedProtocolVersion
      val dumpFilePath = dumpFile.toString

// architecture-handbook-entry-begin: DumpAcsCommitmentToFile
      // Obtain the last acs commitment.
      val lastCommitment: AcsCommitment = participant1.commitments
        .received(
          daName,
          CantonTimestamp.MinValue.toInstant,
          CantonTimestamp.MaxValue.toInstant,
        )
        .last
        .message

      // Dump the commitment to a file.
      utils.write_to_file(
        lastCommitment.toByteString,
        dumpFilePath,
      )

      // Read the dumped commitment back from the file.
      val dumpedLastCommitmentBytes: ByteString =
        utils.read_byte_string_from_file(dumpFilePath)

      val dumpedLastCommitmentOrErr: Either[
        ProtoDeserializationError,
        AcsCommitment,
      ] = AcsCommitment.fromByteString(protocolVersion, dumpedLastCommitmentBytes)
// architecture-handbook-entry-end: DumpAcsCommitmentToFile
      dumpedLastCommitmentOrErr.value shouldBe lastCommitment
    }
  }

  "fail gracefully, if the file does not exist" in { implicit env =>
    import env.*

    a[NoSuchFileException] should be thrownBy {
      utils.read_all_messages_from_file[v30.PossiblyIgnoredSequencedEvent]("paniertes_schnitzel")
    }
  }

  "fail gracefully, if the file contents cannot be deserialized" in { implicit env =>
    import env.*

    File.usingTemporaryFile(this.getClass.getSimpleName) { f =>
      f.writeText("fanta")
      an[InvalidProtocolBufferException] should be thrownBy {
        utils.read_first_message_from_file[v30.PossiblyIgnoredSequencedEvent](f.toString)
      }
    }
  }
}

final class DumpIntegrationTestH2 extends DumpIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}
