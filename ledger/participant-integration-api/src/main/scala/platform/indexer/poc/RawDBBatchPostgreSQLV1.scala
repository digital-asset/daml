// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.poc

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

import scala.collection.mutable

// TODO hurts one to look around here, the whole file is a boilerplate, including related PostgreDAO artifacts (prepared statements and execution of them)
// TODO ideas:
//   - switch to weakly/runtime-typed: probably slower, verification problematic, ugly in a strongly typed context
//   - code generation script to generate from the DAO-s the necessary scala files
//   - scala macros
//   - some persistence mapper which is not getting in the way (unlikely for this specific usage)
case class RawDBBatchPostgreSQLV1(
    eventsBatch: Option[EventsBatch],
    configurationEntriesBatch: Option[ConfigurationEntriesBatch],
    packageEntriesBatch: Option[PackageEntriesBatch],
    packagesBatch: Option[PackagesBatch],
    partiesBatch: Option[PartiesBatch],
    partyEntriesBatch: Option[PartyEntriesBatch],
    commandCompletionsBatch: Option[CommandCompletionsBatch],
    commandDeduplicationBatch: Option[CommandDeduplicationBatch],
)

class EventsBatch(
    val event_kind: Array[Int],
    val event_offset: Array[Array[Byte]],
    val transaction_id: Array[String],
    val ledger_effective_time: Array[String], // timestamp
    val command_id: Array[String],
    val workflow_id: Array[String],
    val application_id: Array[String],
    val submitters: Array[String], // '|' separated list
    val node_index: Array[java.lang.Integer],
    val event_id: Array[String],
    val contract_id: Array[String],
    val template_id: Array[String],
    val flat_event_witnesses: Array[String], // '|' separated list
    val tree_event_witnesses: Array[String], // '|' separated list
    val create_argument: Array[Array[Byte]],
    val create_signatories: Array[String], // '|' separated list
    val create_observers: Array[String], // '|' separated list
    val create_agreement_text: Array[String],
    val create_key_value: Array[Array[Byte]],
    val create_key_hash: Array[Array[Byte]],
    val exercise_choice: Array[String],
    val exercise_argument: Array[Array[Byte]],
    val exercise_result: Array[Array[Byte]],
    val exercise_actors: Array[String], // '|' separated list
    val exercise_child_event_ids: Array[String], // '|' separated list
    val event_sequential_id: Array[Long],
)

class ConfigurationEntriesBatch(
    val ledger_offset: Array[Array[Byte]],
    val recorded_at: Array[String], // timestamp
    val submission_id: Array[String],
    val typ: Array[String],
    val configuration: Array[Array[Byte]],
    val rejection_reason: Array[String],
)

class PackageEntriesBatch(
    val ledger_offset: Array[Array[Byte]],
    val recorded_at: Array[String], // timestamp
    val submission_id: Array[String],
    val typ: Array[String],
    val rejection_reason: Array[String],
)

class PackagesBatch(
    val package_id: Array[String],
    val upload_id: Array[String],
    val source_description: Array[String],
    val size: Array[Long],
    val known_since: Array[String], // timestamp
    val ledger_offset: Array[Array[Byte]],
    val _package: Array[Array[Byte]],
)

class PartiesBatch(
    val party: Array[String],
    val display_name: Array[String],
    val explicit: Array[Boolean],
    val ledger_offset: Array[Array[Byte]],
    val is_local: Array[Boolean],
)

class PartyEntriesBatch(
    val ledger_offset: Array[Array[Byte]],
    val recorded_at: Array[String], // timestamp
    val submission_id: Array[String],
    val party: Array[String],
    val display_name: Array[String],
    val typ: Array[String],
    val rejection_reason: Array[String],
    val is_local: Array[java.lang.Boolean],
)

class CommandCompletionsBatch(
    val completion_offset: Array[Array[Byte]],
    val record_time: Array[String], // timestamp
    val application_id: Array[String],
    val submitters: Array[String], // '|' separated list
    val command_id: Array[String],
    val transaction_id: Array[String],
    val status_code: Array[java.lang.Integer],
    val status_message: Array[String],
)

class CommandDeduplicationBatch(val deduplication_key: Array[String])

object RawDBBatchPostgreSQLV1 {

  case class EventsBatchBuilder(
      event_kind: mutable.ArrayBuilder[Int] = mutable.ArrayBuilder.make(),
      event_offset: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
      transaction_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      ledger_effective_time: mutable.ArrayBuilder[String] =
        mutable.ArrayBuilder.make(), // timestamp
      command_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      workflow_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      application_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      submitters: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(), // '|' separated list
      node_index: mutable.ArrayBuilder[java.lang.Integer] = mutable.ArrayBuilder.make(),
      event_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      contract_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      template_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      flat_event_witnesses: mutable.ArrayBuilder[String] =
        mutable.ArrayBuilder.make(), // '|' separated list
      tree_event_witnesses: mutable.ArrayBuilder[String] =
        mutable.ArrayBuilder.make(), // '|' separated list
      create_argument: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
      create_signatories: mutable.ArrayBuilder[String] =
        mutable.ArrayBuilder.make(), // '|' separated list
      create_observers: mutable.ArrayBuilder[String] =
        mutable.ArrayBuilder.make(), // '|' separated list
      create_agreement_text: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      create_key_value: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
      create_key_hash: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
      exercise_choice: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      exercise_argument: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
      exercise_result: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
      exercise_actors: mutable.ArrayBuilder[String] =
        mutable.ArrayBuilder.make(), // '|' separated list
      exercise_child_event_ids: mutable.ArrayBuilder[String] =
        mutable.ArrayBuilder.make(), // '|' separated list
      event_sequential_id: mutable.ArrayBuilder[Long] = mutable.ArrayBuilder.make(),
  )

  case class ConfigurationEntriesBatchBuilder(
      ledger_offset: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
      recorded_at: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(), // timestamp
      submission_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      typ: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      configuration: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
      rejection_reason: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
  )

  case class PackageEntriesBatchBuilder(
      ledger_offset: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
      recorded_at: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(), // timestamp
      submission_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      typ: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      rejection_reason: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
  )

  case class PackagesBatchBuilder(
      package_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      upload_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      source_description: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      size: mutable.ArrayBuilder[Long] = mutable.ArrayBuilder.make(),
      known_since: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(), // timestamp
      ledger_offset: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
      _package: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
  )

  case class PartiesBatchBuilder(
      party: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      display_name: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      explicit: mutable.ArrayBuilder[Boolean] = mutable.ArrayBuilder.make(),
      ledger_offset: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
      is_local: mutable.ArrayBuilder[Boolean] = mutable.ArrayBuilder.make(),
  )

  case class PartyEntriesBatchBuilder(
      ledger_offset: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
      recorded_at: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(), // timestamp
      submission_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      party: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      display_name: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      typ: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      rejection_reason: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      is_local: mutable.ArrayBuilder[java.lang.Boolean] = mutable.ArrayBuilder.make(),
  )

  case class CommandCompletionsBatchBuilder(
      completion_offset: mutable.ArrayBuilder[Array[Byte]] = mutable.ArrayBuilder.make(),
      record_time: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(), // timestamp
      application_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      submitters: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(), // '|' separated list
      command_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      transaction_id: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
      status_code: mutable.ArrayBuilder[java.lang.Integer] = mutable.ArrayBuilder.make(),
      status_message: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make(),
  )

  case class CommandDeduplicationBatchBuilder(
      deduplication_key: mutable.ArrayBuilder[String] = mutable.ArrayBuilder.make()
  )

  // WARNING! MUTABLE! not thread safe
  case class Builder() {

    import DBDTOV1._

    var eventsBatchBuilder: EventsBatchBuilder = _
    var configurationEntriesBatchBuilder: ConfigurationEntriesBatchBuilder = _
    var packageEntriesBatchBuilder: PackageEntriesBatchBuilder = _
    var packagesBatchBuilder: PackagesBatchBuilder = _
    var partiesBatchBuilder: PartiesBatchBuilder = _
    var partyEntriesBatchBuilder: PartyEntriesBatchBuilder = _
    var commandCompletionsBatchBuilder: CommandCompletionsBatchBuilder = _
    var commandDeduplicationBatchBuilder: CommandDeduplicationBatchBuilder = _

    def add(entry: DBDTOV1): Unit = {
      entry match {
        case e: Event =>
          if (eventsBatchBuilder == null) eventsBatchBuilder = EventsBatchBuilder()
          eventsBatchBuilder.event_kind += e.event_kind
          eventsBatchBuilder.event_offset += e.event_offset.orNull
          eventsBatchBuilder.transaction_id += e.transaction_id.orNull
          eventsBatchBuilder.ledger_effective_time += e.ledger_effective_time
            .map(toPGTimestampString)
            .orNull
          eventsBatchBuilder.command_id += e.command_id.orNull
          eventsBatchBuilder.workflow_id += e.workflow_id.orNull
          eventsBatchBuilder.application_id += e.application_id.orNull
          eventsBatchBuilder.submitters += e.submitters.map(encodeTextArray).orNull
          eventsBatchBuilder.node_index += e.node_index.map(x => x: java.lang.Integer).orNull
          eventsBatchBuilder.event_id += e.event_id.orNull
          eventsBatchBuilder.contract_id += e.contract_id
          eventsBatchBuilder.template_id += e.template_id.orNull
          eventsBatchBuilder.flat_event_witnesses += encodeTextArray(e.flat_event_witnesses)
          eventsBatchBuilder.tree_event_witnesses += encodeTextArray(e.tree_event_witnesses)
          eventsBatchBuilder.create_argument += e.create_argument.orNull
          eventsBatchBuilder.create_signatories += e.create_signatories.map(encodeTextArray).orNull
          eventsBatchBuilder.create_observers += e.create_observers.map(encodeTextArray).orNull
          eventsBatchBuilder.create_agreement_text += e.create_agreement_text.orNull
          eventsBatchBuilder.create_key_value += e.create_key_value.orNull
          eventsBatchBuilder.create_key_hash += e.create_key_hash.orNull
          eventsBatchBuilder.exercise_choice += e.exercise_choice.orNull
          eventsBatchBuilder.exercise_argument += e.exercise_argument.orNull
          eventsBatchBuilder.exercise_result += e.exercise_result.orNull
          eventsBatchBuilder.exercise_actors += e.exercise_actors.map(encodeTextArray).orNull
          eventsBatchBuilder.exercise_child_event_ids += e.exercise_child_event_ids
            .map(encodeTextArray)
            .orNull
          eventsBatchBuilder.event_sequential_id += 0 // this will be filled at later stage in processing

        case e: ConfigurationEntry =>
          if (configurationEntriesBatchBuilder == null)
            configurationEntriesBatchBuilder = ConfigurationEntriesBatchBuilder()
          configurationEntriesBatchBuilder.ledger_offset += e.ledger_offset
          configurationEntriesBatchBuilder.recorded_at += toPGTimestampString(e.recorded_at)
          configurationEntriesBatchBuilder.submission_id += e.submission_id
          configurationEntriesBatchBuilder.typ += e.typ
          configurationEntriesBatchBuilder.configuration += e.configuration
          configurationEntriesBatchBuilder.rejection_reason += e.rejection_reason.orNull

        case e: PackageEntry =>
          if (packageEntriesBatchBuilder == null)
            packageEntriesBatchBuilder = PackageEntriesBatchBuilder()
          packageEntriesBatchBuilder.ledger_offset += e.ledger_offset
          packageEntriesBatchBuilder.recorded_at += toPGTimestampString(e.recorded_at)
          packageEntriesBatchBuilder.submission_id += e.submission_id.orNull
          packageEntriesBatchBuilder.typ += e.typ
          packageEntriesBatchBuilder.rejection_reason += e.rejection_reason.orNull

        case e: Package =>
          if (packagesBatchBuilder == null) packagesBatchBuilder = PackagesBatchBuilder()
          packagesBatchBuilder.package_id += e.package_id
          packagesBatchBuilder.upload_id += e.upload_id
          packagesBatchBuilder.source_description += e.source_description.orNull
          packagesBatchBuilder.size += e.size
          packagesBatchBuilder.known_since += toPGTimestampString(e.known_since)
          packagesBatchBuilder.ledger_offset += e.ledger_offset
          packagesBatchBuilder._package += e._package

        case e: PartyEntry =>
          if (partyEntriesBatchBuilder == null)
            partyEntriesBatchBuilder = PartyEntriesBatchBuilder()
          partyEntriesBatchBuilder.ledger_offset += e.ledger_offset
          partyEntriesBatchBuilder.recorded_at += toPGTimestampString(e.recorded_at)
          partyEntriesBatchBuilder.submission_id += e.submission_id.orNull
          partyEntriesBatchBuilder.party += e.party.orNull
          partyEntriesBatchBuilder.display_name += e.display_name.orNull
          partyEntriesBatchBuilder.typ += e.typ
          partyEntriesBatchBuilder.rejection_reason += e.rejection_reason.orNull
          partyEntriesBatchBuilder.is_local += e.is_local.map(x => x: java.lang.Boolean).orNull

        case e: Party =>
          if (partiesBatchBuilder == null) partiesBatchBuilder = PartiesBatchBuilder()
          partiesBatchBuilder.party += e.party
          partiesBatchBuilder.display_name += e.display_name.orNull
          partiesBatchBuilder.explicit += e.explicit
          partiesBatchBuilder.ledger_offset += e.ledger_offset.orNull
          partiesBatchBuilder.is_local += e.is_local

        case e: CommandCompletion =>
          if (commandCompletionsBatchBuilder == null)
            commandCompletionsBatchBuilder = CommandCompletionsBatchBuilder()
          commandCompletionsBatchBuilder.completion_offset += e.completion_offset
          commandCompletionsBatchBuilder.record_time += toPGTimestampString(e.record_time)
          commandCompletionsBatchBuilder.application_id += e.application_id
          commandCompletionsBatchBuilder.submitters += encodeTextArray(e.submitters)
          commandCompletionsBatchBuilder.command_id += e.command_id
          commandCompletionsBatchBuilder.transaction_id += e.transaction_id.orNull
          commandCompletionsBatchBuilder.status_code += e.status_code
            .map(x => x: java.lang.Integer)
            .orNull
          commandCompletionsBatchBuilder.status_message += e.status_message.orNull

        case e: CommandDeduplication =>
          if (commandDeduplicationBatchBuilder == null)
            commandDeduplicationBatchBuilder = CommandDeduplicationBatchBuilder()
          commandDeduplicationBatchBuilder.deduplication_key += e.deduplication_key
      }
      ()
    }

    def build(): RawDBBatchPostgreSQLV1 = RawDBBatchPostgreSQLV1(
      eventsBatch = Option(eventsBatchBuilder).map(b =>
        new EventsBatch(
          event_kind = b.event_kind.result,
          event_offset = b.event_offset.result,
          transaction_id = b.transaction_id.result,
          ledger_effective_time = b.ledger_effective_time.result,
          command_id = b.command_id.result,
          workflow_id = b.workflow_id.result,
          application_id = b.application_id.result,
          submitters = b.submitters.result,
          node_index = b.node_index.result,
          event_id = b.event_id.result,
          contract_id = b.contract_id.result,
          template_id = b.template_id.result,
          flat_event_witnesses = b.flat_event_witnesses.result,
          tree_event_witnesses = b.tree_event_witnesses.result,
          create_argument = b.create_argument.result,
          create_signatories = b.create_signatories.result,
          create_observers = b.create_observers.result,
          create_agreement_text = b.create_agreement_text.result,
          create_key_value = b.create_key_value.result,
          create_key_hash = b.create_key_hash.result,
          exercise_choice = b.exercise_choice.result,
          exercise_argument = b.exercise_argument.result,
          exercise_result = b.exercise_result.result,
          exercise_actors = b.exercise_actors.result,
          exercise_child_event_ids = b.exercise_child_event_ids.result,
          event_sequential_id = b.event_sequential_id.result, // will be populated later
        )
      ),
      configurationEntriesBatch = Option(configurationEntriesBatchBuilder).map(b =>
        new ConfigurationEntriesBatch(
          ledger_offset = b.ledger_offset.result,
          recorded_at = b.recorded_at.result,
          submission_id = b.submission_id.result,
          typ = b.typ.result,
          configuration = b.configuration.result,
          rejection_reason = b.rejection_reason.result,
        )
      ),
      packageEntriesBatch = Option(packageEntriesBatchBuilder).map(b =>
        new PackageEntriesBatch(
          ledger_offset = b.ledger_offset.result,
          recorded_at = b.recorded_at.result,
          submission_id = b.submission_id.result,
          typ = b.typ.result,
          rejection_reason = b.rejection_reason.result,
        )
      ),
      packagesBatch = Option(packagesBatchBuilder).map(b =>
        new PackagesBatch(
          package_id = b.package_id.result,
          upload_id = b.upload_id.result,
          source_description = b.source_description.result,
          size = b.size.result,
          known_since = b.known_since.result,
          ledger_offset = b.ledger_offset.result,
          _package = b._package.result,
        )
      ),
      partiesBatch = Option(partiesBatchBuilder).map(b =>
        new PartiesBatch(
          party = b.party.result,
          display_name = b.display_name.result,
          explicit = b.explicit.result,
          ledger_offset = b.ledger_offset.result,
          is_local = b.is_local.result,
        )
      ),
      partyEntriesBatch = Option(partyEntriesBatchBuilder).map(b =>
        new PartyEntriesBatch(
          ledger_offset = b.ledger_offset.result,
          recorded_at = b.recorded_at.result,
          submission_id = b.submission_id.result,
          party = b.party.result,
          display_name = b.display_name.result,
          typ = b.typ.result,
          rejection_reason = b.rejection_reason.result,
          is_local = b.is_local.result,
        )
      ),
      commandCompletionsBatch = Option(commandCompletionsBatchBuilder).map(b =>
        new CommandCompletionsBatch(
          completion_offset = b.completion_offset.result,
          record_time = b.record_time.result,
          application_id = b.application_id.result,
          submitters = b.submitters.result,
          command_id = b.command_id.result,
          transaction_id = b.transaction_id.result,
          status_code = b.status_code.result,
          status_message = b.status_message.result,
        )
      ),
      commandDeduplicationBatch = Option(commandDeduplicationBatchBuilder).map(b =>
        new CommandDeduplicationBatch(
          deduplication_key = b.deduplication_key.result
        )
      ),
    )
  }

  // TODO idea: string is a bit chatty for timestamp communication, maybe we can switch to epoch somehow?
  private val PGTimestampFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss") // FIXME +micros
  def toPGTimestampString(instant: Instant): String =
    instant
      .atZone(ZoneOffset.UTC)
      .toLocalDateTime
      .format(PGTimestampFormat)

  def encodeTextArray(from: Iterable[String]): String =
    from.mkString("|") // FIXME safeguard/escape pipe chars in from
}
