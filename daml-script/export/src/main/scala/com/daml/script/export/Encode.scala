// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId, ZonedDateTime}

import com.daml.ledger.api.refinements.ApiTypes.{Choice, ContractId, Party, TemplateId}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.lf.data.Time.{Date, Timestamp}
import com.daml.script.export.TreeUtils._
import org.apache.commons.text.StringEscapeUtils
import org.typelevel.paiges.Doc

private[export] object Encode {

  def encodeExport(export: Export): Doc = {
    Doc.text("{-# LANGUAGE ApplicativeDo #-}") /
      Doc.text("module Export where") /
      Doc.text("import Daml.Script") /
      Doc.stack(export.moduleRefs.map(encodeImport(_))) /
      Doc.hardLine +
      encodePartyType(export.partyMap) /
      Doc.hardLine +
      encodeAllocateParties(export.partyMap) /
      Doc.hardLine +
      Doc.text("testExport : Script ()") /
      (Doc.text("testExport = do") /
        Doc.text("parties <- allocateParties") /
        Doc.text("export parties")).hang(2) /
      Doc.hardLine +
      encodeExportActions(export)
  }

  private def encodeExportActions(export: Export): Doc = {
    Doc.text("export : Parties -> Script ()") /
      (Doc.text("export Parties{..} = do") /
        stackNonEmpty(
          export.actions.map(encodeAction(export.partyMap, export.cidMap, export.cidRefs, _))
            :+ Doc.text("pure ()")
        )).hang(2)
  }

  private def encodeAllocateParties(partyMap: Map[Party, String]): Doc =
    Doc.text("allocateParties : Script Parties") /
      (Doc.text("allocateParties = do") /
        Doc.stack(partyMap.map { case (k, v) =>
          Doc.text(v) + Doc.text(" <- allocateParty \"") + Doc.text(Party.unwrap(k)) + Doc.text(
            "\""
          )
        }) /
        Doc.text("pure Parties{..}")).hang(2)

  private def encodePartyType(partyMap: Map[Party, String]): Doc =
    (Doc.text("data Parties = Parties with") /
      Doc.stack(partyMap.values.map(p => Doc.text(p) + Doc.text(" : Party")))).hang(2)

  private def encodeLocalDate(d: LocalDate): Doc = {
    val formatter = DateTimeFormatter.ofPattern("uuuu 'DA.Date.'MMM d")
    Doc.text("(DA.Date.date ") + Doc.text(formatter.format(d)) + Doc.text(")")
  }

  private[export] def encodeValue(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      v: Value.Sum,
  ): Doc = {
    def go(v: Value.Sum): Doc =
      v match {
        case Sum.Empty => throw new IllegalArgumentException("Empty value")
        case Sum.Record(value) => encodeRecord(partyMap, cidMap, value)
        // TODO Handle sums of products properly
        case Sum.Variant(value) =>
          parens(
            qualifyId(value.getVariantId.copy(entityName = value.constructor)) +
              Doc.text(" ") + go(value.getValue.sum)
          )
        case Sum.ContractId(c) => encodeCid(cidMap, ContractId(c))
        case Sum.List(value) =>
          list(value.elements.map(v => go(v.sum)))
        case Sum.Int64(i) => Doc.str(i)
        case Sum.Numeric(i) => Doc.str(i)
        case Sum.Text(t) =>
          // Java-escaping rules should at least be reasonably close to Daml/Haskell.
          Doc.text("\"") + Doc.text(StringEscapeUtils.escapeJava(t)) + Doc.text("\"")
        case Sum.Party(p) => encodeParty(partyMap, Party(p))
        case Sum.Bool(b) =>
          Doc.text(if (b) {
            "True"
          } else "False")
        case Sum.Unit(_) => Doc.text("()")
        case Sum.Timestamp(micros) =>
          val t: ZonedDateTime = Timestamp.assertFromLong(micros).toInstant.atZone(ZoneId.of("UTC"))
          encodeTimestamp(t)
        case Sum.Date(daysSinceEpoch) =>
          val d = Date.assertFromDaysSinceEpoch(daysSinceEpoch)
          encodeLocalDate(LocalDate.ofEpochDay(d.days.toLong))
        case Sum.Optional(value) =>
          value.value match {
            case None => Doc.text("None")
            case Some(v) => parens(Doc.text("Some ") + go(v.sum))
          }
        case Sum.Map(m) =>
          parens(
            Doc.text("TextMap.fromList ") +
              list(m.entries.map(e => pair(go(Value.Sum.Text(e.key)), go(e.getValue.sum))))
          )
        case Sum.Enum(value) =>
          qualifyId(value.getEnumId.copy(entityName = value.constructor))
        case Sum.GenMap(m) =>
          parens(
            Doc.text("Map.fromList ") + list(
              m.entries.map(e => pair(go(e.getKey.sum), go(e.getValue.sum)))
            )
          )
      }

    go(v)
  }

  private def encodeTimestamp(timestamp: ZonedDateTime): Doc = {
    val formatter = DateTimeFormatter.ofPattern("H m s")
    parens(
      Doc.text("DA.Time.time ") + encodeLocalDate(timestamp.toLocalDate) + Doc.text(" ") + Doc.text(
        formatter.format(timestamp)
      )
    )
  }

  private def quotes(v: Doc) =
    "\"" +: v :+ "\""

  private def parens(v: Doc) =
    Doc.text("(") + v + Doc.text(")")

  private def tuple(xs: Seq[Doc]) =
    parens(Doc.intercalate(Doc.text(", "), xs))

  private def brackets(v: Doc) =
    Doc.text("[") + v + Doc.text("]")

  private def list(xs: Seq[Doc]) =
    brackets(Doc.intercalate(Doc.text(", "), xs))

  private def pair(v1: Doc, v2: Doc) =
    parens(v1 + Doc.text(", ") + v2)

  private def stackNonEmpty(docs: Seq[Doc]): Doc =
    Doc.stack(docs.filter(_.nonEmpty))

  private def encodeRecord(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      r: Record,
  ): Doc = {
    if (r.fields.isEmpty) {
      qualifyId(r.getRecordId)
    } else {
      (qualifyId(r.getRecordId) + Doc.text(" with") /
        Doc.stack(r.fields.map(f => encodeField(partyMap, cidMap, f)))).nested(2)
    }
  }

  private def encodeField(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      field: RecordField,
  ): Doc =
    Doc.text(field.label) + Doc.text(" = ") + encodeValue(partyMap, cidMap, field.getValue.sum)

  private def encodeParty(partyMap: Map[Party, String], s: Party): Doc = Doc.text(partyMap(s))

  private def encodeParties(partyMap: Map[Party, String], ps: Iterable[Party]): Doc =
    Doc.text("[") +
      Doc.intercalate(Doc.text(", "), ps.map(encodeParty(partyMap, _))) +
      Doc.text("]")

  private def encodeCid(cidMap: Map[ContractId, String], cid: ContractId): Doc = {
    // LedgerStrings are strings that match the regexp ``[A-Za-z0-9#:\-_/ ]+
    Doc.text(cidMap(cid))
  }

  private def qualifyId(id: Identifier): Doc =
    Doc.text(id.moduleName) + Doc.text(".") + Doc.text(id.entityName)

  private def encodeTemplateId(id: TemplateId): Doc =
    qualifyId(TemplateId.unwrap(id))

  private def encodeChoice(choice: Choice): Doc =
    quotes(Doc.text(Choice.unwrap(choice)))

  private def encodeCmd(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      cmd: Command,
  ): Doc = cmd match {
    case CreateCommand(createdEvent) =>
      encodeCreatedEvent(partyMap, cidMap, createdEvent)
    case ExerciseCommand(exercisedEvent) =>
      val command = Doc.text("exerciseCmd")
      val cid = encodeCid(cidMap, ContractId(exercisedEvent.contractId))
      val choice = encodeValue(partyMap, cidMap, exercisedEvent.getChoiceArgument.sum)
      command & cid & choice
    case ExerciseByKeyCommand(exercisedEvent, templateId, contractKey) =>
      val command = "exerciseByKeyCmd @" +: qualifyId(templateId)
      val key = encodeValue(partyMap, cidMap, contractKey.sum)
      val choice = encodeValue(partyMap, cidMap, exercisedEvent.getChoiceArgument.sum)
      command.lineOrSpace(key).lineOrSpace(choice).nested(2)
    case CreateAndExerciseCommand(createdEvent, exercisedEvent) =>
      Doc
        .stack(
          Seq(
            Doc.text("createAndExerciseCmd"),
            encodeRecord(partyMap, cidMap, createdEvent.getCreateArguments),
            encodeValue(partyMap, cidMap, exercisedEvent.getChoiceArgument.sum),
          )
        )
        .nested(2)
  }

  private def encodeCreatedEvent(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      created: CreatedEvent,
  ): Doc =
    Doc.text("createCmd ") + encodeRecord(partyMap, cidMap, created.getCreateArguments)

  private def bindCid(cidMap: Map[ContractId, String], c: CreatedContract): Doc = {
    (Doc.text("let") & encodeCid(cidMap, c.cid) & Doc.text("=") & encodePath(
      Doc.text("tree"),
      c.path,
    )).nested(4)
  }

  private def encodeSubmitSimple(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      cidRefs: Set[ContractId],
      submit: SubmitSimple,
  ): Doc = {
    val cids = submit.simpleCommands.map(_.contractId)
    val referencedCids = cids.filter(cid => cidRefs.contains(cid))
    val (bind, returnStmt) = referencedCids match {
      case Seq() if cids.length == 1 =>
        (Doc.text("_ <- "), Doc.empty)
      case Seq() =>
        (Doc.empty, Doc.text("pure ()"))
      case Seq(cid) if cids.length == 1 =>
        val encodedCid = encodeCid(cidMap, cid)
        (encodedCid :+ " <- ", Doc.empty)
      case Seq(cid) =>
        val encodedCid = encodeCid(cidMap, cid)
        (encodedCid :+ " <- ", "pure " +: encodedCid)
      case _ =>
        val encodedCids = referencedCids.map(encodeCid(cidMap, _))
        (tuple(encodedCids) :+ " <- ", "pure " +: tuple(encodedCids))
    }
    val actions = Doc.stack(submit.simpleCommands.map { case SimpleCommand(cmd, cid) =>
      val bind = if (returnStmt.nonEmpty) {
        if (cidRefs.contains(cid)) {
          encodeCid(cidMap, cid) :+ " <- "
        } else {
          Doc.text("_ <- ")
        }
      } else {
        Doc.empty
      }
      bind + encodeCmd(partyMap, cidMap, cmd)
    })
    val body = Doc.stack(Seq(actions, returnStmt).filter(d => d.nonEmpty))
    ((bind + encodeSubmitCall(partyMap, submit) :& "do") / body).hang(2)
  }

  private[export] def encodeSetTime(timestamp: Timestamp): Doc = {
    "setTime" &: encodeTimestamp(timestamp.toInstant.atZone(ZoneId.of("UTC")))
  }

  private[export] def encodeSubmit(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      cidRefs: Set[ContractId],
      submit: Submit,
  ): Doc = {
    submit match {
      case simple: SubmitSimple => encodeSubmitSimple(partyMap, cidMap, cidRefs, simple)
      case tree: SubmitTree => encodeSubmitTree(partyMap, cidMap, cidRefs, tree)
    }
  }

  private def encodeSubmitTree(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      cidRefs: Set[ContractId],
      submit: SubmitTree,
  ): Doc = {
    val cids = treeCreatedCids(submit.tree)
    val referencedCids = cids.filter(c => cidRefs.contains(c.cid))
    val treeBind = Doc
      .stack(
        ("tree <-" &: encodeSubmitCall(partyMap, submit) :& "do") +:
          submit.commands.map(encodeCmd(partyMap, cidMap, _))
      )
      .hang(2)
    val cidBinds = referencedCids.map(bindCid(cidMap, _))
    Doc.stack(treeBind +: cidBinds)
  }

  def encodeAction(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      cidRefs: Set[ContractId],
      action: Action,
  ): Doc = {
    action match {
      case SetTime(timestamp) => encodeSetTime(timestamp)
      case submit: Submit => encodeSubmit(partyMap, cidMap, cidRefs, submit)
    }
  }

  private def encodePath(tree: Doc, path: List[Selector]): Doc = {
    Doc
      .intercalate(
        " $" +: Doc.line,
        ("fromTree" &: tree) +: path.map(encodeSelector),
      )
      .nested(2)
  }

  private def encodeSelector(selector: Selector): Doc = {
    selector match {
      case CreatedSelector(templateId, 0) =>
        "created @" +: encodeTemplateId(templateId)
      case CreatedSelector(templateId, index) =>
        "createdN @" +: encodeTemplateId(templateId) & Doc.str(index)
      case ExercisedSelector(templateId, choice, 0) =>
        "exercised @" +: encodeTemplateId(templateId) & encodeChoice(choice)
      case ExercisedSelector(templateId, choice, index) =>
        "exercisedN @" +: encodeTemplateId(templateId) & encodeChoice(choice) & Doc.str(index)
    }
  }

  private def encodeSubmitCall(partyMap: Map[Party, String], submit: Submit): Doc = {
    val parties = submit match {
      case single: SubmitSingle => encodeParty(partyMap, single.submitter)
      case multi: SubmitMulti => encodeParties(partyMap, multi.submitters)
    }
    submit match {
      case _: SubmitSimpleSingle => "submit" &: parties
      case _: SubmitSimpleMulti => "submitMulti" &: parties :& "[]"
      case _: SubmitTreeSingle => "submitTree" &: parties
      case _: SubmitTreeMulti => "submitTreeMulti" &: parties :& "[]"
    }
  }

  private def encodeImport(moduleName: String) =
    Doc.text("import qualified ") + Doc.text(moduleName)
}
