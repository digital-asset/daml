// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId, ZonedDateTime}

import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.lf.data.Time.{Date, Timestamp}
import com.daml.script.dump.TreeUtils._
import org.apache.commons.text.StringEscapeUtils
import org.typelevel.paiges.Doc
import scalaz.std.list._
import scalaz.std.set._
import scalaz.syntax.foldable._

private[dump] object Encode {
  def encodeTransactionTreeStream(trees: Seq[TransactionTree]): Doc = {
    val parties = trees.toList.foldMap(partiesInTree(_))
    val partyMap = partyMapping(parties)
    val cids = trees.map(treeCids(_))
    val cidMap = cidMapping(cids)
    val refs = trees.toList.foldMap(treeRefs(_))
    val moduleRefs = refs.map(_.moduleName).toSet
    Doc.text("module Dump where") /
      Doc.text("import Daml.Script") /
      Doc.stack(moduleRefs.map(encodeImport(_))) /
      Doc.hardLine +
      encodePartyType(partyMap) /
      Doc.hardLine +
      encodeAllocateParties(partyMap) /
      Doc.hardLine +
      Doc.text("testDump : Script ()") /
      (Doc.text("testDump = do") /
        Doc.text("parties <- allocateParties") /
        Doc.text("dump parties")).hang(2) /
      Doc.hardLine +
      Doc.text("dump : Parties -> Script ()") /
      (Doc.text("dump Parties{..} = do") /
        Doc.stack(trees.map(t => encodeTree(partyMap, cidMap, t))) /
        Doc.text("pure ()")).hang(2)
  }

  private def encodeAllocateParties(partyMap: Map[String, String]): Doc =
    Doc.text("allocateParties : Script Parties") /
      (Doc.text("allocateParties = do") /
        Doc.stack(partyMap.map { case (k, v) =>
          Doc.text(v) + Doc.text(" <- allocateParty \"") + Doc.text(k) + Doc.text("\"")
        }) /
        Doc.text("pure Parties{..}")).hang(2)

  private def encodePartyType(partyMap: Map[String, String]): Doc =
    (Doc.text("data Parties = Parties with") /
      Doc.stack(partyMap.values.map(p => Doc.text(p) + Doc.text(" : Party")))).hang(2)

  private def encodeLocalDate(d: LocalDate): Doc = {
    val formatter = DateTimeFormatter.ofPattern("uuuu MMM d")
    Doc.text("(date ") + Doc.text(formatter.format(d)) + Doc.text(")")
  }

  private[dump] def encodeValue(
      partyMap: Map[String, String],
      cidMap: Map[String, String],
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
        case Sum.ContractId(c) => encodeCid(cidMap, c)
        case Sum.List(value) =>
          list(value.elements.map(v => go(v.sum)))
        case Sum.Int64(i) => Doc.str(i)
        case Sum.Numeric(i) => Doc.str(i)
        case Sum.Text(t) =>
          // Java-escaping rules should at least be reasonably close to Daml/Haskell.
          Doc.text("\"") + Doc.text(StringEscapeUtils.escapeJava(t)) + Doc.text("\"")
        case Sum.Party(p) => encodeParty(partyMap, p)
        case Sum.Bool(b) =>
          Doc.text(if (b) {
            "True"
          } else "False")
        case Sum.Unit(_) => Doc.text("()")
        case Sum.Timestamp(micros) =>
          val t: ZonedDateTime = Timestamp.assertFromLong(micros).toInstant.atZone(ZoneId.of("UTC"))
          val formatter = DateTimeFormatter.ofPattern("H m s")
          parens(
            Doc.text("time ") + encodeLocalDate(t.toLocalDate) + Doc.text(" ") + Doc.text(
              formatter.format(t)
            )
          )
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

  private def parens(v: Doc) =
    Doc.text("(") + v + Doc.text(")")

  private def brackets(v: Doc) =
    Doc.text("[") + v + Doc.text("]")

  private def list(xs: Seq[Doc]) =
    brackets(Doc.intercalate(Doc.text(", "), xs))

  private def pair(v1: Doc, v2: Doc) =
    parens(v1 + Doc.text(", ") + v2)

  private def encodeRecord(
      partyMap: Map[String, String],
      cidMap: Map[String, String],
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
      partyMap: Map[String, String],
      cidMap: Map[String, String],
      field: RecordField,
  ): Doc =
    Doc.text(field.label) + Doc.text(" = ") + encodeValue(partyMap, cidMap, field.getValue.sum)

  private def encodeParty(partyMap: Map[String, String], s: String): Doc = Doc.text(partyMap(s))

  private def encodeParties(partyMap: Map[String, String], ps: Iterable[String]): Doc =
    Doc.text("[") +
      Doc.intercalate(Doc.text(", "), ps.map(encodeParty(partyMap, _))) +
      Doc.text("]")

  private def encodeCid(cidMap: Map[String, String], cid: String): Doc = {
    // LedgerStrings are strings that match the regexp ``[A-Za-z0-9#:\-_/ ]+
    Doc.text(cidMap(cid))
  }

  private def qualifyId(id: Identifier): Doc =
    Doc.text(id.moduleName) + Doc.text(".") + Doc.text(id.entityName)

  private def encodeEv(
      partyMap: Map[String, String],
      cidMap: Map[String, String],
      ev: TreeEvent.Kind,
  ): Doc = ev match {
    case TreeEvent.Kind.Created(created) =>
      Doc.text("createCmd ") + encodeRecord(partyMap, cidMap, created.getCreateArguments)
    case TreeEvent.Kind.Exercised(exercised @ _) =>
      Doc.text("exerciseCmd ") + encodeCid(cidMap, exercised.contractId) + Doc.space + encodeValue(
        partyMap,
        cidMap,
        exercised.getChoiceArgument.sum,
      )
    case TreeEvent.Kind.Empty => throw new IllegalArgumentException("Unknown tree event")
  }

  private def bindCid(cidMap: Map[String, String], c: CreatedContract): Doc = {
    Doc.text("let ") + encodeCid(cidMap, c.cid) + Doc.text(" = createdCid @") +
      qualifyId(c.tplId) + Doc.text(" [") + Doc.intercalate(
        Doc.text(", "),
        c.path.map(encodeSelector(_)),
      ) + Doc.text("] tree")
  }

  private def encodeTree(
      partyMap: Map[String, String],
      cidMap: Map[String, String],
      tree: TransactionTree,
  ): Doc = {
    val rootEvs = tree.rootEventIds.map(tree.eventsById(_).kind)
    val submitters = rootEvs.flatMap(evParties(_)).toSet
    val cids = treeCids(tree)
    (Doc.text("tree <- submitTreeMulti ") + encodeParties(partyMap, submitters) + Doc.text(
      " [] do"
    ) /
      Doc.stack(rootEvs.map(ev => encodeEv(partyMap, cidMap, ev)))).hang(2) /
      Doc.stack(cids.map(bindCid(cidMap, _)))
  }

  private def encodeSelector(selector: Selector): Doc = Doc.str(selector.i)

  private def encodeImport(moduleName: String) =
    Doc.text("import qualified ") + Doc.text(moduleName)

  private def partyMapping(parties: Set[String]): Map[String, String] = {
    // - PartyIdStrings are strings that match the regexp ``[A-Za-z0-9:\-_ ]+``.
    def safeParty(p: String) =
      Seq(":", "-", "_", " ").foldLeft(p) { case (p, x) => p.replace(x, "") }.toLowerCase
    // Map from original party id to Daml identifier
    var partyMap: Map[String, String] = Map.empty
    // Number of times we’ve gotten the same result from safeParty, we resolve collisions with a suffix.
    var usedParties: Map[String, Int] = Map.empty
    parties.foreach { p =>
      val r = safeParty(p)
      usedParties.get(r) match {
        case None =>
          partyMap += p -> s"${r}_0"
          usedParties += r -> 0
        case Some(value) =>
          partyMap += p -> s"${r}_${value + 1}"
          usedParties += r -> (value + 1)
      }
    }
    partyMap
  }

  private def cidMapping(cids: Seq[Seq[CreatedContract]]): Map[String, String] = {
    def lowerFirst(s: String) =
      if (s.isEmpty) {
        s
      } else {
        s.head.toLower.toString + s.tail
      }
    cids.view.zipWithIndex.flatMap { case (cs, treeIndex) =>
      cs.view.zipWithIndex.map { case (c, i) =>
        c.cid -> s"${lowerFirst(c.tplId.entityName)}_${treeIndex}_$i"
      }
    }.toMap
  }
}
