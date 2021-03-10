// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId, ZonedDateTime}

import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction.TreeEvent.Kind
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.daml.lf.data.Time.{Date, Timestamp}
import com.daml.script.dump.TreeUtils._
import org.apache.commons.text.StringEscapeUtils
import org.typelevel.paiges.Doc
import scalaz.std.iterable._
import scalaz.std.set._
import scalaz.syntax.foldable._

private[dump] object Encode {
  def encodeTransactionTreeStream(
      acs: Map[ContractId, CreatedEvent],
      trees: Seq[TransactionTree],
      acsBatchSize: Int,
  ): Doc = {
    val parties = partiesInContracts(acs.values) ++ trees.foldMap(partiesInTree(_))
    val partyMap = partyMapping(parties)

    val acsCidRefs = acs.values.foldMap(createdReferencedCids)
    val treeCidRefs = trees.foldMap(treeReferencedCids)
    val cidRefs = acsCidRefs ++ treeCidRefs

    val unknownCidRefs = acsCidRefs -- acs.keySet
    if (unknownCidRefs.nonEmpty) {
      // TODO[AH] Support this once the ledger has better support for exposing such "hidden" contracts.
      //   Be it archived or divulged contracts.
      throw new RuntimeException(
        s"Encountered archived contracts referenced by active contracts: ${unknownCidRefs.mkString(", ")}"
      )
    }

    val sortedAcs = topoSortAcs(acs)
    val acsCids =
      sortedAcs.map(ev => CreatedContract(ContractId(ev.contractId), ev.getTemplateId, Nil))
    val treeCids = trees.map(treeCreatedCids(_))
    val cidMap = cidMapping(acsCids +: treeCids, cidRefs)

    val refs =
      acs.values.foldMap(ev => valueRefs(Sum.Record(ev.getCreateArguments))) ++ trees.foldMap(
        treeRefs(_)
      )
    val moduleRefs = refs.map(_.moduleName).toSet
    Doc.text("{-# LANGUAGE ApplicativeDo #-}") /
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
        stackNonEmpty(
          encodeACS(partyMap, cidMap, cidRefs, sortedAcs, batchSize = acsBatchSize)
            +: trees.map(t => encodeTree(partyMap, cidMap, cidRefs, t))
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
    val formatter = DateTimeFormatter.ofPattern("uuuu MMM d")
    Doc.text("(date ") + Doc.text(formatter.format(d)) + Doc.text(")")
  }

  private[dump] def encodeValue(
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

  private def encodeEv(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      ev: TreeEvent.Kind,
  ): Doc = ev match {
    case TreeEvent.Kind.Created(created) =>
      encodeCreatedEvent(partyMap, cidMap, created)
    case TreeEvent.Kind.Exercised(exercised @ _) =>
      Doc.text("exerciseCmd ") + encodeCid(
        cidMap,
        ContractId(exercised.contractId),
      ) + Doc.space + encodeValue(
        partyMap,
        cidMap,
        exercised.getChoiceArgument.sum,
      )
    case TreeEvent.Kind.Empty => throw new IllegalArgumentException("Unknown tree event")
  }

  private def encodeCreatedEvent(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      created: CreatedEvent,
  ): Doc =
    Doc.text("createCmd ") + encodeRecord(partyMap, cidMap, created.getCreateArguments)

  private def bindCid(cidMap: Map[ContractId, String], c: CreatedContract): Doc = {
    Doc.text("let ") + encodeCid(cidMap, c.cid) + Doc.text(" = createdCid @") +
      qualifyId(c.tplId) + Doc.text(" [") + Doc.intercalate(
        Doc.text(", "),
        c.path.map(encodeSelector(_)),
      ) + Doc.text("] tree")
  }

  private[dump] def encodeSubmitCreatedEvents(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      cidRefs: Set[ContractId],
      evs: Seq[CreatedEvent],
  ): Doc = {
    encodeSubmitEvents(partyMap, cidMap, cidRefs, evs.map(Kind.Created(_)))
  }

  private[dump] def encodeSubmitEvents(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      cidRefs: Set[ContractId],
      evs: Seq[TreeEvent.Kind],
  ): Doc = {
    val submitters = evs.flatMap(ev => evParties(ev)).toSet
    val cids = ContractId.subst(evs.flatMap {
      case Kind.Created(value) => Some(value.contractId)
      case Kind.Exercised(value) =>
        value.exerciseResult match {
          case Some(value) =>
            value.sum match {
              case Sum.ContractId(value) => Some(value)
              case _ => None
            }
          case None => None
        }
      case Kind.Empty => None
    })
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
    val actions = Doc.stack(evs.zip(cids).map { case (ev, cid) =>
      val bind = if (returnStmt.nonEmpty) {
        if (cidRefs.contains(cid)) {
          encodeCid(cidMap, cid) :+ " <- "
        } else {
          Doc.text("_ <- ")
        }
      } else {
        Doc.empty
      }
      bind + encodeEv(partyMap, cidMap, ev)
    })
    val body = Doc.stack(Seq(actions, returnStmt).filter(d => d.nonEmpty))
    ((bind + submit(partyMap, submitters, isTree = false) :+ " do") / body).hang(2)
  }

  private[dump] def encodeACS(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      cidRefs: Set[ContractId],
      acs: Seq[CreatedEvent],
      batchSize: Int,
  ): Doc = {
    Doc.stack(
      acs
        .grouped(batchSize)
        .map(encodeSubmitCreatedEvents(partyMap, cidMap, cidRefs, _))
        .toSeq
    )
  }

  private[dump] def encodeTree(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      cidRefs: Set[ContractId],
      tree: TransactionTree,
  ): Doc = {
    val rootEvs = tree.rootEventIds.map(tree.eventsById(_).kind)
    val createsOnly: Option[List[TreeEvent.Kind]] = {
      import scalaz.Scalaz._
      rootEvs.toList.traverse {
        case created @ Kind.Created(_) => Some(created)
        case exercised @ Kind.Exercised(value) =>
          for {
            result <- value.exerciseResult
            resultCid <- result.sum match {
              case Sum.ContractId(value) => Some(ContractId(value))
              case _ => None
            }
            creates = {
              var creates = Seq.empty[CreatedEvent]
              traverseEventInTree(exercised, tree) { case (_, ev) =>
                ev match {
                  case Kind.Created(value) => creates ++= Seq(value)
                  case _ =>
                }
              }
              creates
            }
            created <- creates match {
              case Seq(created) if ContractId(created.contractId) == resultCid => Some(exercised)
              case _ => None
            }
          } yield created
        case _ => None
      }
    }
    createsOnly match {
      case Some(evs) =>
        encodeSubmitEvents(partyMap, cidMap, cidRefs, evs)
      case None => {
        val submitters = rootEvs.flatMap(evParties(_)).toSet
        val cids = treeCreatedCids(tree)
        val referencedCids = cids.filter(c => cidRefs.contains(c.cid))
        val treeBind =
          (("tree <- " +: submit(partyMap, submitters, isTree = true) :+ " do") /
            Doc.stack(rootEvs.map(ev => encodeEv(partyMap, cidMap, ev)))).hang(2)
        val cidBinds = referencedCids.map(bindCid(cidMap, _))
        Doc.stack(treeBind +: cidBinds)
      }
    }
  }

  private def encodeSelector(selector: Selector): Doc = Doc.str(selector.i)

  private def submit(partyMap: Map[Party, String], submitters: Set[Party], isTree: Boolean): Doc = {
    val tree = if (isTree) { Doc.text("Tree") }
    else { Doc.empty }
    submitters.toList match {
      case ::(submitter, Nil) => "submit" +: tree & encodeParty(partyMap, submitter)
      case _ => "submit" +: tree :+ "Multi" & encodeParties(partyMap, submitters) :+ " []"
    }
  }

  private def encodeImport(moduleName: String) =
    Doc.text("import qualified ") + Doc.text(moduleName)

  private def partyMapping(parties: Set[Party]): Map[Party, String] = {
    // - PartyIdStrings are strings that match the regexp ``[A-Za-z0-9:\-_ ]+``.
    def safeParty(p: String) =
      Seq(":", "-", "_", " ").foldLeft(p) { case (p, x) => p.replace(x, "") }.toLowerCase
    // Map from original party id to Daml identifier
    var partyMap: Map[Party, String] = Map.empty
    // Number of times we’ve gotten the same result from safeParty, we resolve collisions with a suffix.
    var usedParties: Map[String, Int] = Map.empty
    parties.foreach { p =>
      val r = safeParty(Party.unwrap(p))
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

  private def cidMapping(
      cids: Seq[Seq[CreatedContract]],
      cidRefs: Set[ContractId],
  ): Map[ContractId, String] = {
    def lowerFirst(s: String) =
      if (s.isEmpty) {
        s
      } else {
        s.head.toLower.toString + s.tail
      }
    cids.view.zipWithIndex.flatMap { case (cs, treeIndex) =>
      cs.view.zipWithIndex.collect {
        case (c, i) if cidRefs.contains(c.cid) =>
          c.cid -> s"${lowerFirst(c.tplId.entityName)}_${treeIndex}_$i"
      }
    }.toMap
  }
}
