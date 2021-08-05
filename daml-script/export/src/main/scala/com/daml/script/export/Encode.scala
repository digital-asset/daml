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
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.script.export.TreeUtils._
import org.apache.commons.text.StringEscapeUtils
import org.typelevel.paiges.Doc
import spray.json._

private[export] object Encode {

  def encodeArgs(export: Export): JsObject = {
    JsObject(
      "parties" -> JsObject(export.partyMap.keys.map { case Party(party) =>
        party -> JsString(party)
      }.toMap),
      "contracts" -> JsObject(export.unknownCids.map { case ContractId(c) =>
        c -> JsString(c)
      }.toMap),
    )
  }

  def encodeExport(export: Export): Doc = {
    encodeModuleHeader(export.moduleRefs) /
      Doc.hardLine +
      encodePartyType() /
      Doc.hardLine +
      encodeLookupParty() /
      Doc.hardLine +
      encodeAllocateParties(export.partyMap) /
      Doc.hardLine +
      encodeContractsType() /
      Doc.hardLine +
      encodeLookupContract() /
      Doc.hardLine +
      encodeArgsType() /
      Doc.hardLine +
      encodeTestExport() /
      Doc.hardLine +
      encodeExportActions(export) /
      encodeLegacyInstances(export.legacyTemplates)
  }

  private def encodeLegacyInstances(templates: Map[TemplateId, Ast.GenTemplate[Ast.Expr]]): Doc = {
    if (templates.isEmpty) {
      Doc.empty
    } else {
      Doc.hardLine +
        Doc.stack(
          templates.map { case (tplId, tpl) =>
            System.err.println(s"$tplId: ${tpl.key.map(_.typ)}")
            val docTplId = encodeTemplateId(tplId)
            Doc.stack(
              Seq(
                "instance HasTemplateTypeRep" &: docTplId :& "where _templateTypeRep = GHC.Types.primitive @\"ETemplateTypeRep\"",
                "instance HasToAnyTemplate" &: docTplId :& "where _toAnyTemplate = GHC.Types.primitive @\"EToAnyTemplate\"",
              ) ++ tpl.key.toList.map(key =>
                Doc.text("instance HasToAnyContractKey") & docTplId & parens(
                  encodeType(key.typ)
                ) & Doc.text("where _toAnyContractKey = GHC.Types.primitive @\"EToAnyContractKey\"")
              ) ++
                tpl.choices.map { case (choiceName, choice) =>
                  val choiceDoc = if (choiceName == "Archive") {
                    Doc.text("Archive")
                  } else {
                    val choiceId = Identifier()
                      .withPackageId(TemplateId.unwrap(tplId).packageId)
                      .withModuleName(TemplateId.unwrap(tplId).moduleName)
                      .withEntityName(choiceName)
                    qualifyId(choiceId)
                  }
                  System.err.println(s"$choiceName: ${choice.returnType}")
                  // TODO[AH] Handle precendence in encodeAstType
                  Doc.text("instance HasToAnyChoice") & docTplId & choiceDoc & parens(
                    encodeType(choice.returnType)
                  ) & Doc.text(
                    "where _toAnyChoice = GHC.Types.primitive @\"EToAnyChoice\""
                  )
                }
            )
          }
        )
    }
  }

  private def encodeExportActions(export: Export): Doc = {
    Doc.text("-- | The Daml ledger export.") /
      Doc.text("export : Args -> Script ()") /
      (Doc.text("export Args{parties, contracts} = do") /
        stackNonEmpty(
          export.partyMap.map(Function.tupled(encodePartyBinding)).toSeq ++ export.actions.map(
            encodeAction(export.partyMap, export.cidMap, export.cidRefs, export.pkgLfVersions, _)
          ) :+ Doc.text("pure ()")
        )).hang(2)
  }

  private def encodePartyBinding(party: Party, binding: String): Doc =
    s"let $binding = lookupParty" &: quotes(Doc.text(Party.unwrap(party))) :& "parties"

  private def encodeTestExport(): Doc =
    Doc.text("-- | Test 'export' with freshly allocated parties and") /
      Doc.text("-- no replacements for missing contract ids.") /
      Doc.text("testExport : Script ()") /
      (Doc.text("testExport = do") /
        Doc.text("parties <- allocateParties") /
        Doc.text("let contracts = DA.TextMap.empty") /
        Doc.text("export Args with ..")).nested(2)

  private def encodeArgsType(): Doc =
    Doc.text("-- | Arguments to 'export'. See 'Parties' and 'Contracts' for details.") /
      (Doc.text("data Args = Args with") /
        Doc.text("parties : Parties") /
        Doc.text("contracts : Contracts")).nested(2)

  private def encodeLookupContract(): Doc =
    Doc.text("-- | Look-up a replacement for a missing contract id. Fails if none is found.") /
      Doc.text("lookupContract : DA.Stack.HasCallStack => Text -> Contracts -> ContractId a") /
      (Doc.text("lookupContract old contracts =") /
        (Doc.text("case DA.TextMap.lookup old contracts of") /
          Doc.text("None -> error (\"Missing contract id \" <> old)") /
          Doc.text("Some new -> coerceContractId new")).nested(2)).nested(2)

  private def encodeContractsType(): Doc =
    Doc.text("-- | Mapping from missing contract ids to replacement contract ids.") /
      Doc.text("--") /
      Doc.text("-- You can provide replacement contract ids in an input file to") /
      Doc.text("-- the @--input-file@ argument of @daml script@, or you can provide") /
      Doc.text("-- replacements from within Daml script.") /
      Doc.text("--") /
      Doc.text("-- >>> (replacement, _):_ <- query @T alice_0") /
      Doc.text("-- >>> let args = Args with") /
      Doc.text("-- >>>   parties = Parties with alice_0") /
      Doc.text("-- >>>   contracts = DA.TextMap.fromList [(\"00737...\", replacement)]") /
      Doc.text("-- >>> export args") /
      Doc.text("type Contracts = DA.TextMap.TextMap (ContractId ())")

  private def encodeAllocateParties(partyMap: Map[Party, String]): Doc =
    Doc.text("-- | Allocates fresh parties from the party management service.") /
      Doc.text("allocateParties : Script Parties") /
      Doc.text("allocateParties = DA.Traversable.mapA allocateParty (DA.TextMap.fromList") /
      ("[" &: Doc.intercalate(
        Doc.hardLine :+ ", ",
        partyMap.keys.map { case Party(p) =>
          val party = quotes(Doc.text(p))
          tuple(Seq(party, party))
        },
      ) :& "])").indent(2)

  private def encodeLookupParty(): Doc =
    Doc.text("-- | Look-up a party based on the party name in the original ledger state.") /
      Doc.text("lookupParty : DA.Stack.HasCallStack => Text -> Parties -> Party") /
      (Doc.text("lookupParty old parties =") /
        (Doc.text("case DA.TextMap.lookup old parties of") /
          Doc.text("None -> error (\"Missing party \" <> old)") /
          Doc.text("Some new -> new")).nested(2)).nested(2)

  private def encodePartyType(): Doc =
    Doc.text("-- | Mapping from party names in the original ledger state ") /
      Doc.text("-- to parties to be used in 'export'.") /
      Doc.text("type Parties = DA.TextMap.TextMap Party")

  private def encodeModuleHeader(moduleRefs: Set[String]): Doc =
    Doc.text("{-# LANGUAGE ApplicativeDo #-}") /
      Doc.text("module Export where") /
      Doc.text("import Daml.Script") /
      Doc.stack(moduleRefs.map(encodeImport(_)))

  private def encodeLocalDate(d: LocalDate): Doc = {
    val formatter = DateTimeFormatter.ofPattern("uuuu 'DA.Date.'MMM d")
    Doc.text("(DA.Date.date ") + Doc.text(formatter.format(d)) + Doc.text(")")
  }

  private[export] def encodeValue(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      v: Value.Sum,
  ): Doc = {
    def isTupleRecord(recordId: Identifier): Boolean = {
      val daTypesId = "40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7"
      recordId.packageId == daTypesId && recordId.moduleName == "DA.Types" && recordId.entityName
        .startsWith("Tuple")
    }
    def go(v: Value.Sum): Doc =
      v match {
        case Sum.Empty => throw new IllegalArgumentException("Empty value")
        case Sum.Record(value) if isTupleRecord(value.getRecordId) =>
          tuple(value.fields.map(f => go(f.getValue.sum)))
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

  private[export] def encodeType(ty: Ast.Type): Doc = {
    def unfoldApp(app: Ast.TApp): (Ast.Type, Seq[Ast.Type]) = {
      def go(f: Ast.Type, args: Seq[Ast.Type]): (Ast.Type, Seq[Ast.Type]) = {
        f match {
          case Ast.TApp(tyfun, arg) => go(tyfun, arg +: args)
          case _ => (f, args)
        }
      }
      go(app, Seq())
    }
    ty match {
      case Ast.TVar(name) => Doc.text(name)
      case Ast.TNat(n) => Doc.text(s"$n")
      case Ast.TSynApp(tysyn, args) =>
        qualifyId(
          Identifier()
            .withPackageId(tysyn.packageId)
            .withModuleName(tysyn.qualifiedName.module.dottedName)
            .withEntityName(tysyn.qualifiedName.name.dottedName)
        ) & Doc.intercalate(Doc.space, args.toSeq.map(ty => parens(encodeType(ty))))
      case Ast.TTyCon(tycon) =>
        qualifyId(
          Identifier()
            .withPackageId(tycon.packageId)
            .withModuleName(tycon.qualifiedName.module.dottedName)
            .withEntityName(tycon.qualifiedName.name.dottedName)
        )
      case Ast.TBuiltin(bt) =>
        Doc.text(bt match {
          case Ast.BTInt64 => "Int"
          case Ast.BTNumeric => "Numeric"
          case Ast.BTText => "Text"
          case Ast.BTTimestamp => "Time"
          case Ast.BTParty => "Party"
          case Ast.BTUnit => "()"
          case Ast.BTBool => "Bool"
          case Ast.BTList => "[]"
          case Ast.BTOptional => "Optional"
          case Ast.BTTextMap => "DA.TextMap.TextMap"
          case Ast.BTGenMap => "DA.Map.Map"
          case Ast.BTUpdate => "Update"
          case Ast.BTScenario => "Scenario"
          case Ast.BTDate => "Date"
          case Ast.BTContractId => "ContractId"
          case Ast.BTArrow => "(->)"
          case Ast.BTAny =>
            "Any" // TODO[AH] DA.Internal.LF.Any is not importable. How to handle this?
          case Ast.BTTypeRep => "TypeRep"
          case Ast.BTAnyException => "AnyException"
          case Ast.BTRoundingMode => "RoundingMode"
          case Ast.BTBigNumeric => "BigNumeric"
        })
      case app @ Ast.TApp(_, _) =>
        unfoldApp(app) match {
          case (Ast.TTyCon(tycon), args)
              if tycon.qualifiedName.module.dottedName == "DA.Types" && tycon.qualifiedName.name.dottedName
                .startsWith("Tuple") =>
            tuple(args.map(ty => encodeType(ty)))
          case (tyfun, args) =>
            encodeType(tyfun) & Doc.intercalate(
              Doc.space,
              args.toSeq.map(ty => parens(encodeType(ty))),
            )
        }
      case Ast.TForall(binder, body) =>
        Doc.text("forall") & Doc.text(binder._1) & encodeType(body)
      case Ast.TStruct(_) => Doc.empty // TODO[AH] Not needed for type signatures
    }
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
    cidMap.get(cid) match {
      case Some(value) => Doc.text(value)
      case None => parens("lookupContract" &: quotes(Doc.text(cid.toString)) :& "contracts")
    }
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
      pkgLfVersions: Map[String, LanguageVersion],
      cmd: Command,
  ): Doc = cmd match {
    case CreateCommand(createdEvent) =>
      encodeCreatedEvent(partyMap, cidMap, pkgLfVersions, createdEvent)
    case ExerciseCommand(exercisedEvent) =>
      val pkgId = exercisedEvent.getTemplateId.packageId
      val cid = encodeCid(cidMap, ContractId(exercisedEvent.contractId))
      val choice = encodeValue(partyMap, cidMap, exercisedEvent.getChoiceArgument.sum)
      if (isPreLf_1_8(pkgId, pkgLfVersions)) {
        val command = Doc.text("internalExerciseCmd")
        val templateId = "@" +: encodeTemplateId(TemplateId(exercisedEvent.getTemplateId))
        val typeRepArg = parens("templateTypeRep" &: templateId)
        val cidArg = parens("coerceContractId" &: cid)
        val choiceArg = parens(Doc.text("toAnyChoice") & templateId & choice)
        command & typeRepArg & cidArg & choiceArg
      } else {
        val command = Doc.text("exerciseCmd")
        command & cid & choice
      }
    case ExerciseByKeyCommand(exercisedEvent, templateId, contractKey) =>
      val pkgId = templateId.packageId
      val key = encodeValue(partyMap, cidMap, contractKey.sum)
      val choice = encodeValue(partyMap, cidMap, exercisedEvent.getChoiceArgument.sum)
      if (isPreLf_1_8(pkgId, pkgLfVersions)) {
        val command = Doc.text("internalExerciseByKeyCmd")
        val templateId = "@" +: encodeTemplateId(TemplateId(exercisedEvent.getTemplateId))
        val typeRepArg = parens("templateTypeRep" &: templateId)
        val keyArg = parens(Doc.text("toAnyContractKey") & templateId & key)
        val choiceArg = parens(Doc.text("toAnyChoice") & templateId & choice)
        command & typeRepArg & keyArg & choiceArg
        command.lineOrSpace(typeRepArg).lineOrSpace(keyArg).lineOrSpace(choiceArg).nested(2)
      } else {
        val command = Doc.text("exerciseByKeyCmd")
        command & key & choice
        command.lineOrSpace(key).lineOrSpace(choice).nested(2)
      }
    case CreateAndExerciseCommand(createdEvent, exercisedEvent) =>
      val pkgId = createdEvent.getTemplateId.packageId
      val tpl = encodeRecord(partyMap, cidMap, createdEvent.getCreateArguments)
      val choice = encodeValue(partyMap, cidMap, exercisedEvent.getChoiceArgument.sum)
      if (isPreLf_1_8(pkgId, pkgLfVersions)) {
        val command = Doc.text("internalCreateAndExerciseCmd")
        val templateId = "@" +: encodeTemplateId(TemplateId(createdEvent.getTemplateId))
        val tplArg = parens("toAnyTemplate" &: tpl)
        val choiceArg = parens(Doc.text("toAnyChoice") & templateId & choice)
        Doc
          .stack(Seq(command, tplArg, choiceArg))
          .nested(2)
      } else {
        val command = Doc.text("createAndExerciseCmd")
        Doc
          .stack(Seq(command, tpl, choice))
          .nested(2)
      }
  }

  private def encodeCreatedEvent(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      pkgLfVersions: Map[String, LanguageVersion],
      created: CreatedEvent,
  ): Doc = {
    val tpl = encodeRecord(partyMap, cidMap, created.getCreateArguments)
    val pkgId = created.getTemplateId.packageId
    if (isPreLf_1_8(pkgId, pkgLfVersions)) {
      Doc.text("internalCreateCmd") & parens("toAnyTemplate" &: tpl)
    } else {
      Doc.text("createCmd") & tpl
    }
  }

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
      pkgLfVersions: Map[String, LanguageVersion],
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
      bind + encodeCmd(partyMap, cidMap, pkgLfVersions, cmd)
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
      pkgLfVersions: Map[String, LanguageVersion],
      submit: Submit,
  ): Doc = {
    submit match {
      case simple: SubmitSimple =>
        encodeSubmitSimple(partyMap, cidMap, cidRefs, pkgLfVersions, simple)
      case tree: SubmitTree => encodeSubmitTree(partyMap, cidMap, cidRefs, pkgLfVersions, tree)
    }
  }

  private def encodeSubmitTree(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      cidRefs: Set[ContractId],
      pkgLfVersions: Map[String, LanguageVersion],
      submit: SubmitTree,
  ): Doc = {
    val cids = treeCreatedCids(submit.tree)
    val referencedCids = cids.filter(c => cidRefs.contains(c.cid))
    val treeBind = Doc
      .stack(
        ("tree <-" &: encodeSubmitCall(partyMap, submit) :& "do") +:
          submit.commands.map(encodeCmd(partyMap, cidMap, pkgLfVersions, _))
      )
      .hang(2)
    val cidBinds = referencedCids.map(bindCid(cidMap, _))
    Doc.stack(treeBind +: cidBinds)
  }

  def encodeAction(
      partyMap: Map[Party, String],
      cidMap: Map[ContractId, String],
      cidRefs: Set[ContractId],
      pkgLfVersions: Map[String, LanguageVersion],
      action: Action,
  ): Doc = {
    action match {
      case SetTime(timestamp) => encodeSetTime(timestamp)
      case submit: Submit => encodeSubmit(partyMap, cidMap, cidRefs, pkgLfVersions, submit)
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
