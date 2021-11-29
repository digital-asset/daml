// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import org.typelevel.paiges._
import org.typelevel.paiges.Doc._
import com.daml.lf.ledger.EventId
import com.daml.lf.value.Value
import Value._
import com.daml.lf.ledger._
import com.daml.lf.data.Ref._
import com.daml.lf.scenario.ScenarioLedger.TransactionId
import com.daml.lf.scenario._
import com.daml.lf.transaction.{Node, NodeId, TransactionVersion => TxVersion}
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SBuiltin._

//
// Pretty-printer for the interpreter errors and the scenario ledger
//

private[lf] object Pretty {

  def prettyError(err: SError): Doc =
    text("Error:") & (err match {
      case ex: SErrorDamlException =>
        prettyDamlException(ex.error)
      case SErrorCrash(where, reason) =>
        text(s"CRASH in $where: $reason")
    })

  def prettyParty(p: Party): Doc =
    char('\'') + text(p) + char('\'')

  def prettyDamlException(error: interpretation.Error): Doc = {
    import interpretation.Error._
    error match {
      case FailedAuthorization(nid, fa) =>
        text(prettyFailedAuthorization(nid, fa))
      case UnhandledException(_, value) =>
        text(s"Unhandled exception:") & prettyValue(true)(value)
      case UserError(message) =>
        text(s"User abort: $message")
      case TemplatePreconditionViolated(templateId, loc @ _, arg) =>
        text("Update failed due to precondition violation when creating") &
          prettyTypeConName(templateId) &
          text("with") & prettyValue(true)(arg)
      case ContractNotActive(coid, tid, consumedBy) =>
        text("Update failed due to fetch of an inactive contract") & prettyContractId(coid) &
          char('(') + (prettyTypeConName(tid)) + text(").") /
          text(s"The contract had been consumed in sub-transaction #$consumedBy:")
      case ContractKeyNotFound(gk) =>
        text(
          "Update failed due to fetch-by-key or exercise-by-key which did not find a contract with key"
        ) &
          prettyValue(false)(gk.key) & char('(') + prettyIdentifier(gk.templateId) + char(')')
      case LocalContractKeyNotVisible(coid, gk, actAs, readAs, stakeholders) =>
        text(
          "Update failed due to a fetch, lookup or exercise by key of contract not visible to the reading parties"
        ) & prettyContractId(coid) &
          char('(') + (prettyIdentifier(gk.templateId)) + text(") associated with key ") +
          prettyValue(false)(gk.key) &
          text("No reading party is a stakeholder:") &
          text("actAs:") & intercalate(comma + space, actAs.map(prettyParty))
            .tightBracketBy(char('{'), char('}')) &
          text("readAs:") & intercalate(comma + space, readAs.map(prettyParty))
            .tightBracketBy(char('{'), char('}')) +
          char('.') / text("Stakeholders:") & intercalate(
            comma + space,
            stakeholders.map(prettyParty),
          ) + char('.')
      case DuplicateContractKey(key) =>
        text("Update failed due to a duplicate contract key") & prettyValue(false)(key.key)
      case WronglyTypedContract(coid, expected, actual) =>
        text("Update failed due to wrongly typed contract id") & prettyContractId(coid) /
          text("Expected contract of type") & prettyTypeConName(expected) & text(
            "but got"
          ) & prettyTypeConName(
            actual
          )
      case ContractDoesNotImplementInterface(interfaceId, coid, templateId) =>
        text("Update failed due to contract") & prettyContractId(coid) & text(
          "not implementing an interface"
        ) /
          text("Expected contract to implement interface") & prettyTypeConName(interfaceId) &
          text("but contract has type") & prettyTypeConName(templateId)
      case CreateEmptyContractKeyMaintainers(tid, arg, key) =>
        text("Update failed due to a contract key with an empty sey of maintainers when creating") &
          prettyTypeConName(tid) & text("with") & prettyValue(true)(arg) /
          text("The computed key is") & prettyValue(true)(key)
      case FetchEmptyContractKeyMaintainers(tid, key) =>
        text(
          "Update failed due to a contract key with an empty sey of maintainers when fetching or looking up by key"
        ) &
          prettyTypeConName(tid) /
          text("The provided key is") & prettyValue(true)(key)
      case ContractNotFound(cid) =>
        text("Update failed due to a unknown contract") & prettyContractId(cid)
      case NonComparableValues =>
        text("functions are not comparable")
      case ContractIdComparability(globalCid) =>
        text(s"The global contract ID") & prettyContractId(globalCid) &
          text("conflicts with a local contract ID")
      case ContractIdInContractKey(key) =>
        text("Contract IDs are not supported in contract keys:") &
          prettyContractId(key.cids.head)
      case ValueExceedsMaxNesting =>
        text(s"Value exceeds maximum nesting value of 100")
      case ChoiceGuardFailed(cid, templateId, choiceName, byInterface) => (
        text(s"Choice guard failed for") & prettyTypeConName(templateId) &
          text(s"contract") & prettyContractId(cid) &
          text(s"when exercising choice $choiceName") &
          (byInterface match {
            case None => text("by template")
            case Some(interfaceId) => text("by interface") & prettyTypeConName(interfaceId)
          })
      )
    }
  }

  // A minimal pretty-print of an update transaction node, without recursing into child nodes..
  def prettyPartialTransactionNode(node: Node): Doc =
    node match {
      case Node.Rollback(_) =>
        text("rollback")
      case create: Node.Create =>
        "create" &: prettyContractInst(create.coinst)
      case fetch: Node.Fetch =>
        "fetch" &: prettyContractId(fetch.coid)
      case ex: Node.Exercise =>
        intercalate(text(", "), ex.actingParties.map(p => text(p))) &
          text("exercises") & text(ex.choiceId) + char(':') + prettyIdentifier(ex.templateId) &
          text("on") & prettyContractId(ex.targetCoid) /
          text("with") & prettyValue(false)(ex.chosenValue)
      case lbk: Node.LookupByKey =>
        text("lookup by key") & prettyIdentifier(lbk.templateId) /
          text("key") & prettyKeyWithMaintainers(lbk.key) /
          (lbk.result match {
            case None => text("not found")
            case Some(coid) => text("found") & prettyContractId(coid)
          })
    }

  private def prettyFailedAuthorization(id: NodeId, failure: FailedAuthorization): String = {
    failure match {
      case nc: FailedAuthorization.NoControllers =>
        s"node $id (${nc.templateId}) has no controllers"
      case ma: FailedAuthorization.CreateMissingAuthorization =>
        s"node $id (${ma.templateId}) requires authorizers ${ma.requiredParties
          .mkString(",")}, but only ${ma.authorizingParties.mkString(",")} were given"
      case ma: FailedAuthorization.FetchMissingAuthorization =>
        s"node $id requires one of the stakeholders ${ma.stakeholders} of the fetched contract to be an authorizer, but authorizers were ${ma.authorizingParties}"
      case ma: FailedAuthorization.ExerciseMissingAuthorization =>
        s"node $id (${ma.templateId}) requires authorizers ${ma.requiredParties
          .mkString(",")}, but only ${ma.authorizingParties.mkString(",")} were given"
      case ns: FailedAuthorization.NoSignatories =>
        s"node $id (${ns.templateId}) has no signatories"
      case nlbk: FailedAuthorization.LookupByKeyMissingAuthorization =>
        s"node $id (${nlbk.templateId}) requires authorizers ${nlbk.maintainers} for lookup by key, but it only has ${nlbk.authorizingParties}"
      case mns: FailedAuthorization.MaintainersNotSubsetOfSignatories =>
        s"node $id (${mns.templateId}) has maintainers ${mns.maintainers} which are not a subset of the signatories ${mns.signatories}"
    }
  }

  def prettyValueRef(ref: ValueRef): Doc =
    text(ref.qualifiedName.toString + "@" + ref.packageId)

  def prettyLedger(l: ScenarioLedger): Doc =
    (text("transactions:") / prettyTransactions(l)) / line +
      (text("active contracts:") / prettyActiveContracts(l.ledgerData)).nested(3)

  def prettyTransactions(l: ScenarioLedger): Doc =
    intercalate(line + line, l.scenarioSteps.values.map(prettyScenarioStep(l)))

  def prettyLoc(optLoc: Option[Location]): Doc =
    optLoc
      .map(l =>
        text("[" + l.module.toString + ":")
          + str(l.start._1 + 1 /* 0-indexed */ )
          + text("]")
      )
      .getOrElse(text("[unknown source]"))

  def prettyScenarioStep(l: ScenarioLedger)(step: ScenarioLedger.ScenarioStep): Doc =
    step match {
      case ScenarioLedger.Commit(txId, rtx, optLoc) =>
        val children =
          intercalate(line + line, rtx.transaction.roots.toList.map(prettyEventInfo(l, txId)))
        text("TX") & char('#') + str(txId.id) & str(rtx.effectiveAt) & prettyLoc(optLoc) & text(
          "version:"
        ) & str(rtx.transaction.version.protoValue) /
          children
      case ScenarioLedger.PassTime(dt) =>
        "pass" &: str(dt)
      case amf: ScenarioLedger.AssertMustFail =>
        text("mustFailAt") &
          text("actAs:") & intercalate(comma + space, amf.actAs.map(prettyParty))
            .tightBracketBy(char('{'), char('}')) &
          text("readAs:") & intercalate(comma + space, amf.readAs.map(prettyParty))
            .tightBracketBy(char('{'), char('}')) &
          prettyLoc(amf.optLocation)
    }

  def prettyKeyWithMaintainers(key: Node.KeyWithMaintainers): Doc =
    // the maintainers are induced from the key -- so don't clutter
    prettyValue(false)(key.key)

  def prettyVersionedKeyWithMaintainers(key: Node.VersionedKeyWithMaintainers): Doc =
    // the maintainers are induced from the key -- so don't clutter
    prettyKeyWithMaintainers(key.unversioned)

  def prettyEventInfo(l: ScenarioLedger, txId: TransactionId)(nodeId: NodeId): Doc = {
    def arrowRight(d: Doc) = text("└─>") & d
    def meta(d: Doc) = text("│  ") & d
    val eventId = EventId(txId.id, nodeId)
    val ni = l.ledgerData.nodeInfos(eventId)
    val ppNode = ni.node match {
      case Node.Rollback(children) =>
        text("rollback:") / stack(children.toList.map(prettyEventInfo(l, txId)))
      case create: Node.Create =>
        val d = "create" &: prettyContractInst(create.coinst)
        create.versionedKey match {
          case None => d
          case Some(key) => d / text("key") & prettyVersionedKeyWithMaintainers(key)
        }
      case ea: Node.Fetch =>
        "ensure active" &: prettyContractId(ea.coid)
      case ex: Node.Exercise =>
        val children =
          if (ex.children.nonEmpty)
            text("children:") / stack(ex.children.toList.map(prettyEventInfo(l, txId)))
          else
            text("")
        intercalate(text(", "), ex.actingParties.map(p => text(p))) &
          text("exercises") & text(ex.choiceId) + char(':') + prettyIdentifier(ex.templateId) &
          text("on") & prettyContractId(ex.targetCoid) /
          (text("    ") + text("with") & prettyValue(false)(ex.chosenValue) / children)
            .nested(4)
      case lbk: Node.LookupByKey =>
        text("lookup by key") & prettyIdentifier(lbk.templateId) /
          text("key") & prettyVersionedKeyWithMaintainers(lbk.versionedKey) /
          (lbk.result match {
            case None => text("not found")
            case Some(coid) => text("found") & prettyContractId(coid)
          })
    }

    // TODO(MH): Take explicitness into account.
    val ppDisclosedTo =
      if (ni.disclosures.nonEmpty)
        meta(
          text("known to (since):") &
            intercalate(
              comma + space,
              ni.disclosures.toSeq
                .sortWith { case ((p1, d1), (p2, d2)) =>
                  // FIXME(MH): Does this order make any sense?
                  d1.since <= d2.since && p1 < p2
                }
                .map { case (p, d) =>
                  text(p) & text("(#") + str(d.since.id) + char(')')
                },
            )
        )
      else
        text("")
    val ppReferencedBy =
      if (ni.referencedBy.nonEmpty)
        meta(
          text("referenced by") &
            intercalate(comma + space, ni.referencedBy.toSeq.map(prettyEventId))
        )
      else
        text("")
    val ppArchivedBy =
      ni.consumedBy match {
        case None => text("")
        case Some(nid) => meta("archived by" &: prettyEventId(nid))
      }
    prettyEventId(eventId) & prettyOptVersion(ni.node.optVersion) / stack(
      Seq(ppArchivedBy, ppReferencedBy, ppDisclosedTo, arrowRight(ppNode))
        .filter(_.nonEmpty)
    )
  }

  def prettyOptVersion(opt: Option[TxVersion]) = {
    opt match {
      case Some(v) =>
        text("version:") & str(v.protoValue)
      case None =>
        text("no-version")
    }
  }

  def prettyEventId(n: EventId): Doc =
    text(n.toLedgerString)

  def prettyContractInst(coinst: ContractInstance): Doc =
    (prettyIdentifier(coinst.template) / text("with:") &
      prettyValue(false)(coinst.arg)).nested(4)

  def prettyTypeConName(tycon: TypeConName): Doc =
    text(tycon.qualifiedName.toString) + char('@') + prettyPackageId(tycon.packageId)

  def prettyContractId(coid: ContractId): Doc =
    text(coid.coid)

  def prettyActiveContracts(c: ScenarioLedger.LedgerData): Doc =
    fill(
      comma + space,
      c.activeContracts.toList
        .sortBy(_.toString)
        .map(prettyContractId),
    )

  def prettyPackageId(pkgId: PackageId): Doc =
    text(pkgId.take(8))

  def prettyIdentifier(id: Identifier): Doc =
    text(id.qualifiedName.toString) + char('@') + prettyPackageId(id.packageId)

  def prettyVersionedValue(verbose: Boolean)(v: VersionedValue): Doc =
    prettyValue(verbose)(v.unversioned)

  // Pretty print a value. If verbose then the top-level value is printed with type constructor
  // if possible.
  def prettyValue(verbose: Boolean)(v: Value): Doc =
    v match {
      case ValueInt64(i) => str(i)
      case ValueNumeric(d) => text(data.Numeric.toString(d))
      case ValueRecord(mbId, fs) =>
        (mbId match {
          case None => text("")
          case Some(id) => if (verbose) prettyIdentifier(id) else text("")
        }) +
          char('{') &
          fill(
            text(", "),
            fs.toList.map {
              case (Some(k), v) => text(k) & char('=') & prettyValue(true)(v)
              case (None, v) =>
                text("<no-label>") & char('=') & prettyValue(true)(v)
            },
          ) &
          char('}')
      case ValueVariant(mbId, variant, value) =>
        (mbId match {
          case None => text("")
          case Some(id) =>
            if (verbose)
              prettyIdentifier(id) + char(':')
            else
              text("")
        }) +
          (value match {
            case ValueUnit => text(variant)
            case _ =>
              text(variant) + char('(') + prettyValue(true)(value) + char(')')
          })
      case ValueEnum(mbId, constructor) =>
        (mbId match {
          case None => text("")
          case Some(id) =>
            if (verbose)
              prettyIdentifier(id) + char(':')
            else
              text("")
        }) +
          text(constructor)
      case ValueText(t) => char('"') + text(t) + char('"')
      case ValueContractId(acoid) => text(acoid.coid)
      case ValueUnit => text("<unit>")
      case ValueBool(b) => str(b)
      case ValueList(lst) =>
        char('[') + intercalate(text(", "), lst.map(prettyValue(true)(_)).toImmArray.toSeq) + char(
          ']'
        )
      case ValueTimestamp(t) => str(t)
      case ValueDate(days) => str(days)
      case ValueParty(p) => char('\'') + str(p) + char('\'')
      case ValueOptional(Some(v1)) => text("Option(") + prettyValue(verbose)(v1) + char(')')
      case ValueOptional(None) => text("None")
      case ValueTextMap(map) =>
        val list = map.toImmArray.map { case (k, v) =>
          text(k) + text(" -> ") + prettyValue(verbose)(v)
        }
        text("TextMap(") + intercalate(text(", "), list.toSeq) + text(")")
      case ValueGenMap(entries) =>
        val list = entries.map { case (k, v) =>
          prettyValue(verbose)(k) + text(" -> ") + prettyValue(verbose)(v)
        }
        text("GenMap(") + intercalate(text(", "), list.toSeq) + text(")")
    }

  object SExpr {
    // An incomplete pretty-printer for debugging purposes. Exposed
    // via the ':speedy' repl command.

    import com.daml.lf.language.Ast._
    import com.daml.lf.speedy.SExpr._

    def prettyAlt(index: Int)(alt: SCaseAlt): Doc = {
      val (pat, newIndex) = alt.pattern match {
        case SCPNil => (text("nil"), index)
        case SCPCons => (text("cons"), index + 2)
        case SCPDefault => (text("default"), index)
        case SCPVariant(_, v, _) =>
          (text("var") + char('(') + str(v) + char(')'), index + 1)
        case SCPEnum(_, v, _) =>
          (text("enum") + char('(') + str(v) + char(')'), index)
        case SCPPrimCon(pc) =>
          pc match {
            case PCTrue => (text("true"), index)
            case PCFalse => (text("false"), index)
            case PCUnit => (text("()"), index)
          }
        case SCPNone => (text("none"), index)
        case SCPSome => (text("some"), index + 1)
      }
      (pat & text("=>") + lineOrSpace + prettySExpr(newIndex)(alt.body)).nested(2)
    }

    def prettySELoc(loc: SELoc): Doc = loc match {
      case SELocS(i) => char('S') + str(i)
      case SELocA(i) => char('A') + str(i)
      case SELocF(i) => char('F') + str(i)
    }

    def prettySExpr(index: Int)(e: SExpr): Doc =
      e match {
        case SEVal(defId) =>
          str(defId)
        case SEValue(lit) =>
          lit match {
            case SParty(p) => char('\'') + text(p) + char('\'')
            case SText(t) => char('"') + text(t) + char('"')
            case other => str(other)
          }

        case SECaseAtomic(scrut, alts) =>
          (text("case") & prettySExpr(index)(scrut) & text("of") +
            line +
            intercalate(line, alts.map(prettyAlt(index)))).nested(2)

        case SEBuiltin(x) =>
          x match {
            case SBConsMany(n) => text(s"$$consMany[$n]")
            case SBCons => text(s"$$cons")
            case SBRecCon(id, fields) =>
              text("$record") + char('[') + text(id.qualifiedName.toString) + char('^') + str(
                fields.length
              ) + char(']')
            case _: SBRecUpd =>
              text("$update")
            case _: SBRecUpdMulti =>
              text("$updateMulti")
            case SBRecProj(id, field) =>
              text("$project") + char('[') + text(id.qualifiedName.toString) + char(':') + str(
                field
              ) + char(']')
            case SBVariantCon(id, v, _) =>
              text("$variant") + char('[') + text(id.qualifiedName.toString) + char(':') + text(
                v
              ) + char(
                ']'
              )
            case SBUCreate(ref, None) =>
              text("$create") + char('[') + text(ref.qualifiedName.toString) + char(']')
            case SBUCreate(ref, Some(iface)) =>
              text("$createByInterface") + char('[') + text(ref.qualifiedName.toString) + char(
                ','
              ) + text(iface.qualifiedName.toString) + char(']')
            case SBUFetch(ref) =>
              text("$fetch") + char('[') + text(ref.qualifiedName.toString) + char(']')
            case SBGetTime => text("$getTime")
            case _ => str(x)
          }
        case SEAppGeneral(fun, args) =>
          val prefix = prettySExpr(index)(fun) + text("@E(")
          intercalate(comma + lineOrSpace, args.map(prettySExpr(index)))
            .tightBracketBy(prefix, char(')'))
        case SEAppAtomicFun(fun, args) =>
          val prefix = prettySExpr(index)(fun) + text("@N(")
          intercalate(comma + lineOrSpace, args.map(prettySExpr(index)))
            .tightBracketBy(prefix, char(')'))
        case SEAppAtomicGeneral(fun, args) =>
          val prefix = prettySExpr(index)(fun) + text("@A(")
          intercalate(comma + lineOrSpace, args.map(prettySExpr(index)))
            .tightBracketBy(prefix, char(')'))
        case SEAppAtomicSaturatedBuiltin(builtin, args) =>
          val prefix = prettySExpr(index)(SEBuiltin(builtin)) + text("@B(")
          intercalate(comma + lineOrSpace, args.map(prettySExpr(index)))
            .tightBracketBy(prefix, char(')'))

        case SELocation(loc @ _, body) =>
          prettySExpr(index)(body)

        case SEMakeClo(fv, n, body) =>
          val prefix = char('[') +
            intercalate(space, fv.map(prettySELoc)) + char(']') + text("(\\") +
            intercalate(space, (index to n + index - 1).map((v: Int) => str(v))) &
            text("-> ")
          prettySExpr(index + n)(body).tightBracketBy(prefix, char(')'))

        case loc: SELoc => prettySELoc(loc)

        case SELet1General(rhs, body) =>
          val bounds = List(rhs)
          intercalate(
            comma + lineOrSpace,
            (bounds.zipWithIndex.map { case (x, n) =>
              str(index + n) & char('=') & prettySExpr(index + n)(x)
            }),
          ).tightBracketBy(text("let ["), char(']')) +
            lineOrSpace + text("in") & prettySExpr(index + bounds.length)(body)

        case SELet1Builtin(builtin, args, body) =>
          prettySExpr(index)(SELet1General(SEAppAtomicSaturatedBuiltin(builtin, args), body))
        case SELet1BuiltinArithmetic(builtin, args, body) =>
          prettySExpr(index)(SELet1General(SEAppAtomicSaturatedBuiltin(builtin, args), body))
        case SETryCatch(body, handler) =>
          text("try-catch") + char('(') + prettySExpr(index)(body) + text(", ") +
            prettySExpr(index)(handler) + char(')')

        case SEScopeExercise(body) =>
          text("exercise") + char('(') + prettySExpr(index)(body) + text(")")

        case x: SEImportValue => str(x)
        case x: SELabelClosure => str(x)
        case x: SEDamlException => str(x)
      }
  }

}
