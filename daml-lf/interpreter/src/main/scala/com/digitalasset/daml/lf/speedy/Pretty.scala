// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import scala.util.Try
import org.typelevel.paiges._
import org.typelevel.paiges.Doc._
import com.digitalasset.daml.lf.value.Value
import Value._
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.types.{Ledger => L}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.transaction.Transaction.PartialTransaction
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.speedy.SBuiltin._
import com.digitalasset.daml.lf.types.Ledger.CommitError

//
// Pretty-printer for the interpreter errors and the scenario ledger
//

object Pretty {

  def prettyError(err: SError, ptx: PartialTransaction): Doc =
    text("Error:") & (err match {
      case ex: SErrorDamlException =>
        prettyDamlException(ex, ptx)
      case SErrorCrash(reason) =>
        text(s"CRASH: $reason")

      case serr: SErrorScenario =>
        prettyScenarioError(serr)
    })

  def prettyParty(p: Party): Doc =
    char('\'') + text(p) + char('\'')

  def prettyDamlException(ex: SErrorDamlException, ptx: PartialTransaction): Doc =
    ex match {
      case DamlEArithmeticError(message) =>
        text(message)
      case DamlEUserError(message) =>
        text(s"User abort: $message")
      case DamlETransactionError(reason) =>
        text(s"Transaction error: $reason")
      case DamlEMatchError(reason) =>
        text(reason)
      case DamlETemplatePreconditionViolated(tid, optLoc @ _, arg) =>
        text("Update failed due to precondition violation when creating") &
          prettyTypeConName(tid) &
          text("with") & prettyVersionedValue(true)(arg)

      case DamlELocalContractNotActive(coid, tid, consumedBy) =>
        text("Update failed due to fetch of an inactive contract") & prettyContractId(coid) &
          char('(') + (prettyTypeConName(tid)) + text(").") /
            text(s"The contract had been consumed in sub-transaction #$consumedBy:") +
            (ptx.nodes.get(consumedBy) match {
              // FIXME(JM): How should we show this? If the node pointed to by consumedBy
              // is not in nodes, it must not be done yet, hence the contract is recursively
              // exercised.
              case None =>
                (line + text("Recursive exercise of ") + prettyTypeConName(tid)).nested(4)
              case Some(node) =>
                (line + prettyTransactionNode(node)).nested(4)
            })

      case DamlEWronglyTypedContract(coid, expected, actual) =>
        text("Update failed due to wrongly typed contract id") & prettyContractId(coid) /
          text("Expected contract of type") & prettyTypeConName(expected) & text("but got") & prettyTypeConName(
          actual,
        )

      case DamlESubmitterNotInMaintainers(templateId, submitter, maintainers) =>
        text("Expected the submitter") & prettyParty(submitter) &
          text("to be in maintainers") & intercalate(comma + space, maintainers.map(prettyParty)) &
          text("when looking up template of maintainer") & prettyTypeConName(templateId)
    }

  // A minimal pretty-print of an update transaction node, without recursing into child nodes..
  def prettyTransactionNode(node: Transaction.Node): Doc =
    node match {
      case create: NodeCreate.WithTxValue[ContractId] =>
        "create" &: prettyContractInst(create.coinst)
      case fetch: NodeFetch[ContractId] =>
        "fetch" &: prettyContractId(fetch.coid)
      case ex: NodeExercises.WithTxValue[Transaction.NodeId, ContractId] =>
        intercalate(text(", "), ex.actingParties.map(p => text(p))) &
          text("exercises") & text(ex.choiceId) + char(':') + prettyIdentifier(ex.templateId) &
          text("on") & prettyContractId(ex.targetCoid) /
          text("with") & prettyVersionedValue(false)(ex.chosenValue)
      case lbk: NodeLookupByKey.WithTxValue[ContractId] =>
        text("lookup by key") & prettyIdentifier(lbk.templateId) /
          text("key") & prettyKeyWithMaintainers(lbk.key) /
          (lbk.result match {
            case None => text("not found")
            case Some(coid) => text("found") & prettyContractId(coid)
          })
    }

  def prettyScenarioError(serr: SErrorScenario): Doc =
    text("Scenario failed") & (serr match {
      case ScenarioErrorContractNotEffective(coid, tid, effectiveAt) =>
        text(s"due to a fetch of an inactive contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(tid)) + text(").") &
          text(s"that becomes effective at $effectiveAt")
      case ScenarioErrorContractNotActive(coid, tid, consumedBy) =>
        text("due to a fetch of a consumed contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(tid)) + text(").") /
            text("The contract had been consumed in transaction") & prettyLedgerNodeId(consumedBy)
      case ScenarioErrorContractNotVisible(coid, tid, committer, observers) =>
        text("due to the failure to fetch the contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(tid)) + text(").") /
            text("The contract had not been disclosed to the committer") & prettyParty(committer) + char(
          '.',
        ) /
          text("The contract had been disclosed to:") & intercalate(
          comma + space,
          observers.map(prettyParty),
        ) + char('.')
      case ScenarioErrorCommitError(CommitError.FailedAuthorizations(fas)) =>
        (text("due to failed authorizations:") / prettyFailedAuthorizations(fas)).nested(4)
      case ScenarioErrorCommitError(CommitError.UniqueKeyViolation(gk)) =>
        (text("due to unique key violation for key:") & prettyVersionedValue(false)(gk.gk.key) & text(
          "for template",
        ) & prettyIdentifier(gk.gk.templateId))

      case ScenarioErrorMustFailSucceeded(tx @ _) =>
        // TODO(JM): Further info needed. Location annotations?
        text("due to a mustfailAt that succeeded.")

      case ScenarioErrorInvalidPartyName(_, msg) => text(s"Invalid party: $msg")
    })

  def prettyFailedAuthorizations(fas: L.FailedAuthorizations): Doc =
    intercalate(
      comma + space,
      fas.toSeq.map {
        // FIXME(JM): pretty-print all the parameters.
        case (
            nodeId,
            L.FACreateMissingAuthorization(templateId @ _, optLoc @ _, authorizing, required),
            ) =>
          str(nodeId) & text(": missing authorization for create, authorizing parties:") &
            intercalate(comma + space, authorizing.map(prettyParty)) +
              text(", at least all of the following parties need to authorize:") &
            intercalate(comma + space, required.map(prettyParty))

        case (
            nodeId,
            L.FAMaintainersNotSubsetOfSignatories(
              templateId @ _,
              optLoc @ _,
              signatories,
              maintainers,
            ),
            ) =>
          str(nodeId) & text(": all the maintainers:") &
            intercalate(comma + space, maintainers.map(prettyParty)) +
              text(", need to be signatories:") &
            intercalate(comma + space, signatories.map(prettyParty))

        case (
            nodeId,
            L.FAFetchMissingAuthorization(templateId @ _, optLoc @ _, authorizing, stakeholders),
            ) =>
          str(nodeId) & text(": missing authorization for fetch, authorizing parties:") &
            intercalate(comma + space, authorizing.map(prettyParty)) +
              text(", at least one of the following parties need to authorize:") &
            intercalate(comma + space, stakeholders.map(prettyParty))

        case (
            nodeId,
            L.FAExerciseMissingAuthorization(
              templateId @ _,
              choiceId @ _,
              optLoc @ _,
              authorizing,
              required,
            ),
            ) =>
          str(nodeId) & text(": missing authorization for exercise, authorizing parties:") &
            intercalate(comma + space, authorizing.map(prettyParty)) +
              text(", exactly the following parties need to authorize::") &
            intercalate(comma + space, required.map(prettyParty))

        case (nodeId, L.FAActorMismatch(templateId @ _, choiceId @ _, optLoc @ _, ctrls, actors)) =>
          str(nodeId) + text(": actor mismatch, controllers:") &
            intercalate(comma + space, ctrls.map(prettyParty)) &
            text(", given actors:") &
            intercalate(comma + space, actors.map(prettyParty))

        case (nodeId, L.FANoSignatories(templateId, optLoc @ _)) =>
          str(nodeId) + text(s": $templateId missing signatories")

        case (nodeId, L.FANoControllers(templateId, choiceId, optLoc @ _)) =>
          str(nodeId) + text(s": $templateId $choiceId missing controllers")

        case (
            nodeId,
            L.FALookupByKeyMissingAuthorization(
              templateId @ _,
              optLoc @ _,
              maintainers,
              authorizingParties,
            ),
            ) =>
          str(nodeId) + text(": missing authorization for lookup by key, authorizing parties:") &
            intercalate(comma + space, authorizingParties.map(prettyParty)) +
              text(" are not a superset of maintainers:") &
            intercalate(comma + space, maintainers.map(prettyParty))
      },
    )

  def prettyValueRef(ref: ValueRef): Doc =
    text(ref.qualifiedName.toString + "@" + ref.packageId)

  def prettyLedger(l: L.Ledger): Doc =
    (text("transactions:") / prettyTransactions(l)) / line +
      (text("active contracts:") / prettyActiveContracts(l.ledgerData)).nested(3)

  def prettyTransactions(l: L.Ledger): Doc =
    intercalate(line + line, l.scenarioSteps.values.map(prettyScenarioStep(l)))

  def prettyLoc(optLoc: Option[Location]): Doc =
    optLoc
      .map(l =>
        text("[" + l.module.toString + ":")
          + str(l.start._1 + 1 /* 0-indexed */ )
          + text("]"),
      )
      .getOrElse(text("[unknown source]"))

  def prettyScenarioStep(l: L.Ledger)(step: L.ScenarioStep): Doc =
    step match {
      case L.Commit(txid, rtx, optLoc) =>
        val children =
          intercalate(line + line, rtx.roots.toList.map(prettyNodeInfo(l)))
        text("TX") & char('#') + str(txid.id) & str(rtx.effectiveAt) & prettyLoc(optLoc) /
          children
      case L.PassTime(dt) =>
        "pass" &: str(dt)
      case amf: L.AssertMustFail =>
        text("mustFailAt") & prettyParty(amf.actor) & prettyLoc(amf.optLocation)
    }

  def prettyKeyWithMaintainers(key: KeyWithMaintainers[Transaction.Value[ContractId]]): Doc =
    // the maintainers are induced from the key -- so don't clutter
    prettyVersionedValue(false)(key.key)

  def prettyNodeInfo(l: L.Ledger)(nodeId: L.ScenarioNodeId): Doc = {
    def arrowRight(d: Doc) = text("└─>") & d
    def meta(d: Doc) = text("│  ") & d

    val ni = l.ledgerData.nodeInfos(nodeId) /* Ekke Ekke Ekke Ekke Ptang Zoo Boing! */
    val ppNode = ni.node match {
      case create: NodeCreate[AbsoluteContractId, Transaction.Value[AbsoluteContractId]] =>
        val d = "create" &: prettyContractInst(create.coinst)
        create.key match {
          case None => d
          case Some(key) => d / text("key") & prettyKeyWithMaintainers(key)
        }
      case ea: NodeFetch[AbsoluteContractId] =>
        "ensure active" &: prettyContractId(ea.coid)
      case ex: NodeExercises[L.ScenarioNodeId, AbsoluteContractId, Transaction.Value[
            AbsoluteContractId,
          ]] =>
        val children =
          if (ex.children.nonEmpty)
            text("children:") / stack(ex.children.toList.map(prettyNodeInfo(l)))
          else
            text("")
        intercalate(text(", "), ex.actingParties.map(p => text(p))) &
          text("exercises") & text(ex.choiceId) + char(':') + prettyIdentifier(ex.templateId) &
          text("on") & prettyContractId(ex.targetCoid) /
          (text("    ") + text("with") & prettyVersionedValue(false)(ex.chosenValue) / children)
            .nested(4)
      case lbk: NodeLookupByKey[AbsoluteContractId, Transaction.Value[AbsoluteContractId]] =>
        text("lookup by key") & prettyIdentifier(lbk.templateId) /
          text("key") & prettyKeyWithMaintainers(lbk.key) /
          (lbk.result match {
            case None => text("not found")
            case Some(coid) => text("found") & prettyContractId(coid)
          })
    }

    val ppDisclosedTo =
      if (ni.observingSince.nonEmpty)
        meta(
          text("known to (since):") &
            intercalate(
              comma + space,
              ni.observingSince.toSeq
                .sortWith {
                  case ((p1, id1), (p2, id2)) =>
                    id1 <= id2 && p1 < p2
                }
                .map {
                  case (p, txid) =>
                    text(p) & text("(#") + str(txid.id) + char(')')
                },
            ),
        )
      else
        text("")
    val ppReferencedBy =
      if (ni.referencedBy.nonEmpty)
        meta(
          text("referenced by") &
            intercalate(comma + space, ni.referencedBy.toSeq.map(prettyLedgerNodeId)),
        )
      else
        text("")
    val ppArchivedBy =
      ni.consumedBy match {
        case None => text("")
        case Some(nid) => meta("archived by" &: prettyLedgerNodeId(nid))
      }
    prettyLedgerNodeId(nodeId) / stack(
      Seq(ppArchivedBy, ppReferencedBy, ppDisclosedTo, arrowRight(ppNode))
        .filter(_.nonEmpty),
    )
  }

  def prettyLedgerNodeId(n: L.ScenarioNodeId): Doc =
    char('#') + text(n)

  def prettyContractInst(coinst: ContractInst[Transaction.Value[ContractId]]): Doc =
    (prettyIdentifier(coinst.template) / text("with:") &
      prettyVersionedValue(false)(coinst.arg)).nested(4)

  def prettyTypeConName(tycon: TypeConName): Doc =
    text(tycon.qualifiedName.toString) + char('@') + prettyPackageId(tycon.packageId)

  def prettyContractId(coid: ContractId): Doc =
    coid match {
      case AbsoluteContractId(acoid) => char('#') + text(acoid)
      case RelativeContractId(rcoid, _) => char('#') + str(rcoid)
    }

  def prettyActiveContracts(c: L.LedgerData): Doc = {
    def ltNodeId(a: AbsoluteContractId, b: AbsoluteContractId): Boolean = {
      val ap = a.coid.split(':')
      val bp = b.coid.split(':')
      Try(ap(0).toInt < bp(0).toInt || (ap(0).toInt == bp(0).toInt && ap(1).toInt < bp(1).toInt))
        .getOrElse(false)
    }
    fill(
      comma + space,
      c.activeContracts.toList
        .sortWith(ltNodeId)
        .map((acoid: AbsoluteContractId) => prettyLedgerNodeId(L.ScenarioNodeId(acoid))),
    )
  }

  def prettyPackageId(pkgId: PackageId): Doc =
    text(pkgId.take(8))

  def prettyIdentifier(id: Identifier): Doc =
    text(id.qualifiedName.toString) + char('@') + prettyPackageId(id.packageId)

  def prettyVersionedValue(verbose: Boolean)(v: Transaction.Value[ContractId]): Doc =
    prettyValue(verbose)(v.value) & text("value-version: ") + text(v.version.protoValue)

  // Pretty print a value. If verbose then the top-level value is printed with type constructor
  // if possible.
  def prettyValue(verbose: Boolean)(v: Value[ContractId]): Doc =
    v match {
      case ValueInt64(i) => str(i)
      case ValueNumeric(d) => str(d)
      case ValueRecord(mbId, fs) =>
        (mbId match {
          case None => text("")
          case Some(id) => if (verbose) prettyIdentifier(id) else text("")
        }) +
          char('{') &
          fill(text(", "), fs.toList.map {
            case (Some(k), v) => text(k) & char('=') & prettyValue(true)(v)
            case (None, v) =>
              text("<no-label>") & char('=') & prettyValue(true)(v)
          }) &
          char('}')
      case ValueStruct(fs) =>
        char('{') &
          fill(text(", "), fs.toList.map {
            case (k, v) => text(k) & char('=') & prettyValue(true)(v)
          }) &
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
      case ValueContractId(AbsoluteContractId(acoid)) => char('#') + text(acoid)
      case ValueContractId(RelativeContractId(rcoid, _)) =>
        char('~') + text(rcoid.toString)
      case ValueUnit => text("<unit>")
      case ValueBool(b) => str(b)
      case ValueList(lst) =>
        char('[') + intercalate(text(", "), lst.map(prettyValue(true)(_)).toImmArray.toSeq) + char(
          ']',
        )
      case ValueTimestamp(t) => str(t)
      case ValueDate(days) => str(days)
      case ValueParty(p) => char('\'') + str(p) + char('\'')
      case ValueOptional(Some(v1)) => text("Option(") + prettyValue(verbose)(v1) + char(')')
      case ValueOptional(None) => text("None")
      case ValueTextMap(map) =>
        val list = map.toImmArray.map {
          case (k, v) => text(k) + text(" -> ") + prettyValue(verbose)(v)
        }
        text("TextMap(") + intercalate(text(", "), list.toSeq) + text(")")
      case ValueGenMap(entries) =>
        val list = entries.map {
          case (k, v) => prettyValue(verbose)(k) + text(" -> ") + prettyValue(verbose)(v)
        }
        text("GenMap(") + intercalate(text(", "), list.toSeq) + text(")")
    }

  object SExpr {
    // An incomplete pretty-printer for debugging purposes. Exposed
    // via the ':speedy' repl command.

    import com.digitalasset.daml.lf.language.Ast._
    import com.digitalasset.daml.lf.speedy.SExpr._
    def prettyAlt(index: Int)(alt: SCaseAlt): Doc = {
      val (pat, newIndex) = alt.pattern match {
        case SCPNil => (text("nil"), index)
        case SCPCons => (text("cons"), index + 2)
        case SCPDefault => (text("default"), index)
        case SCPVariant(_, v) =>
          (text("var") + char('(') + str(v) + char(')'), index + 1)
        case SCPEnum(_, v) =>
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
    def prettySExpr(index: Int)(e: SExpr): Doc =
      e match {
        case SEVar(i) => char('@') + str(index - i)
        case SEVal(defId, _) =>
          str(defId)
        case SEValue(lit) =>
          lit match {
            case SParty(p) => char('\'') + text(p) + char('\'')
            case SText(t) => char('"') + text(t) + char('"')
            case other => str(other)
          }

        case SECase(scrut, alts) =>
          (text("case") & prettySExpr(index)(scrut) & text("of") +
            line +
            intercalate(line, alts.map(prettyAlt(index)))).nested(2)

        case SEBuiltin(x) =>
          x match {
            case SBConsMany(n) => text(s"$$consMany[$n]")
            case SBRecCon(id, fields) =>
              text("$record") + char('[') + text(id.qualifiedName.toString) + char('^') + str(
                fields.length,
              ) + char(']')
            case _: SBRecUpd =>
              text("$update")
            case SBRecProj(id, field) =>
              text("$project") + char('[') + text(id.qualifiedName.toString) + char(':') + str(
                field,
              ) + char(']')
            case SBVariantCon(id, v) =>
              text("$variant") + char('[') + text(id.qualifiedName.toString) + char(':') + text(v) + char(
                ']',
              )
            case SBUCreate(ref) =>
              text("$create") + char('[') + text(ref.qualifiedName.toString) + char(']')
            case SBUFetch(ref) =>
              text("$fetch") + char('[') + text(ref.qualifiedName.toString) + char(']')
            case SBGetTime => text("$getTime")
            case _ => str(x)
          }
        case SEApp(fun, args) =>
          val prefix = prettySExpr(index)(fun) + char('(')
          intercalate(comma + lineOrSpace, args.map(prettySExpr(index)))
            .tightBracketBy(prefix, char(')'))
        case SEAbs(n, body) =>
          val prefix = text("(\\") +
            intercalate(space, (index to n + index - 1).map((v: Int) => str(v))) &
            text("-> ")
          prettySExpr(index + n)(body).tightBracketBy(prefix, char(')'))

        case SECatch(body, handler, fin) =>
          text("$catch") + char('(') + prettySExpr(index)(body) + text(", ") +
            prettySExpr(index)(handler) + text(", ") +
            prettySExpr(index)(fin) + char(')')

        case SELocation(loc @ _, body) =>
          prettySExpr(index)(body)

        case SEMakeClo(fv, n, body) =>
          val prefix = char('[') +
            intercalate(space, fv.map((v: Int) => str(v))) + char(']') + text("(\\") +
            intercalate(space, (index to n + index - 1).map((v: Int) => str(v))) &
            text("-> ")
          prettySExpr(index + n)(body).tightBracketBy(prefix, char(')'))

        case SELet(bounds, body) =>
          // let [a, b, c] in X
          intercalate(comma + lineOrSpace, (bounds.zipWithIndex.map {
            case (x, n) =>
              str(index + n) & char('=') & prettySExpr(index + n)(x)
          })).tightBracketBy(text("let ["), char(']')) +
            lineOrSpace + text("in") & prettySExpr(index + bounds.length)(body)

        case other =>
          str(other)
      }
  }

}
