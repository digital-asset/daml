// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import org.typelevel.paiges._
import org.typelevel.paiges.Doc._
import com.daml.lf.ledger.EventId
import com.daml.lf.value.Value
import Value.{NodeId => _, _}
import com.daml.lf.VersionRange
import com.daml.lf.transaction.Node._
import com.daml.lf.ledger._
import com.daml.lf.data.Ref._
import com.daml.lf.scenario.ScenarioLedger.TransactionId
import com.daml.lf.scenario._
import com.daml.lf.transaction.{NodeId, Transaction => Tx}
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SBuiltin._

//
// Pretty-printer for the interpreter errors and the scenario ledger
//

private[lf] object Pretty {

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
          text("with") & prettyValue(true)(arg)

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
                (line + prettyPartialTransactionNode(node)).nested(4)
            })

      case DamlEWronglyTypedContract(coid, expected, actual) =>
        text("Update failed due to wrongly typed contract id") & prettyContractId(coid) /
          text("Expected contract of type") & prettyTypeConName(expected) & text("but got") & prettyTypeConName(
          actual,
        )

      case DamlEDisallowedInputValueVersion(VersionRange(expectedMin, expectedMax), actual) =>
        text("Update failed due to disallowed value version") /
          text("Expected value version between") & text(expectedMin.protoValue) &
          text("and") & text(expectedMax.protoValue) & text("but got") &
          text(actual.protoValue)
    }

  // A minimal pretty-print of an update transaction node, without recursing into child nodes..
  def prettyPartialTransactionNode(node: PartialTransaction.Node): Doc =
    node match {
      case create: NodeCreate[Value.ContractId, Value[Value.ContractId]] =>
        "create" &: prettyContractInst(create.coinst)
      case fetch: NodeFetch[Value.ContractId, Value[Value.ContractId]] =>
        "fetch" &: prettyContractId(fetch.coid)
      case ex: NodeExercises[NodeId, Value.ContractId, Value[Value.ContractId]] =>
        intercalate(text(", "), ex.actingParties.map(p => text(p))) &
          text("exercises") & text(ex.choiceId) + char(':') + prettyIdentifier(ex.templateId) &
          text("on") & prettyContractId(ex.targetCoid) /
          text("with") & prettyValue(false)(ex.chosenValue)
      case lbk: NodeLookupByKey[Value.ContractId, Value[Value.ContractId]] =>
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
            text("The contract had been consumed in transaction") & prettyEventId(consumedBy)
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
      case ScenarioErrorContractKeyNotVisible(coid, gk, committer, stakeholders) =>
        text("due to the failure to fetch the contract") & prettyContractId(coid) &
          char('(') + (prettyIdentifier(gk.templateId)) + text(") associated with key ") +
            prettyValue(false)(gk.key) &
          text("The contract had not been disclosed to the committer") & prettyParty(committer) + char(
          '.',
        ) /
          text("Stakeholders:") & intercalate(
          comma + space,
          stakeholders.map(prettyParty),
        ) + char('.')
      case ScenarioErrorCommitError(ScenarioLedger.CommitError.FailedAuthorizations(fas)) =>
        (text("due to failed authorizations:") / prettyFailedAuthorizations(fas)).nested(4)
      case ScenarioErrorCommitError(ScenarioLedger.CommitError.UniqueKeyViolation(gk)) =>
        (text("due to unique key violation for key:") & prettyValue(false)(gk.gk.key) & text(
          "for template",
        ) & prettyIdentifier(gk.gk.templateId))

      case ScenarioErrorMustFailSucceeded(tx @ _) =>
        // TODO(JM): Further info needed. Location annotations?
        text("due to a mustfailAt that succeeded.")

      case ScenarioErrorInvalidPartyName(_, msg) => text(s"Invalid party: $msg")

      case ScenarioErrorPartyAlreadyExists(party) =>
        text(s"Tried to allocate a party that already exists: $party")

      case ScenarioErrorSerializationError(msg) =>
        text(s"Cannot serialize the transaction: $msg")
    })

  def prettyFailedAuthorizations(fas: FailedAuthorizations): Doc =
    intercalate(
      comma + space,
      fas.toSeq.map {
        // FIXME(JM): pretty-print all the parameters.
        case (
            nodeId,
            FailedAuthorization
              .CreateMissingAuthorization(templateId @ _, optLoc @ _, authorizing, required),
            ) =>
          str(nodeId) & text(": missing authorization for create, authorizing parties:") &
            intercalate(comma + space, authorizing.map(prettyParty)) +
              text(", at least all of the following parties need to authorize:") &
            intercalate(comma + space, required.map(prettyParty))

        case (
            nodeId,
            FailedAuthorization.MaintainersNotSubsetOfSignatories(
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
            FailedAuthorization
              .FetchMissingAuthorization(templateId @ _, optLoc @ _, authorizing, stakeholders),
            ) =>
          str(nodeId) & text(": missing authorization for fetch, authorizing parties:") &
            intercalate(comma + space, authorizing.map(prettyParty)) +
              text(", at least one of the following parties need to authorize:") &
            intercalate(comma + space, stakeholders.map(prettyParty))

        case (
            nodeId,
            FailedAuthorization.ExerciseMissingAuthorization(
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

        case (
            nodeId,
            FailedAuthorization.ActorMismatch(templateId @ _, choiceId @ _, optLoc @ _, actors)) =>
          str(nodeId) + text(": actor mismatch, given actors:") &
            intercalate(comma + space, actors.map(prettyParty))

        case (nodeId, FailedAuthorization.NoSignatories(templateId, optLoc @ _)) =>
          str(nodeId) + text(s": $templateId missing signatories")

        case (nodeId, FailedAuthorization.NoControllers(templateId, choiceId, optLoc @ _)) =>
          str(nodeId) + text(s": $templateId $choiceId missing controllers")

        case (
            nodeId,
            FailedAuthorization.LookupByKeyMissingAuthorization(
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

  def prettyLedger(l: ScenarioLedger): Doc =
    (text("transactions:") / prettyTransactions(l)) / line +
      (text("active contracts:") / prettyActiveContracts(l.ledgerData)).nested(3)

  def prettyTransactions(l: ScenarioLedger): Doc =
    intercalate(line + line, l.scenarioSteps.values.map(prettyScenarioStep(l)))

  def prettyLoc(optLoc: Option[Location]): Doc =
    optLoc
      .map(
        l =>
          text("[" + l.module.toString + ":")
            + str(l.start._1 + 1 /* 0-indexed */ )
            + text("]"))
      .getOrElse(text("[unknown source]"))

  def prettyScenarioStep(l: ScenarioLedger)(step: ScenarioLedger.ScenarioStep): Doc =
    step match {
      case ScenarioLedger.Commit(txId, rtx, optLoc) =>
        val children =
          intercalate(line + line, rtx.transaction.roots.toList.map(prettyEventInfo(l, txId)))
        text("TX") & char('#') + str(txId.id) & str(rtx.effectiveAt) & prettyLoc(optLoc) /
          children
      case ScenarioLedger.PassTime(dt) =>
        "pass" &: str(dt)
      case amf: ScenarioLedger.AssertMustFail =>
        text("mustFailAt") & prettyParty(amf.actor) & prettyLoc(amf.optLocation)
    }

  def prettyKeyWithMaintainers(key: KeyWithMaintainers[Value[ContractId]]): Doc =
    // the maintainers are induced from the key -- so don't clutter
    prettyValue(false)(key.key)

  def prettyVersionedKeyWithMaintainers(key: KeyWithMaintainers[Tx.Value[ContractId]]): Doc =
    // the maintainers are induced from the key -- so don't clutter
    prettyVersionedValue(false)(key.key)

  def prettyEventInfo(l: ScenarioLedger, txId: TransactionId)(nodeId: NodeId): Doc = {
    def arrowRight(d: Doc) = text("└─>") & d
    def meta(d: Doc) = text("│  ") & d
    val eventId = EventId(txId.id, nodeId)
    val ni = l.ledgerData.nodeInfos(eventId)
    val ppNode = ni.node match {
      case create: NodeCreate[ContractId, Tx.Value[ContractId]] =>
        val d = "create" &: prettyVersionedContractInst(create.coinst)
        create.key match {
          case None => d
          case Some(key) => d / text("key") & prettyVersionedKeyWithMaintainers(key)
        }
      case ea: NodeFetch[ContractId, Tx.Value[ContractId]] =>
        "ensure active" &: prettyContractId(ea.coid)
      case ex: NodeExercises[
            NodeId,
            ContractId,
            Tx.Value[ContractId]
          ] =>
        val children =
          if (ex.children.nonEmpty)
            text("children:") / stack(ex.children.toList.map(prettyEventInfo(l, txId)))
          else
            text("")
        intercalate(text(", "), ex.actingParties.map(p => text(p))) &
          text("exercises") & text(ex.choiceId) + char(':') + prettyIdentifier(ex.templateId) &
          text("on") & prettyContractId(ex.targetCoid) /
          (text("    ") + text("with") & prettyVersionedValue(false)(ex.chosenValue) / children)
            .nested(4)
      case lbk: NodeLookupByKey[ContractId, Tx.Value[ContractId]] =>
        text("lookup by key") & prettyIdentifier(lbk.templateId) /
          text("key") & prettyVersionedKeyWithMaintainers(lbk.key) /
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
                .sortWith {
                  case ((p1, d1), (p2, d2)) =>
                    // FIXME(MH): Does this order make any sense?
                    d1.since <= d2.since && p1 < p2
                }
                .map {
                  case (p, d) =>
                    text(p) & text("(#") + str(d.since.id) + char(')')
                },
            ),
        )
      else
        text("")
    val ppReferencedBy =
      if (ni.referencedBy.nonEmpty)
        meta(
          text("referenced by") &
            intercalate(comma + space, ni.referencedBy.toSeq.map(prettyEventId)),
        )
      else
        text("")
    val ppArchivedBy =
      ni.consumedBy match {
        case None => text("")
        case Some(nid) => meta("archived by" &: prettyEventId(nid))
      }
    prettyEventId(eventId) / stack(
      Seq(ppArchivedBy, ppReferencedBy, ppDisclosedTo, arrowRight(ppNode))
        .filter(_.nonEmpty),
    )
  }

  def prettyEventId(n: EventId): Doc =
    text(n.toLedgerString)

  def prettyContractInst(coinst: ContractInst[Value[ContractId]]): Doc =
    (prettyIdentifier(coinst.template) / text("with:") &
      prettyValue(false)(coinst.arg)).nested(4)

  def prettyVersionedContractInst(coinst: ContractInst[Tx.Value[ContractId]]): Doc =
    (prettyIdentifier(coinst.template) / text("with:") &
      prettyVersionedValue(false)(coinst.arg)).nested(4)

  def prettyTypeConName(tycon: TypeConName): Doc =
    text(tycon.qualifiedName.toString) + char('@') + prettyPackageId(tycon.packageId)

  def prettyContractId(coid: ContractId): Doc =
    text(coid.coid)

  def prettyActiveContracts(c: ScenarioLedger.LedgerData): Doc =
    fill(
      comma + space,
      c.activeContracts.toList
        .sortBy(_.toString)
        .map(prettyContractId)
    )

  def prettyPackageId(pkgId: PackageId): Doc =
    text(pkgId.take(8))

  def prettyIdentifier(id: Identifier): Doc =
    text(id.qualifiedName.toString) + char('@') + prettyPackageId(id.packageId)

  def prettyVersionedValue(verbose: Boolean)(v: Tx.Value[ContractId]): Doc =
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
          fill(text(", "), fs.toImmArray.toSeq.map {
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
      case ValueContractId(acoid) => text(acoid.coid)
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
        case SEVar(i) => char('@') + str(index - i)
        case SEVal(defId) =>
          str(defId)
        case SEValue(lit) =>
          lit match {
            case SParty(p) => char('\'') + text(p) + char('\'')
            case SText(t) => char('"') + text(t) + char('"')
            case other => str(other)
          }

        case SECaseAtomic(scrut, alts) => prettySExpr(index)(SECase(scrut, alts))
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
            case _: SBRecUpdMulti =>
              text("$updateMulti")
            case SBRecProj(id, field) =>
              text("$project") + char('[') + text(id.qualifiedName.toString) + char(':') + str(
                field,
              ) + char(']')
            case SBVariantCon(id, v, _) =>
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
            intercalate(space, fv.map(prettySELoc)) + char(']') + text("(\\") +
            intercalate(space, (index to n + index - 1).map((v: Int) => str(v))) &
            text("-> ")
          prettySExpr(index + n)(body).tightBracketBy(prefix, char(')'))

        case loc: SELoc => prettySELoc(loc)

        case SELet(bounds, body) =>
          // let [a, b, c] in X
          intercalate(comma + lineOrSpace, (bounds.zipWithIndex.map {
            case (x, n) =>
              str(index + n) & char('=') & prettySExpr(index + n)(x)
          })).tightBracketBy(text("let ["), char(']')) +
            lineOrSpace + text("in") & prettySExpr(index + bounds.length)(body)
        case SELet1General(rhs, body) =>
          prettySExpr(index)(SELet(Array(rhs), body))
        case SELet1Builtin(builtin, args, body) =>
          prettySExpr(index)(SELet1General(SEAppAtomicSaturatedBuiltin(builtin, args), body))

        case x: SEBuiltinRecursiveDefinition => str(x)
        case x: SEImportValue => str(x)
        case x: SELabelClosure => str(x)
        case x: SEDamlException => str(x)
      }
  }

}
