// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.scenario

import scala.collection.JavaConverters._

import com.digitalasset.daml.lf.data.{Decimal, Ref}
import com.digitalasset.daml.lf.scenario.api.v1
import com.digitalasset.daml.lf.scenario.api.v1.{List => _, _}
import com.digitalasset.daml.lf.speedy.SError
import com.digitalasset.daml.lf.speedy.Speedy
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.transaction.{Transaction => Tx, Node => N}
import com.digitalasset.daml.lf.types.Ledger
import com.digitalasset.daml.lf.value.{Value => V}

case class Conversions(homePackageId: Ref.PackageId) {
  def convertScenarioResult(
      ledger: Ledger.Ledger,
      machine: Speedy.Machine,
      svalue: SValue): ScenarioResult = {

    val (nodes, steps) = convertLedger(ledger)
    val builder = ScenarioResult.newBuilder
      .addAllNodes(nodes.asJava)
      .addAllScenarioSteps(steps.asJava)
      .setReturnValue(convertSValue(svalue))
      .setFinalTime(ledger.currentTime.micros)
    machine.traceLog.iterator.foreach { entry =>
      builder.addTraceLog(convertSTraceMessage(entry))
    }
    builder.build
  }

  def convertScenarioError(
      ledger: Ledger.Ledger,
      machine: Speedy.Machine,
      err: SError.SError): ScenarioError = {
    val (nodes, steps) = convertLedger(ledger)
    val builder = ScenarioError.newBuilder
      .addAllNodes(nodes.asJava)
      .addAllScenarioSteps(steps.asJava)
      .setLedgerTime(ledger.currentTime.micros)

    machine.traceLog.iterator.foreach { entry =>
      builder.addTraceLog(convertSTraceMessage(entry))
    }

    def setCrash(reason: String) = builder.setCrash(reason)

    machine.commitLocation.foreach { loc =>
      builder.setCommitLoc(convertLocation(loc))
    }

    machine.lastLocation.foreach { loc =>
      builder.setLastLoc(convertLocation(loc))
    }

    builder.setPartialTransaction(
      convertPartialTransaction(machine.ptx)
    )

    err match {
      case SError.SErrorCrash(reason) => setCrash(reason)

      case SError.DamlEMatchError(reason) =>
        setCrash(reason)
      case SError.DamlEArithmeticError(reason) =>
        setCrash(reason)
      case SError.DamlEUserError(msg) =>
        builder.setUserError(msg)

      case SError.DamlETransactionError(reason) =>
        setCrash(reason)

      case SError.DamlETemplatePreconditionViolated(tid, optLoc, arg) =>
        val uepvBuilder = ScenarioError.TemplatePreconditionViolated.newBuilder
        optLoc.map(convertLocation).foreach(uepvBuilder.setLocation)
        builder.setTemplatePrecondViolated(
          uepvBuilder
            .setTemplateId(convertIdentifier(tid))
            .setArg(convertValue(arg.value))
            .build
        )
      case SError.DamlELocalContractNotActive(coid, tid, consumedBy) =>
        builder.setUpdateLocalContractNotActive(
          ScenarioError.ContractNotActive.newBuilder
            .setContractRef(mkContractRef(coid, tid))
            .setConsumedBy(NodeId.newBuilder.setId(consumedBy.toString).build)
            .build)
      case SError.ScenarioErrorContractNotEffective(coid, tid, effectiveAt) =>
        builder.setScenarioContractNotEffective(
          ScenarioError.ContractNotEffective.newBuilder
            .setEffectiveAt(effectiveAt.micros)
            .setContractRef(mkContractRef(coid, tid))
            .build
        )

      case SError.ScenarioErrorContractNotActive(coid, tid, consumedBy) =>
        builder.setScenarioContractNotActive(
          ScenarioError.ContractNotActive.newBuilder
            .setContractRef(mkContractRef(coid, tid))
            .setConsumedBy(convertNodeId(consumedBy))
            .build)

      case SError.ScenarioErrorContractNotVisible(coid, tid, committer, observers) =>
        builder.setScenarioContractNotVisible(
          ScenarioError.ContractNotVisible.newBuilder
            .setContractRef(mkContractRef(coid, tid))
            .setCommitter(convertParty(committer))
            .addAllObservers(observers.map(convertParty).asJava)
            .build)

      case SError.ScenarioErrorCommitError(commitError) =>
        builder.setScenarioCommitError(
          convertCommitError(commitError)
        )
      case SError.ScenarioErrorMustFailSucceeded(tx @ _) =>
        builder.setScenarioMustfailSucceeded(empty)

      case SError.ScenarioErrorInvalidPartyName(party, _) =>
        builder.setScenarioInvalidPartyName(party)

      case wtc: SError.DamlEWronglyTypedContract =>
        sys.error(
          s"Got unexpected DamlEWronglyTypedContract error in scenario service: $wtc. Note that in the scenario service this error should never surface since contract fetches are all type checked.")
    }
    builder.build
  }

  def convertCommitError(commitError: Ledger.CommitError): CommitError = {
    val builder = CommitError.newBuilder
    commitError match {
      case Ledger.CommitError.UniqueKeyViolation(gk) =>
        builder.setUniqueKeyViolation(convertGlobalKey(gk.gk))
      case Ledger.CommitError.FailedAuthorizations(fas) =>
        builder.setFailedAuthorizations(convertFailedAuthorizations(fas))
    }
    builder.build
  }

  def convertGlobalKey(globalKey: N.GlobalKey): GlobalKey = {
    GlobalKey.newBuilder
      .setTemplateId(convertIdentifier(globalKey.templateId))
      .setKey(convertValue(globalKey.key.value))
      .build
  }

  def convertSValue(svalue: SValue): Value = {
    def unserializable(what: String): Value =
      Value.newBuilder.setUnserializable(what).build
    svalue match {
      case SValue.SPAP(prim, _, _) =>
        prim match {
          case SValue.PBuiltin(e) =>
            unserializable(s"<BUILTIN#${e.getClass.getName}>")
          case _: SValue.PClosure =>
            unserializable("<CLOSURE>")
        }
      case _ =>
        convertValue(svalue.toValue)
    }
  }

  def convertSTraceMessage(msgAndLoc: (String, Option[Ref.Location])): TraceMessage = {
    val builder = TraceMessage.newBuilder
    msgAndLoc._2.map(loc => builder.setLocation(convertLocation(loc)))
    builder.setMessage(msgAndLoc._1).build
  }

  def convertLedger(ledger: Ledger.Ledger): (Iterable[Node], Iterable[ScenarioStep]) =
    (
      ledger.ledgerData.nodeInfos.map(Function.tupled(convertNode))
      // NOTE(JM): Iteration over IntMap is in key-order. The IntMap's Int is IntMapUtils.Int for some reason.
      ,
      ledger.scenarioSteps.map { case (idx, step) => convertScenarioStep(idx.toInt, step) })

  def convertFailedAuthorizations(fas: Ledger.FailedAuthorizations): FailedAuthorizations = {
    val builder = FailedAuthorizations.newBuilder
    fas.map {
      case (nodeId, fa) =>
        builder.addFailedAuthorizations {
          val faBuilder = FailedAuthorization.newBuilder
          faBuilder.setNodeId(convertTxNodeId(nodeId))
          fa match {
            case Ledger.FACreateMissingAuthorization(
                templateId,
                optLocation,
                authParties,
                reqParties) =>
              val cmaBuilder =
                FailedAuthorization.CreateMissingAuthorization.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
                  .addAllAuthorizingParties(authParties.map(convertParty).asJava)
                  .addAllRequiredAuthorizers(reqParties.map(convertParty).asJava)
              optLocation.map(loc => cmaBuilder.setLocation(convertLocation(loc)))
              faBuilder.setCreateMissingAuthorization(cmaBuilder.build)

            case fma: Ledger.FAFetchMissingAuthorization =>
              val fmaBuilder =
                FailedAuthorization.FetchMissingAuthorization.newBuilder
                  .setTemplateId(convertIdentifier(fma.templateId))
                  .addAllAuthorizingParties(fma.authorizingParties.map(convertParty).asJava)
                  .addAllStakeholders(fma.stakeholders.map(convertParty).asJava)
              fma.optLocation.map(loc => fmaBuilder.setLocation(convertLocation(loc)))
              faBuilder.setFetchMissingAuthorization(fmaBuilder.build)

            case Ledger.FAExerciseMissingAuthorization(
                templateId,
                choiceId,
                optLocation,
                authParties,
                reqParties) =>
              val emaBuilder =
                FailedAuthorization.ExerciseMissingAuthorization.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
                  .setChoiceId(choiceId)
                  .addAllAuthorizingParties(authParties.map(convertParty).asJava)
                  .addAllRequiredAuthorizers(reqParties.map(convertParty).asJava)
              optLocation.map(loc => emaBuilder.setLocation(convertLocation(loc)))
              faBuilder.setExerciseMissingAuthorization(emaBuilder.build)
            case Ledger.FAActorMismatch(templateId, choiceId, optLocation, ctrls, givenActors) =>
              val amBuilder =
                FailedAuthorization.ActorMismatch.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
                  .setChoiceId(choiceId)
                  .addAllControllers(ctrls.map(convertParty).asJava)
                  .addAllGivenActors(givenActors.map(convertParty).asJava)
              optLocation.map(loc => amBuilder.setLocation(convertLocation(loc)))
              faBuilder.setActorMismatch(amBuilder.build)
            case Ledger.FANoSignatories(templateId, optLocation) =>
              val nsBuilder =
                FailedAuthorization.NoSignatories.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
              optLocation.map(loc => nsBuilder.setLocation(convertLocation(loc)))
              faBuilder.setNoSignatories(nsBuilder.build)

            case Ledger.FANoControllers(templateId, choiceId, optLocation) =>
              val ncBuilder =
                FailedAuthorization.NoControllers.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
                  .setChoiceId(choiceId)
              optLocation.map(loc => ncBuilder.setLocation(convertLocation(loc)))
              faBuilder.setNoControllers(ncBuilder.build)

            case Ledger.FALookupByKeyMissingAuthorization(
                templateId,
                optLocation,
                maintainers,
                authorizers) =>
              val lbkmaBuilder =
                FailedAuthorization.LookupByKeyMissingAuthorization.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
                  .addAllMaintainers(maintainers.map(convertParty).asJava)
                  .addAllAuthorizingParties(authorizers.map(convertParty).asJava)
              optLocation.foreach(loc => lbkmaBuilder.setLocation(convertLocation(loc)))
              faBuilder.setLookupByKeyMissingAuthorization(lbkmaBuilder)
          }
          faBuilder.build
        }
    }
    builder.build
  }

  def mkContractRef(coid: V.ContractId, templateId: Ref.Identifier): ContractRef =
    coid match {
      case V.AbsoluteContractId(coid) =>
        ContractRef.newBuilder
          .setRelative(false)
          .setContractId(coid)
          .setTemplateId(convertIdentifier(templateId))
          .build
      case V.RelativeContractId(txnid) =>
        ContractRef.newBuilder
          .setRelative(true)
          .setContractId(txnid.index.toString)
          .setTemplateId(convertIdentifier(templateId))
          .build
    }

  def convertContractId(coid: V.ContractId): String =
    coid match {
      case V.AbsoluteContractId(coid) => coid
      case V.RelativeContractId(txnid) => txnid.index.toString
    }

  def convertScenarioStep(stepId: Int, step: Ledger.ScenarioStep): ScenarioStep = {
    val builder = ScenarioStep.newBuilder
    builder.setStepId(stepId)
    step match {
      case Ledger.Commit(txId, rtx, optLocation) =>
        val commitBuilder = ScenarioStep.Commit.newBuilder
        optLocation.map { loc =>
          commitBuilder.setLocation(convertLocation(loc))
        }
        builder.setCommit(
          commitBuilder
            .setTxId(txId)
            .setTx(convertTransaction(rtx))
            .build)
      case Ledger.PassTime(dt) =>
        builder.setPassTime(dt)
      case Ledger.AssertMustFail(actor, optLocation, time, txId) =>
        val assertBuilder = ScenarioStep.AssertMustFail.newBuilder
        optLocation.map { loc =>
          assertBuilder.setLocation(convertLocation(loc))
        }
        builder
          .setAssertMustFail(
            assertBuilder
              .setActor(convertParty(actor))
              .setTime(time.micros)
              .setTxId(txId)
              .build)
    }
    builder.build
  }

  def convertTransaction(rtx: Ledger.RichTransaction): Transaction =
    Transaction.newBuilder
      .setCommitter(convertParty(rtx.committer))
      .setEffectiveAt(rtx.effectiveAt.micros)
      .addAllRoots(rtx.roots.map(convertNodeId).toSeq.asJava)
      .addAllNodes(rtx.nodes.keys.map(convertNodeId).asJava)
      .addAllDisclosures(rtx.disclosures.toSeq.map {
        case (nodeId, parties) =>
          NodeAndParties.newBuilder
            .setNodeId(convertNodeId(nodeId))
            .addAllParties(parties.map(convertParty).asJava)
            .build
      }.asJava)
      .setFailedAuthorizations(convertFailedAuthorizations(rtx.failedAuthorizations))
      .build

  def convertPartialTransaction(ptx: Tx.PartialTransaction): PartialTransaction = {
    val builder = PartialTransaction.newBuilder
      .addAllNodes(ptx.nodes.map(Function.tupled(convertTxNode)).asJava)
      .addAllRoots(ptx.roots.toImmArray.toSeq.sortBy(_.index).map(convertTxNodeId).asJava)

    ptx.context match {
      case Tx.ContextRoot =>
        Unit
      case Tx.ContextExercises(ctx) =>
        val ecBuilder = ExerciseContext.newBuilder
          .setTargetId(mkContractRef(ctx.targetId, ctx.templateId))
          .setChoiceId(ctx.choiceId)
          .setChosenValue(convertValue(ctx.chosenValue.value))
        ctx.optLocation.map(loc => ecBuilder.setExerciseLocation(convertLocation(loc)))
        builder.setExerciseContext(ecBuilder.build)
    }
    builder.build
  }

  def convertNodeId(nodeId: Ledger.NodeId): NodeId =
    NodeId.newBuilder.setId(nodeId.id).build

  def convertTxNodeId(nodeId: Tx.NodeId): NodeId =
    NodeId.newBuilder.setId(nodeId.index.toString).build

  def convertNode(nodeId: Ledger.NodeId, nodeInfo: Ledger.NodeInfo): Node = {
    val builder = Node.newBuilder
    builder
      .setNodeId(convertNodeId(nodeId))
      .setEffectiveAt(nodeInfo.effectiveAt.micros)
      .addAllReferencedBy(nodeInfo.referencedBy.map(convertNodeId).asJava)
      .addAllObservingSince(nodeInfo.observingSince.toList.map {
        case (party, txId) =>
          PartyAndTransactionId.newBuilder
            .setParty(convertParty(party))
            .setTxId(txId)
            .build
      }.asJava)

    nodeInfo.consumedBy
      .map(nodeId => builder.setConsumedBy(convertNodeId(nodeId)))
    nodeInfo.parent
      .map(nodeId => builder.setParent(convertNodeId(nodeId)))

    nodeInfo.node match {
      case create: N.NodeCreate[V.AbsoluteContractId, Tx.Value[V.AbsoluteContractId]] =>
        val createBuilder =
          Node.Create.newBuilder
            .setContractInstance(
              ContractInstance.newBuilder
                .setTemplateId(convertIdentifier(create.coinst.template))
                .setValue(convertValue(create.coinst.arg.value))
                .build
            )
            .addAllSignatories(create.signatories.map(convertParty).asJava)
            .addAllStakeholders(create.stakeholders.map(convertParty).asJava)

        create.optLocation.map(loc => builder.setLocation(convertLocation(loc)))
        builder.setCreate(createBuilder.build)
      case fetch: N.NodeFetch[V.AbsoluteContractId] =>
        builder.setFetch(
          Node.Fetch.newBuilder
            .setContractId(fetch.coid.coid)
            .setTemplateId(convertIdentifier(fetch.templateId))
            .addAllSignatories(fetch.signatories.map(convertParty).asJava)
            .addAllStakeholders(fetch.stakeholders.map(convertParty).asJava)
            .build
        )
      case ex: N.NodeExercises[
            Ledger.NodeId,
            V.AbsoluteContractId,
            Tx.Value[V.AbsoluteContractId]] =>
        ex.optLocation.map(loc => builder.setLocation(convertLocation(loc)))
        builder.setExercise(
          Node.Exercise.newBuilder
            .setTargetContractId(ex.targetCoid.coid)
            .setTemplateId(convertIdentifier(ex.templateId))
            .setChoiceId(ex.choiceId)
            .setConsuming(ex.consuming)
            .addAllActingParties(ex.actingParties.map(convertParty).asJava)
            .setChosenValue(convertValue(ex.chosenValue.value))
            .addAllSignatories(ex.signatories.map(convertParty).asJava)
            .addAllStakeholders(ex.stakeholders.map(convertParty).asJava)
            .addAllControllers(ex.controllers.map(convertParty).asJava)
            .addAllChildren(ex.children
              .map((nid: Ledger.NodeId) => NodeId.newBuilder.setId(nid.id).build)
              .toSeq
              .asJava)
            .build
        )

      case lbk: N.NodeLookupByKey[V.AbsoluteContractId, Tx.Value[V.AbsoluteContractId]] =>
        lbk.optLocation.foreach(loc => builder.setLocation(convertLocation(loc)))
        val lbkBuilder = Node.LookupByKey.newBuilder
          .setTemplateId(convertIdentifier(lbk.templateId))
          .setKeyWithMaintainers(convertKeyWithMaintainers(lbk.key))
        lbk.result.foreach(cid => lbkBuilder.setContractId(cid.coid))
        builder.setLookupByKey(lbkBuilder)

    }
    builder.build
  }

  def convertKeyWithMaintainers(
      key: N.KeyWithMaintainers[V.VersionedValue[V.ContractId]]): KeyWithMaintainers = {
    KeyWithMaintainers
      .newBuilder()
      .setKey(convertValue(key.key.value))
      .addAllMaintainers(key.maintainers.map(convertParty).asJava)
      .build()
  }

  def convertTxNode( /*ptx: Tx.PartialTransaction, */ nodeId: Tx.NodeId, node: Tx.Node): Node = {
    val builder = Node.newBuilder
    builder
      .setNodeId(NodeId.newBuilder.setId(nodeId.index.toString).build)
    // FIXME(JM): consumedBy, parent, ...
    node match {
      case create: N.NodeCreate.WithTxValue[V.ContractId] =>
        val createBuilder =
          Node.Create.newBuilder
            .setContractInstance(
              ContractInstance.newBuilder
                .setTemplateId(convertIdentifier(create.coinst.template))
                .setValue(convertValue(create.coinst.arg.value))
                .build
            )
            .addAllSignatories(create.signatories.map(convertParty).asJava)
            .addAllStakeholders(create.stakeholders.map(convertParty).asJava)
        create.key.foreach(key =>
          createBuilder.setKeyWithMaintainers(convertKeyWithMaintainers(key)))
        create.optLocation.map(loc => builder.setLocation(convertLocation(loc)))
        builder.setCreate(createBuilder.build)
      case fetch: N.NodeFetch[V.ContractId] =>
        builder.setFetch(
          Node.Fetch.newBuilder
            .setContractId(convertContractId(fetch.coid))
            .setTemplateId(convertIdentifier(fetch.templateId))
            .addAllSignatories(fetch.signatories.map(convertParty).asJava)
            .addAllStakeholders(fetch.stakeholders.map(convertParty).asJava)
            .build
        )
      case ex: N.NodeExercises.WithTxValue[Tx.NodeId, V.ContractId] =>
        ex.optLocation.map(loc => builder.setLocation(convertLocation(loc)))
        builder.setExercise(
          Node.Exercise.newBuilder
            .setTargetContractId(convertContractId(ex.targetCoid))
            .setTemplateId(convertIdentifier(ex.templateId))
            .setChoiceId(ex.choiceId)
            .setConsuming(ex.consuming)
            .addAllActingParties(ex.actingParties.map(convertParty).asJava)
            .setChosenValue(convertValue(ex.chosenValue.value))
            .addAllSignatories(ex.signatories.map(convertParty).asJava)
            .addAllStakeholders(ex.stakeholders.map(convertParty).asJava)
            .addAllControllers(ex.controllers.map(convertParty).asJava)
            .addAllChildren(
              ex.children
                .map(nid => NodeId.newBuilder.setId(nid.index.toString).build)
                .toSeq
                .asJava
            )
            .build
        )

      case lookup: N.NodeLookupByKey.WithTxValue[V.ContractId] =>
        lookup.optLocation.map(loc => builder.setLocation(convertLocation(loc)))
        builder.setLookupByKey({
          val builder = Node.LookupByKey.newBuilder
            .setKeyWithMaintainers(convertKeyWithMaintainers(lookup.key))
          lookup.result.foreach(cid => builder.setContractId(convertContractId(cid)))
          builder.build
        })
    }
    builder.build
  }

  lazy val packageIdSelf: PackageIdentifier =
    PackageIdentifier.newBuilder.setSelf(empty).build

  def convertPackageId(pkg: Ref.PackageId): PackageIdentifier =
    if (pkg == homePackageId)
      // Reconstitute the self package reference.
      packageIdSelf
    else
      PackageIdentifier.newBuilder.setPackageId(pkg).build

  def convertIdentifier(identifier: Ref.Identifier): Identifier =
    Identifier.newBuilder
      .setPackage(convertPackageId(identifier.packageId))
      .setName(identifier.qualifiedName.toString)
      .build

  def convertLocation(loc: Ref.Location): Location = {
    val (sline, scol) = loc.start
    val (eline, ecol) = loc.end
    Location.newBuilder
      .setPackage(convertPackageId(loc.packageId))
      .setModule(loc.module.toString)
      .setStartLine(sline)
      .setStartCol(scol)
      .setEndLine(eline)
      .setEndCol(ecol)
      .build

  }

  val empty: Empty = Empty.newBuilder.build

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def convertValue[Cid](value: V[Cid]): Value = {
    val builder = Value.newBuilder
    value match {
      case V.ValueRecord(tycon, fields) =>
        val rbuilder = Record.newBuilder
        tycon.map(x => rbuilder.setRecordId(convertIdentifier(x)))
        builder.setRecord(
          rbuilder
            .addAllFields(
              fields.toSeq.map {
                case (optName, fieldValue) =>
                  val builder = Field.newBuilder
                  optName.foreach(builder.setLabel)
                  builder
                    .setValue(convertValue(fieldValue))
                    .build
              }.asJava
            )
            .build
        )
      case V.ValueTuple(fields) =>
        builder.setTuple(
          Tuple.newBuilder
            .addAllFields(
              fields.toSeq.map { field =>
                Field.newBuilder
                  .setLabel(field._1)
                  .setValue(convertValue(field._2))
                  .build
              }.asJava
            )
            .build
        )
      case V.ValueVariant(tycon, variant, value) =>
        val vbuilder = Variant.newBuilder
        tycon.map(x => vbuilder.setVariantId(convertIdentifier(x)))
        builder.setVariant(
          vbuilder
            .setConstructor(variant)
            .setValue(convertValue(value))
            .build
        )
      case V.ValueContractId(coid) =>
        coid match {
          case V.AbsoluteContractId(acoid) =>
            builder.setContractId(acoid)
          case V.RelativeContractId(txnid) =>
            builder.setContractId(txnid.index.toString)
        }
      case V.ValueList(values) =>
        builder.setList(
          v1.List.newBuilder
            .addAllElements(
              values
                .map(convertValue)
                .toImmArray
                .toSeq
                .asJava)
            .build
        )
      case V.ValueInt64(v) => builder.setInt64(v)
      case V.ValueDecimal(d) => builder.setDecimal(Decimal.toString(d))
      case V.ValueText(t) => builder.setText(t)
      case V.ValueTimestamp(ts) => builder.setTimestamp(ts.micros)
      case V.ValueDate(d) => builder.setDate(d.days)
      case V.ValueParty(p) => builder.setParty(p)
      case V.ValueBool(b) => builder.setBool(b)
      case V.ValueUnit => builder.setUnit(empty)
      case V.ValueOptional(mbV) =>
        val optionalBuilder = Optional.newBuilder
        mbV match {
          case None => ()
          case Some(v) => optionalBuilder.setValue(convertValue(v))
        }
        builder.setOptional(optionalBuilder)
      case V.ValueMap(map) =>
        val mapBuilder = v1.Map.newBuilder
        map.toImmArray.foreach {
          case (k, v) =>
            mapBuilder.addEntries(v1.Map.Entry.newBuilder().setKey(k).setValue(convertValue(v)))
            ()
        }
        builder.setMap(mapBuilder)
    }
    builder.build
  }

  def convertParty(p: Ref.Party): Party =
    Party.newBuilder.setParty(p).build

}
