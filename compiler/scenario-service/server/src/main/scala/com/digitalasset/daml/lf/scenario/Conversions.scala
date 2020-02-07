// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.scenario

import scala.collection.JavaConverters._
import com.digitalasset.daml.lf.data.{Numeric, Ref}
import com.digitalasset.daml.lf.scenario.api.v1
import com.digitalasset.daml.lf.scenario.api.v1.{List => _, _}
import com.digitalasset.daml.lf.speedy.SError
import com.digitalasset.daml.lf.speedy.Speedy
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.transaction.{Node => N, Transaction => Tx}
import com.digitalasset.daml.lf.types.Ledger
import com.digitalasset.daml.lf.types.Ledger.ScenarioNodeId
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml.lf.value.{Value => V}

case class Conversions(homePackageId: Ref.PackageId) {
  def convertScenarioResult(
      ledger: Ledger.Ledger,
      machine: Speedy.Machine,
      svalue: SValue,
  ): ScenarioResult = {

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
      err: SError.SError,
  ): ScenarioError = {
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

    builder.addAllStackTrace(machine.stackTrace().map(convertLocation).toSeq.asJava)

    builder.setPartialTransaction(
      convertPartialTransaction(machine.ptx),
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
            .build,
        )
      case SError.DamlELocalContractNotActive(coid, tid, consumedBy) =>
        builder.setUpdateLocalContractNotActive(
          ScenarioError.ContractNotActive.newBuilder
            .setContractRef(mkContractRef(coid, tid))
            .setConsumedBy(NodeId.newBuilder.setId(consumedBy.toString).build)
            .build,
        )
      case SError.ScenarioErrorContractNotEffective(coid, tid, effectiveAt) =>
        builder.setScenarioContractNotEffective(
          ScenarioError.ContractNotEffective.newBuilder
            .setEffectiveAt(effectiveAt.micros)
            .setContractRef(mkContractRef(coid, tid))
            .build,
        )

      case SError.ScenarioErrorContractNotActive(coid, tid, consumedBy) =>
        builder.setScenarioContractNotActive(
          ScenarioError.ContractNotActive.newBuilder
            .setContractRef(mkContractRef(coid, tid))
            .setConsumedBy(convertNodeId(consumedBy))
            .build,
        )

      case SError.ScenarioErrorContractNotVisible(coid, tid, committer, observers) =>
        builder.setScenarioContractNotVisible(
          ScenarioError.ContractNotVisible.newBuilder
            .setContractRef(mkContractRef(coid, tid))
            .setCommitter(convertParty(committer))
            .addAllObservers(observers.map(convertParty).asJava)
            .build,
        )

      case SError.ScenarioErrorCommitError(commitError) =>
        builder.setScenarioCommitError(
          convertCommitError(commitError),
        )
      case SError.ScenarioErrorMustFailSucceeded(tx @ _) =>
        builder.setScenarioMustfailSucceeded(empty)

      case SError.ScenarioErrorInvalidPartyName(party, _) =>
        builder.setScenarioInvalidPartyName(party)

      case wtc: SError.DamlEWronglyTypedContract =>
        sys.error(
          s"Got unexpected DamlEWronglyTypedContract error in scenario service: $wtc. Note that in the scenario service this error should never surface since contract fetches are all type checked.",
        )

      case SError.DamlESubmitterNotInMaintainers(templateId, submitter, maintainers) =>
        builder.setSubmitterNotInMaintainers(
          ScenarioError.SubmitterNotInMaintainers.newBuilder
            .setTemplateId(convertIdentifier(templateId))
            .setSubmitter(convertParty(submitter))
            .addAllMaintainers(maintainers.map(convertParty).asJava)
            .build,
        )
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
    try {
      convertValue(svalue.toValue)
    } catch {
      case _: SError.SErrorCrash => {
        // We cannot rely on serializability information since we do not have that available in the IDE.
        // We also cannot simply pattern match on SValue since the unserializable values can be nested, e.g.,
        // a function ina record.
        // We could recurse on SValue to produce slightly better error messages if we
        // encounter an unserializable type but that doesn’t seem worth the effort, especially
        // given that the error would still be on speedy expressions.
        unserializable("Unserializable scenario result")
      }
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
      ledger.scenarioSteps.map {
        case (idx, step) => convertScenarioStep(idx.toInt, step, ledger.ledgerData.coidToNodeId)
      },
    )

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
                reqParties,
                ) =>
              val cmaBuilder =
                FailedAuthorization.CreateMissingAuthorization.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
                  .addAllAuthorizingParties(authParties.map(convertParty).asJava)
                  .addAllRequiredAuthorizers(reqParties.map(convertParty).asJava)
              optLocation.map(loc => cmaBuilder.setLocation(convertLocation(loc)))
              faBuilder.setCreateMissingAuthorization(cmaBuilder.build)

            case Ledger.FAMaintainersNotSubsetOfSignatories(
                templateId,
                optLocation,
                signatories,
                maintainers,
                ) =>
              val maintNotSignBuilder =
                FailedAuthorization.MaintainersNotSubsetOfSignatories.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
                  .addAllSignatories(signatories.map(convertParty).asJava)
                  .addAllMaintainers(maintainers.map(convertParty).asJava)
              optLocation.map(loc => maintNotSignBuilder.setLocation(convertLocation(loc)))
              faBuilder.setMaintainersNotSubsetOfSignatories(maintNotSignBuilder.build)

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
                reqParties,
                ) =>
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
                authorizers,
                ) =>
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
      case V.RelativeContractId(txnid, _) =>
        ContractRef.newBuilder
          .setRelative(true)
          .setContractId(txnid.index.toString)
          .setTemplateId(convertIdentifier(templateId))
          .build
    }

  def convertContractId(coid: V.ContractId): String =
    coid match {
      case V.AbsoluteContractId(coid) => coid
      case V.RelativeContractId(txnid, _) => txnid.index.toString
    }

  def convertScenarioStep(
      stepId: Int,
      step: Ledger.ScenarioStep,
      coidToNodeId: AbsoluteContractId => ScenarioNodeId,
  ): ScenarioStep = {
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
            .setTxId(txId.index)
            .setTx(convertTransaction(rtx, coidToNodeId))
            .build,
        )
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
              .setTxId(txId.index)
              .build,
          )
    }
    builder.build
  }

  def convertTransaction(
      rtx: Ledger.RichTransaction,
      coidToNodeId: AbsoluteContractId => ScenarioNodeId,
  ): Transaction = {
    val convertedGlobalImplicitDisclosure = rtx.globalImplicitDisclosure.map {
      case (coid, parties) => coidToNodeId(coid) -> parties
    }
    Transaction.newBuilder
      .setCommitter(convertParty(rtx.committer))
      .setEffectiveAt(rtx.effectiveAt.micros)
      .addAllRoots(rtx.roots.map(convertNodeId).toSeq.asJava)
      .addAllNodes(rtx.nodes.keys.map(convertNodeId).asJava)
      // previously rtx.disclosures returned both global and local implicit disclosures, but this is not the case anymore
      // therefore we need to explicitly add the contracts that are divulged directly (via ContractId rather than ScenarioNodeId)
      .addAllDisclosures((rtx.disclosures ++ convertedGlobalImplicitDisclosure).toSeq.map {
        case (nodeId, parties) =>
          NodeAndParties.newBuilder
            .setNodeId(convertNodeId(nodeId))
            .addAllParties(parties.map(convertParty).asJava)
            .build
      }.asJava)
      .setFailedAuthorizations(convertFailedAuthorizations(rtx.failedAuthorizations))
      .build
  }

  def convertPartialTransaction(ptx: Tx.PartialTransaction): PartialTransaction = {
    val builder = PartialTransaction.newBuilder
      .addAllNodes(ptx.nodes.map(Function.tupled(convertTxNode)).asJava)
      .addAllRoots(
        ptx.context.children.toImmArray.toSeq.sortBy(_.index).map(convertTxNodeId).asJava,
      )

    ptx.context match {
      case Tx.PartialTransaction.ContextRoot(_, _) =>
      case Tx.PartialTransaction.ContextExercise(ctx, _) =>
        val ecBuilder = ExerciseContext.newBuilder
          .setTargetId(mkContractRef(ctx.targetId, ctx.templateId))
          .setChoiceId(ctx.choiceId)
          .setChosenValue(convertValue(ctx.chosenValue.value))
        ctx.optLocation.map(loc => ecBuilder.setExerciseLocation(convertLocation(loc)))
        builder.setExerciseContext(ecBuilder.build)
    }
    builder.build
  }

  def convertNodeId(nodeId: Ledger.ScenarioNodeId): NodeId =
    NodeId.newBuilder.setId(nodeId).build

  def convertTxNodeId(nodeId: Tx.NodeId): NodeId =
    NodeId.newBuilder.setId(nodeId.index.toString).build

  def convertNode(nodeId: Ledger.ScenarioNodeId, nodeInfo: Ledger.LedgerNodeInfo): Node = {
    val builder = Node.newBuilder
    builder
      .setNodeId(convertNodeId(nodeId))
      .setEffectiveAt(nodeInfo.effectiveAt.micros)
      .addAllReferencedBy(nodeInfo.referencedBy.map(convertNodeId).asJava)
      .addAllObservingSince(nodeInfo.observingSince.toList.map {
        case (party, txId) =>
          PartyAndTransactionId.newBuilder
            .setParty(convertParty(party))
            .setTxId(txId.index)
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
                .build,
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
            .build,
        )
      case ex: N.NodeExercises[
            Ledger.ScenarioNodeId,
            V.AbsoluteContractId,
            Tx.Value[
              V.AbsoluteContractId,
            ]] =>
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
            .addAllChildren(
              ex.children
                .map(nid => NodeId.newBuilder.setId(nid).build)
                .toSeq
                .asJava,
            )
            .build,
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
      key: N.KeyWithMaintainers[V.VersionedValue[V.ContractId]],
  ): KeyWithMaintainers = {
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
                .build,
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
            .build,
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
                .asJava,
            )
            .build,
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
      .setDefinition(loc.definition)
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
              }.asJava,
            )
            .build,
        )
      case V.ValueStruct(fields) =>
        builder.setTuple(
          Tuple.newBuilder
            .addAllFields(
              fields.toSeq.map { field =>
                Field.newBuilder
                  .setLabel(field._1)
                  .setValue(convertValue(field._2))
                  .build
              }.asJava,
            )
            .build,
        )
      case V.ValueVariant(tycon, variant, value) =>
        val vbuilder = Variant.newBuilder
        tycon.foreach(x => vbuilder.setVariantId(convertIdentifier(x)))
        builder.setVariant(
          vbuilder
            .setConstructor(variant)
            .setValue(convertValue(value))
            .build,
        )
      case V.ValueEnum(tycon, constructor) =>
        val eBuilder = Enum.newBuilder.setConstructor(constructor)
        tycon.foreach(x => eBuilder.setEnumId(convertIdentifier(x)))
        builder.setEnum(eBuilder.build)
      case V.ValueContractId(coid) =>
        coid match {
          case V.AbsoluteContractId(acoid) =>
            builder.setContractId(acoid)
          case V.RelativeContractId(txnid, _) =>
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
                .asJava,
            )
            .build,
        )
      case V.ValueInt64(v) => builder.setInt64(v)
      case V.ValueNumeric(d) => builder.setDecimal(Numeric.toString(d))
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
      case V.ValueTextMap(map) =>
        val mapBuilder = v1.Map.newBuilder
        map.toImmArray.foreach {
          case (k, v) =>
            mapBuilder.addEntries(v1.Map.Entry.newBuilder().setKey(k).setValue(convertValue(v)))
            ()
        }
        builder.setMap(mapBuilder)
      case V.ValueGenMap(entries) =>
        val mapBuilder = v1.GenMap.newBuilder
        entries.foreach {
          case (k, v) =>
            mapBuilder.addEntries(
              v1.GenMap.Entry.newBuilder().setKey(convertValue(k)).setValue(convertValue(v)),
            )
            ()
        }
        builder.setGenMap(mapBuilder)
    }
    builder.build
  }

  def convertParty(p: Ref.Party): Party =
    Party.newBuilder.setParty(p).build

}
