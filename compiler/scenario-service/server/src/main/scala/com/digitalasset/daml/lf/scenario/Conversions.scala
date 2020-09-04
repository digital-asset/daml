// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.scenario

import com.daml.lf.data.{ImmArray, Numeric, Ref}
import com.daml.lf.ledger.EventId
import com.daml.lf.scenario.api.{v1 => proto}
import com.daml.lf.speedy.{SError, SValue, PartialTransaction => SPartialTransaction, TraceLog}
import com.daml.lf.transaction.{GlobalKey, Node => N, NodeId, Transaction => Tx}
import com.daml.lf.ledger._
import com.daml.lf.value.{Value => V}

import scala.collection.JavaConverters._

final class Conversions(
    homePackageId: Ref.PackageId,
    ledger: ScenarioLedger,
    ptx: SPartialTransaction,
    traceLog: TraceLog,
    commitLocation: Option[Ref.Location],
    stackTrace: ImmArray[Ref.Location],
) {

  private val empty: proto.Empty = proto.Empty.newBuilder.build

  private val packageIdSelf: proto.PackageIdentifier =
    proto.PackageIdentifier.newBuilder.setSelf(empty).build

  // The ledger data will not contain information from the partial transaction at this point.
  // We need the mapping for converting error message so we manually add it here.
  private val ptxCoidToNodeId = ptx.nodes
    .collect {
      case (nodeId, node: N.NodeCreate[V.ContractId, _]) =>
        node.coid -> ledger.ptxEventId(nodeId)
    }

  private val coidToEventId = ledger.ledgerData.coidToNodeId ++ ptxCoidToNodeId

  private val nodes =
    ledger.ledgerData.nodeInfos.map(Function.tupled(convertNode))

  private val steps = ledger.scenarioSteps.map {
    case (idx, step) => convertScenarioStep(idx.toInt, step)
  }

  def convertScenarioResult(svalue: SValue): proto.ScenarioResult = {
    val builder = proto.ScenarioResult.newBuilder
      .addAllNodes(nodes.asJava)
      .addAllScenarioSteps(steps.asJava)
      .setReturnValue(convertSValue(svalue))
      .setFinalTime(ledger.currentTime.micros)
    traceLog.iterator.foreach { entry =>
      builder.addTraceLog(convertSTraceMessage(entry))
    }
    builder.build
  }

  def convertScenarioError(
      err: SError.SError,
  ): proto.ScenarioError = {
    val builder = proto.ScenarioError.newBuilder
      .addAllNodes(nodes.asJava)
      .addAllScenarioSteps(steps.asJava)
      .setLedgerTime(ledger.currentTime.micros)

    traceLog.iterator.foreach { entry =>
      builder.addTraceLog(convertSTraceMessage(entry))
    }

    def setCrash(reason: String) = builder.setCrash(reason)

    commitLocation.foreach { loc =>
      builder.setCommitLoc(convertLocation(loc))
    }

    builder.addAllStackTrace(stackTrace.map(convertLocation).toSeq.asJava)

    builder.setPartialTransaction(
      convertPartialTransaction(ptx),
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
        val uepvBuilder = proto.ScenarioError.TemplatePreconditionViolated.newBuilder
        optLoc.map(convertLocation).foreach(uepvBuilder.setLocation)
        builder.setTemplatePrecondViolated(
          uepvBuilder
            .setTemplateId(convertIdentifier(tid))
            .setArg(convertValue(arg))
            .build,
        )
      case SError.DamlELocalContractNotActive(coid, tid, consumedBy) =>
        builder.setUpdateLocalContractNotActive(
          proto.ScenarioError.ContractNotActive.newBuilder
            .setContractRef(mkContractRef(coid, tid))
            .setConsumedBy(proto.NodeId.newBuilder.setId(consumedBy.toString).build)
            .build,
        )
      case SError.ScenarioErrorContractNotEffective(coid, tid, effectiveAt) =>
        builder.setScenarioContractNotEffective(
          proto.ScenarioError.ContractNotEffective.newBuilder
            .setEffectiveAt(effectiveAt.micros)
            .setContractRef(mkContractRef(coid, tid))
            .build,
        )

      case SError.ScenarioErrorContractNotActive(coid, tid, consumedBy) =>
        builder.setScenarioContractNotActive(
          proto.ScenarioError.ContractNotActive.newBuilder
            .setContractRef(mkContractRef(coid, tid))
            .setConsumedBy(convertEventId(consumedBy))
            .build,
        )

      case SError.ScenarioErrorContractNotVisible(coid, tid, committer, observers) =>
        builder.setScenarioContractNotVisible(
          proto.ScenarioError.ContractNotVisible.newBuilder
            .setContractRef(mkContractRef(coid, tid))
            .setCommitter(convertParty(committer))
            .addAllObservers(observers.map(convertParty).asJava)
            .build,
        )

      case SError.ScenarioErrorContractKeyNotVisible(coid, gk, committer, stakeholders) =>
        builder.setScenarioContractKeyNotVisible(
          proto.ScenarioError.ContractKeyNotVisible.newBuilder
            .setContractRef(mkContractRef(coid, gk.templateId))
            .setKey(convertValue(gk.key))
            .setCommitter(convertParty(committer))
            .addAllStakeholders(stakeholders.map(convertParty).asJava)
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

      case SError.ScenarioErrorPartyAlreadyExists(party) =>
        builder.setScenarioPartyAlreadyExists(party)

      case SError.ScenarioErrorSerializationError(msg) =>
        sys.error(
          s"Cannot serialization a transaction: $msg"
        )

      case wtc: SError.DamlEWronglyTypedContract =>
        sys.error(
          s"Got unexpected DamlEWronglyTypedContract error in scenario service: $wtc. Note that in the scenario service this error should never surface since contract fetches are all type checked.",
        )

      case divv: SError.DamlEDisallowedInputValueVersion =>
        sys.error(
          s"Got unexpected DamlEDisallowedInputVersion error in scenario service: $divv. Note that in the scenario service this error should never surface since its accept all stable versions.",
        )
    }
    builder.build
  }

  def convertCommitError(commitError: ScenarioLedger.CommitError): proto.CommitError = {
    val builder = proto.CommitError.newBuilder
    commitError match {
      case ScenarioLedger.CommitError.UniqueKeyViolation(gk) =>
        builder.setUniqueKeyViolation(convertGlobalKey(gk.gk))
      case ScenarioLedger.CommitError.FailedAuthorizations(fas) =>
        builder.setFailedAuthorizations(convertFailedAuthorizations(fas))
    }
    builder.build
  }

  def convertGlobalKey(globalKey: GlobalKey): proto.GlobalKey = {
    proto.GlobalKey.newBuilder
      .setTemplateId(convertIdentifier(globalKey.templateId))
      .setKey(convertValue(globalKey.key))
      .build
  }

  def convertSValue(svalue: SValue): proto.Value = {
    def unserializable(what: String): proto.Value =
      proto.Value.newBuilder.setUnserializable(what).build
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

  def convertSTraceMessage(msgAndLoc: (String, Option[Ref.Location])): proto.TraceMessage = {
    val builder = proto.TraceMessage.newBuilder
    msgAndLoc._2.map(loc => builder.setLocation(convertLocation(loc)))
    builder.setMessage(msgAndLoc._1).build
  }

  def convertFailedAuthorizations(fas: FailedAuthorizations): proto.FailedAuthorizations = {
    val builder = proto.FailedAuthorizations.newBuilder
    fas.map {
      case (nodeId, fa) =>
        builder.addFailedAuthorizations {
          val faBuilder = proto.FailedAuthorization.newBuilder
          faBuilder.setNodeId(convertTxNodeId(nodeId))
          fa match {
            case FailedAuthorization.CreateMissingAuthorization(
                templateId,
                optLocation,
                authParties,
                reqParties,
                ) =>
              val cmaBuilder =
                proto.FailedAuthorization.CreateMissingAuthorization.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
                  .addAllAuthorizingParties(authParties.map(convertParty).asJava)
                  .addAllRequiredAuthorizers(reqParties.map(convertParty).asJava)
              optLocation.map(loc => cmaBuilder.setLocation(convertLocation(loc)))
              faBuilder.setCreateMissingAuthorization(cmaBuilder.build)

            case FailedAuthorization.MaintainersNotSubsetOfSignatories(
                templateId,
                optLocation,
                signatories,
                maintainers,
                ) =>
              val maintNotSignBuilder =
                proto.FailedAuthorization.MaintainersNotSubsetOfSignatories.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
                  .addAllSignatories(signatories.map(convertParty).asJava)
                  .addAllMaintainers(maintainers.map(convertParty).asJava)
              optLocation.map(loc => maintNotSignBuilder.setLocation(convertLocation(loc)))
              faBuilder.setMaintainersNotSubsetOfSignatories(maintNotSignBuilder.build)

            case fma: FailedAuthorization.FetchMissingAuthorization =>
              val fmaBuilder =
                proto.FailedAuthorization.FetchMissingAuthorization.newBuilder
                  .setTemplateId(convertIdentifier(fma.templateId))
                  .addAllAuthorizingParties(fma.authorizingParties.map(convertParty).asJava)
                  .addAllStakeholders(fma.stakeholders.map(convertParty).asJava)
              fma.optLocation.map(loc => fmaBuilder.setLocation(convertLocation(loc)))
              faBuilder.setFetchMissingAuthorization(fmaBuilder.build)

            case FailedAuthorization.ExerciseMissingAuthorization(
                templateId,
                choiceId,
                optLocation,
                authParties,
                reqParties,
                ) =>
              val emaBuilder =
                proto.FailedAuthorization.ExerciseMissingAuthorization.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
                  .setChoiceId(choiceId)
                  .addAllAuthorizingParties(authParties.map(convertParty).asJava)
                  .addAllRequiredAuthorizers(reqParties.map(convertParty).asJava)
              optLocation.map(loc => emaBuilder.setLocation(convertLocation(loc)))
              faBuilder.setExerciseMissingAuthorization(emaBuilder.build)
            case FailedAuthorization.ActorMismatch(
                templateId,
                choiceId,
                optLocation,
                givenActors) =>
              val amBuilder =
                proto.FailedAuthorization.ActorMismatch.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
                  .setChoiceId(choiceId)
                  .addAllGivenActors(givenActors.map(convertParty).asJava)
              optLocation.map(loc => amBuilder.setLocation(convertLocation(loc)))
              faBuilder.setActorMismatch(amBuilder.build)
            case FailedAuthorization.NoSignatories(templateId, optLocation) =>
              val nsBuilder =
                proto.FailedAuthorization.NoSignatories.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
              optLocation.map(loc => nsBuilder.setLocation(convertLocation(loc)))
              faBuilder.setNoSignatories(nsBuilder.build)

            case FailedAuthorization.NoControllers(templateId, choiceId, optLocation) =>
              val ncBuilder =
                proto.FailedAuthorization.NoControllers.newBuilder
                  .setTemplateId(convertIdentifier(templateId))
                  .setChoiceId(choiceId)
              optLocation.map(loc => ncBuilder.setLocation(convertLocation(loc)))
              faBuilder.setNoControllers(ncBuilder.build)

            case FailedAuthorization.LookupByKeyMissingAuthorization(
                templateId,
                optLocation,
                maintainers,
                authorizers,
                ) =>
              val lbkmaBuilder =
                proto.FailedAuthorization.LookupByKeyMissingAuthorization.newBuilder
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

  def mkContractRef(coid: V.ContractId, templateId: Ref.Identifier): proto.ContractRef =
    proto.ContractRef.newBuilder
      .setRelative(false)
      .setContractId(coidToEventId(coid).toLedgerString)
      .setTemplateId(convertIdentifier(templateId))
      .build

  def convertScenarioStep(
      stepId: Int,
      step: ScenarioLedger.ScenarioStep,
  ): proto.ScenarioStep = {
    val builder = proto.ScenarioStep.newBuilder
    builder.setStepId(stepId)
    step match {
      case ScenarioLedger.Commit(txId, rtx, optLocation) =>
        val commitBuilder = proto.ScenarioStep.Commit.newBuilder
        optLocation.map { loc =>
          commitBuilder.setLocation(convertLocation(loc))
        }
        builder.setCommit(
          commitBuilder
            .setTxId(txId.index)
            .setTx(convertTransaction(rtx))
            .build,
        )
      case ScenarioLedger.PassTime(dt) =>
        builder.setPassTime(dt)
      case ScenarioLedger.AssertMustFail(actor, optLocation, time, txId) =>
        val assertBuilder = proto.ScenarioStep.AssertMustFail.newBuilder
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
      rtx: ScenarioLedger.RichTransaction,
  ): proto.Transaction = {
    proto.Transaction.newBuilder
      .setCommitter(convertParty(rtx.committer))
      .setEffectiveAt(rtx.effectiveAt.micros)
      .addAllRoots(rtx.transaction.roots.map(convertNodeId(rtx.transactionId, _)).toSeq.asJava)
      .addAllNodes(rtx.transaction.nodes.keys.map(convertNodeId(rtx.transactionId, _)).asJava)
      .setFailedAuthorizations(convertFailedAuthorizations(rtx.failedAuthorizations))
      .build
  }

  def convertPartialTransaction(ptx: SPartialTransaction): proto.PartialTransaction = {
    val builder = proto.PartialTransaction.newBuilder
      .addAllNodes(ptx.nodes.map(convertNode(convertValue(_), _)).asJava)
      .addAllRoots(
        ptx.context.children.toImmArray.toSeq.sortBy(_.index).map(convertTxNodeId).asJava,
      )

    ptx.context.exeContext match {
      case None =>
      case Some(ctx) =>
        val ecBuilder = proto.ExerciseContext.newBuilder
          .setTargetId(mkContractRef(ctx.targetId, ctx.templateId))
          .setChoiceId(ctx.choiceId)
          .setChosenValue(convertValue(ctx.chosenValue))
        ctx.optLocation.map(loc => ecBuilder.setExerciseLocation(convertLocation(loc)))
        builder.setExerciseContext(ecBuilder.build)
    }
    builder.build
  }

  def convertEventId(nodeId: EventId): proto.NodeId =
    proto.NodeId.newBuilder.setId(nodeId.toLedgerString).build

  def convertNodeId(trId: Ref.LedgerString, nodeId: NodeId): proto.NodeId =
    proto.NodeId.newBuilder.setId(EventId(trId, nodeId).toLedgerString).build

  def convertTxNodeId(nodeId: NodeId): proto.NodeId =
    proto.NodeId.newBuilder.setId(nodeId.index.toString).build

  def convertNode(eventId: EventId, nodeInfo: ScenarioLedger.LedgerNodeInfo): proto.Node = {
    val builder = proto.Node.newBuilder
    builder
      .setNodeId(convertEventId(eventId))
      .setEffectiveAt(nodeInfo.effectiveAt.micros)
      .addAllReferencedBy(nodeInfo.referencedBy.map(convertEventId).asJava)
      .addAllDisclosures(nodeInfo.disclosures.toList.map {
        case (party, ScenarioLedger.Disclosure(txId, explicit)) =>
          proto.Disclosure.newBuilder
            .setParty(convertParty(party))
            .setSinceTxId(txId.index)
            .setExplicit(explicit)
            .build
      }.asJava)

    nodeInfo.consumedBy
      .map(eventId => builder.setConsumedBy(convertEventId(eventId)))
    nodeInfo.parent
      .map(eventId => builder.setParent(convertEventId(eventId)))

    nodeInfo.node match {
      case create: N.NodeCreate[V.ContractId, Tx.Value[V.ContractId]] =>
        val createBuilder =
          proto.Node.Create.newBuilder
            .setContractInstance(
              proto.ContractInstance.newBuilder
                .setTemplateId(convertIdentifier(create.coinst.template))
                .setValue(convertValue(create.coinst.arg.value))
                .build,
            )
            .addAllSignatories(create.signatories.map(convertParty).asJava)
            .addAllStakeholders(create.stakeholders.map(convertParty).asJava)

        create.optLocation.map(loc => builder.setLocation(convertLocation(loc)))
        builder.setCreate(createBuilder.build)
      case fetch: N.NodeFetch.WithTxValue[V.ContractId] =>
        builder.setFetch(
          proto.Node.Fetch.newBuilder
            .setContractId(coidToEventId(fetch.coid).toLedgerString)
            .setTemplateId(convertIdentifier(fetch.templateId))
            .addAllSignatories(fetch.signatories.map(convertParty).asJava)
            .addAllStakeholders(fetch.stakeholders.map(convertParty).asJava)
            .build,
        )
      case ex: N.NodeExercises[
            NodeId,
            V.ContractId,
            Tx.Value[
              V.ContractId,
            ]] =>
        ex.optLocation.map(loc => builder.setLocation(convertLocation(loc)))
        builder.setExercise(
          proto.Node.Exercise.newBuilder
            .setTargetContractId(coidToEventId(ex.targetCoid).toLedgerString)
            .setTemplateId(convertIdentifier(ex.templateId))
            .setChoiceId(ex.choiceId)
            .setConsuming(ex.consuming)
            .addAllActingParties(ex.actingParties.map(convertParty).asJava)
            .setChosenValue(convertValue(ex.chosenValue.value))
            .addAllSignatories(ex.signatories.map(convertParty).asJava)
            .addAllStakeholders(ex.stakeholders.map(convertParty).asJava)
            .addAllChildren(
              ex.children
                .map(convertNodeId(eventId.transactionId, _))
                .toSeq
                .asJava,
            )
            .build,
        )

      case lbk: N.NodeLookupByKey[V.ContractId, Tx.Value[V.ContractId]] =>
        lbk.optLocation.foreach(loc => builder.setLocation(convertLocation(loc)))
        val lbkBuilder = proto.Node.LookupByKey.newBuilder
          .setTemplateId(convertIdentifier(lbk.templateId))
          .setKeyWithMaintainers(convertKeyWithMaintainers(convertVersionedValue, lbk.key))
        lbk.result.foreach(cid => lbkBuilder.setContractId(coidToEventId(cid).toLedgerString))
        builder.setLookupByKey(lbkBuilder)

    }
    builder.build
  }

  def convertKeyWithMaintainers[Val](
      convertValue: Val => proto.Value,
      key: N.KeyWithMaintainers[Val],
  ): proto.KeyWithMaintainers = {
    proto.KeyWithMaintainers
      .newBuilder()
      .setKey(convertValue(key.key))
      .addAllMaintainers(key.maintainers.map(convertParty).asJava)
      .build()
  }

  def convertNode[Val](
      convertValue: Val => proto.Value,
      nodeWithId: (NodeId, N.GenNode[NodeId, V.ContractId, Val]),
  ): proto.Node = {
    val (nodeId, node) = nodeWithId
    val builder = proto.Node.newBuilder
    builder
      .setNodeId(proto.NodeId.newBuilder.setId(nodeId.index.toString).build)
    // FIXME(JM): consumedBy, parent, ...
    node match {
      case create: N.NodeCreate[V.ContractId, Val] =>
        val createBuilder =
          proto.Node.Create.newBuilder
            .setContractInstance(
              proto.ContractInstance.newBuilder
                .setTemplateId(convertIdentifier(create.coinst.template))
                .setValue(convertValue(create.coinst.arg))
                .build,
            )
            .addAllSignatories(create.signatories.map(convertParty).asJava)
            .addAllStakeholders(create.stakeholders.map(convertParty).asJava)
        create.key.foreach(key =>
          createBuilder.setKeyWithMaintainers(convertKeyWithMaintainers(convertValue, key)))
        create.optLocation.map(loc => builder.setLocation(convertLocation(loc)))
        builder.setCreate(createBuilder.build)
      case fetch: N.NodeFetch[V.ContractId, Val] =>
        builder.setFetch(
          proto.Node.Fetch.newBuilder
            .setContractId(coidToEventId(fetch.coid).toLedgerString)
            .setTemplateId(convertIdentifier(fetch.templateId))
            .addAllSignatories(fetch.signatories.map(convertParty).asJava)
            .addAllStakeholders(fetch.stakeholders.map(convertParty).asJava)
            .build,
        )
      case ex: N.NodeExercises[NodeId, V.ContractId, Val] =>
        ex.optLocation.map(loc => builder.setLocation(convertLocation(loc)))
        builder.setExercise(
          proto.Node.Exercise.newBuilder
            .setTargetContractId(coidToEventId(ex.targetCoid).toLedgerString)
            .setTemplateId(convertIdentifier(ex.templateId))
            .setChoiceId(ex.choiceId)
            .setConsuming(ex.consuming)
            .addAllActingParties(ex.actingParties.map(convertParty).asJava)
            .setChosenValue(convertValue(ex.chosenValue))
            .addAllSignatories(ex.signatories.map(convertParty).asJava)
            .addAllStakeholders(ex.stakeholders.map(convertParty).asJava)
            .addAllChildren(
              ex.children
                .map(nid => proto.NodeId.newBuilder.setId(nid.index.toString).build)
                .toSeq
                .asJava,
            )
            .build,
        )

      case lookup: N.NodeLookupByKey[V.ContractId, Val] =>
        lookup.optLocation.map(loc => builder.setLocation(convertLocation(loc)))
        builder.setLookupByKey({
          val builder = proto.Node.LookupByKey.newBuilder
            .setKeyWithMaintainers(convertKeyWithMaintainers(convertValue, lookup.key))
          lookup.result.foreach(cid => builder.setContractId(coidToEventId(cid).toLedgerString))
          builder.build
        })
    }
    builder.build
  }

  def convertPackageId(pkg: Ref.PackageId): proto.PackageIdentifier =
    if (pkg == homePackageId)
      // Reconstitute the self package reference.
      packageIdSelf
    else
      proto.PackageIdentifier.newBuilder.setPackageId(pkg).build

  def convertIdentifier(identifier: Ref.Identifier): proto.Identifier =
    proto.Identifier.newBuilder
      .setPackage(convertPackageId(identifier.packageId))
      .setName(identifier.qualifiedName.toString)
      .build

  def convertLocation(loc: Ref.Location): proto.Location = {
    val (sline, scol) = loc.start
    val (eline, ecol) = loc.end
    proto.Location.newBuilder
      .setPackage(convertPackageId(loc.packageId))
      .setModule(loc.module.toString)
      .setDefinition(loc.definition)
      .setStartLine(sline)
      .setStartCol(scol)
      .setEndLine(eline)
      .setEndCol(ecol)
      .build

  }

  private def convertVersionedValue(value: V.VersionedValue[V.ContractId]): proto.Value =
    convertValue(value.value)

  def convertValue(value: V[V.ContractId]): proto.Value = {
    val builder = proto.Value.newBuilder
    value match {
      case V.ValueRecord(tycon, fields) =>
        val rbuilder = proto.Record.newBuilder
        tycon.map(x => rbuilder.setRecordId(convertIdentifier(x)))
        builder.setRecord(
          rbuilder
            .addAllFields(
              fields.toSeq.map {
                case (optName, fieldValue) =>
                  val builder = proto.Field.newBuilder
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
          proto.Tuple.newBuilder
            .addAllFields(
              fields.iterator
                .map { field =>
                  proto.Field.newBuilder
                    .setLabel(field._1)
                    .setValue(convertValue(field._2))
                    .build
                }
                .toSeq
                .asJava,
            )
            .build,
        )
      case V.ValueVariant(tycon, variant, value) =>
        val vbuilder = proto.Variant.newBuilder
        tycon.foreach(x => vbuilder.setVariantId(convertIdentifier(x)))
        builder.setVariant(
          vbuilder
            .setConstructor(variant)
            .setValue(convertValue(value))
            .build,
        )
      case V.ValueEnum(tycon, constructor) =>
        val eBuilder = proto.Enum.newBuilder.setConstructor(constructor)
        tycon.foreach(x => eBuilder.setEnumId(convertIdentifier(x)))
        builder.setEnum(eBuilder.build)
      case V.ValueContractId(coid) =>
        builder.setContractId(coidToEventId(coid).toLedgerString)
      case V.ValueList(values) =>
        builder.setList(
          proto.List.newBuilder
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
        val optionalBuilder = proto.Optional.newBuilder
        mbV match {
          case None => ()
          case Some(v) => optionalBuilder.setValue(convertValue(v))
        }
        builder.setOptional(optionalBuilder)
      case V.ValueTextMap(map) =>
        val mapBuilder = proto.Map.newBuilder
        map.toImmArray.foreach {
          case (k, v) =>
            mapBuilder.addEntries(proto.Map.Entry.newBuilder().setKey(k).setValue(convertValue(v)))
            ()
        }
        builder.setMap(mapBuilder)
      case V.ValueGenMap(entries) =>
        val mapBuilder = proto.GenMap.newBuilder
        entries.foreach {
          case (k, v) =>
            mapBuilder.addEntries(
              proto.GenMap.Entry.newBuilder().setKey(convertValue(k)).setValue(convertValue(v)),
            )
            ()
        }
        builder.setGenMap(mapBuilder)
    }
    builder.build
  }

  def convertParty(p: Ref.Party): proto.Party =
    proto.Party.newBuilder.setParty(p).build

}
