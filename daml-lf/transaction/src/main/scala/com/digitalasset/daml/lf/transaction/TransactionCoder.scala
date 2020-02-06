// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction

import com.digitalasset.daml.lf.data.BackStack
import com.digitalasset.daml.lf.transaction.TransactionOuterClass.Node.NodeTypeCase
import com.digitalasset.daml.lf.data.Ref.{Name, Party}
import com.digitalasset.daml.lf.transaction.Node._
import VersionTimeline.Implicits._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.VersionedValue
import com.digitalasset.daml.lf.value.{ValueCoder, ValueOuterClass, ValueVersion}
import com.digitalasset.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.google.protobuf.ProtocolStringList

import scala.collection.JavaConverters._
import scalaz.syntax.std.boolean._
import scalaz.syntax.traverse.ToTraverseOps
import scalaz.std.either.eitherMonad
import scalaz.std.option._

import scala.collection.immutable.HashMap

object TransactionCoder {

  import ValueCoder.{DecodeCid, EncodeCid, codecContractId}
  type EncodeNid[Nid] = Nid => String
  type EncodeVal[Cid <: Value.ContractId] =
    VersionedValue[Cid] => Either[EncodeError, (ValueVersion, ValueOuterClass.VersionedValue)]

  private val valueVersion1Only: Set[TransactionVersion] = Set("1") map TransactionVersion

  /**
    * Encodes a contract instance with the help of the contractId encoding function
    * @param coinst the contract instance to be encoded
    * @param encodeVal function to encode a value to protobuf
    * @return protobuf wire format contract instance
    */
  def encodeContractInstance[Cid <: Value.ContractId](
      encodeVal: EncodeVal[Cid],
      coinst: Value.ContractInst[Value.VersionedValue[Cid]],
  ): Either[EncodeError, TransactionOuterClass.ContractInstance] = {
    encodeVal(coinst.arg)
      .map {
        case (vversion, arg) =>
          val (_, id) = ValueCoder.encodeIdentifier(coinst.template, Some(vversion))
          TransactionOuterClass.ContractInstance
            .newBuilder()
            .setTemplateId(id)
            .setValue(arg)
            .setAgreement(coinst.agreementText)
            .build()
      }
  }

  /**
    * Decode a contract instance from wire format
    * @param protoCoinst protocol buffer encoded contract instance
    * @param decodeVal value decoding function
    * @return contract instance value
    */
  def decodeContractInstance[Cid <: Value.ContractId](
      decodeVal: ValueOuterClass.VersionedValue => Either[DecodeError, Value.VersionedValue[Cid]],
      protoCoinst: TransactionOuterClass.ContractInstance,
  ): Either[DecodeError, Value.ContractInst[Value.VersionedValue[Cid]]] = {
    ValueCoder.decodeIdentifier(protoCoinst.getTemplateId).flatMap { id =>
      decodeVal(protoCoinst.getValue)
        .map(a => Value.ContractInst(id, a, (protoCoinst.getAgreement)))
    }
  }

  private def encodeKeyWithMaintainers[Cid <: Value.ContractId](
      encodeVal: EncodeVal[Cid],
      key: KeyWithMaintainers[Value.VersionedValue[Cid]],
  ): Either[EncodeError, (ValueVersion, TransactionOuterClass.KeyWithMaintainers)] = {
    encodeVal(key.key).map {
      case (vversion, encodedKey) =>
        (
          vversion,
          TransactionOuterClass.KeyWithMaintainers
            .newBuilder()
            .setKey(encodedKey)
            .addAllMaintainers(key.maintainers.toSet[String].asJava)
            .build(),
        )
    }
  }

  /**
    * encodes a [[GenNode[Nid, Cid]] to protocol buffer
    * @param nodeId node id of the node to be encoded
    * @param node the node to be encoded
    * @param encodeNid node id encoding to string
    * @param encodeCid contract id encoding to string
    * @param encodeVal value encoding function
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return protocol buffer format node
    */
  def encodeNode[Nid, Cid <: Value.ContractId](
      encodeNid: EncodeNid[Nid],
      encodeCid: EncodeCid[Cid],
      encodeVal: EncodeVal[Cid],
      transactionVersion: TransactionVersion,
      nodeId: Nid,
      node: GenNode[Nid, Cid, Value.VersionedValue[Cid]],
  ): Either[EncodeError, TransactionOuterClass.Node] = {
    val nodeBuilder = TransactionOuterClass.Node.newBuilder().setNodeId(encodeNid(nodeId))
    import TransactionVersions.{
      minKeyOrLookupByKey,
      minNoControllers,
      minExerciseResult,
      minContractKeyInExercise,
      minMaintainersInExercise,
    }
    node match {
      case nc @ NodeCreate(_, _, _, _, _, _, _) =>
        encodeContractInstance(encodeVal, nc.coinst).flatMap { inst =>
          val createBuilder = TransactionOuterClass.NodeCreate
            .newBuilder()
            .setContractIdOrStruct(encodeCid, transactionVersion, nc.coid)(
              _.setContractId(_),
              _.setContractIdStruct(_),
            )
            .setContractInstance(inst)
            .addAllStakeholders(nc.stakeholders.toSet[String].asJava)
            .addAllSignatories(nc.signatories.toSet[String].asJava)
          nc.key match {
            case None => Right(nodeBuilder.setCreate(createBuilder).build())
            case Some(key) =>
              if (transactionVersion precedes minKeyOrLookupByKey)
                Left(EncodeError(transactionVersion, isTooOldFor = "NodeCreate's `key` field"))
              else
                encodeKeyWithMaintainers(encodeVal, key).map {
                  case (_, encodedKey) =>
                    createBuilder.setKeyWithMaintainers(encodedKey)
                    nodeBuilder.setCreate(createBuilder).build()
                }
          }
        }

      case nf @ NodeFetch(_, _, _, _, _, _) =>
        val (vversion, etid) = ValueCoder.encodeIdentifier(
          nf.templateId,
          valueVersion1Only(transactionVersion) option ValueVersion("1"),
        )
        val fetchBuilder = TransactionOuterClass.NodeFetch
          .newBuilder()
          .setContractIdOrStruct(encodeCid, transactionVersion, nf.coid)(
            _.setContractId(_),
            _.setContractIdStruct(_),
          )
          .setTemplateId(etid)
          .setValueVersion(vversion.protoValue)
          .addAllStakeholders(nf.stakeholders.toSet[String].asJava)
          .addAllSignatories(nf.signatories.toSet[String].asJava)

        if (transactionVersion precedes TransactionVersions.minFetchActors) {
          if (nf.actingParties.nonEmpty)
            Left(EncodeError(transactionVersion, isTooOldFor = "NodeFetch actors"))
          else Right(nodeBuilder.setFetch(fetchBuilder).build())
        } else {
          val fetchBuilderWithActors =
            fetchBuilder.addAllActors(nf.actingParties.getOrElse(Set.empty).toSet[String].asJava)
          Right(nodeBuilder.setFetch(fetchBuilderWithActors).build())
        }

      case ne @ NodeExercises(_, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
        for {
          argValue <- encodeVal(ne.chosenValue)
          (vversion, arg) = argValue
          retValue <- ne.exerciseResult traverseU encodeVal
          exBuilder = TransactionOuterClass.NodeExercise
            .newBuilder()
            .setChoice(ne.choiceId)
            .setTemplateId(ValueCoder.encodeIdentifier(ne.templateId, Some(vversion))._2)
            .setChosenValue(arg)
            .setConsuming(ne.consuming)
            .setContractIdOrStruct(encodeCid, transactionVersion, ne.targetCoid)(
              _.setContractId(_),
              _.setContractIdStruct(_),
            )
            .addAllActors(ne.actingParties.toSet[String].asJava)
            .addAllChildren(ne.children.map(encodeNid).toList.asJava)
            .addAllSignatories(ne.signatories.toSet[String].asJava)
            .addAllStakeholders(ne.stakeholders.toSet[String].asJava)
          _ <- if (transactionVersion precedes minNoControllers) {
            if (ne.controllers == ne.actingParties) {
              exBuilder.addAllControllers(ne.controllers.toSet[String].asJava)
              Right(())
            } else {
              Left(
                EncodeError(
                  s"As of version $minNoControllers, the controllers and actingParties of an exercise node _must_ be the same, but I got ${ne.controllers} as controllers and ${ne.actingParties} as actingParties.",
                ),
              )
            }
          } else Right(())
          _ <- (retValue, transactionVersion precedes minExerciseResult) match {
            case (Some(rv), false) =>
              exBuilder.setReturnValue(rv._2)
              Right(())
            case (None, false) =>
              Left(
                EncodeError(
                  s"Trying to encode transaction of version $transactionVersion, which requires the exercise return value, but did not get exercise return value in node.",
                ),
              )
            case (_, true) => Right(())
          }
          _ <- Right(
            ne.key
              .map { kWithM =>
                if (transactionVersion precedes minContractKeyInExercise) ()
                else if (transactionVersion precedes minMaintainersInExercise) {
                  encodeVal(kWithM.key).map { encodedKey =>
                    exBuilder.setContractKey(encodedKey._2)
                  }
                } else
                  encodeKeyWithMaintainers(encodeVal, kWithM).map {
                    case (_, encodedKey) =>
                      exBuilder.setKeyWithMaintainers(encodedKey)
                  }
                ()
              }
              .getOrElse(()),
          )
        } yield nodeBuilder.setExercise(exBuilder).build()

      case nlbk @ NodeLookupByKey(_, _, _, _) =>
        if (transactionVersion precedes minKeyOrLookupByKey)
          Left(EncodeError(transactionVersion, isTooOldFor = "NodeLookupByKey transaction nodes"))
        else
          encodeKeyWithMaintainers(encodeVal, nlbk.key).map {
            case (vversion, key) =>
              val nlbkBuilder = TransactionOuterClass.NodeLookupByKey
                .newBuilder()
                .setTemplateId(ValueCoder.encodeIdentifier(nlbk.templateId, Some(vversion))._2)
                .setKeyWithMaintainers(key)
              nlbk.result match {
                case None => ()
                case Some(result) =>
                  nlbkBuilder.setContractIdOrStruct(encodeCid, transactionVersion, result)(
                    _.setContractId(_),
                    _.setContractIdStruct(_),
                  )
              }
              nodeBuilder.setLookupByKey(nlbkBuilder).build()
          }
    }
  }

  private def decodeKeyWithMaintainers[Cid <: Value.ContractId](
      decodeVal: ValueOuterClass.VersionedValue => Either[DecodeError, Value.VersionedValue[Cid]],
      keyWithMaintainers: TransactionOuterClass.KeyWithMaintainers,
  ): Either[DecodeError, KeyWithMaintainers[Value.VersionedValue[Cid]]] =
    for {
      maintainers <- toPartySet(keyWithMaintainers.getMaintainersList)
      key <- decodeVal(keyWithMaintainers.getKey())
    } yield KeyWithMaintainers(key, maintainers)

  /**
    * read a [[GenNode[Nid, Cid]] from protobuf
    * @param protoNode protobuf encoded node
    * @param decodeNid function to read node id from String
    * @param decodeCid function to read contract id from String
    * @param decodeVal function to read value from protobuf
    * @tparam Nid Node id type
    * @tparam Cid Contract id type
    * @return decoded GenNode
    */
  def decodeNode[Nid, Cid <: Value.ContractId](
      decodeNid: String => Either[DecodeError, Nid],
      decodeCid: DecodeCid[Cid],
      decodeVal: ValueOuterClass.VersionedValue => Either[DecodeError, Value.VersionedValue[Cid]],
      txVersion: TransactionVersion,
      protoNode: TransactionOuterClass.Node,
  ): Either[DecodeError, (Nid, GenNode[Nid, Cid, Value.VersionedValue[Cid]])] = {
    val nodeId = decodeNid(protoNode.getNodeId)

    import TransactionVersions.{
      minKeyOrLookupByKey,
      minNoControllers,
      minExerciseResult,
      minContractKeyInExercise,
      minMaintainersInExercise,
    }
    protoNode.getNodeTypeCase match {
      case NodeTypeCase.CREATE =>
        for {
          ni <- nodeId
          protoCreate = protoNode.getCreate
          c <- protoCreate.decodeContractIdOrStruct(decodeCid, txVersion)(
            _.getContractId,
            _.getContractIdStruct,
          )
          ci <- decodeContractInstance(decodeVal, protoCreate.getContractInstance)
          stakeholders <- toPartySet(protoCreate.getStakeholdersList)
          signatories <- toPartySet(protoCreate.getSignatoriesList)
          key <- if (protoCreate.getKeyWithMaintainers == TransactionOuterClass.KeyWithMaintainers.getDefaultInstance)
            Right(None)
          else if (txVersion precedes minKeyOrLookupByKey)
            Left(DecodeError(s"$txVersion is too old to support NodeCreate's `key` field"))
          else decodeKeyWithMaintainers(decodeVal, protoCreate.getKeyWithMaintainers).map(Some(_))
        } yield (ni, NodeCreate(None, c, ci, None, signatories, stakeholders, key))
      case NodeTypeCase.FETCH =>
        val protoFetch = protoNode.getFetch
        for {
          ni <- nodeId
          templateId <- ValueCoder.decodeIdentifier(protoFetch.getTemplateId)
          c <- protoFetch.decodeContractIdOrStruct(decodeCid, txVersion)(
            _.getContractId,
            _.getContractIdStruct,
          )
          actingPartiesSet <- toPartySet(protoFetch.getActorsList)
          _ <- if ((txVersion precedes TransactionVersions.minFetchActors) && actingPartiesSet.nonEmpty)
            Left(DecodeError(txVersion, isTooOldFor = "NodeFetch actors"))
          else Right(())
          actingParties <- if (txVersion precedes TransactionVersions.minFetchActors) Right(None)
          else Right(Some(actingPartiesSet))
          stakeholders <- toPartySet(protoFetch.getStakeholdersList)
          signatories <- toPartySet(protoFetch.getSignatoriesList)
        } yield (ni, NodeFetch(c, templateId, None, actingParties, signatories, stakeholders))

      case NodeTypeCase.EXERCISE =>
        val protoExe = protoNode.getExercise
        val childrenOrError = protoExe.getChildrenList.asScala
          .foldLeft[Either[DecodeError, BackStack[Nid]]](Right(BackStack.empty[Nid])) {
            case (Left(e), _) => Left(e)
            case (Right(ids), s) => decodeNid(s).map(ids :+ _)
          }
          .map(_.toImmArray)

        for {
          rv <- if (txVersion precedes minExerciseResult) {
            if (protoExe.hasReturnValue)
              Left(DecodeError(txVersion, isTooOldFor = "exercise result"))
            else Right(None)
          } else decodeVal(protoExe.getReturnValue).map(Some(_))
          hasKeyWithMaintainersField = (protoExe.getKeyWithMaintainers != TransactionOuterClass.KeyWithMaintainers.getDefaultInstance)
          keyWithMaintainers <- if (protoExe.hasContractKey) {
            if (txVersion precedes minContractKeyInExercise)
              Left(DecodeError(txVersion, isTooOldFor = "contract key in exercise"))
            else if (!(txVersion precedes minMaintainersInExercise))
              Left(
                DecodeError(
                  s"contract key field in exercise must not be present for transactions of version $txVersion",
                ),
              )
            else if (hasKeyWithMaintainersField)
              Left(
                DecodeError(
                  "an exercise may not contain both contract key and contract key with maintainers",
                ),
              )
            else
              decodeVal(protoExe.getContractKey).map(k => Some(KeyWithMaintainers(k, Set.empty)))
          } else if (hasKeyWithMaintainersField) {
            if (txVersion precedes minMaintainersInExercise)
              Left(DecodeError(txVersion, isTooOldFor = "NodeExercises maintainers"))
            else
              decodeKeyWithMaintainers(decodeVal, protoExe.getKeyWithMaintainers).map(k => Some(k))
          } else Right(None)

          ni <- nodeId
          targetCoid <- protoExe.decodeContractIdOrStruct(decodeCid, txVersion)(
            _.getContractId,
            _.getContractIdStruct,
          )
          children <- childrenOrError
          cv <- decodeVal(protoExe.getChosenValue)
          templateId <- ValueCoder.decodeIdentifier(protoExe.getTemplateId)
          actingParties <- toPartySet(protoExe.getActorsList)
          encodedControllers <- toPartySet(protoExe.getControllersList)
          controllers <- if (!(txVersion precedes minNoControllers)) {
            if (encodedControllers.isEmpty) {
              Right(actingParties)
            } else {
              Left(DecodeError(s"As of version $txVersion, exercise controllers must be empty."))
            }
          } else {
            Right(encodedControllers)
          }
          signatories <- toPartySet(protoExe.getSignatoriesList)
          stakeholders <- toPartySet(protoExe.getStakeholdersList)
          choiceName <- toIdentifier(protoExe.getChoice)
        } yield
          (
            ni,
            NodeExercises(
              None,
              targetCoid = targetCoid,
              templateId = templateId,
              choiceId = choiceName,
              optLocation = None,
              consuming = protoExe.getConsuming,
              actingParties = actingParties,
              chosenValue = cv,
              stakeholders = stakeholders,
              signatories = signatories,
              controllers = controllers,
              children = children,
              exerciseResult = rv,
              key = keyWithMaintainers,
            ),
          )
      case NodeTypeCase.LOOKUP_BY_KEY =>
        val protoLookupByKey = protoNode.getLookupByKey
        for {
          _ <- if (txVersion precedes minKeyOrLookupByKey)
            Left(DecodeError(s"$txVersion is too old to support NodeLookupByKey"))
          else Right(())
          ni <- nodeId
          templateId <- ValueCoder.decodeIdentifier(protoLookupByKey.getTemplateId)
          key <- decodeKeyWithMaintainers(decodeVal, protoLookupByKey.getKeyWithMaintainers)
          cid <- protoLookupByKey.decodeOptionalContractIdOrStruct(decodeCid, txVersion)(
            _.getContractId,
            _.getContractIdStruct,
          )
        } yield (ni, NodeLookupByKey[Cid, Value.VersionedValue[Cid]](templateId, None, key, cid))
      case NodeTypeCase.NODETYPE_NOT_SET => Left(DecodeError("Unset Node type"))
    }
  }

  /**
    * Encode a [[GenTransaction[Nid, Cid]]] to protobuf using [[TransactionVersion]] provided by the libary, see
    * [[TransactionVersions.assignVersion]].
    *
    * @param tx the transaction to be encoded
    * @param encodeNid node id encoding function
    * @param encodeCid contract id encoding function
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return protobuf encoded transaction
    */
  def encodeTransaction[Nid, Cid <: Value.ContractId](
      encodeNid: EncodeNid[Nid],
      encodeCid: EncodeCid[Cid],
      tx: GenTransaction[Nid, Cid, VersionedValue[Cid]],
  ): Either[EncodeError, TransactionOuterClass.Transaction] =
    encodeTransactionWithCustomVersion(
      encodeNid,
      encodeCid,
      VersionedTransaction(TransactionVersions.assignVersion(tx), tx),
    )

  /**
    * Encode a transaction to protobuf using [[TransactionVersion]] provided by in the [[VersionedTransaction]] argument.
    *
    * @param transaction the transaction to be encoded
    * @param encodeNid node id encoding function
    * @param encodeCid contract id encoding function
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return protobuf encoded transaction
    */
  private[transaction] def encodeTransactionWithCustomVersion[Nid, Cid <: Value.ContractId](
      encodeNid: EncodeNid[Nid],
      encodeCid: EncodeCid[Cid],
      transaction: VersionedTransaction[Nid, Cid],
  ): Either[EncodeError, TransactionOuterClass.Transaction] = {
    val tx = transaction.transaction
    val txVersion: TransactionVersion = transaction.version
    val roots = tx.roots.map(encodeNid)
    // fold traverses the transaction in deterministic order
    val mbNodes = tx
      .fold[Either[EncodeError, BackStack[TransactionOuterClass.Node]]](
        Right(BackStack.empty),
      ) {
        case (acc, (id, node)) =>
          for {
            stack <- acc
            encodedNode <- encodeNode(
              encodeNid,
              encodeCid,
              (v: VersionedValue[Cid]) =>
                ValueCoder.encodeVersionedValueWithCustomVersion(encodeCid, v).map((v.version, _)),
              txVersion,
              id,
              node,
            )
          } yield stack :+ encodedNode
      }
      .map(_.toImmArray)
    mbNodes.map(nodes => {
      TransactionOuterClass.Transaction
        .newBuilder()
        .setVersion(txVersion.protoValue)
        .addAllRoots(roots.toList.asJava)
        .addAllNodes(nodes.toSeq.asJava)
        .build()
    })
  }

  private def decodeVersion(vs: String): Either[DecodeError, TransactionVersion] =
    TransactionVersions
      .isAcceptedVersion(vs)
      .fold[Either[DecodeError, TransactionVersion]](
        Left(DecodeError(s"Unsupported transaction version $vs")),
      )(v => Right(v))

  /**
    * Reads a [[VersionedTransaction]] from protobuf and checks if
    * [[TransactionVersion]] passed in the protobuf is currently supported.
    *
    * Supported transaction versions configured in [[TransactionVersions]].
    *
    * @param protoTx protobuf encoded transaction
    * @param decodeNid node id decoding function
    * @param decodeCid contract id decoding function
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return  decoded transaction
    */
  def decodeVersionedTransaction[Nid, Cid <: Value.ContractId](
      decodeNid: String => Either[DecodeError, Nid],
      decodeCid: DecodeCid[Cid],
      protoTx: TransactionOuterClass.Transaction,
  ): Either[DecodeError, VersionedTransaction[Nid, Cid]] =
    for {
      version <- decodeVersion(protoTx.getVersion)
      tx <- decodeTransaction(
        decodeNid,
        decodeCid,
        ValueCoder.decodeVersionedValue(decodeCid, _),
        version,
        protoTx,
      )
    } yield VersionedTransaction(version, tx)

  /**
    * Reads a [[GenTransaction[Nid, Cid]]] from protobuf. Does not check if
    * [[TransactionVersion]] passed in the protobuf is currently supported, if you need this check use
    * [[TransactionCoder.decodeVersionedTransaction]].
    *
    * @param protoTx protobuf encoded transaction
    * @param decodeNid node id decoding function
    * @param decodeCid contract id decoding function
    * @param decodeVal function to read value from protobuf
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return  decoded transaction
    */
  private def decodeTransaction[Nid, Cid <: Value.ContractId](
      decodeNid: String => Either[DecodeError, Nid],
      decodeCid: DecodeCid[Cid],
      decodeVal: ValueOuterClass.VersionedValue => Either[DecodeError, Value.VersionedValue[Cid]],
      txVersion: TransactionVersion,
      protoTx: TransactionOuterClass.Transaction,
  ): Either[DecodeError, GenTransaction[Nid, Cid, Value.VersionedValue[Cid]]] = {
    val roots = protoTx.getRootsList.asScala
      .foldLeft[Either[DecodeError, BackStack[Nid]]](Right(BackStack.empty[Nid])) {
        case (Right(acc), s) => decodeNid(s).map(acc :+ _)
        case (Left(e), _) => Left(e)
      }
      .map(_.toImmArray)

    val nodes = protoTx.getNodesList.asScala
      .foldLeft[Either[DecodeError, HashMap[Nid, GenNode[Nid, Cid, Value.VersionedValue[Cid]]]]](
        Right(HashMap.empty)) {
        case (Left(e), _) => Left(e)
        case (Right(acc), s) =>
          decodeNode(decodeNid, decodeCid, decodeVal, txVersion, s).map(acc + _)
      }

    for {
      rs <- roots
      ns <- nodes
    } yield GenTransaction(ns, rs, None)
  }

  def toPartySet(strList: ProtocolStringList): Either[DecodeError, Set[Party]] = {
    val parties = strList
      .asByteStringList()
      .asScala
      .map(bs => Party.fromString(bs.toStringUtf8))

    sequence(parties) match {
      case Left(err) => Left(DecodeError(s"Cannot decode party: $err"))
      case Right(ps) => Right(ps.toSet)
    }
  }

  private def toIdentifier(s: String): Either[DecodeError, Name] =
    Name.fromString(s).left.map(DecodeError)

  /** Node information for a serialized transaction node. Used to compute
    * informees when deserialization is too costly.
    * This method is not supported for transaction version <5 (as NodeInfo does not support it).
    * We're not using e.g. "implicit class" in order to keep the decoding errors explicit.
    * NOTE(JM): Currently used only externally, but kept here to keep in sync
    * with the implementation.
    */
  def protoNodeInfo(
      txVersion: TransactionVersion,
      protoNode: TransactionOuterClass.Node): Either[DecodeError, NodeInfo] =
    if (txVersion precedes TransactionVersions.minFetchActors) {
      Left(DecodeError(s"NodeInfo not supported for transaction version $txVersion"))
    } else {
      protoNode.getNodeTypeCase match {
        case NodeTypeCase.CREATE =>
          val protoCreate = protoNode.getCreate
          for {
            signatories_ <- toPartySet(protoCreate.getSignatoriesList)
            stakeholders_ <- toPartySet(protoCreate.getStakeholdersList)
          } yield {
            new NodeInfo.Create {
              def signatories = signatories_
              def stakeholders = stakeholders_
            }
          }
        case NodeTypeCase.FETCH =>
          val protoFetch = protoNode.getFetch
          for {
            actingParties_ <- toPartySet(protoFetch.getActorsList)
            stakeholders_ <- toPartySet(protoFetch.getStakeholdersList)
            signatories_ <- toPartySet(protoFetch.getSignatoriesList)
          } yield {
            new NodeInfo.Fetch {
              def signatories = signatories_
              def stakeholders = stakeholders_
              def actingParties = Some(actingParties_)
            }
          }

        case NodeTypeCase.EXERCISE =>
          val protoExe = protoNode.getExercise
          for {
            actingParties_ <- toPartySet(protoExe.getActorsList)
            signatories_ <- toPartySet(protoExe.getSignatoriesList)
            stakeholders_ <- toPartySet(protoExe.getStakeholdersList)
          } yield {
            new NodeInfo.Exercise {
              def signatories = signatories_
              def stakeholders = stakeholders_
              def actingParties = actingParties_
              def consuming = protoExe.getConsuming
            }
          }

        case NodeTypeCase.LOOKUP_BY_KEY =>
          val protoLookupByKey = protoNode.getLookupByKey
          for {
            maintainers <- toPartySet(protoLookupByKey.getKeyWithMaintainers.getMaintainersList)
          } yield {
            new NodeInfo.LookupByKey {
              def hasResult =
                protoLookupByKey.getContractId.nonEmpty ||
                  protoLookupByKey.hasContractIdStruct
              def keyMaintainers = maintainers
            }
          }

        case NodeTypeCase.NODETYPE_NOT_SET => Left(DecodeError("Unset Node type"))
      }
    }

}
