// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction

import com.digitalasset.daml.lf.data.{BackStack, ImmArray}
import com.digitalasset.daml.lf.transaction.TransactionOuterClass.Node.NodeTypeCase
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.transaction.Node._
import VersionTimeline.Implicits._
import com.digitalasset.daml.lf.value.Value.{ContractInst, VersionedValue}
import com.digitalasset.daml.lf.value.{ValueCoder, ValueOuterClass, ValueVersion}
import com.digitalasset.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.google.protobuf.ProtocolStringList

import scala.collection.JavaConverters._
import scalaz.syntax.std.boolean._
import scalaz.syntax.traverse.ToTraverseOps
import scalaz.std.either.eitherMonad

object TransactionCoder {

  import ValueCoder.{DecodeCid, EncodeCid, codecContractId}
  type EncodeNid[-Nid] = Nid => String
  type EncodeVal[-Val] = Val => Either[EncodeError, (ValueVersion, ValueOuterClass.VersionedValue)]

  private val valueVersion1Only: Set[TransactionVersion] = Set("1") map TransactionVersion

  /**
    * Encodes a contract instance with the help of the contractId encoding function
    * @param coinst the contract instance to be encoded
    * @param encodeVal function to encode a value to protobuf
    * @tparam Val value type
    * @return protobuf wire format contract instance
    */
  def encodeContractInstance[Val](
      encodeVal: EncodeVal[Val],
      coinst: ContractInst[Val]): Either[EncodeError, TransactionOuterClass.ContractInstance] = {
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
    * @tparam Val value type
    * @return contract instance value
    */
  def decodeContractInstance[Val](
      decodeVal: ValueOuterClass.VersionedValue => Either[DecodeError, Val],
      protoCoinst: TransactionOuterClass.ContractInstance)
    : Either[DecodeError, ContractInst[Val]] = {
    ValueCoder.decodeIdentifier(protoCoinst.getTemplateId).flatMap { id =>
      decodeVal(protoCoinst.getValue)
        .map(a => ContractInst(id, a, protoCoinst.getAgreement))
    }
  }

  private def encodeKeyWithMaintainers[Val](encodeVal: EncodeVal[Val], key: KeyWithMaintainers[Val])
    : Either[EncodeError, (ValueVersion, TransactionOuterClass.KeyWithMaintainers)] = {
    encodeVal(key.key).map {
      case (vversion, encodedKey) =>
        (
          vversion,
          TransactionOuterClass.KeyWithMaintainers
            .newBuilder()
            .setKey(encodedKey)
            .addAllMaintainers(key.maintainers.map(_.underlyingString).asJava)
            .build())
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
  def encodeNode[Nid, Cid, Val](
      encodeNid: EncodeNid[Nid],
      encodeCid: EncodeCid[Cid],
      encodeVal: EncodeVal[Val],
      transactionVersion: TransactionVersion,
      nodeId: Nid,
      node: GenNode[Nid, Cid, Val]): Either[EncodeError, TransactionOuterClass.Node] = {
    val nodeBuilder = TransactionOuterClass.Node.newBuilder().setNodeId(encodeNid(nodeId))
    import TransactionVersions.minKeyOrLookupByKey
    node match {
      case c: NodeCreate[Cid, Val] =>
        encodeContractInstance(encodeVal, c.coinst).flatMap { inst =>
          val createBuilder = TransactionOuterClass.NodeCreate
            .newBuilder()
            .setContractIdOrStruct(encodeCid, transactionVersion, c.coid)(
              _.setContractId(_),
              _.setContractIdStruct(_))
            .setContractInstance(inst)
            .addAllStakeholders(c.stakeholders.map(_.underlyingString).asJava)
            .addAllSignatories(c.signatories.map(_.underlyingString).asJava)
          c.key match {
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

      case f: NodeFetch[Cid] =>
        val (vversion, etid) = ValueCoder.encodeIdentifier(
          f.templateId,
          valueVersion1Only(transactionVersion) option ValueVersion("1"))
        val fetchBuilder = TransactionOuterClass.NodeFetch
          .newBuilder()
          .setContractIdOrStruct(encodeCid, transactionVersion, f.coid)(
            _.setContractId(_),
            _.setContractIdStruct(_))
          .setTemplateId(etid)
          .setValueVersion(vversion.protoValue)
          .addAllStakeholders(f.stakeholders.map(_.underlyingString).asJava)
          .addAllSignatories(f.signatories.map(_.underlyingString).asJava)

        if (transactionVersion precedes TransactionVersions.minFetchActors) {
          if (f.actingParties.nonEmpty)
            Left(EncodeError(transactionVersion, isTooOldFor = "NodeFetch actors"))
          else Right(nodeBuilder.setFetch(fetchBuilder).build())
        } else {
          val fetchBuilderWithActors =
            fetchBuilder.addAllActors(
              f.actingParties.getOrElse(Set.empty).map(_.underlyingString).asJava)
          Right(nodeBuilder.setFetch(fetchBuilderWithActors).build())
        }

      case e: NodeExercises[Nid, Cid, Val] =>
        encodeVal(e.chosenValue).map {
          case (vversion, arg) =>
            val exBuilder =
              TransactionOuterClass.NodeExercise
                .newBuilder()
                .setChoice(e.choiceId)
                .setTemplateId(ValueCoder.encodeIdentifier(e.templateId, Some(vversion))._2)
                .setChosenValue(arg)
                .setConsuming(e.consuming)
                .setContractIdOrStruct(encodeCid, transactionVersion, e.targetCoid)(
                  _.setContractId(_),
                  _.setContractIdStruct(_))
                .addAllActors(e.actingParties.map(_.underlyingString).asJava)
                .addAllChildren(e.children.map(encodeNid).toList.asJava)
                .addAllControllers(e.controllers.map(_.underlyingString).asJava)
                .addAllSignatories(e.signatories.map(_.underlyingString).asJava)
                .addAllStakeholders(e.stakeholders.map(_.underlyingString).asJava)

            nodeBuilder.setExercise(exBuilder).build()
        }

      case nlbk: NodeLookupByKey[Cid, Val] =>
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
                    _.setContractIdStruct(_))
              }
              nodeBuilder.setLookupByKey(nlbkBuilder).build()
          }
    }
  }

  private def decodeKeyWithMaintainers[Val](
      decodeVal: ValueOuterClass.VersionedValue => Either[DecodeError, Val],
      keyWithMaintainers: TransactionOuterClass.KeyWithMaintainers)
    : Either[DecodeError, KeyWithMaintainers[Val]] =
    for {
      mainteners <- toPartySet(keyWithMaintainers.getMaintainersList)
      key <- decodeVal(keyWithMaintainers.getKey())
    } yield KeyWithMaintainers(key, mainteners)

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
  def decodeNode[Nid, Cid, Val](
      decodeNid: String => Either[DecodeError, Nid],
      decodeCid: DecodeCid[Cid],
      decodeVal: ValueOuterClass.VersionedValue => Either[DecodeError, Val],
      txVersion: TransactionVersion,
      protoNode: TransactionOuterClass.Node): Either[DecodeError, (Nid, GenNode[Nid, Cid, Val])] = {
    val nodeId = decodeNid(protoNode.getNodeId)

    import TransactionVersions.minKeyOrLookupByKey
    protoNode.getNodeTypeCase match {
      case NodeTypeCase.CREATE =>
        for {
          ni <- nodeId
          protoCreate = protoNode.getCreate
          c <- protoCreate.decodeContractIdOrStruct(decodeCid, txVersion)(
            _.getContractId,
            _.getContractIdStruct)
          ci <- decodeContractInstance(decodeVal, protoCreate.getContractInstance)
          stakeholders <- toPartySet(protoCreate.getStakeholdersList)
          signatories <- toPartySet(protoCreate.getSignatoriesList)
          key <- if (protoCreate.getKeyWithMaintainers == TransactionOuterClass.KeyWithMaintainers.getDefaultInstance)
            Right(None)
          else if (txVersion precedes minKeyOrLookupByKey)
            Left(DecodeError(s"$txVersion is too old to support NodeCreate's `key` field"))
          else decodeKeyWithMaintainers(decodeVal, protoCreate.getKeyWithMaintainers).map(Some(_))
        } yield (ni, NodeCreate(c, ci, None, signatories, stakeholders, key))
      case NodeTypeCase.FETCH =>
        val protoFetch = protoNode.getFetch
        for {
          ni <- nodeId
          templateId <- ValueCoder.decodeIdentifier(protoFetch.getTemplateId)
          c <- protoFetch.decodeContractIdOrStruct(decodeCid, txVersion)(
            _.getContractId,
            _.getContractIdStruct)
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
          ni <- nodeId
          targetCoid <- protoExe.decodeContractIdOrStruct(decodeCid, txVersion)(
            _.getContractId,
            _.getContractIdStruct)
          children <- childrenOrError
          cv <- decodeVal(protoExe.getChosenValue)
          templateId <- ValueCoder.decodeIdentifier(protoExe.getTemplateId)
          controllers <- toPartySet(protoExe.getControllersList)
          signatories <- toPartySet(protoExe.getSignatoriesList)
          stakeholders <- toPartySet(protoExe.getStakeholdersList)
          actingParties <- toPartySet(protoExe.getActorsList)
        } yield
          (
            ni,
            NodeExercises[Nid, Cid, Val](
              targetCoid = targetCoid,
              templateId = templateId,
              choiceId = protoExe.getChoice,
              optLocation = None,
              consuming = protoExe.getConsuming,
              actingParties = actingParties,
              chosenValue = cv,
              stakeholders = stakeholders,
              signatories = signatories,
              controllers = controllers,
              children = children
            ))
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
            _.getContractIdStruct)
        } yield (ni, NodeLookupByKey[Cid, Val](templateId, None, key, cid))
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
  private[transaction] def encodeTransaction[Nid, Cid](
      encodeNid: EncodeNid[Nid],
      encodeCid: EncodeCid[Cid],
      tx: GenTransaction[Nid, Cid, VersionedValue[Cid]])
    : Either[EncodeError, TransactionOuterClass.Transaction] =
    encodeTransactionWithCustomVersion(
      encodeNid,
      encodeCid,
      VersionedTransaction(TransactionVersions.assignVersion(tx), tx))

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
  def encodeTransactionWithCustomVersion[Nid, Cid](
      encodeNid: EncodeNid[Nid],
      encodeCid: EncodeCid[Cid],
      transaction: VersionedTransaction[Nid, Cid])
    : Either[EncodeError, TransactionOuterClass.Transaction] = {
    val tx = transaction.transaction
    val txVersion: TransactionVersion = transaction.version
    val roots = tx.roots.map(encodeNid)
    // use `ImmArray` rather than `toStream` or similar since `traverseU` for `ImmArray` is stack safe.
    // TODO(FM) it would be nice to have a streaming data structure with a stack-safe `Traversable`.
    val mbNodes = ImmArray(tx.nodes).traverseU {
      case (id, node) =>
        encodeNode(
          encodeNid,
          encodeCid,
          (v: VersionedValue[Cid]) =>
            ValueCoder.encodeVersionedValueWithCustomVersion(encodeCid, v).map((v.version, _)),
          txVersion,
          id,
          node)
    }
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
        Left(DecodeError(s"Unsupported transaction version $vs")))(v => Right(v))

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
  def decodeVersionedTransaction[Nid, Cid, Val](
      decodeNid: String => Either[DecodeError, Nid],
      decodeCid: DecodeCid[Cid],
      protoTx: TransactionOuterClass.Transaction)
    : Either[DecodeError, VersionedTransaction[Nid, Cid]] =
    for {
      version <- decodeVersion(protoTx.getVersion)
      tx <- decodeTransaction(
        decodeNid,
        decodeCid,
        ValueCoder.decodeVersionedValue(decodeCid, _),
        version,
        protoTx)
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
    * @tparam Val value type
    * @return  decoded transaction
    */
  private def decodeTransaction[Nid, Cid, Val](
      decodeNid: String => Either[DecodeError, Nid],
      decodeCid: DecodeCid[Cid],
      decodeVal: ValueOuterClass.VersionedValue => Either[DecodeError, Val],
      txVersion: TransactionVersion,
      protoTx: TransactionOuterClass.Transaction)
    : Either[DecodeError, GenTransaction[Nid, Cid, Val]] = {
    val roots = protoTx.getRootsList.asScala
      .foldLeft[Either[DecodeError, BackStack[Nid]]](Right(BackStack.empty[Nid])) {
        case (Right(acc), s) => decodeNid(s).map(acc :+ _)
        case (Left(e), _) => Left(e)
      }
      .map(_.toImmArray)

    val nodes = protoTx.getNodesList.asScala
      .foldLeft[Either[DecodeError, Map[Nid, GenNode[Nid, Cid, Val]]]](Right(Map.empty)) {
        case (Left(e), _) => Left(e)
        case (Right(acc), s) =>
          decodeNode(decodeNid, decodeCid, decodeVal, txVersion, s).map(acc + _)
      }

    for {
      rs <- roots
      ns <- nodes
    } yield GenTransaction(ns, rs)
  }

  private def toPartySet(strList: ProtocolStringList): Either[DecodeError, Set[Party]] = {
    val parties = strList
      .asByteStringList()
      .asScala
      .map(bs => Party.fromString(bs.toStringUtf8))

    sequence(parties) match {
      case Left(err) => Left(DecodeError(s"Cannot decode party: $err"))
      case Right(ps) => Right(ps.toSet)
    }
  }

}
