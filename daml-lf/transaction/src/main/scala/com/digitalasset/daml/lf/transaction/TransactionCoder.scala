// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.{BackStack, Ref}
import com.daml.lf.transaction.TransactionOuterClass.Node.NodeTypeCase
import com.daml.lf.data.Ref.{Name, Party}
import com.daml.lf.transaction.Node._
import com.daml.lf.value.{Value, ValueCoder, ValueOuterClass}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.google.protobuf.{GeneratedMessageV3, ProtocolStringList}

import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.immutable.HashMap
import scala.jdk.CollectionConverters._

object TransactionCoder {

  abstract class EncodeNid[-Nid] private[lf] {
    def asString(id: Nid): String
  }

  abstract class DecodeNid[+Nid] private[lf] {
    def fromString(s: String): Either[DecodeError, Nid]
  }

  val NidEncoder: EncodeNid[NodeId] = new EncodeNid[NodeId] {
    override def asString(id: NodeId): String = id.index.toString
  }

  val NidDecoder: DecodeNid[NodeId] = new DecodeNid[NodeId] {
    override def fromString(s: String): Either[DecodeError, NodeId] =
      scalaz.std.string
        .parseInt(s)
        .fold(_ => Left(DecodeError(s"cannot parse node Id $s")), idx => Right(NodeId(idx)))
  }

  def EventIdEncoder(trId: Ref.LedgerString): EncodeNid[NodeId] =
    new EncodeNid[NodeId] {
      override def asString(id: NodeId): String = ledger.EventId(trId, id).toLedgerString
    }

  def EventIdDecoder(trId: Ref.LedgerString): DecodeNid[NodeId] =
    new DecodeNid[NodeId] {
      override def fromString(s: String): Either[DecodeError, NodeId] =
        ledger.EventId
          .fromString(s)
          .fold(
            _ => Left(DecodeError(s"cannot decode noid: $s")),
            eventId =>
              Either.cond(
                eventId.transactionId == trId,
                eventId.nodeId,
                DecodeError(
                  s"eventId with unexpected transaction ID, expected $trId but found ${eventId.transactionId}"
                ),
              ),
          )
    }

  def encodeVersionedValue[Cid](
      cidEncoder: ValueCoder.EncodeCid[Cid],
      enclosingVersion: TransactionVersion,
      value: Value.VersionedValue[Cid],
  ): Either[EncodeError, ValueOuterClass.VersionedValue] =
    if (enclosingVersion == value.version)
      ValueCoder.encodeVersionedValue(cidEncoder, value)
    else
      Left(
        EncodeError(
          s"A node of version $enclosingVersion cannot contain value of different version version ${value.version}"
        )
      )

  private[this] def encodeValue[Cid](
      cidEncoder: ValueCoder.EncodeCid[Cid],
      nodeVersion: TransactionVersion,
      value: Value[Cid],
  ): Either[EncodeError, ValueOuterClass.Value] =
    ValueCoder.encodeValue(cidEncoder, nodeVersion, value)

  private[this] def encodeVersionedValue[Cid](
      cidEncoder: ValueCoder.EncodeCid[Cid],
      nodeVersion: TransactionVersion,
      value: Value[Cid],
  ): Either[EncodeError, ValueOuterClass.VersionedValue] =
    ValueCoder.encodeVersionedValue(cidEncoder, nodeVersion, value)

  private[this] def decodeValue[Cid](
      cidDecoder: ValueCoder.DecodeCid[Cid],
      nodeVersion: TransactionVersion,
      value: ValueOuterClass.VersionedValue,
  ): Either[DecodeError, Value[Cid]] =
    ValueCoder.decodeVersionedValue(cidDecoder, value).flatMap {
      case Value.VersionedValue(`nodeVersion`, value) => Right(value)
      case Value.VersionedValue(version, _) =>
        Left(
          DecodeError(
            s"A node of version $nodeVersion cannot contain values of different version (${version})"
          )
        )
    }

  /** Encodes a contract instance with the help of the contractId encoding function
    *
    * @param coinst    the contract instance to be encoded
    * @param encodeCid function to encode a cid to protobuf
    * @return protobuf wire format contract instance
    */
  def encodeContractInstance[Cid](
      encodeCid: ValueCoder.EncodeCid[Cid],
      coinst: Value.ContractInst[Value.VersionedValue[Cid]],
  ): Either[EncodeError, TransactionOuterClass.ContractInstance] =
    ValueCoder
      .encodeVersionedValue(encodeCid, coinst.arg)
      .map(
        TransactionOuterClass.ContractInstance
          .newBuilder()
          .setTemplateId(ValueCoder.encodeIdentifier(coinst.template))
          .setValue(_)
          .setAgreement(coinst.agreementText)
          .build()
      )

  private def encodeContractInstance[Cid](
      encodeCid: ValueCoder.EncodeCid[Cid],
      version: TransactionVersion,
      templateId: Ref.Identifier,
      arg: Value[Cid],
      agreementText: String,
  ) =
    encodeVersionedValue(encodeCid, version, arg).map(
      TransactionOuterClass.ContractInstance
        .newBuilder()
        .setTemplateId(ValueCoder.encodeIdentifier(templateId))
        .setValue(_)
        .setAgreement(agreementText)
        .build()
    )

  /** Decode a contract instance from wire format
    *
    * @param protoCoinst protocol buffer encoded contract instance
    * @param decodeCid   cid decoding function
    * @return contract instance value
    */
  def decodeContractInstance[Cid](
      decodeCid: ValueCoder.DecodeCid[Cid],
      protoCoinst: TransactionOuterClass.ContractInstance,
  ): Either[DecodeError, Value.ContractInst[Value[Cid]]] =
    for {
      id <- ValueCoder.decodeIdentifier(protoCoinst.getTemplateId)
      value <- ValueCoder.decodeValue(decodeCid, protoCoinst.getValue)
    } yield Value.ContractInst(id, value, (protoCoinst.getAgreement))

  private[this] def decodeContractInstance[Cid](
      decodeCid: ValueCoder.DecodeCid[Cid],
      nodeVersion: TransactionVersion,
      protoCoinst: TransactionOuterClass.ContractInstance,
  ): Either[DecodeError, Value.ContractInst[Value[Cid]]] =
    for {
      id <- ValueCoder.decodeIdentifier(protoCoinst.getTemplateId)
      value <- decodeValue(decodeCid, nodeVersion, protoCoinst.getValue)
    } yield Value.ContractInst(id, value, protoCoinst.getAgreement)

  def decodeVersionedContractInstance[Cid](
      decodeCid: ValueCoder.DecodeCid[Cid],
      protoCoinst: TransactionOuterClass.ContractInstance,
  ): Either[DecodeError, Value.ContractInst[Value.VersionedValue[Cid]]] =
    for {
      id <- ValueCoder.decodeIdentifier(protoCoinst.getTemplateId)
      value <- ValueCoder.decodeVersionedValue(decodeCid, protoCoinst.getValue)
    } yield Value.ContractInst(id, value, (protoCoinst.getAgreement))

  private[this] def encodeKeyWithMaintainers[Cid](
      encodeCid: ValueCoder.EncodeCid[Cid],
      version: TransactionVersion,
      key: KeyWithMaintainers[Value[Cid]],
  ): Either[EncodeError, TransactionOuterClass.KeyWithMaintainers] = {
    val builder =
      TransactionOuterClass.KeyWithMaintainers
        .newBuilder()
        .addAllMaintainers(key.maintainers.toSet[String].asJava)
    if (version < TransactionVersion.minNoVersionValue) {
      ValueCoder
        .encodeVersionedValue(encodeCid, version, key.key)
        .map(builder.setKeyVersioned(_).build())
    } else {
      ValueCoder
        .encodeValue(encodeCid, version, key.key)
        .map(builder.setKeyUnversioned(_).build())
    }
  }

  private[this] def encodeAndSetContractKey[Cid](
      encodeCid: ValueCoder.EncodeCid[Cid],
      version: TransactionVersion,
      key: Option[KeyWithMaintainers[Value[Cid]]],
      setKey: TransactionOuterClass.KeyWithMaintainers => GeneratedMessageV3.Builder[_],
  ) = {
    key match {
      case Some(key) =>
        encodeKeyWithMaintainers(encodeCid, version, key).map { k => setKey(k); () }
      case None =>
        Right(())
    }
  }

  private[this] def encodeAndSetValue[Cid](
      encodeCid: ValueCoder.EncodeCid[Cid],
      version: TransactionVersion,
      value: Value[Cid],
      setVersioned: ValueOuterClass.VersionedValue => GeneratedMessageV3.Builder[_],
      setUnversioned: ValueOuterClass.Value => GeneratedMessageV3.Builder[_],
  ): Either[EncodeError, Unit] = {
    if (version < TransactionVersion.minNoVersionValue) {
      encodeVersionedValue(encodeCid, version, value).map { v =>
        setVersioned(v);
        {}
      }
    } else {
      encodeValue(encodeCid, version, value).map { v =>
        setUnversioned(v);
        {}
      }
    }
  }

  /** encodes a [[GenNode[Nid, Cid]] to protocol buffer
    *
    * @param nodeId    node id of the node to be encoded
    * @param node      the node to be encoded
    * @param encodeNid node id encoding to string
    * @param encodeCid contract id encoding to string
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return protocol buffer format node
    */
  private[lf] def encodeNode[Nid, Cid](
      encodeNid: EncodeNid[Nid],
      encodeCid: ValueCoder.EncodeCid[Cid],
      enclosingVersion: TransactionVersion,
      nodeId: Nid,
      node: GenNode[Nid, Cid],
  ): Either[EncodeError, TransactionOuterClass.Node] = {
    val nodeVersion = node.version
    if (enclosingVersion < node.version)
      Left(
        EncodeError(
          s"A transaction of version $enclosingVersion cannot contain nodes of newer version (${node.version})"
        )
      )
    else {
      val nodeBuilder =
        TransactionOuterClass.Node.newBuilder().setNodeId(encodeNid.asString(nodeId))
      nodeBuilder.setVersion(node.version.protoValue)

      node match {
        case nc @ NodeCreate(_, _, _, _, _, _, _, _, _) =>
          val builder = TransactionOuterClass.NodeCreate.newBuilder()
          nc.stakeholders.foreach(builder.addStakeholders)
          nc.signatories.foreach(builder.addSignatories)
          builder.setContractIdStruct(encodeCid.encode(nc.coid))
          for {
            _ <-
              if (nodeVersion < TransactionVersion.minNoVersionValue) {
                encodeContractInstance(
                  encodeCid,
                  nc.version,
                  nc.templateId,
                  nc.arg,
                  nc.agreementText,
                )
                  .map(builder.setContractInstance)
              } else {
                encodeValue(encodeCid, nodeVersion, nc.coinst.arg).map { arg =>
                  builder.setTemplateId(ValueCoder.encodeIdentifier(nc.templateId))
                  builder.setArgUnversioned(arg)
                  builder.setAgreement(nc.coinst.agreementText)
                }
              }
            _ <- encodeAndSetContractKey(
              encodeCid,
              nodeVersion,
              nc.key,
              builder.setKeyWithMaintainers,
            )
          } yield nodeBuilder.setCreate(builder).build()

        case nf @ NodeFetch(_, _, _, _, _, _, _, _, _) =>
          val builder = TransactionOuterClass.NodeFetch.newBuilder()
          builder.setTemplateId(ValueCoder.encodeIdentifier(nf.templateId))
          nf.stakeholders.foreach(builder.addStakeholders)
          nf.signatories.foreach(builder.addSignatories)
          builder.setContractIdStruct(encodeCid.encode(nf.coid))
          nf.actingParties.foreach(builder.addActors)
          for {
            _ <- encodeAndSetContractKey(
              encodeCid,
              nodeVersion,
              nf.key,
              builder.setKeyWithMaintainers,
            )
          } yield nodeBuilder.setFetch(builder).build()

        case ne @ NodeExercises(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          val builder = TransactionOuterClass.NodeExercise.newBuilder()
          builder.setContractIdStruct(encodeCid.encode(ne.targetCoid))
          builder.setChoice(ne.choiceId)
          builder.setTemplateId(ValueCoder.encodeIdentifier(ne.templateId))
          builder.setConsuming(ne.consuming)
          ne.actingParties.foreach(builder.addActors)
          ne.children.foreach { id => builder.addChildren(encodeNid.asString(id)); () }
          ne.signatories.foreach(builder.addSignatories)
          ne.stakeholders.foreach(builder.addStakeholders)
          ne.choiceObservers.foreach(builder.addObservers)
          for {
            _ <- Either.cond(
              test = ne.version >= TransactionVersion.minChoiceObservers ||
                ne.choiceObservers.isEmpty,
              right = (),
              left = EncodeError(node.version, isTooOldFor = "non-empty choice-observers"),
            )
            _ <- encodeAndSetValue(
              encodeCid,
              nodeVersion,
              ne.chosenValue,
              builder.setArgVersioned,
              builder.setArgUnversioned,
            )
            _ <- ne.exerciseResult match {
              case Some(value) =>
                encodeAndSetValue(
                  encodeCid,
                  nodeVersion,
                  value,
                  builder.setResultVersioned,
                  builder.setResultUnversioned,
                )
              case None => Left(EncodeError("NodeExercises without result"))
            }
            _ <- encodeAndSetContractKey(
              encodeCid,
              nodeVersion,
              ne.key,
              builder.setKeyWithMaintainers,
            )
          } yield nodeBuilder.setExercise(builder).build()

        case nlbk @ NodeLookupByKey(_, _, _, _, _) =>
          val builder = TransactionOuterClass.NodeLookupByKey.newBuilder()
          builder.setTemplateId(ValueCoder.encodeIdentifier(nlbk.templateId))
          nlbk.result.foreach(cid => builder.setContractIdStruct(encodeCid.encode(cid)))
          for {
            encodedKey <- encodeKeyWithMaintainers(encodeCid, nlbk.version, nlbk.key)
          } yield {
            builder.setKeyWithMaintainers(encodedKey)
            nodeBuilder.setLookupByKey(builder).build()
          }
      }
    }
  }

  private[this] def decodeKeyWithMaintainers[Cid](
      decodeCid: ValueCoder.DecodeCid[Cid],
      version: TransactionVersion,
      keyWithMaintainers: TransactionOuterClass.KeyWithMaintainers,
  ): Either[DecodeError, KeyWithMaintainers[Value[Cid]]] = {
    for {
      maintainers <- toPartySet(keyWithMaintainers.getMaintainersList)
      key <- decodeValue(
        decodeCid,
        version,
        keyWithMaintainers.getKeyVersioned,
        keyWithMaintainers.getKeyUnversioned,
      )
    } yield KeyWithMaintainers(key, maintainers)
  }

  private val RightNone = Right(None)

  private[this] def decodeOptionalKeyWithMaintainers[Cid](
      decodeCid: ValueCoder.DecodeCid[Cid],
      version: TransactionVersion,
      keyWithMaintainers: TransactionOuterClass.KeyWithMaintainers,
  ): Either[DecodeError, Option[KeyWithMaintainers[Value[Cid]]]] = {
    if (keyWithMaintainers == TransactionOuterClass.KeyWithMaintainers.getDefaultInstance) {
      RightNone
    } else {
      for {
        maintainers <- toPartySet(keyWithMaintainers.getMaintainersList)
        key <- decodeValue(
          decodeCid,
          version,
          keyWithMaintainers.getKeyVersioned,
          keyWithMaintainers.getKeyUnversioned,
        )
      } yield Some(KeyWithMaintainers(key, maintainers))
    }
  }

  // package private for test, do not use outside TransactionCoder
  private[lf] def decodeValue[Cid](
      decodeCid: ValueCoder.DecodeCid[Cid],
      version: TransactionVersion,
      versionedProto: => ValueOuterClass.VersionedValue,
      unversionedProto: => ValueOuterClass.Value,
  ): Either[DecodeError, Value[Cid]] = {
    if (version < TransactionVersion.minNoVersionValue) {
      decodeValue(decodeCid, version, versionedProto)
    } else {
      ValueCoder.decodeValue(decodeCid, version, unversionedProto)
    }
  }

  /** read a [[GenNode[Nid, Cid]] from protobuf
    *
    * @param protoNode protobuf encoded node
    * @param decodeNid function to read node id from String
    * @param decodeCid function to read contract id from String
    * @tparam Nid Node id type
    * @tparam Cid Contract id type
    * @return decoded GenNode
    */
  private[lf] def decodeVersionedNode[Nid, Cid](
      decodeNid: DecodeNid[Nid],
      decodeCid: ValueCoder.DecodeCid[Cid],
      transactionVersion: TransactionVersion,
      protoNode: TransactionOuterClass.Node,
  ): Either[DecodeError, (Nid, GenNode[Nid, Cid])] =
    for {
      nodeVersion <-
        if (transactionVersion < TransactionVersion.minNodeVersion) {
          Right(transactionVersion)
        } else {
          decodeVersion(protoNode.getVersion) match {
            case Right(nodeVersion) =>
              if (transactionVersion < nodeVersion)
                Left(
                  DecodeError(
                    s"A transaction of version $transactionVersion cannot contain node of newer version (${protoNode.getVersion})"
                  )
                )
              else
                Right(nodeVersion)
            case Left(err) => Left(err)
          }
        }
      node <- decodeNode(decodeNid, decodeCid, nodeVersion, protoNode)
    } yield node

  private[this] def decodeNode[Nid, Cid](
      decodeNid: DecodeNid[Nid],
      decodeCid: ValueCoder.DecodeCid[Cid],
      nodeVersion: TransactionVersion,
      protoNode: TransactionOuterClass.Node,
  ): Either[DecodeError, (Nid, GenNode[Nid, Cid])] = {
    val nodeId = decodeNid.fromString(protoNode.getNodeId)

    protoNode.getNodeTypeCase match {
      case NodeTypeCase.CREATE =>
        val protoCreate = protoNode.getCreate
        for {
          ni <- nodeId
          c <- decodeCid.decode(protoCreate.getContractIdStruct)
          ci <-
            if (nodeVersion < TransactionVersion.minNoVersionValue) {
              decodeContractInstance(decodeCid, nodeVersion, protoCreate.getContractInstance)
            } else {
              for {
                tmplId <- ValueCoder.decodeIdentifier(protoCreate.getTemplateId)
                arg <- ValueCoder.decodeValue(decodeCid, nodeVersion, protoCreate.getArgUnversioned)
              } yield Value.ContractInst(tmplId, arg, protoCreate.getAgreement)
            }
          stakeholders <- toPartySet(protoCreate.getStakeholdersList)
          signatories <- toPartySet(protoCreate.getSignatoriesList)
          key <- decodeOptionalKeyWithMaintainers(
            decodeCid,
            nodeVersion,
            protoCreate.getKeyWithMaintainers,
          )
        } yield ni -> NodeCreate(
          c,
          ci.template,
          ci.arg,
          ci.agreementText,
          None,
          signatories,
          stakeholders,
          key,
          nodeVersion,
        )
      case NodeTypeCase.FETCH =>
        val protoFetch = protoNode.getFetch
        for {
          ni <- nodeId
          templateId <- ValueCoder.decodeIdentifier(protoFetch.getTemplateId)
          c <- decodeCid.decode(protoFetch.getContractIdStruct)
          actingParties <- toPartySet(protoFetch.getActorsList)
          stakeholders <- toPartySet(protoFetch.getStakeholdersList)
          signatories <- toPartySet(protoFetch.getSignatoriesList)
          key <- decodeOptionalKeyWithMaintainers(
            decodeCid,
            nodeVersion,
            protoFetch.getKeyWithMaintainers,
          )
        } yield ni -> NodeFetch(
          c,
          templateId,
          None,
          actingParties,
          signatories,
          stakeholders,
          key,
          false,
          nodeVersion,
        )

      case NodeTypeCase.EXERCISE =>
        val protoExe = protoNode.getExercise
        val childrenOrError = protoExe.getChildrenList.asScala
          .foldLeft[Either[DecodeError, BackStack[Nid]]](Right(BackStack.empty[Nid])) {
            case (Left(e), _) => Left(e)
            case (Right(ids), s) => decodeNid.fromString(s).map(ids :+ _)
          }
          .map(_.toImmArray)

        for {
          rv <- decodeValue(
            decodeCid,
            nodeVersion,
            protoExe.getResultVersioned,
            protoExe.getResultUnversioned,
          )
          keyWithMaintainers <-
            decodeOptionalKeyWithMaintainers(decodeCid, nodeVersion, protoExe.getKeyWithMaintainers)
          ni <- nodeId
          targetCoid <- decodeCid.decode(protoExe.getContractIdStruct)
          children <- childrenOrError
          cv <- decodeValue(
            decodeCid,
            nodeVersion,
            protoExe.getArgVersioned,
            protoExe.getArgUnversioned,
          )
          templateId <- ValueCoder.decodeIdentifier(protoExe.getTemplateId)
          actingParties <- toPartySet(protoExe.getActorsList)
          signatories <- toPartySet(protoExe.getSignatoriesList)
          stakeholders <- toPartySet(protoExe.getStakeholdersList)
          choiceObservers <-
            if (nodeVersion < TransactionVersion.minChoiceObservers) {
              Right(Set.empty[Party])
            } else {
              toPartySet(protoExe.getObserversList)
            }
          choiceName <- toIdentifier(protoExe.getChoice)
        } yield ni -> NodeExercises(
          targetCoid = targetCoid,
          templateId = templateId,
          choiceId = choiceName,
          optLocation = None,
          consuming = protoExe.getConsuming,
          actingParties = actingParties,
          chosenValue = cv,
          stakeholders = stakeholders,
          signatories = signatories,
          choiceObservers = choiceObservers,
          children = children,
          exerciseResult = Some(rv),
          key = keyWithMaintainers,
          byKey = false,
          version = nodeVersion,
        )
      case NodeTypeCase.LOOKUP_BY_KEY =>
        val protoLookupByKey = protoNode.getLookupByKey
        for {
          ni <- nodeId
          templateId <- ValueCoder.decodeIdentifier(protoLookupByKey.getTemplateId)
          key <-
            decodeKeyWithMaintainers(decodeCid, nodeVersion, protoLookupByKey.getKeyWithMaintainers)
          cid <- decodeCid.decodeOptional(protoLookupByKey.getContractIdStruct)
        } yield ni -> NodeLookupByKey[Cid](templateId, None, key, cid, nodeVersion)
      case NodeTypeCase.NODETYPE_NOT_SET => Left(DecodeError("Unset Node type"))
    }
  }

  /** Encode a [[GenTransaction[Nid, Cid]]] to protobuf using [[TransactionVersion]] provided by the libary.
    *
    * @param tx        the transaction to be encoded
    * @param encodeNid node id encoding function
    * @param encodeCid contract id encoding function
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return protobuf encoded transaction
    */
  def encodeTransaction[Nid, Cid <: ContractId](
      encodeNid: EncodeNid[Nid],
      encodeCid: ValueCoder.EncodeCid[Cid],
      tx: VersionedTransaction[Nid, Cid],
  ): Either[EncodeError, TransactionOuterClass.Transaction] =
    encodeTransactionWithCustomVersion(
      encodeNid,
      encodeCid,
      tx,
    )

  /** Encode a transaction to protobuf using [[TransactionVersion]] provided by in the [[VersionedTransaction]] argument.
    *
    * @param transaction the transaction to be encoded
    * @param encodeNid   node id encoding function
    * @param encodeCid   contract id encoding function
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return protobuf encoded transaction
    */
  private[transaction] def encodeTransactionWithCustomVersion[Nid, Cid](
      encodeNid: EncodeNid[Nid],
      encodeCid: ValueCoder.EncodeCid[Cid],
      transaction: VersionedTransaction[Nid, Cid],
  ): Either[EncodeError, TransactionOuterClass.Transaction] = {
    val builder = TransactionOuterClass.Transaction
      .newBuilder()
      .setVersion(transaction.version.protoValue)
    transaction.roots.foreach { nid =>
      builder.addRoots(encodeNid.asString(nid))
      ()
    }

    transaction
      .fold[Either[EncodeError, TransactionOuterClass.Transaction.Builder]](
        Right(builder)
      ) { case (builderOrError, (nid, _)) =>
        for {
          builder <- builderOrError
          encodedNode <- encodeNode(
            encodeNid,
            encodeCid,
            transaction.version,
            nid,
            transaction.nodes(nid),
          )
        } yield builder.addNodes(encodedNode)
      }
      .map(_.build)
  }

  def decodeVersion(vs: String): Either[DecodeError, TransactionVersion] =
    TransactionVersion.fromString(vs).left.map(DecodeError)

  /** Reads a [[VersionedTransaction]] from protobuf and checks if
    * [[TransactionVersion]] passed in the protobuf is currently supported.
    *
    * Supported transaction versions configured in [[TransactionVersion]].
    *
    * @param protoTx   protobuf encoded transaction
    * @param decodeNid node id decoding function
    * @param decodeCid contract id decoding function
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return decoded transaction
    */
  def decodeTransaction[Nid, Cid](
      decodeNid: DecodeNid[Nid],
      decodeCid: ValueCoder.DecodeCid[Cid],
      protoTx: TransactionOuterClass.Transaction,
  ): Either[DecodeError, VersionedTransaction[Nid, Cid]] =
    for {
      version <- decodeVersion(protoTx.getVersion)
      tx <- decodeTransaction(
        decodeNid,
        decodeCid,
        version,
        protoTx,
      )
    } yield tx

  /** Reads a [[GenTransaction[Nid, Cid]]] from protobuf. Does not check if
    * [[TransactionVersion]] passed in the protobuf is currently supported, if you need this check use
    * [[TransactionCoder.decodeTransaction]].
    *
    * @param protoTx   protobuf encoded transaction
    * @param decodeNid node id decoding function
    * @param decodeCid contract id decoding function
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return decoded transaction
    */
  private def decodeTransaction[Nid, Cid](
      decodeNid: DecodeNid[Nid],
      decodeCid: ValueCoder.DecodeCid[Cid],
      txVersion: TransactionVersion,
      protoTx: TransactionOuterClass.Transaction,
  ): Either[DecodeError, VersionedTransaction[Nid, Cid]] = {
    val roots = protoTx.getRootsList.asScala
      .foldLeft[Either[DecodeError, BackStack[Nid]]](Right(BackStack.empty[Nid])) {
        case (Right(acc), s) => decodeNid.fromString(s).map(acc :+ _)
        case (Left(e), _) => Left(e)
      }
      .map(_.toImmArray)

    val nodes = protoTx.getNodesList.asScala
      .foldLeft[Either[DecodeError, HashMap[Nid, GenNode[Nid, Cid]]]](Right(HashMap.empty)) {
        case (Left(e), _) => Left(e)
        case (Right(acc), s) =>
          decodeVersionedNode(decodeNid, decodeCid, txVersion, s).map(acc + _)
      }

    for {
      rs <- roots
      ns <- nodes
    } yield VersionedTransaction(txVersion, ns, rs)
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

  def decodeVersion(node: TransactionOuterClass.Node): Either[DecodeError, TransactionVersion] =
    if (node.getVersion.isEmpty)
      Right(TransactionVersion.minVersion)
    else
      decodeVersion(node.getVersion)

  /** Node information for a serialized transaction node. Used to compute
    * informees when deserialization is too costly.
    * This method is not supported for transaction version <5 (as NodeInfo does not support it).
    * We're not using e.g. "implicit class" in order to keep the decoding errors explicit.
    * NOTE(JM): Currently used only externally, but kept here to keep in sync
    * with the implementation.
    */
  def protoNodeInfo(
      txVersion: TransactionVersion,
      protoNode: TransactionOuterClass.Node,
  ): Either[DecodeError, NodeInfo] =
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
            def actingParties = actingParties_
          }
        }

      case NodeTypeCase.EXERCISE =>
        val protoExe = protoNode.getExercise
        for {
          actingParties_ <- toPartySet(protoExe.getActorsList)
          signatories_ <- toPartySet(protoExe.getSignatoriesList)
          stakeholders_ <- toPartySet(protoExe.getStakeholdersList)
          choiceObservers_ <-
            if (txVersion < TransactionVersion.minChoiceObservers)
              Right(Set.empty[Party])
            else
              toPartySet(protoExe.getObserversList)
        } yield {
          new NodeInfo.Exercise {
            def signatories = signatories_
            def stakeholders = stakeholders_
            def actingParties = actingParties_
            def choiceObservers = choiceObservers_
            def consuming = protoExe.getConsuming
          }
        }

      case NodeTypeCase.LOOKUP_BY_KEY =>
        val protoLookupByKey = protoNode.getLookupByKey
        for {
          maintainers <- toPartySet(protoLookupByKey.getKeyWithMaintainers.getMaintainersList)
        } yield {
          new NodeInfo.LookupByKey {
            def hasResult = protoLookupByKey.hasContractIdStruct
            def keyMaintainers = maintainers
          }
        }

      case NodeTypeCase.NODETYPE_NOT_SET => Left(DecodeError("Unset Node type"))
    }

  private[this] def keyHash(
      nodeVersion: TransactionVersion,
      rawTmplId: ValueOuterClass.Identifier,
      rawKey: ValueOuterClass.Value,
  ): Either[DecodeError, GlobalKey] =
    for {
      tmplId <- ValueCoder.decodeIdentifier(rawTmplId)
      value <- ValueCoder.decodeValue(ValueCoder.NoCidDecoder, nodeVersion, rawKey)
      key <- GlobalKey.build(tmplId, value).left.map(DecodeError)
    } yield key

  /*
   * Fast decoder for contract key of Create node.
   * Does not decode or validate the rest of the node.
   */
  def nodeKey(
      nodeVersion: TransactionVersion,
      protoCreate: TransactionOuterClass.NodeCreate,
  ): Either[DecodeError, Option[GlobalKey]] = {
    if (protoCreate.hasKeyWithMaintainers) {
      val (rawTmplId, rawKey) =
        if (nodeVersion < TransactionVersion.minNoVersionValue) {
          protoCreate.getContractInstance.getTemplateId -> protoCreate.getKeyWithMaintainers.getKeyVersioned.getValue
        } else {
          protoCreate.getTemplateId -> protoCreate.getKeyWithMaintainers.getKeyUnversioned
        }
      keyHash(nodeVersion, rawTmplId, rawKey).map(Some(_))
    } else {
      Right(None)
    }
  }

  /*
   * Fast decoder for contract key of Exercise node.
   * Does not decode or validate the rest of the node.
   */
  def nodeKey(
      nodeVersion: TransactionVersion,
      protoExercise: TransactionOuterClass.NodeExercise,
  ): Either[DecodeError, Option[GlobalKey]] =
    if (protoExercise.hasKeyWithMaintainers) {
      val rawKey =
        if (nodeVersion < TransactionVersion.minNoVersionValue) {
          protoExercise.getKeyWithMaintainers.getKeyVersioned.getValue
        } else {
          protoExercise.getKeyWithMaintainers.getKeyUnversioned
        }
      keyHash(nodeVersion, protoExercise.getTemplateId, rawKey).map(Some(_))
    } else {
      Right(None)
    }

}
