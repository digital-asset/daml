// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.{Ref, BackStack}
import com.daml.lf.transaction.TransactionOuterClass.Node.NodeTypeCase
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{Party, Name}
import com.daml.lf.value.{ValueOuterClass, Value, ValueCoder}
import com.daml.lf.value.ValueCoder.{EncodeError, DecodeError}
import com.daml.scalautil.Statement.discard
import com.google.protobuf.{ByteString, GeneratedMessageV3, ProtocolStringList}

import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.immutable.HashMap
import scala.collection.immutable.TreeSet
import scala.jdk.CollectionConverters._

object TransactionCoder {

  abstract class EncodeNid private[lf] {
    def asString(id: NodeId): String
  }

  abstract class DecodeNid private[lf] {
    def fromString(s: String): Either[DecodeError, NodeId]
  }

  val NidEncoder: EncodeNid = new EncodeNid {
    override def asString(id: NodeId): String = id.index.toString
  }

  val NidDecoder: DecodeNid = new DecodeNid {
    override def fromString(s: String): Either[DecodeError, NodeId] =
      scalaz.std.string
        .parseInt(s)
        .fold(_ => Left(DecodeError(s"cannot parse node Id $s")), idx => Right(NodeId(idx)))
  }

  def EventIdEncoder(trId: Ref.LedgerString): EncodeNid =
    new EncodeNid {
      override def asString(id: NodeId): String = ledger.EventId(trId, id).toLedgerString
    }

  def EventIdDecoder(trId: Ref.LedgerString): DecodeNid =
    new DecodeNid {
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

  def encodeVersionedValue(
      cidEncoder: ValueCoder.EncodeCid,
      enclosingVersion: TransactionVersion,
      value: Value.VersionedValue,
  ): Either[EncodeError, ValueOuterClass.VersionedValue] =
    if (enclosingVersion == value.version)
      ValueCoder.encodeVersionedValue(cidEncoder, value)
    else
      Left(
        EncodeError(
          s"A node of version $enclosingVersion cannot contain value of different version version ${value.version}"
        )
      )

  private[this] def encodeValue(
      cidEncoder: ValueCoder.EncodeCid,
      nodeVersion: TransactionVersion,
      value: Value,
  ): Either[EncodeError, ByteString] =
    ValueCoder.encodeValue(cidEncoder, nodeVersion, value)

  private[this] def encodeVersionedValue(
      cidEncoder: ValueCoder.EncodeCid,
      nodeVersion: TransactionVersion,
      value: Value,
  ): Either[EncodeError, ValueOuterClass.VersionedValue] =
    ValueCoder.encodeVersionedValue(cidEncoder, nodeVersion, value)

  private[this] def decodeValue(
      cidDecoder: ValueCoder.DecodeCid,
      nodeVersion: TransactionVersion,
      value: ValueOuterClass.VersionedValue,
  ): Either[DecodeError, Value] =
    ValueCoder.decodeVersionedValue(cidDecoder, value).flatMap {
      case Versioned(`nodeVersion`, value) => Right(value)
      case Versioned(version, _) =>
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
  def encodeContractInstance(
      encodeCid: ValueCoder.EncodeCid,
      coinst: Versioned[Value.ContractInstanceWithAgreement],
  ): Either[EncodeError, TransactionOuterClass.ContractInstance] =
    ValueCoder
      .encodeVersionedValue(encodeCid, coinst.version, coinst.unversioned.contractInstance.arg)
      .map(
        TransactionOuterClass.ContractInstance
          .newBuilder()
          .setTemplateId(ValueCoder.encodeIdentifier(coinst.unversioned.contractInstance.template))
          .setArgVersioned(_)
          .setAgreement(coinst.unversioned.agreementText)
          .build()
      )

  private def encodeContractInstance(
      encodeCid: ValueCoder.EncodeCid,
      version: TransactionVersion,
      templateId: Ref.Identifier,
      arg: Value,
      agreementText: String,
  ) =
    encodeVersionedValue(encodeCid, version, arg).map(
      TransactionOuterClass.ContractInstance
        .newBuilder()
        .setTemplateId(ValueCoder.encodeIdentifier(templateId))
        .setArgVersioned(_)
        .setAgreement(agreementText)
        .build()
    )

  /** Decode a contract instance from wire format
    *
    * @param protoCoinst protocol buffer encoded contract instance
    * @param decodeCid   cid decoding function
    * @return contract instance value
    */
  def decodeContractInstance(
      decodeCid: ValueCoder.DecodeCid,
      protoCoinst: TransactionOuterClass.ContractInstance,
  ): Either[DecodeError, Value.ContractInstance] =
    for {
      id <- ValueCoder.decodeIdentifier(protoCoinst.getTemplateId)
      value <- ValueCoder.decodeValue(decodeCid, protoCoinst.getArgVersioned)
    } yield Value.ContractInstance(id, value)

  private[this] def decodeContractInstance(
      decodeCid: ValueCoder.DecodeCid,
      nodeVersion: TransactionVersion,
      protoCoinst: TransactionOuterClass.ContractInstance,
  ): Either[DecodeError, Value.ContractInstanceWithAgreement] =
    for {
      id <- ValueCoder.decodeIdentifier(protoCoinst.getTemplateId)
      value <- decodeValue(decodeCid, nodeVersion, protoCoinst.getArgVersioned)
    } yield Value.ContractInstanceWithAgreement(
      Value.ContractInstance(id, value),
      protoCoinst.getAgreement,
    )

  def decodeVersionedContractInstance(
      decodeCid: ValueCoder.DecodeCid,
      protoCoinst: TransactionOuterClass.ContractInstance,
  ): Either[DecodeError, Versioned[Value.ContractInstanceWithAgreement]] =
    for {
      id <- ValueCoder.decodeIdentifier(protoCoinst.getTemplateId)
      value <- ValueCoder.decodeVersionedValue(decodeCid, protoCoinst.getArgVersioned)
    } yield value.map(arg =>
      Value.ContractInstanceWithAgreement(Value.ContractInstance(id, arg), protoCoinst.getAgreement)
    )

  private[transaction] def encodeKeyWithMaintainers(
      version: TransactionVersion,
      key: GlobalKeyWithMaintainers,
  ): Either[EncodeError, TransactionOuterClass.KeyWithMaintainers] = {
    val builder = TransactionOuterClass.KeyWithMaintainers.newBuilder()
    key.maintainers.foreach(builder.addMaintainers(_))
    if (version < TransactionVersion.minNoVersionValue) {
      ValueCoder
        .encodeVersionedValue(ValueCoder.UnsafeNoCidEncoder, version, key.value)
        .map(builder.setKeyVersioned(_).build())
    } else {
      ValueCoder
        .encodeValue(ValueCoder.UnsafeNoCidEncoder, version, key.value)
        .map(builder.setKeyUnversioned(_).build())
    }
  }

  private[this] def encodeAndSetContractKey(
      version: TransactionVersion,
      key: Option[GlobalKeyWithMaintainers],
      setKey: TransactionOuterClass.KeyWithMaintainers => GeneratedMessageV3.Builder[_],
  ) = {
    key match {
      case Some(key) =>
        encodeKeyWithMaintainers(version, key).map(k => discard(setKey(k)))
      case None =>
        Right(())
    }
  }

  private[this] def encodeAndSetValue(
      encodeCid: ValueCoder.EncodeCid,
      version: TransactionVersion,
      value: Value,
      setVersioned: ValueOuterClass.VersionedValue => GeneratedMessageV3.Builder[_],
      setUnversioned: ByteString => GeneratedMessageV3.Builder[_],
  ): Either[EncodeError, Unit] = {
    if (version < TransactionVersion.minNoVersionValue) {
      encodeVersionedValue(encodeCid, version, value).map(v => discard(setVersioned(v)))
    } else {
      encodeValue(encodeCid, version, value).map(v => discard(setUnversioned(v)))
    }
  }

  /** encodes a [[Node[Nid]] to protocol buffer
    *
    * @param nodeId    node id of the node to be encoded
    * @param node      the node to be encoded
    * @param encodeNid node id encoding to string
    * @param encodeCid contract id encoding to string
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return protocol buffer format node
    */
  private[lf] def encodeNode(
      encodeNid: EncodeNid,
      encodeCid: ValueCoder.EncodeCid,
      enclosingVersion: TransactionVersion,
      nodeId: NodeId,
      node: Node,
      disableVersionCheck: Boolean =
        false, // true allows encoding of bad protos (for testing of decode checks)
  ): Either[EncodeError, TransactionOuterClass.Node] = {

    val nodeBuilder =
      TransactionOuterClass.Node.newBuilder().setNodeId(encodeNid.asString(nodeId))

    node match {
      case Node.Rollback(children) =>
        val builder = TransactionOuterClass.NodeRollback.newBuilder()
        children.foreach(id => discard(builder.addChildren(encodeNid.asString(id))))
        for {
          _ <- Either.cond(
            test = enclosingVersion >= TransactionVersion.minExceptions || disableVersionCheck,
            right = (),
            left = EncodeError(enclosingVersion, isTooOldFor = "rollback nodes"),
          )
        } yield nodeBuilder.setRollback(builder).build()

      case node: Node.Action =>
        val nodeVersion = node.version
        if (enclosingVersion < nodeVersion)
          Left(
            EncodeError(
              s"A transaction of version $enclosingVersion cannot contain nodes of newer version ($nodeVersion)"
            )
          )
        else {
          if (enclosingVersion >= TransactionVersion.minNodeVersion)
            discard(nodeBuilder.setVersion(nodeVersion.protoValue))

          node match {

            case nc @ Node.Create(_, _, _, _, _, _, _, _) =>
              val builder = TransactionOuterClass.NodeCreate.newBuilder()
              nc.stakeholders.foreach(builder.addStakeholders)
              nc.signatories.foreach(builder.addSignatories)
              discard(builder.setContractIdStruct(encodeCid.encode(nc.coid)))
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
                    encodeValue(encodeCid, nodeVersion, nc.arg).map(arg =>
                      builder
                        .setTemplateId(ValueCoder.encodeIdentifier(nc.templateId))
                        .setArgUnversioned(arg)
                        .setAgreement(nc.agreementText)
                    )
                  }
                _ <- encodeAndSetContractKey(
                  nodeVersion,
                  nc.keyOpt,
                  builder.setKeyWithMaintainers,
                )
              } yield nodeBuilder.setCreate(builder).build()

            case nf @ Node.Fetch(_, _, _, _, _, _, _, _) =>
              val builder = TransactionOuterClass.NodeFetch.newBuilder()
              discard(builder.setTemplateId(ValueCoder.encodeIdentifier(nf.templateId)))
              nf.stakeholders.foreach(builder.addStakeholders)
              nf.signatories.foreach(builder.addSignatories)
              discard(builder.setContractIdStruct(encodeCid.encode(nf.coid)))
              if (nodeVersion >= TransactionVersion.minByKey) {
                discard(builder.setByKey(nf.byKey))
              }
              nf.actingParties.foreach(builder.addActors)
              for {
                _ <- encodeAndSetContractKey(
                  nodeVersion,
                  nf.keyOpt,
                  builder.setKeyWithMaintainers,
                )
              } yield nodeBuilder.setFetch(builder).build()

            case ne @ Node.Exercise(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
              val builder = TransactionOuterClass.NodeExercise.newBuilder()
              discard(
                builder
                  .setContractIdStruct(encodeCid.encode(ne.targetCoid))
                  .setChoice(ne.choiceId)
                  .setTemplateId(ValueCoder.encodeIdentifier(ne.templateId))
                  .setConsuming(ne.consuming)
              )
              ne.actingParties.foreach(builder.addActors)
              ne.children.foreach(id => discard(builder.addChildren(encodeNid.asString(id))))
              ne.signatories.foreach(builder.addSignatories)
              ne.stakeholders.foreach(builder.addStakeholders)
              ne.choiceObservers.foreach(builder.addObservers)

              ne.choiceAuthorizers match {
                case None => ()
                case Some(xs) => xs.foreach(builder.addAuthorizers)
              }

              if (nodeVersion >= TransactionVersion.minByKey) {
                discard(builder.setByKey(ne.byKey))
              }
              if (nodeVersion >= TransactionVersion.minInterfaces) {
                ne.interfaceId.foreach(iface =>
                  builder.setInterfaceId(ValueCoder.encodeIdentifier(iface))
                )
              }
              for {
                _ <- Either.cond(
                  test = ne.version >= TransactionVersion.minChoiceObservers ||
                    ne.choiceObservers.isEmpty,
                  right = (),
                  left = EncodeError(nodeVersion, isTooOldFor = "non-empty choice-observers"),
                )
                _ <- Either.cond(
                  test = ne.version >= TransactionVersion.minChoiceAuthorizers ||
                    !(ne.choiceAuthorizers.isDefined),
                  right = (),
                  left = EncodeError(nodeVersion, isTooOldFor = "explicit choice-authorizers"),
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
                  case None =>
                    Either.cond(
                      test = ne.version >= TransactionVersion.minExceptions || disableVersionCheck,
                      right = (),
                      left = EncodeError(nodeVersion, isTooOldFor = "NodeExercises without result"),
                    )
                }
                _ <- encodeAndSetContractKey(
                  nodeVersion,
                  ne.keyOpt,
                  builder.setKeyWithMaintainers,
                )
              } yield nodeBuilder.setExercise(builder).build()

            case nlbk @ Node.LookupByKey(_, _, _, _) =>
              val builder = TransactionOuterClass.NodeLookupByKey.newBuilder()
              discard(builder.setTemplateId(ValueCoder.encodeIdentifier(nlbk.templateId)))
              nlbk.result.foreach(cid =>
                discard(builder.setContractIdStruct(encodeCid.encode(cid)))
              )
              for {
                encodedKey <- encodeKeyWithMaintainers(nlbk.version, nlbk.key)
              } yield {
                discard(builder.setKeyWithMaintainers(encodedKey))
                nodeBuilder.setLookupByKey(builder).build()
              }
          }
        }
    }
  }

  private[this] def decodeKeyWithMaintainers(
      version: TransactionVersion,
      templateId: Ref.TypeConName,
      keyWithMaintainers: TransactionOuterClass.KeyWithMaintainers,
  ): Either[DecodeError, GlobalKeyWithMaintainers] = {
    for {
      maintainers <- toPartySet(keyWithMaintainers.getMaintainersList)
      value <- decodeValue(
        ValueCoder.NoCidDecoder,
        version,
        keyWithMaintainers.getKeyVersioned,
        keyWithMaintainers.getKeyUnversioned,
      )
      gkey <- GlobalKey.build(templateId, value).left.map(hashErr => DecodeError(hashErr.msg))
    } yield GlobalKeyWithMaintainers(gkey, maintainers)
  }

  private[transaction] def strictDecodeKeyWithMaintainers(
      version: TransactionVersion,
      templateId: Ref.TypeConName,
      keyWithMaintainers: TransactionOuterClass.KeyWithMaintainers,
  ): Either[DecodeError, GlobalKeyWithMaintainers] =
    for {
      maintainers <- toOrderedPartySet(keyWithMaintainers.getMaintainersList)
      _ <- Either.cond(maintainers.nonEmpty, (), DecodeError("key without maintainers"))
      value <- decodeValue(
        ValueCoder.NoCidDecoder,
        version,
        keyWithMaintainers.getKeyVersioned,
        keyWithMaintainers.getKeyUnversioned,
      )
      gkey <- GlobalKey.build(templateId, value).left.map(hashErr => DecodeError(hashErr.msg))
    } yield GlobalKeyWithMaintainers(gkey, maintainers)

  private val RightNone = Right(None)

  private[this] def decodeOptionalKeyWithMaintainers(
      version: TransactionVersion,
      templateId: Ref.TypeConName,
      keyWithMaintainers: TransactionOuterClass.KeyWithMaintainers,
  ): Either[DecodeError, Option[GlobalKeyWithMaintainers]] =
    if (keyWithMaintainers == TransactionOuterClass.KeyWithMaintainers.getDefaultInstance)
      RightNone
    else
      decodeKeyWithMaintainers(version, templateId, keyWithMaintainers).map(Some(_))

  // package private for test, do not use outside TransactionCoder
  private[lf] def decodeValue(
      decodeCid: ValueCoder.DecodeCid,
      version: TransactionVersion,
      versionedProto: => ValueOuterClass.VersionedValue,
      unversionedProto: => ByteString,
  ): Either[DecodeError, Value] = {
    if (version < TransactionVersion.minNoVersionValue) {
      decodeValue(decodeCid, version, versionedProto)
    } else {
      ValueCoder.decodeValue(decodeCid, version, unversionedProto)
    }
  }

  /** read a [[Node[Nid]] from protobuf
    *
    * @param protoNode protobuf encoded node
    * @param decodeNid function to read node id from String
    * @param decodeCid function to read contract id from String
    * @tparam Nid Node id type
    * @tparam Cid Contract id type
    * @return decoded GenNode
    */
  private[lf] def decodeVersionedNode(
      decodeNid: DecodeNid,
      decodeCid: ValueCoder.DecodeCid,
      transactionVersion: TransactionVersion,
      protoNode: TransactionOuterClass.Node,
  ): Either[DecodeError, (NodeId, Node)] =
    for {
      nodeVersion <- decodeNodeVersion(transactionVersion, protoNode)
      node <- decodeNode(decodeNid, decodeCid, nodeVersion, protoNode)
    } yield node

  private[this] def decodeNode(
      decodeNid: DecodeNid,
      decodeCid: ValueCoder.DecodeCid,
      nodeVersion: TransactionVersion,
      protoNode: TransactionOuterClass.Node,
  ): Either[DecodeError, (NodeId, Node)] = {
    val nodeId = decodeNid.fromString(protoNode.getNodeId)

    protoNode.getNodeTypeCase match {
      case NodeTypeCase.ROLLBACK =>
        val protoRollback = protoNode.getRollback
        for {
          _ <- Either.cond(
            test = nodeVersion >= TransactionVersion.minExceptions,
            right = (),
            left = DecodeError(
              s"rollback node (supported since ${TransactionVersion.minExceptions}) unexpected in transaction of version $nodeVersion"
            ),
          )
          ni <- nodeId
          children <- decodeChildren(decodeNid, protoRollback.getChildrenList)
        } yield ni -> Node.Rollback(children)
      case NodeTypeCase.CREATE =>
        val protoCreate = protoNode.getCreate
        for {
          ni <- nodeId
          c <- decodeCid.decode(protoCreate.getContractIdStruct)
          entry <-
            if (nodeVersion < TransactionVersion.minNoVersionValue) {
              decodeContractInstance(decodeCid, nodeVersion, protoCreate.getContractInstance)
            } else {
              for {
                tmplId <- ValueCoder.decodeIdentifier(protoCreate.getTemplateId)
                arg <- ValueCoder.decodeValue(decodeCid, nodeVersion, protoCreate.getArgUnversioned)
              } yield Value.ContractInstanceWithAgreement(
                Value.ContractInstance(tmplId, arg),
                protoCreate.getAgreement,
              )
            }
          Value.ContractInstanceWithAgreement(ci, agreementText) = entry
          stakeholders <- toPartySet(protoCreate.getStakeholdersList)
          signatories <- toPartySet(protoCreate.getSignatoriesList)
          keyOpt <- decodeOptionalKeyWithMaintainers(
            nodeVersion,
            ci.template,
            protoCreate.getKeyWithMaintainers,
          )
        } yield ni -> Node.Create(
          coid = c,
          templateId = ci.template,
          arg = ci.arg,
          agreementText = agreementText,
          signatories = signatories,
          stakeholders = stakeholders,
          keyOpt = keyOpt,
          version = nodeVersion,
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
          keyOpt <- decodeOptionalKeyWithMaintainers(
            nodeVersion,
            templateId,
            protoFetch.getKeyWithMaintainers,
          )
          byKey =
            if (nodeVersion >= TransactionVersion.minByKey)
              protoFetch.getByKey
            else false
        } yield ni -> Node.Fetch(
          coid = c,
          templateId = templateId,
          actingParties = actingParties,
          signatories = signatories,
          stakeholders = stakeholders,
          keyOpt = keyOpt,
          byKey = byKey,
          version = nodeVersion,
        )

      case NodeTypeCase.EXERCISE =>
        val protoExe = protoNode.getExercise
        for {
          templateId <- ValueCoder.decodeIdentifier(protoExe.getTemplateId)
          rvOpt <-
            if (!protoExe.hasResultVersioned && protoExe.getResultUnversioned.isEmpty) {
              Either.cond(
                test = nodeVersion >= TransactionVersion.minExceptions,
                right = None,
                left = DecodeError(
                  s"NodeExercises without result (supported since ${TransactionVersion.minExceptions}) unexpected in transaction of version $nodeVersion"
                ),
              )
            } else {
              decodeValue(
                decodeCid,
                nodeVersion,
                protoExe.getResultVersioned,
                protoExe.getResultUnversioned,
              ).map(v => Some(v))
            }
          keyWithMaintainers <-
            decodeOptionalKeyWithMaintainers(
              nodeVersion,
              templateId,
              protoExe.getKeyWithMaintainers,
            )
          ni <- nodeId
          targetCoid <- decodeCid.decode(protoExe.getContractIdStruct)
          children <- decodeChildren(decodeNid, protoExe.getChildrenList)
          cv <- decodeValue(
            decodeCid,
            nodeVersion,
            protoExe.getArgVersioned,
            protoExe.getArgUnversioned,
          )
          actingParties <- toPartySet(protoExe.getActorsList)
          signatories <- toPartySet(protoExe.getSignatoriesList)
          stakeholders <- toPartySet(protoExe.getStakeholdersList)
          choiceObservers <-
            if (nodeVersion < TransactionVersion.minChoiceObservers) {
              Right(Set.empty[Party])
            } else {
              toPartySet(protoExe.getObserversList)
            }
          choiceAuthorizers <-
            if (nodeVersion < TransactionVersion.minChoiceAuthorizers) { Right(None) }
            else {
              for { choiceAuthorizersList <- toPartySet(protoExe.getAuthorizersList) } yield
                if (choiceAuthorizersList.isEmpty) None else Some(choiceAuthorizersList)
            }
          choiceName <- toIdentifier(protoExe.getChoice)
          byKey =
            if (nodeVersion >= TransactionVersion.minByKey)
              protoExe.getByKey
            else false
          interfaceId <-
            if (nodeVersion >= TransactionVersion.minInterfaces && protoExe.hasInterfaceId) {
              ValueCoder.decodeIdentifier(protoExe.getInterfaceId).map(Some(_))
            } else {
              Right(None)
            }
        } yield ni -> Node.Exercise(
          targetCoid = targetCoid,
          templateId = templateId,
          interfaceId = interfaceId,
          choiceId = choiceName,
          consuming = protoExe.getConsuming,
          actingParties = actingParties,
          chosenValue = cv,
          stakeholders = stakeholders,
          signatories = signatories,
          choiceObservers = choiceObservers,
          choiceAuthorizers = choiceAuthorizers,
          children = children,
          exerciseResult = rvOpt,
          keyOpt = keyWithMaintainers,
          byKey = byKey,
          version = nodeVersion,
        )
      case NodeTypeCase.LOOKUP_BY_KEY =>
        val protoLookupByKey = protoNode.getLookupByKey
        for {
          ni <- nodeId
          templateId <- ValueCoder.decodeIdentifier(protoLookupByKey.getTemplateId)
          key <-
            decodeKeyWithMaintainers(
              nodeVersion,
              templateId,
              protoLookupByKey.getKeyWithMaintainers,
            )
          cid <- decodeCid.decodeOptional(protoLookupByKey.getContractIdStruct)
        } yield ni -> Node.LookupByKey(templateId, key, cid, nodeVersion)
      case NodeTypeCase.NODETYPE_NOT_SET => Left(DecodeError("Unset Node type"))
    }
  }

  private[this] def decodeChildren(
      decodeNid: DecodeNid,
      strList: ProtocolStringList,
  ): Either[DecodeError, ImmArray[NodeId]] = {
    strList.asScala
      .foldLeft[Either[DecodeError, BackStack[NodeId]]](Right(BackStack.empty)) {
        case (Left(e), _) => Left(e)
        case (Right(ids), s) => decodeNid.fromString(s).map(ids :+ _)
      }
      .map(_.toImmArray)
  }

  /** Encode a [[Transaction[Nid]]] to protobuf using [[TransactionVersion]] provided by the libary.
    *
    * @param tx        the transaction to be encoded
    * @param encodeNid node id encoding function
    * @param encodeCid contract id encoding function
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return protobuf encoded transaction
    */
  def encodeTransaction(
      encodeNid: EncodeNid,
      encodeCid: ValueCoder.EncodeCid,
      tx: VersionedTransaction,
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
  private[transaction] def encodeTransactionWithCustomVersion(
      encodeNid: EncodeNid,
      encodeCid: ValueCoder.EncodeCid,
      transaction: VersionedTransaction,
  ): Either[EncodeError, TransactionOuterClass.Transaction] = {
    val builder = TransactionOuterClass.Transaction
      .newBuilder()
      .setVersion(transaction.version.protoValue)
    transaction.roots.foreach(nid => discard(builder.addRoots(encodeNid.asString(nid))))

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

  def decodeNodeVersion(
      txVersion: TransactionVersion,
      protoNode: TransactionOuterClass.Node,
  ): Either[DecodeError, TransactionVersion] = {
    if (txVersion < TransactionVersion.minNodeVersion) {
      Right(txVersion)
    } else {
      protoNode.getNodeTypeCase match {
        case NodeTypeCase.ROLLBACK => Right(txVersion)
        case _ =>
          decodeVersion(protoNode.getVersion) match {
            case Right(nodeVersion) if (txVersion < nodeVersion) =>
              Left(
                DecodeError(
                  s"A transaction of version $txVersion cannot contain node of newer version (${protoNode.getVersion})"
                )
              )
            case otherwise => otherwise
          }
      }
    }
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
  def decodeTransaction(
      decodeNid: DecodeNid,
      decodeCid: ValueCoder.DecodeCid,
      protoTx: TransactionOuterClass.Transaction,
  ): Either[DecodeError, VersionedTransaction] =
    for {
      version <- decodeVersion(protoTx.getVersion)
      tx <- decodeTransaction(
        decodeNid,
        decodeCid,
        version,
        protoTx,
      )
    } yield tx

  /** Reads a [[Transaction[Nid]]] from protobuf. Does not check if
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
  private def decodeTransaction(
      decodeNid: DecodeNid,
      decodeCid: ValueCoder.DecodeCid,
      txVersion: TransactionVersion,
      protoTx: TransactionOuterClass.Transaction,
  ): Either[DecodeError, VersionedTransaction] = {
    val roots = protoTx.getRootsList.asScala
      .foldLeft[Either[DecodeError, BackStack[NodeId]]](Right(BackStack.empty[NodeId])) {
        case (Right(acc), s) => decodeNid.fromString(s).map(acc :+ _)
        case (Left(e), _) => Left(e)
      }
      .map(_.toImmArray)

    val nodes = protoTx.getNodesList.asScala
      .foldLeft[Either[DecodeError, HashMap[NodeId, Node]]](Right(HashMap.empty)) {
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

  // like toParty but requires entries to be strictly ordered
  def toOrderedPartySet(strList: ProtocolStringList): Either[DecodeError, TreeSet[Party]] =
    if (strList.isEmpty)
      Right(TreeSet.empty)
    else {
      val parties = strList
        .asByteStringList()
        .asScala
        .map(bs => Party.fromString(bs.toStringUtf8))

      sequence(parties) match {
        case Left(err) => Left(DecodeError(s"Cannot decode party: $err"))
        case Right(ps) =>
          // TODO. Write a linear function that convert ordered sequences of elements into
          //  a TreeSet, similarly to what is done for TreeMap with com.data.TreeMap
          (ps zip ps.tail)
            .collectFirst {
              case (p1, p2) if p1 >= p2 => DecodeError("the parties are not strictly ordered ")
            }
            .toLeft(TreeSet.from(ps))
      }
    }

  private def toIdentifier(s: String): Either[DecodeError, Name] =
    Name.fromString(s).left.map(DecodeError)

  def decodeVersion(node: TransactionOuterClass.Node): Either[DecodeError, TransactionVersion] =
    if (node.getVersion.isEmpty)
      Right(TransactionVersion.minVersion)
    else
      decodeVersion(node.getVersion)

  private[this] def keyHash(
      nodeVersion: TransactionVersion,
      rawTmplId: ValueOuterClass.Identifier,
      rawKey: ByteString,
  ): Either[DecodeError, GlobalKey] =
    for {
      tmplId <- ValueCoder.decodeIdentifier(rawTmplId)
      value <- ValueCoder.decodeValue(ValueCoder.NoCidDecoder, nodeVersion, rawKey)
      key <- GlobalKey.build(tmplId, value).left.map(hashErr => DecodeError(hashErr.msg))
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

  private[this] def ensureNoUnknownFields(
      proto: com.google.protobuf.Message
  ): Either[DecodeError, Unit] = {
    val unknownFields = proto.getUnknownFields.asMap()
    Either.cond(
      unknownFields.isEmpty,
      (),
      DecodeError(
        s"unexpected field(s) ${unknownFields.keySet().asScala.mkString(", ")}  in ${proto.getClass.getSimpleName} message"
      ),
    )
  }

  private[transaction] def encodeVersioned(
      version: TransactionVersion,
      payload: ByteString,
  ): ByteString = {
    val builder = TransactionOuterClass.Versioned.newBuilder()
    discard(builder.setVersion(version.index))
    discard(builder.setPayload(payload))
    builder.build().toByteString
  }

  private[transaction] def decodeVersioned(
      bytes: ByteString
  ): Either[DecodeError, Versioned[ByteString]] =
    for {
      proto <- scala.util
        .Try(TransactionOuterClass.Versioned.parseFrom(bytes))
        .toEither
        .left
        .map(e => DecodeError(s"exception $e while decoding the versioned object"))
      _ <- ensureNoUnknownFields(proto)
      version <- TransactionVersion.fromInt(proto.getVersion).left.map(DecodeError)
      payload = proto.getPayload
    } yield Versioned(version, payload)

  def encodeFatContractInstance(
      contractInstance: FatContractInstance
  ): Either[EncodeError, ByteString] = {
    import contractInstance._
    for {
      encodedArg <- ValueCoder.encodeValue(ValueCoder.CidEncoder, version, createArg)
      encodedKeyOpt <- contractKeyWithMaintainers match {
        case None =>
          Right(None)
        case Some(key) =>
          encodeKeyWithMaintainers(version, key).map(Some(_))
      }
    } yield {
      val builder = TransactionOuterClass.FatContractInstance.newBuilder()
      contractId match {
        case cid: Value.ContractId.V1 =>
          discard(builder.setContractId(cid.toBytes.toByteString))
      }
      discard(builder.setTemplateId(ValueCoder.encodeIdentifier(templateId)))
      discard(builder.setCreateArg(encodedArg))
      encodedKeyOpt.foreach(builder.setContractKeyWithMaintainers)
      nonMaintainerSignatories.foreach(builder.addNonMaintainerSignatories)
      nonSignatoryStackhodlers.foreach(builder.addNonSignatoryStackhodlers)
      discard(builder.setCreateAt(createAt.micros))
      discard(builder.setCantonData(cantonData.toByteString))
      encodeVersioned(version, builder.build().toByteString)
    }
  }

  def decodeFatContractInstance(bytes: ByteString): Either[DecodeError, FatContractInstance] =
    for {
      versionedBlob <- decodeVersioned(bytes)
      Versioned(version, unversioned) = versionedBlob
      _ <- Either.cond(
        version >= TransactionVersion.V14,
        (),
        DecodeError(s"version $version does not support FatContractInstance"),
      )
      proto <- scala.util
        .Try(TransactionOuterClass.FatContractInstance.parseFrom(unversioned))
        .toEither
        .left
        .map(e => DecodeError(s"exception $e while decoding the object"))
      _ <- ensureNoUnknownFields(proto)
      contractId <- Value.ContractId.V1
        .fromBytes(data.Bytes.fromByteString(proto.getContractId))
        .left
        .map(DecodeError)
      templateId <- ValueCoder.decodeIdentifier(proto.getTemplateId)
      createArg <- ValueCoder.decodeValue(ValueCoder.CidDecoder, version, proto.getCreateArg)
      keyWithMaintainers <-
        if (proto.hasContractKeyWithMaintainers)
          strictDecodeKeyWithMaintainers(version, templateId, proto.getContractKeyWithMaintainers)
            .map(Some(_))
        else
          RightNone
      maintainers = keyWithMaintainers.fold(TreeSet.empty[Party])(k => TreeSet.from(k.maintainers))
      nonMaintainerSignatories <- toOrderedPartySet(proto.getNonMaintainerSignatoriesList)
      _ <- Either.cond(
        maintainers.nonEmpty || nonMaintainerSignatories.nonEmpty,
        (),
        DecodeError("maintainers or non_maintainer_signatories should be non empty"),
      )
      nonSignatoryStakeholders <- toOrderedPartySet(proto.getNonSignatoryStackhodlersList)
      signatories <- maintainers.find(nonMaintainerSignatories) match {
        case Some(p) =>
          Left(DecodeError(s"party $p is declared as maintainer and nonMaintainerSignatory"))
        case None => Right(maintainers | nonMaintainerSignatories)
      }
      stakeholders <- nonSignatoryStakeholders.find(signatories) match {
        case Some(p) =>
          Left(DecodeError(s"party $p is declared as signatory and nonSignatoryStakeholder"))
        case None => Right(signatories | nonSignatoryStakeholders)
      }
      createAt <- data.Time.Timestamp.fromLong(proto.getCreateAt).left.map(DecodeError)
      cantonData = proto.getCantonData
    } yield FatContractInstanceImpl(
      version = versionedBlob.version,
      contractId = contractId,
      templateId = templateId,
      createArg = createArg,
      signatories = signatories,
      stakeholders = stakeholders,
      createAt = createAt,
      contractKeyWithMaintainers = keyWithMaintainers,
      cantonData = data.Bytes.fromByteString(cantonData),
    )

}
