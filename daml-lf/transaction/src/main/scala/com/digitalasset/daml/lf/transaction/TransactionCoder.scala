// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.data.{BackStack, Ref}
import com.daml.lf.transaction.TransactionOuterClass.Node.NodeTypeCase
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{Name, Party}
import com.daml.lf.value.{Value, ValueCoder, ValueOuterClass}
import com.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.daml.scalautil.Statement.discard
import com.google.protobuf.{ByteString, GeneratedMessageV3, ProtocolStringList}

import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.immutable.{HashMap, TreeSet}
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
    for {
      value <- ValueCoder
        .encodeVersionedValue(encodeCid, coinst.version, coinst.unversioned.contractInstance.arg)
      pkgName <- encodePackageName(coinst.unversioned.contractInstance.packageName, coinst.version)
    } yield TransactionOuterClass.ContractInstance
      .newBuilder()
      .setPackageName(pkgName)
      .setTemplateId(ValueCoder.encodeIdentifier(coinst.unversioned.contractInstance.template))
      .setArgVersioned(value)
      .setAgreement(coinst.unversioned.agreementText)
      .build()

  def decodePackageName(s: String): Either[DecodeError, Ref.PackageName] =
    Ref.PackageName.fromString(s).left.map(err => DecodeError(s"Invalid package name '$s': $err"))

  def decodePackageName(
      s: String,
      version: TransactionVersion,
  ): Either[DecodeError, Option[Ref.PackageName]] =
    if (s.nonEmpty)
      if (version >= TransactionVersion.minUpgrade)
        decodePackageName(s).map(Some(_))
      else
        Left(
          DecodeError(s"packageName is not supported by transaction version ${version.protoValue}")
        )
    else
    // TODO: https://github.com/digital-asset/daml/issues/17995
    //  drop the `|| true`, once canton populate the package name
    if (version < TransactionVersion.minUpgrade || true)
      Right(None)
    else
      Left(DecodeError(s"packageName is required for transaction version  ${version.protoValue}"))

  def encodePackageName(
      pkgName: Option[Ref.PackageName],
      version: TransactionVersion,
  ): Either[EncodeError, String] =
    pkgName match {
      case Some(name) =>
        if (version >= TransactionVersion.minUpgrade)
          Right(name)
        else
          Left(
            EncodeError(
              s"packageName is not supported by transaction version ${version.protoValue}"
            )
          )
      case None =>
        // TODO: https://github.com/digital-asset/daml/issues/17995
        //  drop the `|| true`, once canton populate the package name
        if (version < TransactionVersion.minUpgrade || true)
          Right("")
        else
          Left(
            EncodeError(s"packageName is required for transaction version  ${version.protoValue}")
          )
    }

  /** Decode a contract instance from wire format
    *
    * @param protoCoinst protocol buffer encoded contract instance
    * @param decodeCid   cid decoding function
    * @return contract instance value
    */
  def decodeContractInstance(
      decodeCid: ValueCoder.DecodeCid,
      protoCoinst: TransactionOuterClass.ContractInstance,
  ): Either[DecodeError, Versioned[Value.ContractInstance]] =
    decodeVersionedContractInstance(decodeCid, protoCoinst).map(_.map(_.contractInstance))

  def decodeVersionedContractInstance(
      decodeCid: ValueCoder.DecodeCid,
      protoCoinst: TransactionOuterClass.ContractInstance,
  ): Either[DecodeError, Versioned[Value.ContractInstanceWithAgreement]] =
    for {
      id <- ValueCoder.decodeIdentifier(protoCoinst.getTemplateId)
      value <- ValueCoder.decodeVersionedValue(decodeCid, protoCoinst.getArgVersioned)
      pkgName <- decodePackageName(protoCoinst.getPackageName, value.version)
    } yield value.map(arg =>
      Value.ContractInstanceWithAgreement(
        Value.ContractInstance(
          pkgName,
          id,
          arg,
        ),
        protoCoinst.getAgreement,
      )
    )

  private[transaction] def encodeKeyWithMaintainers(
      version: TransactionVersion,
      key: GlobalKeyWithMaintainers,
  ): Either[EncodeError, TransactionOuterClass.KeyWithMaintainers] = {
    val builder = TransactionOuterClass.KeyWithMaintainers.newBuilder()
    key.maintainers.foreach(builder.addMaintainers(_))
    ValueCoder
      .encodeValue(ValueCoder.UnsafeNoCidEncoder, version, key.value)
      .map(builder.setKeyUnversioned(_).build())
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
      setUnversioned: ByteString => GeneratedMessageV3.Builder[_],
  ): Either[EncodeError, Unit] =
    encodeValue(encodeCid, version, value).map(v => discard(setUnversioned(v)))

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
  ): Either[EncodeError, TransactionOuterClass.Node] = {

    val nodeBuilder =
      TransactionOuterClass.Node.newBuilder().setNodeId(encodeNid.asString(nodeId))

    node match {
      case Node.Rollback(children) =>
        val builder = TransactionOuterClass.NodeRollback.newBuilder()
        children.foreach(id => discard(builder.addChildren(encodeNid.asString(id))))
        Right(nodeBuilder.setRollback(builder).build())

      case node: Node.Action =>
        val nodeVersion = node.version
        if (enclosingVersion < nodeVersion)
          Left(
            EncodeError(
              s"A transaction of version $enclosingVersion cannot contain nodes of newer version ($nodeVersion)"
            )
          )
        else {
          discard(nodeBuilder.setVersion(nodeVersion.protoValue))

          node match {

            case nc @ Node.Create(_, _, _, _, _, _, _, _, _) =>
              val builder = TransactionOuterClass.NodeCreate.newBuilder()
              nc.stakeholders.foreach(builder.addStakeholders)
              nc.signatories.foreach(builder.addSignatories)
              discard(builder.setContractIdStruct(encodeCid.encode(nc.coid)))
              for {
                _ <- encodeValue(encodeCid, nodeVersion, nc.arg).map(arg =>
                  builder
                    .setTemplateId(ValueCoder.encodeIdentifier(nc.templateId))
                    .setArgUnversioned(arg)
                    .setAgreement(nc.agreementText)
                )
                encodedPkgName <- encodePackageName(nc.packageName, nodeVersion)
                _ = builder.setPackageName(encodedPkgName)
                _ <- encodeAndSetContractKey(
                  nodeVersion,
                  nc.keyOpt,
                  builder.setKeyWithMaintainers,
                )
              } yield nodeBuilder.setCreate(builder).build()

            case nf @ Node.Fetch(_, _, _, _, _, _, _, _, _) =>
              val builder = TransactionOuterClass.NodeFetch.newBuilder()
              discard(builder.setTemplateId(ValueCoder.encodeIdentifier(nf.templateId)))
              nf.stakeholders.foreach(builder.addStakeholders)
              nf.signatories.foreach(builder.addSignatories)
              discard(builder.setContractIdStruct(encodeCid.encode(nf.coid)))
              discard(builder.setByKey(nf.byKey))
              nf.actingParties.foreach(builder.addActors)
              for {
                encodedPkgName <- encodePackageName(nf.packageName, nodeVersion)
                _ = builder.setPackageName(encodedPkgName)
                _ <- encodeAndSetContractKey(
                  nodeVersion,
                  nf.keyOpt,
                  builder.setKeyWithMaintainers,
                )
              } yield nodeBuilder.setFetch(builder).build()

            case ne @ Node.Exercise(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
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
              discard(builder.setByKey(ne.byKey))
              if (nodeVersion >= TransactionVersion.minInterfaces) {
                ne.interfaceId.foreach(iface =>
                  builder.setInterfaceId(ValueCoder.encodeIdentifier(iface))
                )
              }
              for {
                encodedPkgName <- encodePackageName(ne.packageName, nodeVersion)
                _ = builder.setPackageName(encodedPkgName)
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
                  builder.setArgUnversioned,
                )
                _ <- ne.exerciseResult match {
                  case Some(value) =>
                    encodeAndSetValue(
                      encodeCid,
                      nodeVersion,
                      value,
                      builder.setResultUnversioned,
                    )
                  case None =>
                    Right(())
                }
                _ <- encodeAndSetContractKey(
                  nodeVersion,
                  ne.keyOpt,
                  builder.setKeyWithMaintainers,
                )
              } yield nodeBuilder.setExercise(builder).build()

            case nlbk @ Node.LookupByKey(_, _, _, _, _) =>
              val builder = TransactionOuterClass.NodeLookupByKey.newBuilder()
              discard(builder.setTemplateId(ValueCoder.encodeIdentifier(nlbk.templateId)))
              nlbk.result.foreach(cid =>
                discard(builder.setContractIdStruct(encodeCid.encode(cid)))
              )
              for {
                encodedPkgName <- encodePackageName(nlbk.packageName, nodeVersion)
                _ = builder.setPackageName(encodedPkgName)
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
      packageName: Option[Ref.PackageName],
  ): Either[DecodeError, GlobalKeyWithMaintainers] = {
    for {
      maintainers <- toPartySet(keyWithMaintainers.getMaintainersList)
      value <- decodeValue(
        ValueCoder.NoCidDecoder,
        version,
        keyWithMaintainers.getKeyUnversioned,
      )
      gkey <- GlobalKey
        .build(templateId, value, KeyPackageName(packageName, version))
        .left
        .map(hashErr => DecodeError(hashErr.msg))
    } yield GlobalKeyWithMaintainers(gkey, maintainers)
  }

  private[transaction] def strictDecodeKeyWithMaintainers(
      version: TransactionVersion,
      templateId: Ref.TypeConName,
      keyWithMaintainers: TransactionOuterClass.KeyWithMaintainers,
      packageName: Option[Ref.PackageName],
  ): Either[DecodeError, GlobalKeyWithMaintainers] =
    for {
      maintainers <- toPartyTreeSet(keyWithMaintainers.getMaintainersList)
      _ <- Either.cond(maintainers.nonEmpty, (), DecodeError("key without maintainers"))
      value <- decodeValue(
        ValueCoder.NoCidDecoder,
        version,
        keyWithMaintainers.getKeyUnversioned,
      )
      gkey <- GlobalKey
        .build(templateId, value, KeyPackageName(packageName, version))
        .left
        .map(hashErr => DecodeError(hashErr.msg))
    } yield GlobalKeyWithMaintainers(gkey, maintainers)

  private val RightNone = Right(None)

  private[this] def decodeOptionalKeyWithMaintainers(
      version: TransactionVersion,
      templateId: Ref.TypeConName,
      keyWithMaintainers: TransactionOuterClass.KeyWithMaintainers,
      packageName: Option[Ref.PackageName],
  ): Either[DecodeError, Option[GlobalKeyWithMaintainers]] =
    if (keyWithMaintainers == TransactionOuterClass.KeyWithMaintainers.getDefaultInstance)
      RightNone
    else
      decodeKeyWithMaintainers(version, templateId, keyWithMaintainers, packageName).map(Some(_))

  // package private for test, do not use outside TransactionCoder
  private[lf] def decodeValue(
      decodeCid: ValueCoder.DecodeCid,
      version: TransactionVersion,
      unversionedProto: => ByteString,
  ): Either[DecodeError, Value] =
    ValueCoder.decodeValue(decodeCid, version, unversionedProto)

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
          ni <- nodeId
          children <- decodeChildren(decodeNid, protoRollback.getChildrenList)
        } yield ni -> Node.Rollback(children)
      case NodeTypeCase.CREATE =>
        val protoCreate = protoNode.getCreate
        for {
          ni <- nodeId
          pkgName <- decodePackageName(protoCreate.getPackageName, nodeVersion)
          c <- decodeCid.decode(protoCreate.getContractIdStruct)
          tmplId <- ValueCoder.decodeIdentifier(protoCreate.getTemplateId)
          arg <- ValueCoder.decodeValue(decodeCid, nodeVersion, protoCreate.getArgUnversioned)
          stakeholders <- toPartySet(protoCreate.getStakeholdersList)
          signatories <- toPartySet(protoCreate.getSignatoriesList)
          keyOpt <- decodeOptionalKeyWithMaintainers(
            nodeVersion,
            tmplId,
            protoCreate.getKeyWithMaintainers,
            pkgName,
          )
        } yield ni -> Node.Create(
          coid = c,
          packageName = pkgName,
          templateId = tmplId,
          arg = arg,
          agreementText = protoCreate.getAgreement,
          signatories = signatories,
          stakeholders = stakeholders,
          keyOpt = keyOpt,
          version = nodeVersion,
        )
      case NodeTypeCase.FETCH =>
        val protoFetch = protoNode.getFetch
        for {
          ni <- nodeId
          pkgName <- decodePackageName(protoFetch.getPackageName, nodeVersion)
          templateId <- ValueCoder.decodeIdentifier(protoFetch.getTemplateId)
          c <- decodeCid.decode(protoFetch.getContractIdStruct)
          actingParties <- toPartySet(protoFetch.getActorsList)
          stakeholders <- toPartySet(protoFetch.getStakeholdersList)
          signatories <- toPartySet(protoFetch.getSignatoriesList)
          keyOpt <- decodeOptionalKeyWithMaintainers(
            nodeVersion,
            templateId,
            protoFetch.getKeyWithMaintainers,
            pkgName,
          )
          byKey = protoFetch.getByKey
        } yield ni -> Node.Fetch(
          coid = c,
          packageName = pkgName,
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
          pkgName <- decodePackageName(protoExe.getPackageName, nodeVersion)
          templateId <- ValueCoder.decodeIdentifier(protoExe.getTemplateId)
          rvOpt <-
            if (!protoExe.hasResultVersioned && protoExe.getResultUnversioned.isEmpty) {
              Right(None)
            } else {
              decodeValue(
                decodeCid,
                nodeVersion,
                protoExe.getResultUnversioned,
              ).map(v => Some(v))
            }
          keyWithMaintainers <-
            decodeOptionalKeyWithMaintainers(
              nodeVersion,
              templateId,
              protoExe.getKeyWithMaintainers,
              pkgName,
            )
          ni <- nodeId
          targetCoid <- decodeCid.decode(protoExe.getContractIdStruct)
          children <- decodeChildren(decodeNid, protoExe.getChildrenList)
          cv <- decodeValue(
            decodeCid,
            nodeVersion,
            protoExe.getArgUnversioned,
          )
          actingParties <- toPartySet(protoExe.getActorsList)
          signatories <- toPartySet(protoExe.getSignatoriesList)
          stakeholders <- toPartySet(protoExe.getStakeholdersList)
          choiceObservers <- toPartySet(protoExe.getObserversList)
          choiceAuthorizers <-
            if (nodeVersion < TransactionVersion.minChoiceAuthorizers) { Right(None) }
            else {
              for { choiceAuthorizersList <- toPartySet(protoExe.getAuthorizersList) } yield
                if (choiceAuthorizersList.isEmpty) None else Some(choiceAuthorizersList)
            }
          choiceName <- toIdentifier(protoExe.getChoice)
          byKey = protoExe.getByKey
          interfaceId <-
            if (nodeVersion >= TransactionVersion.minInterfaces && protoExe.hasInterfaceId) {
              ValueCoder.decodeIdentifier(protoExe.getInterfaceId).map(Some(_))
            } else {
              Right(None)
            }
        } yield ni -> Node.Exercise(
          targetCoid = targetCoid,
          packageName = pkgName,
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
          pkgName <- decodePackageName(protoLookupByKey.getPackageName, nodeVersion)
          templateId <- ValueCoder.decodeIdentifier(protoLookupByKey.getTemplateId)
          key <-
            decodeKeyWithMaintainers(
              nodeVersion,
              templateId,
              protoLookupByKey.getKeyWithMaintainers,
              pkgName,
            )
          cid <- decodeCid.decodeOptional(protoLookupByKey.getContractIdStruct)
        } yield ni -> Node.LookupByKey(pkgName, templateId, key, cid, nodeVersion)
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

  // Similar to toPartySet`but
  // - requires strList` to be strictly ordered, fails otherwhise
  // - produces a TreeSet instead of a Set
  // Note this function has a linear complexity, See data.TreeSet.fromStrictlyOrderedEntries
  def toPartyTreeSet(strList: ProtocolStringList): Either[DecodeError, TreeSet[Party]] =
    if (strList.isEmpty)
      Right(TreeSet.empty)
    else {
      val parties = strList
        .asByteStringList()
        .asScala
        .map(bs => Party.fromString(bs.toStringUtf8))

      sequence(parties) match {
        case Left(err) =>
          Left(DecodeError(s"Cannot decode party: $err"))
        case Right(ps) =>
          scala.util
            .Try(data.TreeSet.fromStrictlyOrderedEntries(ps))
            .toEither
            .left
            .map(e => DecodeError(e.getMessage))
      }
    }

  private def toIdentifier(s: String): Either[DecodeError, Name] =
    Name.fromString(s).left.map(DecodeError)

  def decodeVersion(node: TransactionOuterClass.Node): Either[DecodeError, TransactionVersion] =
    if (node.getVersion.isEmpty)
      Right(TransactionVersion.minVersion)
    else
      decodeVersion(node.getVersion)

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
    discard(builder.setVersion(version.protoValue))
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
      version <- TransactionVersion.fromString(proto.getVersion).left.map(DecodeError)
      payload = proto.getPayload
    } yield Versioned(version, payload)

  def encodeFatContractInstance(
      contractInstance: FatContractInstance
  ): Either[EncodeError, ByteString] = {
    import contractInstance._
    for {
      encodedArg <- ValueCoder.encodeValue(ValueCoder.CidEncoder, version, createArg)
      encodedPackageName <- encodePackageName(packageName, version)
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
      discard(builder.setPackageName(encodedPackageName))
      discard(builder.setTemplateId(ValueCoder.encodeIdentifier(templateId)))
      discard(builder.setCreateArg(encodedArg))
      encodedKeyOpt.foreach(builder.setContractKeyWithMaintainers)
      nonMaintainerSignatories.foreach(builder.addNonMaintainerSignatories)
      nonSignatoryStakeholders.foreach(builder.addNonSignatoryStakeholders)
      discard(builder.setCreatedAt(createdAt.micros))
      discard(builder.setCantonData(cantonData.toByteString))
      encodeVersioned(version, builder.build().toByteString)
    }
  }

  private[lf] def assertEncodeFatContractInstance(
      contractInstance: FatContractInstance
  ): data.Bytes =
    encodeFatContractInstance(contractInstance).fold(
      e => throw new IllegalArgumentException(e.errorMessage),
      data.Bytes.fromByteString,
    )

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
      pkgName <- decodePackageName(proto.getPackageName, version)
      templateId <- ValueCoder.decodeIdentifier(proto.getTemplateId)
      createArg <- ValueCoder.decodeValue(ValueCoder.CidDecoder, version, proto.getCreateArg)
      keyWithMaintainers <-
        if (proto.hasContractKeyWithMaintainers)
          strictDecodeKeyWithMaintainers(
            version,
            templateId,
            proto.getContractKeyWithMaintainers,
            pkgName,
          )
            .map(Some(_))
        else
          RightNone
      maintainers = keyWithMaintainers.fold(TreeSet.empty[Party])(k => TreeSet.from(k.maintainers))
      nonMaintainerSignatories <- toPartyTreeSet(proto.getNonMaintainerSignatoriesList)
      _ <- Either.cond(
        maintainers.nonEmpty || nonMaintainerSignatories.nonEmpty,
        (),
        DecodeError("maintainers or non_maintainer_signatories should be non empty"),
      )
      nonSignatoryStakeholders <- toPartyTreeSet(proto.getNonSignatoryStakeholdersList)
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
      createdAt <- data.Time.Timestamp.fromLong(proto.getCreatedAt).left.map(DecodeError)
      cantonData = proto.getCantonData
    } yield FatContractInstanceImpl(
      version = versionedBlob.version,
      contractId = contractId,
      packageName = pkgName,
      templateId = templateId,
      createArg = createArg,
      signatories = signatories,
      stakeholders = stakeholders,
      createdAt = createdAt,
      contractKeyWithMaintainers = keyWithMaintainers,
      cantonData = data.Bytes.fromByteString(cantonData),
    )

}
