// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.{Ref, BackStack}
import com.daml.lf.transaction.TransactionOuterClass.Node.NodeTypeCase
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{Party, Name}
import com.daml.lf.value.ValueCoder.decodeCoid
import com.daml.lf.value.ValueCoder.ensureNoUnknownFields
import com.daml.lf.value.{ValueOuterClass, Value, ValueCoder}
import com.daml.lf.value.ValueCoder.{EncodeError, DecodeError}
import com.daml.scalautil.Statement.discard
import com.google.protobuf.{ByteString, ProtocolStringList}

import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.immutable.{HashMap, TreeSet}
import scala.jdk.CollectionConverters._

object TransactionCoder {

  private[this] def encodeNodeId(id: NodeId): String = id.index.toString

  private[this] def decodeNodeId(s: String): Either[DecodeError, NodeId] =
    scalaz.std.string
      .parseInt(s)
      .fold(_ => Left(DecodeError(s"cannot parse node Id $s")), idx => Right(NodeId(idx)))

  def encodeVersionedValue(
      enclosingVersion: TransactionVersion,
      value: Value.VersionedValue,
  ): Either[EncodeError, ValueOuterClass.VersionedValue] =
    if (enclosingVersion == value.version)
      ValueCoder.encodeVersionedValue(value)
    else
      Left(
        EncodeError(
          s"A node of version $enclosingVersion cannot contain value of different version version ${value.version}"
        )
      )

  private[this] def encodeValue(
      nodeVersion: TransactionVersion,
      value: Value,
  ): Either[EncodeError, ByteString] =
    ValueCoder.encodeValue(nodeVersion, value)

  /** Encodes a contract instance with the help of the contractId encoding function
    *
    * @param coinst    the contract instance to be encoded
    * @return protobuf wire format contract instance
    */
  def encodeContractInstance(
      coinst: Versioned[Value.ContractInstance]
  ): Either[EncodeError, TransactionOuterClass.ContractInstance] =
    for {
      value <- ValueCoder.encodeVersionedValue(coinst.version, coinst.unversioned.arg)
    } yield TransactionOuterClass.ContractInstance
      .newBuilder()
      .setPackageName(coinst.unversioned.packageName)
      .setTemplateId(ValueCoder.encodeIdentifier(coinst.unversioned.template))
      .setArgVersioned(value)
      .build()

  def decodePackageName(s: String): Either[DecodeError, Ref.PackageName] =
    Either
      .cond(
        s.nonEmpty,
        s,
        DecodeError(s"packageName must not be empty"),
      )
      .flatMap(
        Ref.PackageName
          .fromString(_)
          .left
          .map(err => DecodeError(s"Invalid package name '$s': $err"))
      )

  /** Decode a contract instance from wire format
    *
    * @param protoCoinst protocol buffer encoded contract instance
    * @return contract instance value
    */
  def decodeContractInstance(
      protoCoinst: TransactionOuterClass.ContractInstance
  ): Either[DecodeError, Versioned[Value.ContractInstance]] =
    for {
      _ <- ensureNoUnknownFields(protoCoinst)
      id <- ValueCoder.decodeIdentifier(protoCoinst.getTemplateId)
      value <- ValueCoder.decodeVersionedValue(protoCoinst.getArgVersioned)
      pkgName <- decodePackageName(protoCoinst.getPackageName)
    } yield value.map(arg => Value.ContractInstance(pkgName, id, arg))

  private[transaction] def encodeKeyWithMaintainers(
      version: TransactionVersion,
      key: GlobalKeyWithMaintainers,
  ): Either[EncodeError, TransactionOuterClass.KeyWithMaintainers] =
    if (version >= TransactionVersion.minVersion) {
      val builder = TransactionOuterClass.KeyWithMaintainers.newBuilder()
      key.maintainers.foreach(builder.addMaintainers(_))
      ValueCoder
        .encodeValue(valueVersion = version, v0 = key.value)
        .map(builder.setKey(_).build())
    } else
      Left(EncodeError(s"Contract key are not supported by ${version.protoValue}"))

  private[this] def encodeOptKeyWithMaintainers(
      version: TransactionVersion,
      key: Option[GlobalKeyWithMaintainers],
  ): Either[EncodeError, Option[TransactionOuterClass.KeyWithMaintainers]] =
    key match {
      case Some(key) =>
        encodeKeyWithMaintainers(version, key).map(Some(_))
      case None =>
        Right(None)
    }

  private[this] def encodeOptionalValue(
      version: TransactionVersion,
      valueOpt: Option[Value],
  ): Either[EncodeError, ByteString] =
    valueOpt match {
      case Some(value) =>
        encodeValue(version, value)
      case None =>
        Right(ByteString.empty())
    }

  /** encodes a [[Node[Nid]] to protocol buffer
    *
    * @param enclosingVersion the version of the transaction
    * @param nodeId    node id of the node to be encoded
    * @param node      the node to be encoded
    * @return protocol buffer format node
    */
  private[lf] def encodeNode(
      enclosingVersion: TransactionVersion,
      nodeId: NodeId,
      node: Node,
  ) = {
    val nodeBuilder =
      TransactionOuterClass.Node.newBuilder().setNodeId(encodeNodeId(nodeId))

    node match {
      case Node.Rollback(children) =>
        val builder = TransactionOuterClass.Node.Rollback.newBuilder()
        children.foreach(id => discard(builder.addChildren(encodeNodeId(id))))
        Right(nodeBuilder.setRollback(builder).build())

      case node: Node.Action =>
        val nodeVersion = node.version
        if (enclosingVersion < nodeVersion)
          Left(
            EncodeError(
              s"A transaction of version ${enclosingVersion.protoValue} cannot contain nodes of newer version ($nodeVersion)"
            )
          )
        else {
          discard(nodeBuilder.setVersion(nodeVersion.protoValue))

          node match {
            case nc: Node.Create =>
              val fatContractInstance = FatContractInstance.fromCreateNode(
                create = nc,
                createTime = data.Time.Timestamp.Epoch,
                cantonData = data.Bytes.Empty,
              )
              for {
                unversioned <- encodeFatContractInstanceInternal(fatContractInstance)
              } yield nodeBuilder.setCreate(unversioned).build()

            case nf: Node.Fetch =>
              for {
                unversioned <- encodeFetch(nf)
              } yield nodeBuilder.setFetch(unversioned).build()

            case ne: Node.Exercise =>
              for {
                unversioned <- encodeExercise(ne)
              } yield nodeBuilder.setExercise(unversioned).build()

            case nlbk: Node.LookupByKey =>
              for {
                unversionned <- encodeLookUp(nlbk)
              } yield nodeBuilder.setLookupByKey(unversionned).build()

          }
        }
    }
  }

  private[this] def encodeFetch(
      node: Node.Fetch
  ): Either[EncodeError, TransactionOuterClass.Node.Fetch] = {
    val version = node.version
    val maintainers = node.keyOpt.fold(Set.empty[Party])(_.maintainers)
    val signatories = node.signatories
    val stakeholders = node.stakeholders
    require(maintainers.subsetOf(signatories))
    require(signatories.subsetOf(stakeholders))
    val non_maintainer_signatories = signatories -- maintainers
    val non_signatory_stakeholders = stakeholders -- signatories

    val builder = TransactionOuterClass.Node.Fetch.newBuilder()
    discard(builder.setContractId(node.coid.toBytes.toByteString))
    discard(builder.setPackageName(node.packageName))
    discard(builder.setTemplateId(ValueCoder.encodeIdentifier(node.templateId)))
    non_maintainer_signatories.foreach(builder.addNonMaintainerSignatories)
    non_signatory_stakeholders.foreach(builder.addNonSignatoryStakeholders)
    node.actingParties.foreach(builder.addActors)

    for {
      protoKey <- encodeOptKeyWithMaintainers(version, node.keyOpt)
      _ = protoKey.foreach(builder.setKeyWithMaintainers)
      _ <-
        if (node.byKey)
          Either.cond(
            version >= TransactionVersion.minContractKeys,
            discard(builder.setByKey(true)),
            EncodeError(s"Node field byKey is not supported by ${version.protoValue}"),
          )
        else
          Right(())
    } yield builder.build()
  }

  private[this] def encodeExercise(
      node: Node.Exercise
  ): Either[EncodeError, TransactionOuterClass.Node.Exercise] = {
    val builder = TransactionOuterClass.Node.Exercise.newBuilder()
    val fetch = Node.Fetch(
      coid = node.targetCoid,
      packageName = node.packageName,
      templateId = node.templateId,
      actingParties = node.actingParties,
      signatories = node.signatories,
      stakeholders = node.stakeholders,
      keyOpt = node.keyOpt,
      byKey = node.byKey,
      version = node.version,
    )
    for {
      protoFetch <- encodeFetch(fetch)
      _ = builder.setFetch(protoFetch)
      _ = node.interfaceId.foreach(id => builder.setInterfaceId(ValueCoder.encodeIdentifier(id)))
      _ = builder.setChoice(node.choiceId)
      protoArg <- encodeValue(node.version, node.chosenValue)
      _ = builder.setArg(protoArg)
      _ = builder.setConsuming(node.consuming)
      _ = node.children.foreach(id => discard(builder.addChildren(encodeNodeId(id))))
      protoResult <- encodeOptionalValue(node.version, node.exerciseResult)
      _ = builder.setResult(protoResult)
      _ = node.choiceObservers.foreach(builder.addObservers)
      _ <- node.choiceAuthorizers match {
        case Some(authorizers) =>
          if (node.version <= TransactionVersion.minChoiceAuthorizers)
            Left(
              EncodeError(s"choice authorizers are not supported by ${node.version.protoValue}")
            )
          else
            Either.cond(
              node.choiceAuthorizers.nonEmpty,
              authorizers.foreach(builder.addAuthorizers),
              EncodeError(s"choice authorizers cannot be empty"),
            )
        case None =>
          Right(())
      }
    } yield builder.build()
  }

  private[this] def encodeLookUp(
      node: Node.LookupByKey
  ): Either[EncodeError, TransactionOuterClass.Node.LookupByKey] =
    for {
      _ <- Either.cond(
        node.version >= TransactionVersion.minContractKeys,
        (),
        EncodeError(s"Contract keys not supported by ${node.version.protoValue}"),
      )
      builder = TransactionOuterClass.Node.LookupByKey.newBuilder()
      _ = discard(builder.setPackageName(node.packageName))
      _ = discard(builder.setTemplateId(ValueCoder.encodeIdentifier(node.templateId)))
      _ = node.result.foreach(cid => discard(builder.setContractId(cid.toBytes.toByteString)))
      encodedKey <- encodeKeyWithMaintainers(node.version, node.key)
    } yield builder.setKeyWithMaintainers(encodedKey).build()

  private[this] def decodeKeyWithMaintainers(
      version: TransactionVersion,
      templateId: Ref.TypeConName,
      msg: TransactionOuterClass.KeyWithMaintainers,
  ): Either[DecodeError, GlobalKeyWithMaintainers] = {
    for {
      _ <- ensureNoUnknownFields(msg)
      maintainers <- toPartySet(msg.getMaintainersList)
      value <- decodeValue(
        version = version,
        unversionedProto = msg.getKey,
      )
      gkey <- GlobalKey
        .build(templateId, value)
        .left
        .map(hashErr => DecodeError(hashErr.msg))
    } yield GlobalKeyWithMaintainers(gkey, maintainers)
  }

  private[transaction] def strictDecodeKeyWithMaintainers(
      version: TransactionVersion,
      templateId: Ref.TypeConName,
      msg: TransactionOuterClass.KeyWithMaintainers,
  ): Either[DecodeError, GlobalKeyWithMaintainers] =
    for {
      _ <- ensureNoUnknownFields(msg)
      maintainers <- toPartyTreeSet(msg.getMaintainersList)
      _ <- Either.cond(maintainers.nonEmpty, (), DecodeError("key without maintainers"))
      value <- decodeValue(
        version = version,
        unversionedProto = msg.getKey,
      )
      gkey <- GlobalKey
        .build(templateId, value)
        .left
        .map(hashErr => DecodeError(hashErr.msg))
    } yield GlobalKeyWithMaintainers(gkey, maintainers)

  private val RightNone = Right(None)

  // package private for test, do not use outside TransactionCoder
  private[lf] def decodeValue(
      version: TransactionVersion,
      unversionedProto: ByteString,
  ): Either[DecodeError, Value] =
    ValueCoder.decodeValue(version, unversionedProto)

  private[lf] def decodeOptionalValue(
      version: TransactionVersion,
      unversionedProto: ByteString,
  ): Either[DecodeError, Option[Value]] =
    if (unversionedProto.isEmpty)
      Right(None)
    else
      ValueCoder.decodeValue(version, unversionedProto).map(Some(_))

  /** read a [[Node[Nid]] from protobuf
    *
    * @param protoNode protobuf encoded node
    * @return decoded GenNode
    */
  private[lf] def decodeVersionedNode(
      transactionVersion: TransactionVersion,
      protoNode: TransactionOuterClass.Node,
  ): Either[DecodeError, (NodeId, Node)] =
    decodeNode(transactionVersion, protoNode)

  private[lf] def decodeNode(
      txVersion: TransactionVersion,
      msg: TransactionOuterClass.Node,
  ): Either[DecodeError, (NodeId, Node)] =
    for {
      _ <- ensureNoUnknownFields(msg)
      nodeId <- decodeNodeId(msg.getNodeId)
      node <- msg.getNodeTypeCase match {
        case NodeTypeCase.ROLLBACK =>
          decodeRollback(msg.getVersion, msg.getRollback)
        case NodeTypeCase.CREATE =>
          decodeCreate(txVersion, msg.getVersion, msg.getCreate)
        case NodeTypeCase.FETCH =>
          decodeFetch(txVersion, msg.getVersion, msg.getFetch)
        case NodeTypeCase.EXERCISE =>
          decodeExercise(txVersion, msg.getVersion, msg.getExercise)
        case NodeTypeCase.LOOKUP_BY_KEY =>
          decodeLookup(txVersion, msg.getVersion, msg.getLookupByKey)
        case NodeTypeCase.NODETYPE_NOT_SET => Left(DecodeError("Unset Node type"))
      }
    } yield (nodeId, node)

  private[this] def decodeRollback(
      nodeVersionStr: String,
      msg: TransactionOuterClass.Node.Rollback,
  ) =
    for {
      _ <- ensureNoUnknownFields(msg)
      _ <- Either.cond(
        nodeVersionStr.isEmpty,
        (),
        DecodeError("unexpected node version for Rollback node"),
      )
      children <- decodeChildren(msg.getChildrenList)
    } yield Node.Rollback(children)

  private[this] def decodeCreate(
      txVersion: TransactionVersion,
      nodeVersionStr: String,
      msg: TransactionOuterClass.FatContractInstance,
  ): Either[DecodeError, Node.Create] =
    for {
      // call to decodeFatContractInstance checks for unknown fields
      nodeVersion <- decodeActionNodeVersion(txVersion, nodeVersionStr)
      contract <- decodeFatContractInstance(nodeVersion, msg)
      _ <- Either.cond(
        contract.createdAt.micros == 0L,
        (),
        DecodeError("unexpected created_at field in create node"),
      )
      _ <- Either.cond(
        contract.cantonData.isEmpty,
        (),
        DecodeError("unexpected canton_data field in create node"),
      )
    } yield contract.toCreateNode

  private[this] def decodeFetch(
      txVersion: TransactionVersion,
      nodeVersionStr: String,
      msg: TransactionOuterClass.Node.Fetch,
  ): Either[DecodeError, Node.Fetch] = {
    for {
      _ <- ensureNoUnknownFields(msg)
      nodeVersion <- decodeActionNodeVersion(txVersion, nodeVersionStr)
      cid <- decodeCoid(msg.getContractId)
      pkgName <- decodePackageName(msg.getPackageName)
      templateId <- ValueCoder.decodeIdentifier(msg.getTemplateId)
      nonMaintainerSignatories <- toPartySet(msg.getNonMaintainerSignatoriesList)
      nonSignatoryStakeholdersList <- toPartySet(msg.getNonSignatoryStakeholdersList)
      actingParties <- toPartySet(msg.getActorsList)
      keyOpt <-
        if (msg.hasKeyWithMaintainers)
          decodeKeyWithMaintainers(
            nodeVersion,
            templateId,
            msg.getKeyWithMaintainers,
          ).map(Some(_))
        else
          Right(None)
      maintainers = keyOpt.fold(Set.empty[Party])(_.maintainers)
      signatories = nonMaintainerSignatories ++ maintainers
      stakeholders = nonSignatoryStakeholdersList ++ signatories
      byKey <-
        if (msg.getByKey)
          Either.cond(
            nodeVersion >= TransactionVersion.minContractKeys,
            true,
            DecodeError(s"transaction key is not supported by ${nodeVersion.protoValue}"),
          )
        else
          Right(false)
    } yield Node.Fetch(
      coid = cid,
      packageName = pkgName,
      templateId = templateId,
      actingParties = actingParties,
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = keyOpt,
      byKey = byKey,
      version = nodeVersion,
    )
  }

  private[this] def decodeExercise(
      txVersion: TransactionVersion,
      nodeVersionStr: String,
      msg: TransactionOuterClass.Node.Exercise,
  ): Either[DecodeError, Node.Exercise] = {
    for {
      _ <- ensureNoUnknownFields(msg)
      fetch <- decodeFetch(txVersion, nodeVersionStr, msg.getFetch)
      nodeVersion = fetch.version
      interfaceId <-
        if (msg.hasInterfaceId) {
          ValueCoder.decodeIdentifier(msg.getInterfaceId).map(Some(_))
        } else {
          Right(None)
        }
      choiceName <- toIdentifier(msg.getChoice)
      arg <- decodeValue(nodeVersion, msg.getArg)
      children <- decodeChildren(msg.getChildrenList)
      result <- decodeOptionalValue(nodeVersion, msg.getResult)
      observers <- toPartySet(msg.getObserversList)
      authorizers <-
        if (msg.getAuthorizersCount == 0)
          Right(None)
        else if (nodeVersion < TransactionVersion.minChoiceAuthorizers)
          Left(DecodeError(s"Exercise Authorizer not supported by ${nodeVersion.protoValue}"))
        else
          toPartySet(msg.getAuthorizersList).map(Some(_))
    } yield Node.Exercise(
      targetCoid = fetch.coid,
      packageName = fetch.packageName,
      templateId = fetch.templateId,
      interfaceId = interfaceId,
      choiceId = choiceName,
      consuming = msg.getConsuming,
      actingParties = fetch.actingParties,
      chosenValue = arg,
      stakeholders = fetch.stakeholders,
      signatories = fetch.signatories,
      choiceObservers = observers,
      choiceAuthorizers = authorizers,
      children = children,
      exerciseResult = result,
      keyOpt = fetch.keyOpt,
      byKey = fetch.byKey,
      version = fetch.version,
    )
  }

  private[this] def decodeLookup(
      txVersion: TransactionVersion,
      nodeVersionStr: String,
      msg: TransactionOuterClass.Node.LookupByKey,
  ) =
    for {
      _ <- ensureNoUnknownFields(msg)
      nodeVersion <- decodeActionNodeVersion(txVersion, nodeVersionStr)
      _ <- Either.cond(
        txVersion >= TransactionVersion.minContractKeys,
        (),
        DecodeError(s"Contract ket not supported by ${nodeVersion.protoValue}"),
      )
      pkgName <- decodePackageName(msg.getPackageName)
      templateId <- ValueCoder.decodeIdentifier(msg.getTemplateId)
      key <-
        decodeKeyWithMaintainers(
          nodeVersion,
          templateId,
          msg.getKeyWithMaintainers,
        )
      cid <- ValueCoder.decodeOptionalCoid(msg.getContractId)
    } yield Node.LookupByKey(pkgName, templateId, key, cid, nodeVersion)

  private[this] def decodeChildren(
      strList: ProtocolStringList
  ): Either[DecodeError, ImmArray[NodeId]] = {
    strList.asScala
      .foldLeft[Either[DecodeError, BackStack[NodeId]]](Right(BackStack.empty)) {
        case (Left(e), _) => Left(e)
        case (Right(ids), s) => decodeNodeId(s).map(ids :+ _)
      }
      .map(_.toImmArray)
  }

  /** Encode a [[Transaction[Nid]]] to protobuf using [[TransactionVersion]] provided by the libary.
    *
    * @param tx        the transaction to be encoded
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return protobuf encoded transaction
    */
  def encodeTransaction(
      tx: VersionedTransaction
  ): Either[EncodeError, TransactionOuterClass.Transaction] =
    encodeTransactionWithCustomVersion(
      tx
    )

  /** Encode a transaction to protobuf using [[TransactionVersion]] provided by in the [[VersionedTransaction]] argument.
    *
    * @param transaction the transaction to be encoded
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return protobuf encoded transaction
    */
  private[transaction] def encodeTransactionWithCustomVersion(
      transaction: VersionedTransaction
  ): Either[EncodeError, TransactionOuterClass.Transaction] = {
    val builder = TransactionOuterClass.Transaction
      .newBuilder()
      .setVersion(transaction.version.protoValue)
    transaction.roots.foreach(nid => discard(builder.addRoots(encodeNodeId(nid))))

    transaction
      .fold[Either[EncodeError, TransactionOuterClass.Transaction.Builder]](
        Right(builder)
      ) { case (builderOrError, (nid, _)) =>
        for {
          builder <- builderOrError
          encodedNode <- encodeNode(
            transaction.version,
            nid,
            transaction.nodes(nid),
          )
        } yield builder.addNodes(encodedNode)
      }
      .map(_.build)
  }

  def decodeActionNodeVersion(
      txVersion: TransactionVersion,
      nodeVersionStr: String,
  ): Either[DecodeError, TransactionVersion] =
    for {
      nodeVersion <- decodeVersion(nodeVersionStr)
      _ <- Either.cond(
        nodeVersion <= txVersion,
        (),
        DecodeError(
          s"A transaction of version ${txVersion.protoValue} cannot contain node of newer version (${nodeVersion.protoValue})"
        ),
      )
    } yield nodeVersion

  def decodeVersion(vs: String): Either[DecodeError, TransactionVersion] =
    TransactionVersion.fromString(vs).left.map(DecodeError)

  /** Reads a [[VersionedTransaction]] from protobuf and checks if
    * [[TransactionVersion]] passed in the protobuf is currently supported.
    *
    * Supported transaction versions configured in [[TransactionVersion]].
    *
    * @param protoTx protobuf encoded transaction
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return decoded transaction
    */
  def decodeTransaction(
      protoTx: TransactionOuterClass.Transaction
  ): Either[DecodeError, VersionedTransaction] =
    for {
      _ <- ensureNoUnknownFields(protoTx)
      version <- decodeVersion(protoTx.getVersion)
      tx <- decodeTransaction(version, protoTx)
    } yield tx

  /** Reads a [[Transaction[Nid]]] from protobuf. Does not check if
    * [[TransactionVersion]] passed in the protobuf is currently supported, if you need this check use
    * [[TransactionCoder.decodeTransaction]].
    *
    * @param msg   protobuf encoded transaction
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return decoded transaction
    */
  private[this] def decodeTransaction(
      txVersion: TransactionVersion,
      msg: TransactionOuterClass.Transaction,
  ): Either[DecodeError, VersionedTransaction] = for {
    _ <- ensureNoUnknownFields(msg)
    roots <- msg.getRootsList.asScala
      .foldLeft[Either[DecodeError, BackStack[NodeId]]](Right(BackStack.empty[NodeId])) {
        case (Right(acc), s) => decodeNodeId(s).map(acc :+ _)
        case (Left(e), _) => Left(e)
      }
      .map(_.toImmArray)
    nodes <- msg.getNodesList.asScala
      .foldLeft[Either[DecodeError, HashMap[NodeId, Node]]](Right(HashMap.empty)) {
        case (Left(e), _) => Left(e)
        case (Right(acc), s) =>
          decodeVersionedNode(txVersion, s).map(acc + _)
      }
  } yield VersionedTransaction(txVersion, nodes, roots)

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
      _ <- ValueCoder.ensureNoUnknownFields(proto)
      version <- TransactionVersion.fromString(proto.getVersion).left.map(DecodeError)
      payload = proto.getPayload
    } yield Versioned(version, payload)

  def encodeFatContractInstanceInternal(
      contractInstance: FatContractInstance
  ): Either[EncodeError, TransactionOuterClass.FatContractInstance] = {
    import contractInstance._
    for {
      encodedArg <- ValueCoder.encodeValue(version, createArg)
      encodedKeyOpt <- contractKeyWithMaintainers match {
        case None =>
          Right(None)
        case Some(key) =>
          encodeKeyWithMaintainers(version, key).map(Some(_))
      }
    } yield {
      val builder = TransactionOuterClass.FatContractInstance.newBuilder()
      discard(builder.setContractId(contractId.toBytes.toByteString))
      discard(builder.setPackageName(packageName))
      discard(builder.setTemplateId(ValueCoder.encodeIdentifier(templateId)))
      discard(builder.setCreateArg(encodedArg))
      encodedKeyOpt.foreach(builder.setContractKeyWithMaintainers)
      nonMaintainerSignatories.foreach(builder.addNonMaintainerSignatories)
      nonSignatoryStakeholders.foreach(builder.addNonSignatoryStakeholders)
      discard(builder.setCreatedAt(createdAt.micros))
      discard(builder.setCantonData(cantonData.toByteString))
      builder.build()
    }
  }

  def encodeFatContractInstance(
      contractInstance: FatContractInstance
  ): Either[EncodeError, ByteString] =
    for {
      unversioned <- encodeFatContractInstanceInternal(contractInstance)
    } yield encodeVersioned(contractInstance.version, unversioned.toByteString)

  private[lf] def assertEncodeFatContractInstance(
      contractInstance: FatContractInstance
  ): data.Bytes =
    encodeFatContractInstance(contractInstance).fold(
      e => throw new IllegalArgumentException(e.errorMessage),
      data.Bytes.fromByteString,
    )

  private[lf] def decodeFatContractInstance(
      version: TransactionVersion,
      msg: TransactionOuterClass.FatContractInstance,
  ): Either[DecodeError, FatContractInstance] =
    for {
      _ <- ValueCoder.ensureNoUnknownFields(msg)
      contractId <- ValueCoder.decodeCoid(msg.getContractId)
      pkgName <- decodePackageName(msg.getPackageName)
      templateId <- ValueCoder.decodeIdentifier(msg.getTemplateId)
      createArg <- ValueCoder.decodeValue(version = version, bytes = msg.getCreateArg)
      keyWithMaintainers <-
        if (msg.hasContractKeyWithMaintainers)
          strictDecodeKeyWithMaintainers(version, templateId, msg.getContractKeyWithMaintainers)
            .map(Some(_))
        else
          RightNone
      maintainers = keyWithMaintainers.fold(TreeSet.empty[Party])(k => TreeSet.from(k.maintainers))
      nonMaintainerSignatories <- toPartyTreeSet(msg.getNonMaintainerSignatoriesList)
      _ <- Either.cond(
        maintainers.nonEmpty || nonMaintainerSignatories.nonEmpty,
        (),
        DecodeError("maintainers or non_maintainer_signatories should be non empty"),
      )
      nonSignatoryStakeholders <- toPartyTreeSet(msg.getNonSignatoryStakeholdersList)
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
      createdAt <- data.Time.Timestamp.fromLong(msg.getCreatedAt).left.map(DecodeError)
      cantonData = msg.getCantonData
    } yield FatContractInstanceImpl(
      version = version,
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

  def decodeFatContractInstance(bytes: ByteString): Either[DecodeError, FatContractInstance] =
    for {
      versionedBlob <- decodeVersioned(bytes)
      Versioned(version, unversioned) = versionedBlob
      proto <- scala.util
        .Try(TransactionOuterClass.FatContractInstance.parseFrom(unversioned))
        .toEither
        .left
        .map(e => DecodeError(s"exception $e while decoding the object"))
      fatContractInstance <- decodeFatContractInstance(version, proto)
    } yield fatContractInstance

}
