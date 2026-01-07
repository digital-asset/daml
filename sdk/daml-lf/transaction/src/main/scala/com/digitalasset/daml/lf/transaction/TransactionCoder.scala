// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.data.{BackStack, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.transaction.TransactionOuterClass.Node.NodeTypeCase
import com.digitalasset.daml.lf.data.Ref.{Name, Party}
import com.digitalasset.daml.lf.value.{DecodeError, EncodeError, Value, ValueOuterClass}
import com.daml.scalautil.Statement.discard
import com.google.protobuf.{ByteString, ProtocolStringList}

import scala.annotation.nowarn
import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.immutable.{HashMap, TreeSet}
import scala.jdk.CollectionConverters._

object TransactionCoder extends TransactionCoder(allowNullCharacters = false)

class TransactionCoder(allowNullCharacters: Boolean) {

  val ValueCoder = new value.ValueCoder(allowNullCharacters = allowNullCharacters).internal

  /** Decode a contract instance from wire format
    *
    * @param protoCoinst protocol buffer encoded contract instance
    * @return contract instance value
    */
  def decodeContractInstance(
      protoCoinst: TransactionOuterClass.ThinContractInstance
  ): Either[DecodeError, Versioned[Value.ThinContractInstance]] =
    ensuresNoUnknownFieldsThenDecode(protoCoinst)(internal.decodeContractInstance)

  /** Encodes a contract instance with the help of the contractId encoding function
    *
    * @param coinst the contract instance to be encoded
    * @return protobuf wire format contract instance
    */
  def encodeContractInstance(
      coinst: Versioned[Value.ThinContractInstance]
  ): Either[EncodeError, TransactionOuterClass.ThinContractInstance] =
    internal.encodeContractInstance(coinst)

  /** Reads a [[VersionedTransaction]] from protobuf and checks if
    * [[SerializationVersion]] passed in the protobuf is currently supported.
    *
    * Supported serialization versions configured in [[SerializationVersion]].
    *
    * @param protoTx protobuf encoded transaction
    * @return decoded transaction
    */
  def decodeTransaction(
      protoTx: TransactionOuterClass.Transaction
  ): Either[DecodeError, VersionedTransaction] =
    ensuresNoUnknownFieldsThenDecode(protoTx)(internal.decodeTransaction)

  /** Encode a [[Transaction]] to protobuf using [[SerializationVersion]] provided by the libary.
    *
    * @param tx the transaction to be encoded
    * @return protobuf encoded transaction
    */
  def encodeTransaction(
      tx: VersionedTransaction
  ): Either[EncodeError, TransactionOuterClass.Transaction] =
    internal.encodeTransaction(tx)

  def decodeFatContractInstance(bytes: ByteString): Either[DecodeError, FatContractInstance] =
    internal.decodeFatContractInstance(bytes)

  def encodeFatContractInstance(
      contractInstance: FatContractInstance
  ): Either[EncodeError, ByteString] =
    internal.encodeFatContractInstance(contractInstance)

  private[transaction] def encodeVersioned(
      version: SerializationVersion,
      payload: ByteString,
  ): ByteString =
    internal.encodeVersioned(version, payload)

  private[transaction] def decodeVersioned(
      bytes: ByteString
  ): Either[DecodeError, Versioned[ByteString]] =
    internal.decodeVersioned(bytes)

  private[transaction] object internal {

    private[this] def encodeNodeId(id: NodeId): String = id.index.toString

    private[this] def decodeNodeId(s: String): Either[DecodeError, NodeId] =
      scalaz.std.string
        .parseInt(s)
        .fold(_ => Left(DecodeError(s"cannot parse node Id $s")), idx => Right(NodeId(idx)))

    def encodeVersionedValue(
        enclosingVersion: SerializationVersion,
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
        nodeVersion: SerializationVersion,
        value: Value,
    ): Either[EncodeError, ByteString] =
      ValueCoder.encodeValue(nodeVersion, value)

    def encodeContractInstance(
        coinst: Versioned[Value.ThinContractInstance]
    ): Either[EncodeError, TransactionOuterClass.ThinContractInstance] =
      for {
        value <- ValueCoder.encodeVersionedValue(coinst.version, coinst.unversioned.arg)
      } yield {
        val builder = TransactionOuterClass.ThinContractInstance.newBuilder()
        discard(builder.setPackageName(coinst.unversioned.packageName))
        discard(builder.setTemplateId(ValueCoder.encodeIdentifier(coinst.unversioned.template)))
        discard(builder.setArgVersioned(value))
        builder.build()
      }

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

    def decodeContractInstance(
        protoCoinst: TransactionOuterClass.ThinContractInstance
    ): Either[DecodeError, Versioned[Value.ThinContractInstance]] =
      for {
        id <- ValueCoder.decodeIdentifier(protoCoinst.getTemplateId)
        value <- ValueCoder.decodeVersionedValue(protoCoinst.getArgVersioned)
        pkgName <- decodePackageName(protoCoinst.getPackageName)
      } yield value.map(arg => Value.ThinContractInstance(pkgName, id, arg))

    private[transaction] def encodeKeyWithMaintainers(
        version: SerializationVersion,
        key: GlobalKeyWithMaintainers,
    ): Either[EncodeError, TransactionOuterClass.KeyWithMaintainers] =
      if (version >= SerializationVersion.minVersion) {
        val builder = TransactionOuterClass.KeyWithMaintainers.newBuilder()
        TreeSet.from(key.maintainers).foreach(builder.addMaintainers(_))
        ValueCoder
          .encodeValue(valueVersion = version, v0 = key.value)
          .map(builder.setKey(_).build())
      } else
        Left(EncodeError(s"Contract key are not supported by $version"))

    private[this] def encodeOptKeyWithMaintainers(
        version: SerializationVersion,
        key: Option[GlobalKeyWithMaintainers],
    ): Either[EncodeError, Option[TransactionOuterClass.KeyWithMaintainers]] =
      key match {
        case Some(key) =>
          encodeKeyWithMaintainers(version, key).map(Some(_))
        case None =>
          Right(None)
      }

    private[this] def encodeOptionalValue(
        version: SerializationVersion,
        valueOpt: Option[Value],
    ): Either[EncodeError, ByteString] =
      valueOpt match {
        case Some(value) =>
          encodeValue(version, value)
        case None =>
          Right(ByteString.empty())
      }

    /** encodes a [[Node]] to protocol buffer
      *
      * @param enclosingVersion the version of the transaction
      * @param nodeId           node id of the node to be encoded
      * @param node             the node to be encoded
      * @return protocol buffer format node
      */
    private[lf] def encodeNode(
        enclosingVersion: SerializationVersion,
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
                s"A transaction of version $enclosingVersion cannot contain nodes of newer version ($nodeVersion)"
              )
            )
          else {
            discard(nodeBuilder.setVersion(SerializationVersion.toProtoValue(nodeVersion)))

            node match {
              case nc: Node.Create =>
                val fatContractInstance = FatContractInstance.fromCreateNode(
                  create = nc,
                  createTime = CreationTime.CreatedAt(data.Time.Timestamp.Epoch),
                  authenticationData = data.Bytes.Empty,
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
      node.interfaceId.foreach(iface => builder.setInterfaceId(ValueCoder.encodeIdentifier(iface)))
      non_maintainer_signatories.foreach(builder.addNonMaintainerSignatories)
      non_signatory_stakeholders.foreach(builder.addNonSignatoryStakeholders)
      node.actingParties.foreach(builder.addActors)

      for {
        protoKey <- encodeOptKeyWithMaintainers(version, node.keyOpt)
        _ = protoKey.foreach(builder.setKeyWithMaintainers)
        _ <-
          if (node.byKey)
            Either.cond(
              version >= SerializationVersion.minContractKeys,
              discard(builder.setByKey(true)),
              EncodeError(s"Node field byKey is not supported by version $version"),
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
        interfaceId = node.interfaceId,
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
            if (node.version < SerializationVersion.minChoiceAuthorizers)
              Left(
                EncodeError(s"choice authorizers are not supported by version ${node.version}")
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
          node.version >= SerializationVersion.minContractKeys,
          (),
          EncodeError(s"Contract keys not supported by version ${node.version}"),
        )
        builder = TransactionOuterClass.Node.LookupByKey.newBuilder()
        _ = discard(builder.setPackageName(node.packageName))
        _ = discard(builder.setTemplateId(ValueCoder.encodeIdentifier(node.templateId)))
        _ = node.result.foreach(cid => discard(builder.setContractId(cid.toBytes.toByteString)))
        encodedKey <- encodeKeyWithMaintainers(node.version, node.key)
      } yield builder.setKeyWithMaintainers(encodedKey).build()

    private[this] def decodeKeyWithMaintainers(
        version: SerializationVersion,
        templateId: Ref.TypeConId,
        packageName: Ref.PackageName,
        msg: TransactionOuterClass.KeyWithMaintainers,
    ): Either[DecodeError, GlobalKeyWithMaintainers] = {
      for {
        maintainers <- toPartySet(msg.getMaintainersList)
        value <- decodeValue(
          version = version,
          unversionedProto = msg.getKey,
        )
        gkey <- GlobalKey
          .build(templateId, value, packageName)
          .left
          .map(hashErr => DecodeError(hashErr.msg))
      } yield GlobalKeyWithMaintainers(gkey, maintainers)
    }

    private[transaction] def strictDecodeKeyWithMaintainers(
        version: SerializationVersion,
        templateId: Ref.TypeConId,
        packageName: Ref.PackageName,
        msg: TransactionOuterClass.KeyWithMaintainers,
    ): Either[DecodeError, GlobalKeyWithMaintainers] =
      for {
        kwm <- decodeKeyWithMaintainers(version, templateId, packageName, msg)
        _ <- Either.cond(kwm.maintainers.nonEmpty, (), DecodeError("key without maintainers"))
      } yield kwm

    private val RightNone = Right(None)

    // package private for test, do not use outside TransactionCoder
    private[lf] def decodeValue(
        version: SerializationVersion,
        unversionedProto: ByteString,
    ): Either[DecodeError, Value] =
      ValueCoder.decodeValue(version, unversionedProto)

    private[lf] def decodeOptionalValue(
        version: SerializationVersion,
        unversionedProto: ByteString,
    ): Either[DecodeError, Option[Value]] =
      if (unversionedProto.isEmpty)
        Right(None)
      else
        ValueCoder.decodeValue(version, unversionedProto).map(Some(_))

    /** read a [[Node]] from protobuf
      *
      * @param protoNode protobuf encoded node
      * @return decoded GenNode
      */
    private[lf] def decodeVersionedNode(
        serializationVersion: SerializationVersion,
        protoNode: TransactionOuterClass.Node,
    ): Either[DecodeError, (NodeId, Node)] =
      decodeNode(serializationVersion, protoNode)

    private[lf] def decodeNode(
        txVersion: SerializationVersion,
        msg: TransactionOuterClass.Node,
    ): Either[DecodeError, (NodeId, Node)] =
      for {
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
        _ <- Either.cond(
          nodeVersionStr.isEmpty,
          (),
          DecodeError("unexpected node version for Rollback node"),
        )
        children <- decodeChildren(msg.getChildrenList)
      } yield Node.Rollback(children)

    private[this] def decodeCreate(
        txVersion: SerializationVersion,
        nodeVersionStr: String,
        msg: TransactionOuterClass.FatContractInstance,
    ): Either[DecodeError, Node.Create] =
      for {
        // call to decodeFatContractInstance checks for unknown fields
        nodeVersion <- decodeActionNodeVersion(txVersion, nodeVersionStr)
        contract <- decodeFatContractInstance(nodeVersion, msg)
        _ <- Either.cond(
          contract.createdAt == CreationTime.CreatedAt(Time.Timestamp.Epoch),
          (),
          DecodeError("unexpected created_at field in create node"),
        )
        _ <- Either.cond(
          contract.authenticationData.isEmpty,
          (),
          DecodeError("unexpected canton_data field in create node"),
        )
      } yield contract.toCreateNode

    private[this] def decodeFetch(
        txVersion: SerializationVersion,
        nodeVersionStr: String,
        msg: TransactionOuterClass.Node.Fetch,
    ): Either[DecodeError, Node.Fetch] = {
      for {
        nodeVersion <- decodeActionNodeVersion(txVersion, nodeVersionStr)
        cid <- ValueCoder.decodeCoid(msg.getContractId)
        pkgName <- decodePackageName(msg.getPackageName)
        templateId <- ValueCoder.decodeIdentifier(msg.getTemplateId)
        interfaceId <-
          if (msg.hasInterfaceId) {
            ValueCoder.decodeIdentifier(msg.getInterfaceId).map(Some(_))
          } else {
            Right(None)
          }
        nonMaintainerSignatories <- toPartySet(msg.getNonMaintainerSignatoriesList)
        nonSignatoryStakeholdersList <- toPartySet(msg.getNonSignatoryStakeholdersList)
        actingParties <- toPartySet(msg.getActorsList)
        keyOpt <-
          if (msg.hasKeyWithMaintainers)
            decodeKeyWithMaintainers(
              nodeVersion,
              templateId,
              pkgName,
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
              nodeVersion >= SerializationVersion.minContractKeys,
              true,
              DecodeError(s"transaction key is not supported by version $nodeVersion"),
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
        interfaceId = interfaceId,
      )
    }

    private[this] def decodeExercise(
        txVersion: SerializationVersion,
        nodeVersionStr: String,
        msg: TransactionOuterClass.Node.Exercise,
    ): Either[DecodeError, Node.Exercise] = {
      for {
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
          else if (nodeVersion < SerializationVersion.minChoiceAuthorizers)
            Left(DecodeError(s"Exercise Authorizer not supported by version $nodeVersion"))
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
        txVersion: SerializationVersion,
        nodeVersionStr: String,
        msg: TransactionOuterClass.Node.LookupByKey,
    ) =
      for {
        nodeVersion <- decodeActionNodeVersion(txVersion, nodeVersionStr)
        _ <- Either.cond(
          txVersion >= SerializationVersion.minContractKeys,
          (),
          DecodeError(s"Contract ket not supported by version $nodeVersion"),
        )
        pkgName <- decodePackageName(msg.getPackageName)
        templateId <- ValueCoder.decodeIdentifier(msg.getTemplateId)
        key <-
          decodeKeyWithMaintainers(
            nodeVersion,
            templateId,
            pkgName,
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

    private[transaction] def encodeTransaction(
        transaction: VersionedTransaction
    ): Either[EncodeError, TransactionOuterClass.Transaction] = {
      val builder = TransactionOuterClass.Transaction
        .newBuilder()
        .setVersion(SerializationVersion.toProtoValue(transaction.version))
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
        txVersion: SerializationVersion,
        nodeVersionStr: String,
    ): Either[DecodeError, SerializationVersion] =
      for {
        nodeVersion <- decodeVersion(nodeVersionStr)
        _ <- Either.cond(
          nodeVersion <= txVersion,
          (),
          DecodeError(
            s"A transaction of version $txVersion cannot contain node of newer version (version $nodeVersion)"
          ),
        )
      } yield nodeVersion

    def decodeVersion(vs: String): Either[DecodeError, SerializationVersion] =
      SerializationVersion.fromString(vs).left.map(DecodeError)

    def decodeTransaction(
        protoTx: TransactionOuterClass.Transaction
    ): Either[DecodeError, VersionedTransaction] =
      for {
        version <- decodeVersion(protoTx.getVersion)
        tx <- decodeTransaction(version, protoTx)
      } yield tx

    private[this] def decodeTransaction(
        txVersion: SerializationVersion,
        msg: TransactionOuterClass.Transaction,
    ): Either[DecodeError, VersionedTransaction] = for {
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

    // Similar to toPartySet but
    // - requires strList to be strictly ordered, fails otherwise
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
        version: SerializationVersion,
        payload: ByteString,
    ): ByteString = {
      val builder = TransactionOuterClass.Versioned.newBuilder()
      discard(builder.setVersion(SerializationVersion.toProtoValue(version)))
      discard(builder.setPayload(payload))
      builder.build().toByteString
    }

    private[TransactionCoder] def parseVersioned(bytes: ByteString) =
      try {
        val msg = TransactionOuterClass.Versioned.parseFrom(bytes)
        ensuresNoUnknownFields(msg).map(_ => msg)
      } catch {
        case scala.util.control.NonFatal(e) =>
          Left(DecodeError(s"exception $e while decoding the versioned object"))
      }

    private[TransactionCoder] def decodeVersioned(
        bytes: ByteString
    ): Either[DecodeError, Versioned[ByteString]] =
      for {
        proto <- parseVersioned(bytes)
        version <- SerializationVersion.fromString(proto.getVersion).left.map(DecodeError)
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
        discard(builder.setCreatedAt(CreationTime.encode(createdAt)))
        discard(builder.setAuthenticationData(authenticationData.toByteString))
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
        txVersion: SerializationVersion,
        msg: TransactionOuterClass.FatContractInstance,
    ): Either[DecodeError, FatContractInstance] =
      for {
        contractId <- ValueCoder.decodeCoid(msg.getContractId)
        pkgName <- decodePackageName(msg.getPackageName)
        templateId <- ValueCoder.decodeIdentifier(msg.getTemplateId)
        createArg <- ValueCoder.decodeValue(version = txVersion, bytes = msg.getCreateArg)
        keyWithMaintainers <-
          if (msg.hasContractKeyWithMaintainers)
            strictDecodeKeyWithMaintainers(
              txVersion,
              templateId,
              pkgName,
              msg.getContractKeyWithMaintainers,
            )
              .map(Some(_))
          else
            RightNone
        maintainers = keyWithMaintainers.fold(TreeSet.empty[Party])(k =>
          TreeSet.from(k.maintainers)
        )
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
        createdAt <- CreationTime.decode(msg.getCreatedAt).left.map(DecodeError)
        authenticationData = msg.getAuthenticationData
      } yield FatContractInstanceImpl(
        version = txVersion,
        contractId = contractId,
        packageName = pkgName,
        templateId = templateId,
        createArg = createArg,
        signatories = signatories,
        stakeholders = stakeholders,
        createdAt = createdAt,
        contractKeyWithMaintainers = keyWithMaintainers,
        authenticationData = data.Bytes.fromByteString(authenticationData),
      )

    private def parseFatContractInstance(bytes: ByteString) =
      try {
        val msg = TransactionOuterClass.FatContractInstance.parseFrom(bytes)
        ensuresNoUnknownFields(msg).map(_ => msg)
      } catch {
        case scala.util.control.NonFatal(e) =>
          Left(DecodeError(s"exception $e while decoding the object"))
      }

    @nowarn(
      "cat=unused-pat-vars"
    ) // suppress wrong warnings that version and unversioned are unused
    def decodeFatContractInstance(bytes: ByteString): Either[DecodeError, FatContractInstance] =
      for {
        versionedBlob <- decodeVersioned(bytes)
        Versioned(version, unversioned) = versionedBlob
        proto <- parseFatContractInstance(unversioned)
        fatContractInstance <- decodeFatContractInstance(version, proto)
      } yield fatContractInstance

  }
}
