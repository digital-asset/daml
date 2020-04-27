// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.lf.data.{BackStack, Ref}
import com.daml.lf.transaction.TransactionOuterClass.Node.NodeTypeCase
import com.daml.lf.data.Ref.{Name, Party}
import com.daml.lf.transaction.Node._
import VersionTimeline.Implicits._
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, VersionedValue}
import com.daml.lf.value.{ValueCoder, ValueOuterClass, ValueVersion}
import com.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.google.protobuf.ProtocolStringList

import scala.collection.JavaConverters._
import scalaz.syntax.std.boolean._
import scalaz.syntax.traverse.ToTraverseOps
import scalaz.std.either.eitherMonad
import scalaz.std.option._

import scala.collection.immutable.HashMap

object TransactionCoder {

  abstract class EncodeNid[-Nid] private[lf] {
    def asString(id: Nid): String
  }
  abstract class DecodeNid[+Nid] private[lf] {
    def fromString(s: String): Either[DecodeError, Nid]
  }

  val NidEncoder: EncodeNid[Value.NodeId] = new EncodeNid[Value.NodeId] {
    override def asString(id: Value.NodeId): String = id.index.toString
  }

  val NidDecoder: DecodeNid[Value.NodeId] = new DecodeNid[Value.NodeId] {
    override def fromString(s: String): Either[DecodeError, Value.NodeId] =
      scalaz.std.string
        .parseInt(s)
        .fold(_ => Left(DecodeError(s"cannot parse node Id $s")), idx => Right(Value.NodeId(idx)))
  }

  val EventIdEncoder: EncodeNid[Ref.LedgerString] = new EncodeNid[Ref.LedgerString] {
    override def asString(id: Ref.LedgerString): String = id
  }

  val EventIdDecoder: DecodeNid[Ref.LedgerString] = new DecodeNid[Ref.LedgerString] {
    override def fromString(s: String): Either[DecodeError, Ref.LedgerString] =
      Ref.LedgerString
        .fromString(s)
        .left
        .map(_ => DecodeError(s"cannot decode noid: $s"))
  }

  def encodeValue[Cid](
      cidEncoder: ValueCoder.EncodeCid[Cid],
      value: VersionedValue[Cid],
  ): Either[EncodeError, (ValueVersion, ValueOuterClass.VersionedValue)] =
    ValueCoder.encodeVersionedValueWithCustomVersion(cidEncoder, value).map((value.version, _))

  def decodeValue[Cid](
      cidDecoder: ValueCoder.DecodeCid[Cid],
      value: ValueOuterClass.VersionedValue,
  ): Either[DecodeError, Value.VersionedValue[Cid]] =
    ValueCoder.decodeVersionedValue(cidDecoder, value)

  private val valueVersion1Only: Set[TransactionVersion] = Set("1") map TransactionVersion

  /**
    * Encodes a contract instance with the help of the contractId encoding function
    * @param coinst the contract instance to be encoded
    * @param encodeCid function to encode a cid to protobuf
    * @return protobuf wire format contract instance
    */
  def encodeContractInstance[Cid](
      encodeCid: ValueCoder.EncodeCid[Cid],
      coinst: Value.ContractInst[Value.VersionedValue[Cid]],
  ): Either[EncodeError, TransactionOuterClass.ContractInstance] =
    encodeValue(encodeCid, coinst.arg).map {
      case (vversion, arg) =>
        val (_, id) = ValueCoder.encodeIdentifier(coinst.template, Some(vversion))
        TransactionOuterClass.ContractInstance
          .newBuilder()
          .setTemplateId(id)
          .setValue(arg)
          .setAgreement(coinst.agreementText)
          .build()
    }

  /**
    * Decode a contract instance from wire format
    * @param protoCoinst protocol buffer encoded contract instance
    * @param decodeCid cid decoding function
    * @return contract instance value
    */
  def decodeContractInstance[Cid](
      decodeCid: ValueCoder.DecodeCid[Cid],
      protoCoinst: TransactionOuterClass.ContractInstance,
  ): Either[DecodeError, Value.ContractInst[Value.VersionedValue[Cid]]] =
    for {
      id <- ValueCoder.decodeIdentifier(protoCoinst.getTemplateId)
      value <- decodeValue(decodeCid, protoCoinst.getValue)
    } yield Value.ContractInst(id, value, (protoCoinst.getAgreement))

  private def encodeKeyWithMaintainers[Cid](
      encodeCid: ValueCoder.EncodeCid[Cid],
      key: KeyWithMaintainers[Value.VersionedValue[Cid]],
  ): Either[EncodeError, (ValueVersion, TransactionOuterClass.KeyWithMaintainers)] =
    encodeValue(encodeCid, key.key).map {
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

  /**
    * encodes a [[GenNode[Nid, Cid]] to protocol buffer
    * @param nodeId node id of the node to be encoded
    * @param node the node to be encoded
    * @param encodeNid node id encoding to string
    * @param encodeCid contract id encoding to string
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return protocol buffer format node
    */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def encodeNode[Nid, Cid](
      encodeNid: EncodeNid[Nid],
      encodeCid: ValueCoder.EncodeCid[Cid],
      transactionVersion: TransactionVersion,
      nodeId: Nid,
      node: GenNode[Nid, Cid, Value.VersionedValue[Cid]],
  ): Either[EncodeError, TransactionOuterClass.Node] = {
    val nodeBuilder = TransactionOuterClass.Node.newBuilder().setNodeId(encodeNid.asString(nodeId))
    import TransactionVersions.{
      minKeyOrLookupByKey,
      minNoControllers,
      minExerciseResult,
      minContractKeyInExercise,
      minMaintainersInExercise,
      minContractKeyInFetch,
    }
    node match {
      case nc @ NodeCreate(_, _, _, _, _, _) =>
        val createBuilder =
          TransactionOuterClass.NodeCreate
            .newBuilder()
            .addAllStakeholders(nc.stakeholders.toSet[String].asJava)
            .addAllSignatories(nc.signatories.toSet[String].asJava)

        for {
          encodedCid <- encodeCid.encode(transactionVersion, nc.coid)
          inst <- encodeContractInstance(encodeCid, nc.coinst)
          optKey <- nc.key match {
            case None => Right(None)
            case Some(key) =>
              if (transactionVersion precedes minKeyOrLookupByKey)
                Left(EncodeError(transactionVersion, isTooOldFor = "NodeCreate's `key` field"))
              else
                encodeKeyWithMaintainers(encodeCid, key).map(Some(_))
          }

        } yield {
          encodedCid.fold(createBuilder.setContractId, createBuilder.setContractIdStruct)
          createBuilder.setContractInstance(inst)
          optKey.foreach {
            case (_, encodedKey) => createBuilder.setKeyWithMaintainers(encodedKey)
          }
          nodeBuilder.setCreate(createBuilder).build()
        }

      case nf @ NodeFetch(_, _, _, _, _, _, _) =>
        val (vversion, etid) = ValueCoder.encodeIdentifier(
          nf.templateId,
          valueVersion1Only(transactionVersion) option ValueVersion("1"),
        )
        val fetchBuilder = TransactionOuterClass.NodeFetch
          .newBuilder()
          .setTemplateId(etid)
          .setValueVersion(vversion.protoValue)
          .addAllStakeholders(nf.stakeholders.toSet[String].asJava)
          .addAllSignatories(nf.signatories.toSet[String].asJava)

        for {
          encodedCid <- encodeCid.encode(transactionVersion, nf.coid)
          actors <- Either.cond(
            nf.actingParties.isEmpty || !(transactionVersion precedes TransactionVersions.minFetchActors),
            nf.actingParties.getOrElse(Set.empty),
            EncodeError(transactionVersion, isTooOldFor = "NodeFetch actors")
          )
          optKey <- nf.key match {
            case None => Right(None)
            case Some(key) =>
              if (transactionVersion precedes minContractKeyInFetch)
                Left(EncodeError(transactionVersion, isTooOldFor = "NodeFetch's `key` field"))
              else
                encodeKeyWithMaintainers(encodeCid, key).map(Some(_))
          }
        } yield {
          encodedCid.fold(fetchBuilder.setContractId, fetchBuilder.setContractIdStruct)
          actors.foreach(fetchBuilder.addActors)
          optKey.foreach {
            case (_, encodedKey) => fetchBuilder.setKeyWithMaintainers(encodedKey)
          }
          nodeBuilder.setFetch(fetchBuilder).build()
        }

      case ne @ NodeExercises(_, _, _, _, _, _, _, _, _, _, _, _, _) =>
        for {
          argValue <- encodeValue(encodeCid, ne.chosenValue)
          (vversion, arg) = argValue
          retValue <- ne.exerciseResult traverseU (v => encodeValue(encodeCid, v))
          exBuilder = TransactionOuterClass.NodeExercise
            .newBuilder()
            .setChoice(ne.choiceId)
            .setTemplateId(ValueCoder.encodeIdentifier(ne.templateId, Some(vversion))._2)
            .setChosenValue(arg)
            .setConsuming(ne.consuming)
            .addAllActors(ne.actingParties.toSet[String].asJava)
            .addAllChildren(ne.children.map(encodeNid.asString).toList.asJava)
            .addAllSignatories(ne.signatories.toSet[String].asJava)
            .addAllStakeholders(ne.stakeholders.toSet[String].asJava)
          encodedCid <- encodeCid.encode(transactionVersion, ne.targetCoid)
          controllers <- if (transactionVersion precedes minNoControllers)
            Either.cond(
              ne.controllers == ne.actingParties,
              ne.controllers,
              EncodeError(
                s"As of version $minNoControllers, the controllers and actingParties of an exercise node _must_ be the same, but I got ${ne.controllers} as controllers and ${ne.actingParties} as actingParties.",
              )
            )
          else
            Right(Set.empty)
          result <- if (transactionVersion precedes minExerciseResult)
            Right(None)
          else
            Either.cond(
              test = retValue.nonEmpty,
              right = retValue,
              left = EncodeError(
                s"Trying to encode transaction of version $transactionVersion, which requires the exercise return value, but did not get exercise return value in node.",
              )
            )
          _ <- Right(
            ne.key
              .map { kWithM =>
                if (transactionVersion precedes minContractKeyInExercise) ()
                else if (transactionVersion precedes minMaintainersInExercise) {
                  encodeValue(encodeCid, kWithM.key).map { encodedKey =>
                    exBuilder.setContractKey(encodedKey._2)
                  }
                } else
                  encodeKeyWithMaintainers(encodeCid, kWithM).map {
                    case (_, encodedKey) =>
                      exBuilder.setKeyWithMaintainers(encodedKey)
                  }
                ()
              }
              .getOrElse(()),
          )
        } yield {
          encodedCid.fold(exBuilder.setContractId, exBuilder.setContractIdStruct)
          controllers.foreach(exBuilder.addControllers)
          result.foreach { case (_, v) => exBuilder.setReturnValue(v) }
          nodeBuilder.setExercise(exBuilder).build()
        }

      case nlbk @ NodeLookupByKey(_, _, _, _) =>
        val nlbkBuilder = TransactionOuterClass.NodeLookupByKey.newBuilder()
        for {
          _ <- Either.cond(
            test = !(transactionVersion precedes minKeyOrLookupByKey),
            right = (),
            left =
              EncodeError(transactionVersion, isTooOldFor = "NodeLookupByKey transaction nodes")
          )
          versionAndKey <- encodeKeyWithMaintainers(encodeCid, nlbk.key)
          encodedCid <- nlbk.result traverseU (cid => encodeCid.encode(transactionVersion, cid))
        } yield {
          val (vversion, key) = versionAndKey
          nlbkBuilder
            .setTemplateId(ValueCoder.encodeIdentifier(nlbk.templateId, Some(vversion))._2)
            .setKeyWithMaintainers(key)
          encodedCid.foreach(_.fold(nlbkBuilder.setContractId, nlbkBuilder.setContractIdStruct))
          nodeBuilder.setLookupByKey(nlbkBuilder).build()
        }
    }
  }
  private def decodeKeyWithMaintainers[Cid](
      decodeCid: ValueCoder.DecodeCid[Cid],
      keyWithMaintainers: TransactionOuterClass.KeyWithMaintainers,
  ): Either[DecodeError, KeyWithMaintainers[Value.VersionedValue[Cid]]] =
    for {
      maintainers <- toPartySet(keyWithMaintainers.getMaintainersList)
      key <- decodeValue(decodeCid, keyWithMaintainers.getKey)
    } yield KeyWithMaintainers(key, maintainers)

  /**
    * read a [[GenNode[Nid, Cid]] from protobuf
    * @param protoNode protobuf encoded node
    * @param decodeNid function to read node id from String
    * @param decodeCid function to read contract id from String
    * @tparam Nid Node id type
    * @tparam Cid Contract id type
    * @return decoded GenNode
    */
  def decodeNode[Nid, Cid](
      decodeNid: DecodeNid[Nid],
      decodeCid: ValueCoder.DecodeCid[Cid],
      txVersion: TransactionVersion,
      protoNode: TransactionOuterClass.Node,
  ): Either[DecodeError, (Nid, GenNode[Nid, Cid, Value.VersionedValue[Cid]])] = {
    val nodeId = decodeNid.fromString(protoNode.getNodeId)

    import TransactionVersions.{
      minKeyOrLookupByKey,
      minNoControllers,
      minExerciseResult,
      minContractKeyInExercise,
      minMaintainersInExercise,
      minContractKeyInFetch,
    }
    protoNode.getNodeTypeCase match {
      case NodeTypeCase.CREATE =>
        for {
          ni <- nodeId
          protoCreate = protoNode.getCreate
          c <- decodeCid.decode(
            txVersion,
            protoCreate.getContractId,
            protoCreate.getContractIdStruct,
          )
          ci <- decodeContractInstance(decodeCid, protoCreate.getContractInstance)
          stakeholders <- toPartySet(protoCreate.getStakeholdersList)
          signatories <- toPartySet(protoCreate.getSignatoriesList)
          key <- if (protoCreate.getKeyWithMaintainers == TransactionOuterClass.KeyWithMaintainers.getDefaultInstance)
            Right(None)
          else if (txVersion precedes minKeyOrLookupByKey)
            Left(DecodeError(s"$txVersion is too old to support NodeCreate's `key` field"))
          else decodeKeyWithMaintainers(decodeCid, protoCreate.getKeyWithMaintainers).map(Some(_))
        } yield (ni, NodeCreate(c, ci, None, signatories, stakeholders, key))
      case NodeTypeCase.FETCH =>
        val protoFetch = protoNode.getFetch
        for {
          ni <- nodeId
          templateId <- ValueCoder.decodeIdentifier(protoFetch.getTemplateId)
          c <- decodeCid.decode(txVersion, protoFetch.getContractId, protoFetch.getContractIdStruct)
          actingPartiesSet <- toPartySet(protoFetch.getActorsList)
          _ <- if ((txVersion precedes TransactionVersions.minFetchActors) && actingPartiesSet.nonEmpty)
            Left(DecodeError(txVersion, isTooOldFor = "NodeFetch actors"))
          else Right(())
          actingParties <- if (txVersion precedes TransactionVersions.minFetchActors) Right(None)
          else Right(Some(actingPartiesSet))
          stakeholders <- toPartySet(protoFetch.getStakeholdersList)
          signatories <- toPartySet(protoFetch.getSignatoriesList)
          key <- if (protoFetch.getKeyWithMaintainers == TransactionOuterClass.KeyWithMaintainers.getDefaultInstance)
            Right(None)
          else if (txVersion precedes minContractKeyInFetch)
            Left(DecodeError(s"$txVersion is too old to support NodeFetch's `key` field"))
          else decodeKeyWithMaintainers(decodeCid, protoFetch.getKeyWithMaintainers).map(Some(_))
        } yield (ni, NodeFetch(c, templateId, None, actingParties, signatories, stakeholders, key))

      case NodeTypeCase.EXERCISE =>
        val protoExe = protoNode.getExercise
        val childrenOrError = protoExe.getChildrenList.asScala
          .foldLeft[Either[DecodeError, BackStack[Nid]]](Right(BackStack.empty[Nid])) {
            case (Left(e), _) => Left(e)
            case (Right(ids), s) => decodeNid.fromString(s).map(ids :+ _)
          }
          .map(_.toImmArray)

        for {
          rv <- if (txVersion precedes minExerciseResult) {
            if (protoExe.hasReturnValue)
              Left(DecodeError(txVersion, isTooOldFor = "exercise result"))
            else Right(None)
          } else decodeValue(decodeCid, protoExe.getReturnValue).map(Some(_))
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
              decodeValue(decodeCid, protoExe.getContractKey)
                .map(k => Some(KeyWithMaintainers(k, Set.empty)))
          } else if (hasKeyWithMaintainersField) {
            if (txVersion precedes minMaintainersInExercise)
              Left(DecodeError(txVersion, isTooOldFor = "NodeExercises maintainers"))
            else
              decodeKeyWithMaintainers(decodeCid, protoExe.getKeyWithMaintainers).map(k => Some(k))
          } else Right(None)

          ni <- nodeId
          targetCoid <- decodeCid.decode(
            txVersion,
            protoExe.getContractId,
            protoExe.getContractIdStruct,
          )
          children <- childrenOrError
          cv <- decodeValue(decodeCid, protoExe.getChosenValue)
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
          key <- decodeKeyWithMaintainers(decodeCid, protoLookupByKey.getKeyWithMaintainers)
          cid <- decodeCid.decodeOptional(
            txVersion,
            protoLookupByKey.getContractId,
            protoLookupByKey.getContractIdStruct,
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
  def encodeTransaction[Nid, Cid <: ContractId](
      encodeNid: EncodeNid[Nid],
      encodeCid: ValueCoder.EncodeCid[Cid],
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
  private[transaction] def encodeTransactionWithCustomVersion[Nid, Cid](
      encodeNid: EncodeNid[Nid],
      encodeCid: ValueCoder.EncodeCid[Cid],
      transaction: VersionedTransaction[Nid, Cid],
  ): Either[EncodeError, TransactionOuterClass.Transaction] = {
    val tx = transaction.transaction
    val txVersion: TransactionVersion = transaction.version
    val builder = tx
      .fold[Either[EncodeError, TransactionOuterClass.Transaction.Builder]](
        Right(TransactionOuterClass.Transaction.newBuilder()),
      ) {
        case (builderOrError, (id, node)) =>
          for {
            builder <- builderOrError
            encodedNode <- encodeNode(
              encodeNid,
              encodeCid,
              txVersion,
              id,
              node,
            )
          } yield builder.addNodes(encodedNode)
      }
    builder.map(b => {
      b.setVersion(transaction.version.protoValue)
        .addAllRoots(tx.roots.map(encodeNid.asString).toSeq.asJava)
        .build()
    })
  }

  def decodeVersion(vs: String): Either[DecodeError, TransactionVersion] =
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
  def decodeVersionedTransaction[Nid, Cid](
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
    } yield VersionedTransaction(version, tx)

  /**
    * Reads a [[GenTransaction[Nid, Cid]]] from protobuf. Does not check if
    * [[TransactionVersion]] passed in the protobuf is currently supported, if you need this check use
    * [[TransactionCoder.decodeVersionedTransaction]].
    *
    * @param protoTx protobuf encoded transaction
    * @param decodeNid node id decoding function
    * @param decodeCid contract id decoding function
    * @tparam Nid node id type
    * @tparam Cid contract id type
    * @return  decoded transaction
    */
  private def decodeTransaction[Nid, Cid](
      decodeNid: DecodeNid[Nid],
      decodeCid: ValueCoder.DecodeCid[Cid],
      txVersion: TransactionVersion,
      protoTx: TransactionOuterClass.Transaction,
  ): Either[DecodeError, GenTransaction[Nid, Cid, Value.VersionedValue[Cid]]] = {
    val roots = protoTx.getRootsList.asScala
      .foldLeft[Either[DecodeError, BackStack[Nid]]](Right(BackStack.empty[Nid])) {
        case (Right(acc), s) => decodeNid.fromString(s).map(acc :+ _)
        case (Left(e), _) => Left(e)
      }
      .map(_.toImmArray)

    val nodes = protoTx.getNodesList.asScala
      .foldLeft[Either[DecodeError, HashMap[Nid, GenNode[Nid, Cid, Value.VersionedValue[Cid]]]]](
        Right(HashMap.empty)) {
        case (Left(e), _) => Left(e)
        case (Right(acc), s) =>
          decodeNode(decodeNid, decodeCid, txVersion, s).map(acc + _)
      }

    for {
      rs <- roots
      ns <- nodes
    } yield GenTransaction(ns, rs)
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
          _ <- if (txVersion precedes TransactionVersions.minFetchActors)
            Left(DecodeError(txVersion, isTooOldFor = "NodeFetch actors"))
          else Right(())
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
          _ <- if (txVersion precedes TransactionVersions.minKeyOrLookupByKey)
            Left(DecodeError(txVersion, isTooOldFor = "NodeLookupByKey"))
          else Right(())
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
