package com.daml.ledger.participant.state.kvutils

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party, SimpleString}
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.{Engine, Error => EngineError}
import com.digitalasset.daml.lf.lfpackage.Decode
import com.digitalasset.daml.lf.transaction.Node.{
  NodeCreate,
  NodeExercises,
  NodeFetch,
  NodeLookupByKey
}
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractId,
  ContractInst,
  NodeId,
  RelativeContractId,
  VersionedValue
}
import com.digitalasset.daml.lf.value.ValueCoder.DecodeError
import com.digitalasset.daml.lf.value.ValueOuterClass
import com.digitalasset.daml_lf.DamlLf.Archive
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString

import scala.util.Try

/**
  * Utilities for implementing participant state on top of a key-value based ledger.
  * Implements helpers and serialization/deserialization into protocol buffer messages
  * defined in daml_kvutils.proto.
  */
object KeyValueUtils {

  /** Entry identifiers are opaque strings chosen by the implementation that provide a unique
    * reference to the entry. This may be the database key used to store the entry, or a combination
    * of a key and offset within a batch. Conceptually entry identifier should have 1-1 mapping into
    * participant-state [[Offset]] as the implementation using these utilities is expected to produce the
    * offset for each entry.
    *
    * When used as transactionId they're rendered in hexadecimal.
    */
  type KVEntryId = ByteString

  /** A relative contract identifier is a reference to the index of the create node. */
  type KVRelativeContractId = Int

  /** Absolute Contract Identifiers are references to a transaction node. */
  type KVAbsoluteContractId = (KVEntryId, KVRelativeContractId)

  /** A contract instance along with its ledger effective time from which it is active. */
  type StampedContractInstance = (Timestamp, ContractInst[Transaction.Value[AbsoluteContractId]])

  /** KVEntry is the serialized 'DamlKVEntry', containing either the submitted transaction and related metadata
    * or the DAML-LF archive. */
  type KVEntryBlob = ByteString

  /** The inputs to the transaction. */
  final case class Inputs(
      /* The contracts referenced in the transaction.
       * When committing the activeness of the referenced contracts at the ledger effective time
       * of the transaction must be validated.
       */
      contracts: List[KVAbsoluteContractId],
      /* FIXME(JM): Contract key inputs */

      /* The DAML-LF packages referenced in the transaction. */
      // FIXME(JM): We need this information from DAML Engine!
      packages: List[PackageId]
  )

  /** The effects of the transaction, that is what contracts
    * were consumed and created, and what contract keys were updated.
    */
  case class Effects(
      /** The contracts consumed by this transaction.
        * When committing the transaction these contracts must be marked consumed.
        * A contract should be marked consumed when the transaction is committed,
        * regardless of the ledger effective time of the transaction (e.g. a transaction
        * with an earlier ledger effective time that gets committed later would find the
        * contract inactive).
        */
      consumedContracts: List[KVAbsoluteContractId],
      /** The contracts created by this transaction.
        * When the transaction is committed, keys marking the activeness of these
        * contracts should be created. The key should be a combination of the transaction
        * id and the relative contract id (that is, the node index).
        */
      createdContracts: List[KVRelativeContractId]

      // FIXME(JM): updated contract keys
  )

  /** Transaction information required for validation, consistency checks
    * and for updating contract activeness. */
  case class TxInfo(
      /** The party that submitted the transaction. */
      submitter: Party,
      /** The ledger effective time of the transaction. The activeness of the declared inputs should
        * be checked in relation to this (except if archived, see comment near [[Effects.consumedContracts]]).
        */
      ledgerEffectiveTime: Timestamp,
      /** The inputs to the transaction. */
      inputs: Inputs,
      /** The effects of the transaction: the contracts consumed and created. */
      effects: Effects,
      /** The decoded transaction. */
      tx: SubmittedTransaction,
  )

  /** Given a KVTransaction and a contract id, deserialize the transaction and
    * and produce the absolute contract instance.
    *
    * FIXME(JM): We should cache some of the work to deserialize the transactions.
    */
  def lookupStampedContractInstance(
      entryId: KVEntryId,
      entry: KVEntryBlob,
      coid: KVAbsoluteContractId): Option[StampedContractInstance] = {
    val decodedKvTx = decodeKVTransaction(entry)
    val relTx = TransactionCoding.decodeTransaction(decodedKvTx.getTransaction)
    relTx.nodes
      .get(NodeId.unsafeFromIndex(coid._2))
      .flatMap { (node: Transaction.Node) =>
        node match {
          case create: NodeCreate[ContractId, VersionedValue[ContractId]] =>
            Some(
              parseTimestamp(decodedKvTx.getLedgerEffectiveTime) ->
                create.coinst.mapValue(
                  _.mapContractId(toAbsCoid(entryId, _))
                )
            )
          case _ =>
            // TODO(JM): Logging?
            None
        }
      }
  }

  /** Transform the submitted transaction into entry blob. */
  def transactionToEntry(
      submitterInfo: SubmitterInfo,
      meta: TransactionMeta,
      tx: SubmittedTransaction): KVEntryBlob = {
    DamlKVEntry.newBuilder
      .setTransaction(
        DamlKVTransaction.newBuilder
          .setTransaction(TransactionCoding.encodeTransaction(tx))
          .setCommandId(submitterInfo.commandId)
          .setSubmitter(submitterInfo.submitter.underlyingString)
          .setApplicationId(submitterInfo.applicationId)
          .setLedgerEffectiveTime(buildTimestamp(meta.ledgerEffectiveTime))
          .setMaximumRecordTime(buildTimestamp(submitterInfo.maxRecordTime))
          .setWorkflowId(meta.workflowId)
          .build
      )
      .build
      .toByteString
  }

  def archiveToEntry(archive: Archive): KVEntryBlob = {
    DamlKVEntry.newBuilder
      .setArchive(
        DamlKVArchive.newBuilder
          .setPackageId(archive.getHash)
          .setArchiveBytes(archive.toByteString)
      )
      .build
      .toByteString
  }

  /** Convert the entry blob into a participant state [[Update]].
    * The caller is expected to provide the record time of the batch into which the entry
    * was committed and to produce the accompanying [[Offset]] from the entryId.
    *
    * @param entryId: The transaction identifier assigned to the transaction.
    * @param entry: The entry blob.
    * @param recordTime: The record time of the batch into which the entry was committed.
    */
  def entryToUpdate(entryId: KVEntryId, entry: KVEntryBlob, recordTime: Timestamp): Update = {
    val parsedEntry = DamlKVEntry.parseFrom(entry)
    parsedEntry.getPayloadCase match {
      case DamlKVEntry.PayloadCase.ARCHIVE =>
        makePublicPackageUploaded(parsedEntry.getArchive)

      case DamlKVEntry.PayloadCase.TRANSACTION =>
        makeTransactionAccepted(entryId, parsedEntry.getTransaction, recordTime)

      case x =>
        throw new RuntimeException("entryToUpdate: Unknown payload case: $x")
    }
  }

  private def makePublicPackageUploaded(archive: DamlKVArchive): Update.PublicPackageUploaded =
    Update.PublicPackageUploaded(
      Archive.parseFrom(archive.getArchiveBytes)
    )

  /** Transform the DamlKVTransaction into the [[Update.TransactionAccepted]] event. */
  private def makeTransactionAccepted(
      entryId: KVEntryId,
      kvTx: DamlKVTransaction,
      recordTime: Timestamp): Update.TransactionAccepted = {
    val relTx = TransactionCoding.decodeTransaction(kvTx.getTransaction)
    val hexTxId = BaseEncoding.base16.encode(entryId.toByteArray)

    Update.TransactionAccepted(
      optSubmitterInfo = Some(
        SubmitterInfo(
          submitter = SimpleString.assertFromString(kvTx.getSubmitter),
          applicationId = kvTx.getApplicationId,
          commandId = kvTx.getCommandId,
          maxRecordTime = parseTimestamp(kvTx.getMaximumRecordTime),
        )),
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = parseTimestamp(kvTx.getLedgerEffectiveTime),
        workflowId = kvTx.getWorkflowId,
      ),
      transaction = makeCommittedTransaction(entryId, relTx),
      transactionId = hexTxId,
      recordTime = recordTime,
      referencedContracts = List.empty // TODO(JM): rename this to additionalContracts. Always empty here.
    )
  }

  /** Compute the transaction information from the KVTransaction. */
  def computeTxInfo(entry: KVEntryBlob): TxInfo = {
    val decodedKvTx = decodeKVTransaction(entry)
    val relTx = TransactionCoding.decodeTransaction(decodedKvTx.getTransaction)
    val (inputs, effects) = computeInputsAndEffects(relTx)

    TxInfo(
      submitter = SimpleString.assertFromString(decodedKvTx.getSubmitter),
      ledgerEffectiveTime = parseTimestamp(decodedKvTx.getLedgerEffectiveTime),
      inputs = inputs,
      effects = effects,
      tx = relTx,
    )
  }

  // ------------------------------------------------------

  private def decodeKVTransaction(blob: KVEntryBlob): DamlKVTransaction =
    // FIXME(JM): throws a protobuf exception if this wasn't a transaction. we might want something more specific?
    DamlKVEntry.parser.parseFrom(blob.newCodedInput).getTransaction

  private def computeInputsAndEffects(tx: SubmittedTransaction): (Inputs, Effects) = {
    // FIXME(JM): Get referenced packages from the transaction (once they're added to it)
    def addInput(inputs: Inputs, coid: ContractId): Inputs =
      coid match {
        case acoid: AbsoluteContractId =>
          inputs.copy(
            contracts = decodeAbsoluteContractId(acoid) :: inputs.contracts
          )
        case _ =>
          inputs
      }

    tx.fold(
      GenTransaction.TopDown,
      (Inputs(List.empty, List.empty), Effects(List.empty, List.empty))) {
      case ((inputs, effects), (nodeId, node)) =>
        node match {
          case fetch: NodeFetch[ContractId] =>
            (addInput(inputs, fetch.coid), effects)
          case create: NodeCreate[_, _] =>
            (
              inputs,
              effects.copy(
                createdContracts = create.coid
                  .asInstanceOf[RelativeContractId]
                  .txnid
                  .index :: effects.createdContracts
              ))
          case exe: NodeExercises[_, ContractId, _] =>
            (
              addInput(inputs, exe.targetCoid),
              if (exe.consuming) {
                exe.targetCoid match {
                  case acoid: AbsoluteContractId =>
                    effects.copy(
                      consumedContracts = decodeAbsoluteContractId(acoid) :: effects.consumedContracts
                    )
                  case _ =>
                    effects
                }
              } else {
                effects
              }
            )
          case l: NodeLookupByKey[_, _] =>
            // FIXME(JM): track fetched keys
            (inputs, effects)
        }
    }
  }

  private def toAbsCoid(txId: KVEntryId, coid: ContractId): AbsoluteContractId = {
    val hexTxId =
      BaseEncoding.base16.encode(txId.toByteArray)
    coid match {
      case a @ AbsoluteContractId(_) => a
      case RelativeContractId(txnid) =>
        // NOTE(JM): Must match with decodeAbsoluteContractId
        AbsoluteContractId(s"$hexTxId:${txnid.index}")
    }
  }

  private def makeCommittedTransaction(
      txId: KVEntryId,
      tx: SubmittedTransaction): CommittedTransaction = {
    tx
    /* Assign absolute contract ids */
      .mapContractIdAndValue(
        toAbsCoid(txId, _),
        _.mapContractId(toAbsCoid(txId, _))
      )
  }

  private def decodeAbsoluteContractId(acoid: AbsoluteContractId): KVAbsoluteContractId =
    acoid.coid.split(':').toList match {
      case hexTxId :: nodeId :: Nil =>
        (ByteString.copyFrom(BaseEncoding.base16().decode(hexTxId)), nodeId.toInt)
      case _ => sys.error(s"decodeAbsoluteContractId: Cannot decode '$acoid'")
    }

  private def buildTimestamp(ts: Time.Timestamp): com.google.protobuf.Timestamp = {
    val instant = ts.toInstant
    com.google.protobuf.Timestamp.newBuilder
      .setSeconds(instant.getEpochSecond)
      .setNanos(instant.getNano)
      .build
  }

  private def parseTimestamp(ts: com.google.protobuf.Timestamp): Time.Timestamp =
    Time.Timestamp.assertFromInstant(Instant.ofEpochSecond(ts.getSeconds, ts.getNanos.toLong))

  // ------------------------------------------
  // Entry validation.
  // TODO:
  // - given an entry blob, deserializes it and runs the DAML engine validation on the transaction.
  // - DAML engine must be able to load packages as needed, hence that functionality must be provided by the
  //   caller.

  def validateTransaction(
      engine: Engine,
      // The packages used as inputs to the entry.
      // FIXME(JM): The engine caches these so we could retrieve these lazily as needed.
      inputPackages: Map[PackageId, ByteString],
      inputTransactions: Map[KVEntryId, KVEntryBlob],
      txInfo: TxInfo): Either[EngineError, Unit] = {

    def lookupContract(coid: AbsoluteContractId) = {
      val kvCoid = decodeAbsoluteContractId(coid)
      inputTransactions.get(kvCoid._1).flatMap { entry =>
        lookupStampedContractInstance(kvCoid._1, entry, kvCoid).flatMap {
          case (activeAt, coinst) =>
            if (activeAt > txInfo.ledgerEffectiveTime)
              None
            else
              Some(coinst)
        }
      }
    }

    def lookupPackage(pkgId: PackageId) =
      inputPackages.get(pkgId).map { archiveBytes =>
        Decode.decodeArchive(Archive.parseFrom(archiveBytes))._2
      }

    engine
      .validate(txInfo.tx, txInfo.ledgerEffectiveTime)
      .consume(lookupContract, lookupPackage, _ => sys.error("unimplemented"))
  }

  // ------------------------------------------

  object TransactionCoding {

    import com.digitalasset.daml.lf.transaction.TransactionCoder
    import com.digitalasset.daml.lf.value.ValueCoder

    def encodeTransaction(tx: SubmittedTransaction): TransactionOuterClass.Transaction = {
      TransactionCoder
        .encodeTransactionWithCustomVersion(
          nidEncoder,
          cidEncoder,
          VersionedTransaction(TransactionVersions.assignVersion(tx), tx))
        .fold(err => sys.error(s"encodeTransaction error: $err"), identity)
    }

    def decodeTransaction(tx: TransactionOuterClass.Transaction): SubmittedTransaction = {
      TransactionCoder
        .decodeVersionedTransaction(
          nidDecoder,
          cidDecoder,
          tx
        )
        .fold(err => sys.error(s"decodeTransaction error: $err"), _.transaction)
    }

    // FIXME(JM): Should we have a well-defined schema for this?
    private val cidEncoder: ValueCoder.EncodeCid[ContractId] = {
      val asStruct: ContractId => (String, Boolean) = {
        case RelativeContractId(nid) => (s"~${nid.index}", true)
        case AbsoluteContractId(coid) => (s"$coid", false)
      }
      ValueCoder.EncodeCid(asStruct(_)._1, asStruct)
    }
    private val cidDecoder: ValueCoder.DecodeCid[ContractId] = {
      def fromString(x: String): Either[DecodeError, ContractId] = {
        if (x.startsWith("~"))
          Try(x.tail.toInt).toOption match {
            case None =>
              Left(DecodeError(s"Invalid relative contract id: $x"))
            case Some(i) =>
              Right(RelativeContractId(NodeId.unsafeFromIndex(i)))
          } else
          Right(AbsoluteContractId(x))
      }

      ValueCoder.DecodeCid(
        fromString,
        { case (i, _) => fromString(i) }
      )
    }

    private val nidDecoder: String => Either[ValueCoder.DecodeError, NodeId] =
      nid => Right(NodeId.unsafeFromIndex(nid.toInt))
    private val nidEncoder: TransactionCoder.EncodeNid[NodeId] =
      nid => nid.index.toString
    private val valEncoder: TransactionCoder.EncodeVal[Transaction.Value[ContractId]] =
      a => ValueCoder.encodeVersionedValueWithCustomVersion(cidEncoder, a).map((a.version, _))
    private val valDecoder: ValueOuterClass.VersionedValue => Either[
      ValueCoder.DecodeError,
      Transaction.Value[ContractId]] =
      a => ValueCoder.decodeVersionedValue(cidDecoder, a)

  }
}
