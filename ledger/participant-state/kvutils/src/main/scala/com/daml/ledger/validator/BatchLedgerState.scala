// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.util.concurrent.ConcurrentHashMap

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.Envelope
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.breakOut

trait RawBatchLedgerState {
  def inTransaction[T](body: RawBatchLedgerOps => Future[T])(
      implicit executionContext: ExecutionContext): Future[T]
}

object RawBatchLedgerState {
  def adaptLedgerStateAccess[T](lsa: LedgerStateAccess[T]): RawBatchLedgerState = {
    new RawBatchLedgerState {
      override def inTransaction[T](body: RawBatchLedgerOps => Future[T])(
          implicit executionContext: ExecutionContext): Future[T] =
        lsa.inTransaction { lso =>
          val rawOps = new BatchLedgerOps {
            override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
              lso.readState(keys)
            override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
              lso.writeState(keyValuePairs)
            override def appendToLog(key: Key, value: Value): Future[Unit] =
              lso.appendToLog(key, value).map(_ => ())
          }
          body(rawOps.toRawBatchLedgerOps)
        }
    }
  }

  def adaptBatchLedgerState[T](ledgerState: BatchLedgerState): RawBatchLedgerState = {
    new RawBatchLedgerState {
      override def inTransaction[T](body: RawBatchLedgerOps => Future[T])(
          implicit executionContext: ExecutionContext): Future[T] =
        ledgerState.inTransaction { ops =>
          val rawOps = RawBatchLedgerOps.cachingLedgerOps((ops.toRawBatchLedgerOps))
          body(rawOps)
        }
    }
  }
}

/** The public interface for integrations. */
trait BatchLedgerState {
  def inTransaction[T](body: BatchLedgerOps => Future[T]): Future[T]
}

/** Low-level direct ledger operations that expose the internal types.
  * We layer concrete implementations on top of this, which allows for transformations,
  * caching of decoded values etc.
  *
  * The reasoning here is that these are integration details that the batch validator
  * does not need to care about.
  */
trait RawBatchLedgerOps {
  def readState(keys: Seq[DamlStateKey]): Future[Seq[Option[DamlStateValue]]]
  def commit(
      entryId: DamlLogEntryId,
      entry: DamlLogEntry,
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
      outputState: Map[DamlStateKey, DamlStateValue]): Future[Unit]
}

object RawBatchLedgerOps {

  /** A caching adapter for ledger operations. Local to the state access transaction.
    * This is crucial for caching access to large frequently accessed state, for example
    * package state values. */
  def cachingLedgerOps(ops: RawBatchLedgerOps)(
      implicit executionContext: ExecutionContext): RawBatchLedgerOps = {
    val cache = new ConcurrentHashMap[DamlStateKey, DamlStateValue]()

    new RawBatchLedgerOps {
      override def readState(keys: Seq[DamlStateKey]): Future[Seq[Option[DamlStateValue]]] = {
        val cachedValues = keys.flatMap { key =>
          Option(cache.get(key)).map(key -> Some(_)).toList
        }.toMap

        val remainingKeys = keys.toSet -- cachedValues.keySet

        ops
          .readState(remainingKeys.toSeq)
          .map(remainingValues => remainingKeys.zip(remainingValues).toMap)
          .map { remaining =>
            remaining.collect {
              case (key, Some(value)) => cache.put(key, value)
            }
            val all: Map[DamlStateKey, Option[DamlStateValue]] = cachedValues ++ remaining
            keys.map(all(_))
          }
      }

      override def commit(
          entryId: DamlLogEntryId,
          entry: DamlLogEntry,
          inputState: Map[DamlStateKey, Option[DamlStateValue]],
          outputState: Map[DamlStateKey, DamlStateValue]): Future[Unit] =
        ops.commit(entryId, entry, inputState, outputState)
    }
  }

}

/** Ledger operations with keys and values as bytes. */
trait BatchLedgerOps {
  type Key = ByteString
  type Value = ByteString

  /**
    * Reads values of a set of keys from the backing store.
    * @param keys  list of keys to look up data for
    * @return  values corresponding to the requested keys, in the same order as requested
    */
  def readState(keys: Seq[Key]): Future[Seq[Option[Value]]]

  /**
    * Writes a list of key-value pairs to the backing store.  In case a key already exists its value is overwritten.
    */
  def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit]

  /**
    * Writes a single log entry to the backing store.  The implementation may return Future.failed in case the key
    * (i.e., the log entry ID) already exists.
    */
  def appendToLog(key: Key, value: Value): Future[Unit]

  /** Adapt the raw bytes interface into the low-level interface. */
  def toRawBatchLedgerOps(implicit executionContext: ExecutionContext): RawBatchLedgerOps = {
    val rawLedgerOps = this
    new RawBatchLedgerOps {
      override def readState(keys: Seq[DamlStateKey]): Future[Seq[Option[DamlStateValue]]] =
        rawLedgerOps
          .readState(keys.map(_.toByteString))
          .map(values =>
            values.map(optValue =>
              optValue.map(Envelope.openStateValue(_).getOrElse(sys.error("Open failed")))))

      override def commit(
          entryId: DamlLogEntryId,
          entry: DamlLogEntry,
          inputState: Map[DamlStateKey, Option[DamlStateValue]],
          outputState: Map[DamlStateKey, DamlStateValue]): Future[Unit] = {

        rawLedgerOps.writeState(outputState.map {
          case (key, value) =>
            key.toByteString -> Envelope.enclose(value)
        }(breakOut))

        rawLedgerOps.appendToLog(
          entryId.toByteString,
          Envelope.enclose(entry)
        )
      }
    }
  }
}
