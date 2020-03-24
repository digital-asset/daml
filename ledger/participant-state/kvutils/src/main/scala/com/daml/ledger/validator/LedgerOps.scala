// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.Envelope
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

trait LedgerState {
  def inTransaction[T](body: LedgerOps => Future[T]): Future[T]
}

object LedgerState {

  /*
  def adaptLedgerStateAccess[T](lsa: LedgerStateAccess[T]): LedgerState = {
    new LedgerState {
      override def inTransaction[T](body: LedgerOps => Future[T]): Future[T] =
        lsa.inTransaction { lso =>
          val rawOps = new RawLedgerOps {
            override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
              lso.readState(keys)
            override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
              lso.writeState(keyValuePairs)
            override def appendToLog(key: Key, value: Value): Future[Unit] =
              lso.appendToLog(key, value).map(_ => ())
          }
          body()
        }
    }
  }*/

}

/** Low-level direct ledger operations that expose the internal types.
  * We layer concrete implementations on top of this, which allows for transformations,
  * caching of decoded values etc.
  */
trait LedgerOps {
  def readState(keys: Seq[DamlStateKey]): Future[Seq[Option[DamlStateValue]]]
  def writeState(values: Seq[(DamlStateKey, DamlStateValue)]): Future[Unit]
  def appendToLog(entryId: DamlLogEntryId, entry: DamlLogEntry): Future[Unit]
}

object LedgerOps {

  /** A caching adapter for ledger operations. Local to the state access transaction. */
  def cachingLedgerOps(ops: LedgerOps): LedgerOps = {
    ???
  }

}

/** Ledger operations that operate on raw bytes */
trait RawLedgerOps {
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
  def toLedgerOps(implicit executionContext: ExecutionContext): LedgerOps = {
    val rawLedgerOps = this
    new LedgerOps {
      override def readState(keys: Seq[DamlStateKey]): Future[Seq[Option[DamlStateValue]]] =
        rawLedgerOps
          .readState(keys.map(_.toByteString))
          .map(values =>
            values.map(optValue =>
              optValue.map(Envelope.openStateValue(_).getOrElse(sys.error("Open failed")))))

      override def writeState(values: Seq[(DamlStateKey, DamlStateValue)]): Future[Unit] =
        rawLedgerOps.writeState(
          values.map {
            case (key, value) =>
              key.toByteString -> Envelope.enclose(value)
          }
        )

      override def appendToLog(entryId: DamlLogEntryId, entry: DamlLogEntry): Future[Unit] =
        rawLedgerOps.appendToLog(entryId.toByteString, Envelope.enclose(entry))
    }
  }
}
