// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers}
import com.daml.lf.value.Value

sealed abstract class Error {
  def subError: Error.SubError
  final def msg: String = subError.msg
  final def detailMsg: String = msg
}

object Error {

  sealed abstract class SubError extends RuntimeException with scala.util.control.NoStackTrace {
    // msg is intended to be a single line message
    def msg: String
    // details for debugging should be included here
    def detailMsg: String
    def toError: Error
  }

  // Error happening during Package loading
  final case class Package(subError: Package.SubError) extends Error

  object Package {

    sealed abstract class SubError extends Error.SubError {
      final def toError: Package = Package(this)
      override def detailMsg: String = msg
    }

    // TODO: get ride of Generic
    final case class Generic(override val msg: String, override val detailMsg: String)
        extends SubError
    object Generic {
      def apply(msg: String): Generic = new Generic(msg, msg)
    }

    final case class Validation(validationError: validation.ValidationError) extends SubError {
      def msg: String = validationError.pretty
    }

  }

  // Error happening during command/transaction preprocessing
  final case class Preprocessing(subError: Preprocessing.SubError) extends Error

  object Preprocessing {

    sealed abstract class SubError extends Error.SubError {
      final def toError: Preprocessing = Preprocessing(this)
      override def detailMsg: String = msg
    }

    // TODO: get ride of Generic
    final case class Generic(override val msg: String, override val detailMsg: String)
        extends SubError
    object Generic {
      def apply(msg: String): Generic = new Generic(msg, msg)
    }

    final case class Lookup(lookupError: language.LookupError) extends SubError {
      def msg: String = lookupError.pretty
    }

    private[engine] object MissingPackage {
      def unapply(error: Lookup): Option[Ref.PackageId] =
        error.lookupError match {
          case language.LookupError.Package(packageId) => Some(packageId)
          case _ => None
        }
    }
  }

  // Error happening during interpretation
  final case class Interpretation(subError: Interpretation.SubError) extends Error

  object Interpretation {

    sealed abstract class SubError extends Error.SubError {
      final def toError: Interpretation = Interpretation(this)
      override def detailMsg: String = msg
    }

    // TODO: get ride of Generic
    final case class Generic(override val msg: String, override val detailMsg: String)
        extends SubError

    object Generic {
      def apply(msg: String): Generic = new Generic(msg, msg)
    }

    final case class ContractNotFound(ci: Value.ContractId) extends SubError {
      override def msg = s"Contract could not be found with id $ci"
    }

    final case class GlobalKeyNotFound(globalKey: GlobalKeyWithMaintainers) extends SubError {
      override def msg = s"dependency error: couldn't find key ${globalKey.globalKey}"
    }

    /** See com.daml.lf.transaction.Transaction.DuplicateContractKey
      * for more information.
      */
    final case class DuplicateContractKey(key: GlobalKey) extends SubError {
      override def msg = s"Duplicate contract key $key"
    }

    final case class Authorization(override val msg: String) extends SubError {
      override def detailMsg: String = msg
    }

  }

  // Error happening during transaction validation
  final case class Validation(subError: Validation.SubError) extends Error

  object Validation {

    sealed abstract class SubError extends Error.SubError {
      final def toError: Validation = Validation(this)
      override def detailMsg: String = msg
    }

    // TODO: get ride of Generic
    final case class Generic(msg: String, override val detailMsg: String) extends SubError
    object Generic {
      def apply(msg: String): Generic = new Generic(msg, msg)
    }

    final case class ReplayMismatch(
        mismatch: transaction.ReplayMismatch[transaction.NodeId, Value.ContractId]
    ) extends SubError {
      override def msg: String = mismatch.msg

      override def detailMsg: String = ""
    }

  }

}
