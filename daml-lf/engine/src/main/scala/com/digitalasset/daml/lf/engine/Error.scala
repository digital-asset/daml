// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value

sealed abstract class Error {
  def msg: String
}

object Error {

  // Error happening during Package loading
  final case class Package(packageError: Package.Error) extends Error {
    def msg: String = packageError.msg
  }

  object Package {

    sealed abstract class Error {
      def msg: String
    }

    // TODO https://github.com/digital-asset/daml/issues/9974
    //  get rid of Generic
    final case class Generic(override val msg: String) extends Error

    final case class Validation(validationError: validation.ValidationError) extends Error {
      def msg: String = validationError.pretty
    }

  }

  // Error happening during command/transaction preprocessing
  final case class Preprocessing(processingError: Preprocessing.Error) extends Error {
    def msg: String = processingError.msg
  }

  object Preprocessing {

    sealed abstract class Error extends RuntimeException with scala.util.control.NoStackTrace {
      def msg: String
    }

    // TODO https://github.com/digital-asset/daml/issues/9974
    //  get rid of Generic
    final case class Generic(override val msg: String) extends Error

    final case class Lookup(lookupError: language.LookupError) extends Error {
      def msg: String = lookupError.pretty
    }

    private[engine] object MissingPackage {
      def apply(pkgId: Ref.PackageId): Lookup =
        Lookup(language.LookupError.Package(pkgId))
      def unapply(error: Lookup): Option[Ref.PackageId] =
        error.lookupError match {
          case language.LookupError.Package(packageId) => Some(packageId)
          case _ => None
        }
    }
  }

  // Error happening during interpretation
  final case class Interpretation(interpretationError: Interpretation.Error) extends Error {
    def msg: String = interpretationError.msg
  }

  object Interpretation {

    sealed abstract class Error {
      def msg: String
    }

    // TODO https://github.com/digital-asset/daml/issues/9974
    //  get rid of Generic
    final case class Generic(override val msg: String, detailMsg: String) extends Error
    object Generic {
      def apply(msg: String): Generic = Generic(msg, msg)
    }

    final case class ContractNotFound(ci: Value.ContractId) extends Error {
      override def msg = s"Contract could not be found with id $ci"
    }

    final case class ContractKeyNotFound(key: GlobalKey) extends Error {
      override def msg = s"dependency error: couldn't find key: $key"
    }

    /** See com.daml.lf.transaction.Transaction.DuplicateContractKey
      * for more information.
      */
    final case class DuplicateContractKey(key: GlobalKey) extends Error {
      override def msg = s"Duplicate contract key $key"
    }

    final case class Authorization(override val msg: String) extends Error

  }

  // Error happening during transaction validation
  final case class Validation(validationError: Validation.Error) extends Error {
    override def msg = validationError.msg
  }

  object Validation {

    sealed abstract class Error {
      def msg: String
    }

    // TODO: get rid of Generic
    final case class Generic(msg: String) extends Error

    final case class ReplayMismatch(
        mismatch: transaction.ReplayMismatch[transaction.NodeId, Value.ContractId]
    ) extends Error {
      override def msg: String = mismatch.msg
    }

  }

}
