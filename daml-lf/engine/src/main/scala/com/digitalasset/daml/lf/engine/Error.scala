// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.daml.lf.transaction.{GlobalKey, NodeId}
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

    final case class Internal(nameOfFunc: String, override val msg: String, detailMsg: String = "")
        extends Error

    final case class Validation(validationError: validation.ValidationError) extends Error {
      def msg: String = validationError.pretty
    }

    final case class MissingPackages(packageIds: Set[Ref.PackageId]) extends Error {
      val s = if (packageIds.size <= 1) "" else "s"
      override def msg: String = s"Couldn't find package$s ${packageIds.mkString(",")}"
    }
    private[engine] object MissingPackage {
      def apply(packageId: Ref.PackageId): MissingPackages = MissingPackages(Set(packageId))
    }

    final case class AllowedLanguageVersion(
        packageId: Ref.PackageId,
        languageVersion: language.LanguageVersion,
        allowedLanguageVersions: VersionRange[language.LanguageVersion],
    ) extends Error {
      def msg: String =
        s"Disallowed language version in package $packageId: " +
          s"Expected version between ${allowedLanguageVersions.min.pretty} and ${allowedLanguageVersions.max.pretty} but got ${languageVersion.pretty}"
    }

    final case class SelfConsistency(
        packageIds: Set[Ref.PackageId],
        missingDependencies: Set[Ref.PackageId],
    ) extends Error {
      def msg: String =
        s"The set of packages ${packageIds.mkString("{'", "', '", "'}")} is not self consistent, " +
          s"the missing dependencies are ${missingDependencies.mkString("{'", "', '", "'}")}."
    }

  }

  // Error happening during command/transaction preprocessing
  final case class Preprocessing(processingError: Preprocessing.Error) extends Error {
    def msg: String = processingError.msg
  }

  object Preprocessing {

    sealed abstract class Error
        extends RuntimeException
        with scala.util.control.NoStackTrace
        with Product {
      def msg: String

      override def toString: String =
        productPrefix + productIterator.mkString("(", ",", ")")
    }

    final case class Internal(
        nameOfFunc: String,
        override val msg: String,
        detailMsg: String = "",
    ) extends Error

    final case class Lookup(lookupError: language.LookupError) extends Error {
      override def msg: String = lookupError.pretty
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

    final case class Type(
        typ: Ast.Type,
        value: Value[Value.ContractId],
        override val msg: String,
    ) extends Error

    final case class ValueNesting(value: Value[Value.ContractId]) extends Error {
      override def msg: String =
        s"Provided value exceeds maximum nesting level of ${Value.MAXIMUM_NESTING}"
    }

    final case class ContractIdInContractKey(
        templateId: Ref.TypeConName,
        key: Value[Value.ContractId],
        contractId: Value.ContractId,
    ) extends Error {
      override def msg: String =
        s"Contract IDs are not supported in contract key of $templateId: $contractId"
    }

    final case class RootNode(nodeId: NodeId, override val msg: String) extends Error

    final case class ContractIdFreshness(
        localContractIds: Set[Value.ContractId],
        globalContractIds: Set[Value.ContractId],
    ) extends Error {
      assert(localContractIds exists globalContractIds)
      def msg: String = "Conflicting discriminators between a global and local contract ID."
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
