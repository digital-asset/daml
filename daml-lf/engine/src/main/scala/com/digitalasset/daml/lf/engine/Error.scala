// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref
import com.daml.lf.language.{Ast, LookupError}
import com.daml.lf.transaction.NodeId
import com.daml.lf.value.Value

sealed abstract class Error {
  def message: String
}

object Error {

  // Error happening during Package loading
  final case class Package(packageError: Package.Error) extends Error {
    def message: String = packageError.message
  }

  object Package {

    sealed abstract class Error {
      def message: String
    }

    final case class Internal(
        location: String,
        override val message: String,
    ) extends Error
        with InternalError

    final case class Validation(validationError: validation.ValidationError) extends Error {
      def message: String = validationError.pretty
    }

    final case class MissingPackage(packageId: Ref.PackageId, context: language.Reference)
        extends Error {
      override def message: String = LookupError.MissingPackage.pretty(packageId, context)
    }

    object MissingPackage {
      def apply(packageId: Ref.PackageId): MissingPackage =
        MissingPackage(packageId, language.Reference.Package(packageId))
    }

    final case class AllowedLanguageVersion(
        packageId: Ref.PackageId,
        languageVersion: language.LanguageVersion,
        allowedLanguageVersions: VersionRange[language.LanguageVersion],
    ) extends Error {
      def message: String =
        s"Disallowed language version in package $packageId: " +
          s"Expected version between ${allowedLanguageVersions.min.pretty} and ${allowedLanguageVersions.max.pretty} but got ${languageVersion.pretty}"
    }

    final case class SelfConsistency(
        packageIds: Set[Ref.PackageId],
        missingDependencies: Set[Ref.PackageId],
    ) extends Error {
      def message: String =
        s"The set of packages ${packageIds.mkString("{'", "', '", "'}")} is not self consistent, " +
          s"the missing dependencies are ${missingDependencies.mkString("{'", "', '", "'}")}."
    }

  }

  // Error happening during command/transaction preprocessing
  final case class Preprocessing(processingError: Preprocessing.Error) extends Error {
    def message: String = processingError.message
  }

  object Preprocessing {

    sealed abstract class Error
        extends RuntimeException
        with scala.util.control.NoStackTrace
        with Product {
      def message: String

      override def toString: String =
        productPrefix + productIterator.mkString("(", ",", ")")
    }

    final case class Internal(
        location: String,
        override val message: String,
    ) extends Error
        with InternalError

    final case class Lookup(lookupError: language.LookupError) extends Error {
      override def message: String = lookupError.pretty
    }

    final case class TypeMismatch(
        typ: Ast.Type,
        value: Value[Value.ContractId],
        override val message: String,
    ) extends Error

    final case class ValueNesting(value: Value[Value.ContractId]) extends Error {
      override def message: String =
        s"Provided value exceeds maximum nesting level of ${Value.MAXIMUM_NESTING}"
    }

    final case class NonSuffixedCid(cid: Value.ContractId.V1) extends Error {
      override def message: String =
        s"Provided Contract ID $cid is not suffixed"
    }

    final case class RootNode(nodeId: NodeId, override val message: String) extends Error

  }

  // Error happening during interpretation
  final case class Interpretation(
      interpretationError: Interpretation.Error,
      // detailMessage describes the state of the machine when the error occurs
      detailMessage: Option[String],
  ) extends Error {
    def message: String = interpretationError.message
  }

  object Interpretation {

    sealed abstract class Error {
      def message: String
    }

    final case class Internal(
        location: String,
        override val message: String,
    ) extends Error
        with InternalError

    final case class DamlException(error: interpretation.Error) extends Error {
      // TODO https://github.com/digital-asset/daml/issues/9974
      //  For now we try to preserve the exact same message (for the ledger API)
      //  Review once all the errors are properly structured
      override def message: String = error match {
        case interpretation.Error.ContractNotFound(cid) =>
          // TODO https://github.com/digital-asset/daml/issues/9974
          //   we should probably use ${cid.coid} instead of $cid
          s"Contract could not be found with id $cid"
        case interpretation.Error.ContractKeyNotFound(key) =>
          s"dependency error: couldn't find key: $key"
        case _ =>
          s"Interpretation error: Error: ${speedy.Pretty.prettyDamlException(error).render(80)}"
      }
    }

  }

  // Error happening during transaction validation
  final case class Validation(validationError: Validation.Error) extends Error {
    override def message = validationError.message
  }

  object Validation {

    sealed abstract class Error {
      def message: String
    }

    final case class ReplayMismatch(
        mismatch: transaction.ReplayMismatch[transaction.NodeId, Value.ContractId]
    ) extends Error {
      override def message: String = mismatch.message
    }

  }

}
