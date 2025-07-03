// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.{Ast, LookupError}
import com.digitalasset.daml.lf.transaction.NodeId
import com.digitalasset.daml.lf.value.Value

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
        cause: Option[Throwable],
    ) extends Error
        with InternalError

    final case class Validation(validationError: validation.ValidationError) extends Error {
      def message: String = validationError.pretty
    }

    final case class MissingPackage(packageRef: Ref.PackageRef, context: language.Reference)
        extends Error {
      override def message: String = LookupError.MissingPackage.pretty(packageRef, context)
    }

    object MissingPackage {
      def apply(packageRef: Ref.PackageRef): MissingPackage =
        MissingPackage(packageRef, language.Reference.Package(packageRef))

      def apply(packageId: Ref.PackageId): MissingPackage = apply(Ref.PackageRef.Id(packageId))

      def apply(packageId: Ref.PackageId, context: language.Reference): MissingPackage =
        apply(Ref.PackageRef.Id(packageId), context)
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

    final case class DarSelfConsistency(
        mainPackageId: Ref.PackageId,
        transitiveDependencies: Set[Ref.PackageId],
        missingDependencies: Set[Ref.PackageId],
        extraDependencies: Set[Ref.PackageId],
    ) extends Error {
      def message: String =
        s"For package $mainPackageId, the set of package dependencies ${transitiveDependencies
            .mkString("{'", "', '", "'}")} is not self consistent, " +
          (if (missingDependencies.nonEmpty)
             s"the missing dependencies are ${missingDependencies.mkString("{'", "', '", "'}")} "
           else "") +
          (if (missingDependencies.nonEmpty && extraDependencies.nonEmpty) "and " else "") +
          (if (extraDependencies.nonEmpty)
             s"the extra dependencies are ${extraDependencies.mkString("{'", "', '", "'}")}"
           else "")
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
        cause: Option[Throwable],
    ) extends Error
        with InternalError

    final case class Lookup(lookupError: language.LookupError) extends Error {
      override def message: String = lookupError.pretty
    }

    final case class TypeMismatch(
        typ: Ast.Type,
        value: Value,
        override val message: String,
    ) extends Error

    final case class ValueNesting(culprit: Value) extends Error {
      override def message: String =
        s"Provided value exceeds maximum nesting level of ${Value.MAXIMUM_NESTING}"
    }

    final case class IllegalContractId(cid: Value.ContractId, reason: IllegalContractId.Reason)
        extends Error {
      override def message: String =
        s"""Illegal Contract ID "${cid.coid}", """ + reason.details
    }

    object IllegalContractId {
      sealed abstract class Reason extends Serializable with Product {
        def details: String
      }

      case object NonSuffixV1ContractId extends Reason {
        def details = "non-suffixed V1 Contract IDs are forbidden"

        def apply(cid: Value.ContractId.V1): IllegalContractId = IllegalContractId(cid, this)
      }

      case object NonSuffixV2ContractId extends Reason {
        def details = "non-suffixed V2 Contract IDs are forbidden"

        def apply(cid: Value.ContractId.V2): IllegalContractId = IllegalContractId(cid, this)
      }
    }

    final case class RootNode(nodeId: NodeId, override val message: String) extends Error

    final case class DuplicateDisclosedContractId(
        contractId: Value.ContractId
    ) extends Error {
      override def message: String =
        s"Duplicate disclosed contract ID ${contractId.coid}"
    }

    final case class DuplicateDisclosedContractKey(keyHash: crypto.Hash) extends Error {
      override def message: String =
        s"Duplicate disclosed contract key hash ${keyHash.toHexString}"
    }

    final case class UnresolvedPackageName(pkgName: Ref.PackageName, context: language.Reference)
        extends Error {
      override def message: String =
        s"unresolved package name $pkgName " + LookupError.contextDetails(context)
    }

    final case class UnexpectedContractKeyPrefetch(
        templateId: Ref.TypeConRef,
        key: Value,
    ) extends Error {
      override def message: String =
        s"Template of prefetched contract key does not define a key: ${templateId} ($key)"
    }

    final case class ContractIdInContractKey(key: Value) extends Error {
      override def message: String =
        s"Contract IDs are not supported in contract keys: ${key.cids.head.coid}"
    }
  }

  // Error happening during interpretation
  final case class Interpretation(
      interpretationError: Interpretation.Error,
      transactionTrace: Option[String],
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
        cause: Option[Throwable],
    ) extends Error
        with InternalError

    final case class DamlException(error: interpretation.Error) extends Error {
      override def message: String = error match {
        case interpretation.Error.ContractNotFound(cid) =>
          s"Contract could not be found with id ${cid.coid}"
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
        mismatch: transaction.ReplayMismatch
    ) extends Error {
      override def message: String = mismatch.message
    }
  }
}
