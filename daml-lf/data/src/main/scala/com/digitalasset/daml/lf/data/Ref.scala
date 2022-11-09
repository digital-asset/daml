// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package data

import com.daml.scalautil.Statement.discard

object Ref {

  val IdString: IdString = new IdStringImpl

  type Name = IdString.Name
  val Name: IdString.Name.type = IdString.Name

  /* Encoding of byte array */
  type HexString = IdString.HexString
  val HexString: IdString.HexString.type = IdString.HexString

  type PackageName = IdString.PackageName
  val PackageName: IdString.PackageName.type = IdString.PackageName

  type PackageVersion = IdString.PackageVersion
  val PackageVersion: IdString.PackageVersion.type = IdString.PackageVersion

  /** Party identifiers are non-empty US-ASCII strings built from letters, digits, space, colon, minus and,
    * underscore. We use them to represent [Party] literals. In this way, we avoid
    * empty identifiers, escaping problems, and other similar pitfalls.
    */
  type Party = IdString.Party
  val Party: IdString.Party.type = IdString.Party

  /** Reference to a package via a package identifier. The identifier is the ascii7
    * lowercase hex-encoded hash of the package contents found in the Daml-LF Archive.
    */
  type PackageId = IdString.PackageId
  val PackageId: IdString.PackageId.type = IdString.PackageId

  /** Identifiers for contract IDs. */
  type ContractIdString = IdString.ContractIdString
  val ContractIdString: IdString.ContractIdString.type = IdString.ContractIdString

  type LedgerString = IdString.LedgerString
  val LedgerString: IdString.LedgerString.type = IdString.LedgerString

  /** Identifiers for submitting client applications. */
  type ApplicationId = IdString.ApplicationId
  val ApplicationId: IdString.ApplicationId.type = IdString.ApplicationId

  /** Identifiers for participant node users, which act as clients to the Ledger API.
    *
    * The concept of participant node users has been introduced after the concept
    * of client applications, which is why the code contains both of them.
    * The [[UserId]] identifiers for participant node users are a strict subset of the
    * [[ApplicationId]] identifiers for Ledger API client applications.
    * The Ledger API and its backing code use [[UserId]] where possible
    * and [[ApplicationId]] otherwise.
    */
  type UserId = IdString.UserId
  val UserId: IdString.UserId.type = IdString.UserId

  /** Identifiers used to correlate a command submission with its result. */
  type CommandId = LedgerString
  val CommandId: LedgerString.type = LedgerString

  /** Identifiers used to correlate a submission with its result. */
  type SubmissionId = LedgerString
  val SubmissionId: LedgerString.type = LedgerString

  /** Uniquely identifies a transaction. */
  type TransactionId = LedgerString
  val TransactionId: LedgerString.type = LedgerString

  /** Identifiers used for correlating a submission with a workflow. */
  type WorkflowId = LedgerString
  val WorkflowId: LedgerString.type = LedgerString

  type ParticipantId = IdString.ParticipantId
  val ParticipantId: IdString.ParticipantId.type = IdString.ParticipantId

  sealed trait IdentityProviderId {
    def toRequestString: String

    def toDb: Option[Ref.IdentityProviderId.Id]
  }
  object IdentityProviderId {
    final case object Default extends IdentityProviderId {
      override def toRequestString: String = ""

      override def toDb: Option[Id] = None
    }

    final case class Id(value: LedgerString) extends IdentityProviderId {
      override def toRequestString: String = value

      override def toDb: Option[Id] = Some(this)
    }

    object Id {
      def fromString(id: String): Either[String, IdentityProviderId.Id] = {
        LedgerString.fromString(id).map(Id.apply)
      }

      def assertFromString(id: String) = {
        Id(LedgerString.assertFromString(id))
      }
    }

    def apply(identityProviderId: String): IdentityProviderId =
      Option(identityProviderId).filter(_.nonEmpty) match {
        case Some(id) => Id(LedgerString.assertFromString(id))
        case None => Default
      }

    def fromString(identityProviderId: String): Either[String, IdentityProviderId] =
      Option(identityProviderId).filter(_.nonEmpty) match {
        case Some(id) => LedgerString.fromString(id).map(Id.apply)
        case None => Right(Default)
      }

    def fromDb(identityProviderId: Option[IdentityProviderId.Id]): IdentityProviderId =
      identityProviderId match {
        case None => IdentityProviderId.Default
        case Some(id) => id
      }
  }

  /* Location annotation */
  case class Location(
      packageId: PackageId,
      module: ModuleName,
      definition: String,
      start: (Int, Int),
      end: (Int, Int),
  ) {
    def pretty: String = s"definition $packageId:$module:$definition (start: $start, end: $end)"
  }

  // we do not use String.split because `":foo".split(":")`
  // results in `List("foo")` rather than `List("", "foo")`
  private def split(s: String, splitCh: Char): ImmArray[String] = {
    val splitCodepoint = splitCh.toInt
    val segments = ImmArray.newBuilder[String]
    val currentString = new java.lang.StringBuilder()
    s.codePoints()
      .forEach(ch => {
        if (ch == splitCodepoint) {
          discard(segments += currentString.toString)
          currentString.setLength(0)
        } else {
          val _ = currentString.appendCodePoint(ch)
        }
      })
    discard(segments += currentString.toString)
    segments.result()
  }

  private def splitInTwo(s: String, splitCh: Char): Option[(String, String)] = {
    val splitIndex = s.indexOf(splitCh.toInt)
    if (splitIndex < 0) None
    else Some((s.substring(0, splitIndex), s.substring(splitIndex + 1)))
  }

  final class DottedName private (val segments: ImmArray[Name])
      extends Equals
      with Ordered[DottedName] {
    def dottedName: String = segments.toSeq.mkString(".")

    override def equals(obj: Any): Boolean =
      obj match {
        case that: DottedName => segments == that.segments
        case _ => false
      }

    override def hashCode(): Int = segments.hashCode()

    def canEqual(that: Any): Boolean = that.isInstanceOf[DottedName]

    override def toString: String = dottedName

    override def compare(that: DottedName): Int = {
      import Name.ordering

      import scala.math.Ordering.Implicits._

      implicitly[Ordering[Seq[Name]]].compare(segments.toSeq, that.segments.toSeq)
    }

  }

  object DottedName {
    val maxLength = 1000

    def fromString(s: String): Either[String, DottedName] =
      if (s.isEmpty)
        Left(s"Expected a non-empty string")
      else
        fromSegments(split(s, '.').toSeq)

    @throws[IllegalArgumentException]
    final def assertFromString(s: String): DottedName =
      assertRight(fromString(s))

    def fromSegments(strings: Iterable[String]): Either[String, DottedName] = {
      val init: Either[String, BackStack[Name]] = Right(BackStack.empty)
      val validatedSegments = strings.foldLeft(init)((acc, string) =>
        for {
          stack <- acc
          segment <- Name.fromString(string)
        } yield stack :+ segment
      )
      for {
        segments <- validatedSegments
        name <- fromNames(segments.toImmArray)
      } yield name
    }

    @throws[IllegalArgumentException]
    def assertFromSegments(s: Iterable[String]): DottedName =
      assertRight(fromSegments(s))

    def fromNames(names: ImmArray[Name]): Either[String, DottedName] =
      if (names.isEmpty)
        Left("No segments provided")
      else {
        val length = names.foldLeft(-1)(_ + _.length + 1)
        if (length > maxLength)
          Left(s"""DottedName is too long (max: $maxLength)""")
        else
          Right(new DottedName(names))
      }

    @throws[IllegalArgumentException]
    def assertFromNames(names: ImmArray[Name]): DottedName =
      assertRight(fromNames(names))

    /** You better know what you're doing if you use this one -- specifically you need to comply
      * to the lexical specification embodied by `fromStrings`.
      */
    def unsafeFromNames(segments: ImmArray[Name]): DottedName = {
      new DottedName(segments)
    }
  }

  final case class QualifiedName private (module: ModuleName, name: DottedName)
      extends Ordered[QualifiedName] {
    override def toString: String = module.toString + ":" + name.toString
    def qualifiedName: String = toString

    override def compare(that: QualifiedName): Int = {
      val diffModule = this.module compare that.module
      if (diffModule != 0)
        diffModule
      else
        this.name compare that.name
    }
  }
  object QualifiedName {
    def fromString(s: String): Either[String, QualifiedName] = {
      val segments = split(s, ':')
      if (segments.length != 2)
        Left(s"Expected script identifier of the form ModuleName:scriptName but got $s")
      else
        ModuleName.fromString(segments(0)).flatMap { module =>
          DottedName.fromString(segments(1)).map { name =>
            QualifiedName(module, name)
          }
        }
    }

    @throws[IllegalArgumentException]
    def assertFromString(s: String): QualifiedName =
      assertRight(fromString(s))
  }

  /* A fully-qualified identifier pointing to a definition in the
   * specified package. */
  final case class Identifier(packageId: PackageId, qualifiedName: QualifiedName)
      extends Ordered[Identifier] {
    override def toString: String = packageId + ":" + qualifiedName.toString

    override def compare(that: Identifier): Int = {
      val diffPkgId = this.packageId compare that.packageId
      if (diffPkgId != 0)
        diffPkgId
      else
        this.qualifiedName compare that.qualifiedName
    }
  }

  object Identifier {
    def fromString(s: String): Either[String, Identifier] = {
      splitInTwo(s, ':').fold[Either[String, Identifier]](
        Left(s"Separator ':' between package identifier and qualified name not found in $s")
      ) { case (packageIdString, qualifiedNameString) =>
        for {
          packageId <- PackageId.fromString(packageIdString)
          qualifiedName <- QualifiedName.fromString(qualifiedNameString)
        } yield Identifier(packageId, qualifiedName)
      }
    }

    @throws[IllegalArgumentException]
    def assertFromString(s: String): Identifier =
      assertRight(fromString(s))
  }

  /* Choice name in a template. */
  type ChoiceName = Name
  val ChoiceName = Name

  /* Method name in an interface */
  type MethodName = Name
  val MethodName = Name

  type ModuleName = DottedName
  val ModuleName = DottedName

  /** Reference to a value defined in the specified module. */
  type ValueRef = Identifier
  val ValueRef = Identifier

  /** Reference to a value defined in the specified module. */
  type DefinitionRef = Identifier
  val DefinitionRef = Identifier

  /** Reference to a type constructor. */
  type TypeConName = Identifier
  val TypeConName = Identifier

  /** Reference to a type synonym. */
  type TypeSynName = Identifier
  val TypeSynName = Identifier

  /*
     max size when converted to string: 3068 ASCII chars
   */
  final case class QualifiedChoiceName(interfaceId: Option[Identifier], choiceName: ChoiceName) {
    override def toString: String = interfaceId match {
      case None => choiceName
      case Some(ifaceId) => "#" + ifaceId.toString + "#" + choiceName
    }
  }

  object QualifiedChoiceName {
    def fromString(s: String): Either[String, QualifiedChoiceName] =
      if (s.startsWith("#")) {
        val i = s.indexOf('#', 1)
        if (i < 0)
          Left("Cannot parse qualified choice name")
        else
          for {
            ifaceId <- Identifier.fromString(s.substring(1, i))
            chName <- ChoiceName.fromString(s.substring(i + 1, s.length))
          } yield QualifiedChoiceName(Some(ifaceId), chName)
      } else
        ChoiceName.fromString(s).map(QualifiedChoiceName(None, _))

    def assertFromString(s: String): QualifiedChoiceName =
      assertRight(fromString(s))
  }

}
