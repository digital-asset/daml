// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package data

import com.daml.scalautil.Statement.discard
import scalaz.Scalaz.eitherMonad
import scalaz.syntax.traverse._
import scalaz.std.list._

object Ref {

  val IdString: IdString = new IdStringImpl

  type Name = IdString.Name
  val Name: IdString.Name.type = IdString.Name

  /* Encoding of byte array */
  type HexString = IdString.HexString
  val HexString: IdString.HexString.type = IdString.HexString

  type PackageName = IdString.PackageName
  val PackageName: IdString.PackageName.type = IdString.PackageName

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

  /** Identifiers for participant node users, which act as clients to the Ledger API.
    *
    * The concept of participant node users has been introduced after the concept
    * of client applications, which is why the code contains both of them.
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

  final case class FullReference[X](pkg: X, qualifiedName: QualifiedName) {

    override def toString = s"$pkg:${qualifiedName.toString}"

    // to keep backward compatibility of Identifier#packageId
    def packageId(implicit ev: X <:< PackageId): PackageId = ev(pkg)

    // TODO: https://github.com/digital-asset/daml/issues/17995
    //   drop this method
    def assertToTypeConId(implicit ev: X <:< PackageRef): TypeConId = ev(pkg) match {
      case PackageRef.Id(id) =>
        copy(pkg = id)
      case PackageRef.Name(_) =>
        throw new IllegalArgumentException("call by package name is not supported")
    }

    def toRef(implicit ev: X <:< PackageId): TypeConRef =
      copy(pkg = PackageRef.Id(pkg))
  }

  object FullReference {
    implicit def ordering[X: Ordering]: Ordering[FullReference[X]] =
      Ordering.by(f => (f.pkg, f.qualifiedName))
  }

  sealed abstract class FullReferenceCompanion[X] {

    def pkgFromString(s: String): Either[String, X]

    final def fromString(s: String): Either[String, FullReference[X]] =
      splitInTwo(s, ':') match {
        case Some((packageString, qualifiedNameString)) =>
          for {
            pkgRef <- pkgFromString(packageString)
            qualifiedName <- QualifiedName.fromString(qualifiedNameString)
          } yield FullReference(pkgRef, qualifiedName)
        case None =>
          Left(s"Separator ':' between package identifier and qualified name not found in $s")
      }

    @throws[IllegalArgumentException]
    final def assertFromString(s: String): FullReference[X] = assertRight(fromString(s))

    final def apply(pkg: X, qualifiedName: QualifiedName): FullReference[X] =
      FullReference(pkg, qualifiedName)

    final def unapply(f: FullReference[X]): Some[(X, QualifiedName)] = Some(
      (f.pkg, f.qualifiedName)
    )
  }

  type Identifier = FullReference[PackageId]
  object Identifier extends FullReferenceCompanion[PackageId] {
    override def pkgFromString(s: String): Either[String, PackageId] = Ref.PackageId.fromString(s)
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
  type TypeConId = Identifier
  val TypeConId = Identifier

  /** Reference to a type synonym. */
  type TypeSynId = Identifier
  val TypeSynId = Identifier

  sealed abstract class PackageRef extends Product with Serializable
  object PackageRef {
    final case class Name(name: PackageName) extends PackageRef {
      override def toString: String = "#" + name
    }

    final case class Id(id: PackageId) extends PackageRef {
      override def toString: String = id
    }

    def fromString(s: String): Either[String, PackageRef] =
      if (s.startsWith("#"))
        PackageName.fromString(s.drop(1)).map(Name)
      else
        PackageId.fromString(s).map(Id)

    def assertFromString(s: String): PackageRef =
      assertRight(fromString(s))
  }

  type TypeConRef = FullReference[PackageRef]
  val TypeConRef = new FullReferenceCompanion[PackageRef] {
    override def pkgFromString(s: String): Either[String, PackageRef] = PackageRef.fromString(s)
  }

  type IdTypeConRef = FullReference[PackageRef.Id]
  type NameTypeConRef = FullReference[PackageRef.Name]

  object NameTypeConRef extends FullReferenceCompanion[PackageRef.Name] {
    override def pkgFromString(s: String): Either[String, PackageRef.Name] =
      PackageRef.fromString(s).flatMap {
        case name: PackageRef.Name => Right(name)
        case _: PackageRef.Id => Left("Expected PackageRef.Name, but got PackageRef.Id")
      }
  }

  final case class FullIdentifier(
      pkgId: PackageId,
      pkgName: PackageName,
      qualifiedName: QualifiedName,
  ) {
    def toIdentifier: Identifier = FullReference(pkgId, qualifiedName)
    def toIdTypeConRef: IdTypeConRef = FullReference(PackageRef.Id(pkgId), qualifiedName)
    def toNameTypeConRef: NameTypeConRef = FullReference(PackageRef.Name(pkgName), qualifiedName)
  }

  object FullIdentifier {
    def fromIdentifier(identifier: Identifier, pkgName: PackageName): FullIdentifier =
      FullIdentifier(identifier.pkg, pkgName, identifier.qualifiedName)
    def fromIdTypeConRef(idTypeConRef: IdTypeConRef, pkgName: PackageName): FullIdentifier =
      FullIdentifier(idTypeConRef.pkg.id, pkgName, idTypeConRef.qualifiedName)
    def fromNameTypeConRef(nameTypeConRef: NameTypeConRef, pkgId: PackageId): FullIdentifier =
      FullIdentifier(pkgId, nameTypeConRef.pkg.name, nameTypeConRef.qualifiedName)
  }

  implicit class IdentifierConverter(identifier: Identifier) {
    def toFullIdentifier(pkgName: PackageName): FullIdentifier =
      FullIdentifier.fromIdentifier(identifier, pkgName)
  }
  implicit class IdTypeConRefConverter(idTypeConRef: IdTypeConRef) {
    def toFullIdentifier(pkgName: PackageName): FullIdentifier =
      FullIdentifier.fromIdTypeConRef(idTypeConRef, pkgName)
  }
  implicit class NameTypeConRefConverter(nameTypeConRef: NameTypeConRef) {
    def toFullIdentifier(pkgId: PackageId): FullIdentifier =
      FullIdentifier.fromNameTypeConRef(nameTypeConRef, pkgId)
  }

  /*
     max size when converted to string: 3068 ASCII chars
   */
  final case class QualifiedChoiceId(interfaceId: Option[Identifier], choiceName: ChoiceName) {
    override def toString: String = interfaceId match {
      case None => choiceName
      case Some(ifaceId) => "#" + ifaceId.toString + "#" + choiceName
    }
  }

  object QualifiedChoiceId {
    def fromString(s: String): Either[String, QualifiedChoiceId] =
      if (s.startsWith("#")) {
        val i = s.indexOf('#', 1)
        if (i < 0)
          Left("Cannot parse qualified choice name")
        else
          for {
            ifaceId <- Identifier.fromString(s.substring(1, i))
            chName <- ChoiceName.fromString(s.substring(i + 1, s.length))
          } yield QualifiedChoiceId(Some(ifaceId), chName)
      } else
        ChoiceName.fromString(s).map(QualifiedChoiceId(None, _))

    def assertFromString(s: String): QualifiedChoiceId =
      assertRight(fromString(s))
  }

  /** Package versions are non-empty strings consisting of segments of digits (without leading zeros)
    *      separated by dots: "(0|[1-9][0-9]*)(\.(0|[1-9][0-9]*))*"
    */
  final case class PackageVersion private[data] (segments: ImmArray[Int])
      extends Ordered[PackageVersion] {
    override def toString: String = segments.toSeq.mkString(".")

    override def compare(that: PackageVersion): Int = {
      import scala.math.Ordering.Implicits._

      implicitly[Ordering[Seq[Int]]].compare(segments.toSeq, that.segments.toSeq)
    }
  }

  object PackageVersion {
    private val MaxPackageVersionLength = 255
    def fromString(s: String): Either[String, PackageVersion] = for {
      _ <- Either.cond(
        s.length <= MaxPackageVersionLength,
        (),
        s"Package version string length (${s.length}) exceeds the maximum supported length ($MaxPackageVersionLength)",
      )
      _ <- Either.cond(
        s.matches("(0|[1-9][0-9]*)(\\.(0|[1-9][0-9]*))*"),
        (),
        s"Invalid package version string: `$s`. Package versions are non-empty strings consisting of segments of digits (without leading zeros) separated by dots.",
      )
      rawSegments = s.split("\\.").toList
      segments <- rawSegments.traverse(rawSegmentStr =>
        rawSegmentStr.toIntOption.toRight(s"Failed parsing $rawSegmentStr as an integer")
      )
    } yield PackageVersion(ImmArray.from(segments))

    @throws[IllegalArgumentException]
    def assertFromString(s: String): PackageVersion = assertRight(fromString(s))

    def fromInts(s: Seq[Int]): Either[String, PackageVersion] = {
      val ver = PackageVersion(ImmArray.from(s))
      fromString(ver.toString)
    }

    @throws[IllegalArgumentException]
    def assertFromInts(s: Seq[Int]): PackageVersion = assertRight(fromInts(s))
  }
}
