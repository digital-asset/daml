// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import com.digitalasset.codegen.types.Namespace
import com.digitalasset.daml.lf.{Dar, UniversalArchiveReader, iface}
import iface.{Type => _, _}
import com.digitalasset.daml.lf.iface.reader.{Errors, Interface, InterfaceReader}
import java.io._

import scala.collection.breakOut
import com.digitalasset.codegen.dependencygraph._
import com.digitalasset.codegen.exception.PackageInterfaceException
import com.digitalasset.codegen.lf.EnvironmentInterface.environmentInterfaceSemigroup
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import lf.{DefTemplateWithRecord, EnvironmentInterface, LFUtil, ScopedDataType}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.iface.reader.Errors.ErrorLoc
import com.digitalasset.daml_lf.DamlLf
import com.typesafe.scalalogging.Logger
import scalaz._
import scalaz.std.tuple._
import scalaz.std.list._
import scalaz.std.set._
import scalaz.std.string._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.option._
import scalaz.syntax.bind._
import scalaz.syntax.traverse1._

import scala.util.{Failure, Success}

object CodeGen {

  private val logger: Logger = Logger(getClass)

  type Payload = (PackageId, DamlLf.ArchivePayload)

  sealed abstract class Mode extends Serializable with Product
  case object Novel extends Mode

  val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
  import universe._

  import Util.{FilePlan, WriteParams, partitionEithers}

  /*
   * Given a DAML package (in DAR or DALF format), a package name and an output
   * directory, this function writes a bunch of generated .scala files
   * to 'outputDir' that mirror the namespace of the DAML package.
   *
   * This function throws exception when an unexpected error happens. Unexpected errors are:
   * - input file not found or not readable
   * - package interface extraction failed
   */
  @throws[FileNotFoundException](cause = "input file not found")
  @throws[SecurityException](cause = "input file not readable")
  @throws[PackageInterfaceException](
    cause = "either decoding a package from a file or extracting" +
      " the package interface failed")
  def generateCode(files: List[File], packageName: String, outputDir: File, mode: Mode): Unit =
    files match {
      case Nil =>
        throw PackageInterfaceException("Expected at least one DAR or DALF input file.")
      case f :: fs =>
        generateCodeSafe(NonEmptyList(f, fs: _*), packageName, outputDir, mode)
          .fold(es => throw PackageInterfaceException(formatErrors(es)), identity)
    }

  private def formatErrors(es: NonEmptyList[String]): String =
    es.toList.mkString("\n")

  def generateCodeSafe(
      files: NonEmptyList[File],
      packageName: String,
      outputDir: File,
      mode: Mode): ValidationNel[String, Unit] =
    decodeInterfaces(files).map { ifaces: NonEmptyList[EnvironmentInterface] =>
      val combinedIface: EnvironmentInterface = combineEnvInterfaces(ifaces)
      packageInterfaceToScalaCode(util(mode, packageName, combinedIface, outputDir))
    }

  private def util(
      mode: Mode,
      packageName: String,
      iface: EnvironmentInterface,
      outputDir: File): Util = mode match {
    case Novel => LFUtil(packageName, iface, outputDir)
  }

  private def decodeInterfaces(
      files: NonEmptyList[File]): ValidationNel[String, NonEmptyList[EnvironmentInterface]] = {
    val reader = UniversalArchiveReader()
    val parse: File => String \/ Dar[Payload] = parseFile(reader)
    files.traverseU(f => decodeInterface(parse)(f).validationNel)
  }

  private def parseFile(reader: UniversalArchiveReader[Payload])(f: File): String \/ Dar[Payload] =
    reader.readFile(f) match {
      case Success(p) => \/.right(p)
      case Failure(e) =>
        logger.error("Scala Codegen error", e)
        \/.left(e.getLocalizedMessage)
    }

  private def decodeInterface(parse: File => String \/ Dar[Payload])(
      file: File): String \/ EnvironmentInterface =
    parse(file).flatMap(decodeInterface)

  private def decodeInterface(dar: Dar[Payload]): String \/ EnvironmentInterface = {
    import scalaz.syntax.traverse._
    dar.traverseU(decodeInterface).map(combineInterfaces)
  }

  private def decodeInterface(p: Payload): String \/ Interface =
    \/.fromTryCatchNonFatal {
      val packageId: PackageId = p._1
      logger.info(s"decoding archive with Package ID: ${packageId.underlyingString: String}")
      val (errors, out) = Interface.read(p)
      if (!errors.empty) {
        \/.left(formatDecodeErrors(packageId, errors))
      } else \/.right(out)
    }.leftMap(_.getLocalizedMessage).join

  private def formatDecodeErrors(
      packageId: PackageId,
      errors: Errors[ErrorLoc, InterfaceReader.InvalidDataTypeDefinition]): String =
    (Cord(s"Errors decoding LF archive (Package ID: ${packageId.underlyingString: String}):\n") ++
      InterfaceReader.InterfaceReaderError.treeReport(errors)).toString

  private def combineInterfaces(dar: Dar[Interface]): EnvironmentInterface =
    EnvironmentInterface.fromReaderInterfaces(dar)

  private def combineEnvInterfaces(as: NonEmptyList[EnvironmentInterface]): EnvironmentInterface = {
    val z = EnvironmentInterface(Map.empty)
    as.foldLeft(z)((a1, a2) => environmentInterfaceSemigroup.append(a1, a2))
  }

  private def packageInterfaceToScalaCode(util: Util): Unit = {
    val interface = util.iface

    val orderedDependencies
      : OrderedDependencies[Identifier, TypeDeclOrTemplateWrapper[util.TemplateInterface]] =
      util.orderedDependencies(interface)
    val (supportedTemplateIds, typeDeclsToGenerate): (
        Map[Identifier, util.TemplateInterface],
        Vector[ScopedDataType.FWT]) = {

      /* Here we collect templates and the
       * [[TypeDecl]]s without generating code for them.
       */
      val templateIdOrTypeDecls
        : Vector[(Identifier, util.TemplateInterface) Either ScopedDataType.FWT] =
        orderedDependencies.deps.flatMap {
          case (templateId, Node(TypeDeclWrapper(typeDecl), _, _)) =>
            Seq(Right(ScopedDataType fromDefDataType (templateId, typeDecl)))
          case (templateId, Node(TemplateWrapper(templateInterface), _, _)) =>
            Seq(Left((templateId, templateInterface)))
        }

      partitionEithers(templateIdOrTypeDecls).leftMap(_.toMap)
    }

    // Each record/variant has Scala code generated for it individually, unless their names are related
    writeTemplatesAndTypes(util)(WriteParams(supportedTemplateIds, typeDeclsToGenerate))

    logger.info(
      s"""Scala Codegen result:
          |Number of generated templates: ${supportedTemplateIds.size}
          |Number of not generated templates: ${util
           .templateCount(interface) - supportedTemplateIds.size}
          |Details: ${orderedDependencies.errors.map(_.msg).mkString("\n")}""".stripMargin
    )
  }

  private[codegen] def produceTemplateAndTypeFilesLF(
      wp: WriteParams[DefTemplateWithRecord.FWT],
      util: lf.LFUtil): TraversableOnce[FilePlan] = {
    import wp._

    // New prep steps for LF codegen
    // 1. collect records, search variants and splat/filter
    val (unassociatedRecords, splattedVariants) = splatVariants(recordsAndVariants)

    // 2. put templates/types into single Namespace.fromHierarchy
    val treeified: Namespace[String, Option[lf.HierarchicalOutput.TemplateOrDatatype]] =
      Namespace.fromHierarchy {
        def widenDDT[R, V](iddt: Iterable[ScopedDataType.DT[R, V]]) = iddt
        val ntdRights =
          (widenDDT(unassociatedRecords.map {
            case ((q, tp), rec) => ScopedDataType(q, ImmArraySeq(tp: _*), rec)
          }) ++ splattedVariants)
            .map(sdt => (sdt.name, \/-(sdt)))
        val tmplLefts = supportedTemplateIds.transform((_, v) => -\/(v))
        (ntdRights ++ tmplLefts) map {
          case (ddtIdent @ Identifier(_, qualName), body) =>
            (qualName.module.segments.toList ++ qualName.name.segments.toList, (ddtIdent, body))
        }
      }

    // fold up the tree to discover the hierarchy's roots, each of which produces a file
    val (treeErrors, topFiles) = lf.HierarchicalOutput.discoverFiles(treeified, util)
    val filePlans = topFiles.map { case (fil, trees) => \/-((None, fil, trees)) } ++ treeErrors
      .map(-\/(_))

    // Finally we generate the "event decoder" and "package ID source"
    val specials =
      Seq(
        lf.EventDecoderGen.generate(util, supportedTemplateIds.keySet),
        lf.PackageIDsGen.generate(util))

    val specialPlans = specials map { case (fp, t) => \/-((None, fp, t)) }

    filePlans ++ specialPlans
  }

  type LHSIndexedRecords[+RT] = Map[(Identifier, List[String]), Record[RT]]

  private[this] def splitNTDs[RT, VT](recordsAndVariants: Iterable[ScopedDataType.DT[RT, VT]])
    : (LHSIndexedRecords[RT], List[ScopedDataType[Variant[VT]]]) =
    partitionEithers(recordsAndVariants map {
      case sdt @ ScopedDataType(qualName, typeVars, ddt) =>
        ddt match {
          case r: Record[RT] => Left(((qualName, typeVars.toList), r))
          case v: Variant[VT] => Right(sdt copy (dataType = v))
        }
    })(breakOut, breakOut)

  /** Replace every VT that refers to some apparently-nominalized record
    * type in the argument list with the fields of that record, and drop
    * those records that arose from nominalization.
    *
    * The nature of each variant data constructor ("field") can be
    * figured by examining the _2: left means splatted, right means
    * unchanged.
    */
  private[this] def splatVariants[RT <: iface.Type, VT <: iface.Type](
      recordsAndVariants: Iterable[ScopedDataType.DT[RT, VT]])
    : (LHSIndexedRecords[RT], List[ScopedDataType[Variant[List[(String, RT)] \/ VT]]]) = {

    val (recordMap, variants) = splitNTDs(recordsAndVariants)

    val noDeletion = Set.empty[(Identifier, List[String])]
    // both traverseU can change to traverse with -Ypartial-unification
    // or Scala 2.13
    val (deletedRecords, newVariants) =
      variants.traverseU {
        case ScopedDataType(ident @ Identifier(packageId, qualName), vTypeVars, Variant(fields)) =>
          val typeVarDelegate = Util simplyDelegates vTypeVars
          val (deleted, sdt) = fields.traverseU {
            case (vn, vt) =>
              val syntheticRecord = Identifier(
                packageId,
                qualName copy (name =
                  DottedName.assertFromSegments(qualName.name.segments.slowSnoc(vn).toSeq)))
              val key = (syntheticRecord, vTypeVars.toList)
              typeVarDelegate(vt)
                .filter((_: Identifier) == syntheticRecord)
                .flatMap(_ => recordMap get key)
                .cata(nr => (Set(key), (vn, -\/(nr.fields.toList))), (noDeletion, (vn, \/-(vt))))
          }
          (deleted, ScopedDataType(ident, vTypeVars, Variant(sdt)))
      }

    (recordMap -- deletedRecords, newVariants)
  }

  private[this] def writeTemplatesAndTypes(util: Util)(
      wp: WriteParams[util.TemplateInterface]): Unit = {
    util.templateAndTypeFiles(wp) foreach {
      case -\/(msg) => logger.debug(msg)
      case \/-((msg, filePath, trees)) =>
        msg foreach (m => logger.debug(m))
        writeCode(filePath, trees)
    }
  }

  private def writeCode(filePath: File, trees: Iterable[Tree]): Unit =
    if (trees.nonEmpty) {
      filePath.getParentFile.mkdirs()
      val writer = new PrintWriter(filePath)
      try {
        writer.println(Util.autoGenerationHeader)
        trees.foreach(tree => writer.println(showCode(tree)))
      } finally {
        writer.close()
      }
    } else {
      logger.warn(s"WARNING: nothing to generate, empty trees passed, file: $filePath")
    }
}
