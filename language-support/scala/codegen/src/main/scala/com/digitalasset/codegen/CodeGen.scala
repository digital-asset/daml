// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen

import java.io._

import com.daml.codegen.types.Namespace
import com.daml.lf.archive.{ArchivePayload, Dar, UniversalArchiveReader}
import com.daml.lf.iface
import com.daml.lf.data.Ref
import com.daml.lf.iface.{Type => _, _}
import com.daml.lf.iface.reader.{Errors, InterfaceReader}
import com.daml.codegen.dependencygraph._
import com.daml.codegen.exception.PackageInterfaceException
import lf.{DefTemplateWithRecord, LFUtil, ScopedDataType}
import com.daml.lf.data.Ref._
import com.daml.lf.iface.reader.Errors.ErrorLoc
import com.typesafe.scalalogging.Logger
import scalaz.{Enum => _, _}
import scalaz.std.tuple._
import scalaz.std.list._
import scalaz.std.set._
import scalaz.std.string._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.option._
import scalaz.syntax.bind._
import scalaz.syntax.traverse1._

import scala.collection.compat._
import scala.util.matching.Regex

object CodeGen {

  private val logger: Logger = Logger(getClass)

  sealed abstract class Mode extends Serializable with Product
  case object Novel extends Mode

  val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
  import universe._

  import Util.{FilePlan, WriteParams}

  /*
   * Given a Daml package (in DAR or DALF format), a package name and an output
   * directory, this function writes a bunch of generated .scala files
   * to 'outputDir' that mirror the namespace of the Daml package.
   *
   * This function throws exception when an unexpected error happens. Unexpected errors are:
   * - input file not found or not readable
   * - package interface extraction failed
   */
  @throws[FileNotFoundException](cause = "input file not found")
  @throws[SecurityException](cause = "input file not readable")
  @throws[PackageInterfaceException](
    cause = "either decoding a package from a file or extracting" +
      " the package interface failed"
  )
  def generateCode(
      files: List[File],
      packageName: String,
      outputDir: File,
      mode: Mode,
      roots: Seq[String] = Seq(),
  ): Unit =
    files match {
      case Nil =>
        throw PackageInterfaceException("Expected at least one DAR or DALF input file.")
      case f :: fs =>
        generateCodeSafe(NonEmptyList(f, fs: _*), packageName, outputDir, mode, roots)
          .fold(es => throw PackageInterfaceException(formatErrors(es)), identity)
    }

  private def formatErrors(es: NonEmptyList[String]): String =
    es.toList.mkString("\n")

  def generateCodeSafe(
      files: NonEmptyList[File],
      packageName: String,
      outputDir: File,
      mode: Mode,
      roots: Seq[String],
  ): ValidationNel[String, Unit] =
    decodeInterfaces(files).map { ifaces: NonEmptyList[EnvironmentInterface] =>
      val combinedIface: EnvironmentInterface =
        combineEnvInterfaces(ifaces map filterTemplatesBy(roots map (_.r)))
      packageInterfaceToScalaCode(util(mode, packageName, combinedIface, outputDir))
    }

  private def util(
      mode: Mode,
      packageName: String,
      iface: EnvironmentInterface,
      outputDir: File,
  ): Util = mode match {
    case Novel => LFUtil(packageName, iface, outputDir)
  }

  private def decodeInterfaces(
      files: NonEmptyList[File]
  ): ValidationNel[String, NonEmptyList[EnvironmentInterface]] = {
    files.traverse(f => decodeInterface(parseFile(_))(f).validationNel)
  }

  private def parseFile(f: File): String \/ Dar[ArchivePayload] =
    UniversalArchiveReader.readFile(f) match {
      case Right(p) => \/.right(p)
      case Left(e) =>
        logger.error("Scala Codegen error", e)
        \/.left(e.getLocalizedMessage)
    }

  private def decodeInterface(parse: File => String \/ Dar[ArchivePayload])(
      file: File
  ): String \/ EnvironmentInterface =
    parse(file).flatMap(decodeInterface)

  private def decodeInterface(dar: Dar[ArchivePayload]): String \/ EnvironmentInterface = {
    import scalaz.syntax.traverse._
    dar.traverse(decodeInterface).map(combineInterfaces)
  }

  private def decodeInterface(p: ArchivePayload): String \/ Interface =
    \/.attempt {
      logger.info(s"decoding archive with Package ID: ${p.pkgId}")
      val (errors, out) = Interface.read(p)
      (if (!errors.empty) {
         -\/(formatDecodeErrors(p.pkgId, errors))
       } else \/-(out)): String \/ Interface
    }(_.getLocalizedMessage).join

  private def formatDecodeErrors(
      packageId: PackageId,
      errors: Errors[ErrorLoc, InterfaceReader.InvalidDataTypeDefinition],
  ): String =
    (Cord(s"Errors decoding LF archive (Package ID: $packageId):\n") ++
      InterfaceReader.InterfaceReaderError.treeReport(errors)).toString

  private def combineInterfaces(dar: Dar[Interface]): EnvironmentInterface =
    EnvironmentInterface.fromReaderInterfaces(dar)

  private def combineEnvInterfaces(as: NonEmptyList[EnvironmentInterface]): EnvironmentInterface =
    as.suml1

  // Template names can be filtered by given regexes (default: use all templates)
  // If a template does not match any regex, it becomes a "normal" datatype.
  private[codegen] def filterTemplatesBy(
      regexes: Seq[Regex]
  )(ei: EnvironmentInterface): EnvironmentInterface = {

    def matchesRoots(qualName: Ref.Identifier): Boolean =
      regexes.exists(_.findFirstIn(qualName.qualifiedName.qualifiedName).isDefined)
    // scala-2.13-M4: _.matches(qualName.qualifiedName.qualifiedName)

    if (regexes.isEmpty) ei
    else {
      ei.copy(typeDecls = ei.typeDecls transform {
        case (id, tpl @ InterfaceType.Template(_, _)) if !matchesRoots(id) =>
          InterfaceType.Normal(tpl.`type`)
        case (_, other) => other
      })
    }
  }

  private def packageInterfaceToScalaCode(util: Util): Unit = {
    val interface = util.iface

    val orderedDependencies
        : OrderedDependencies[Identifier, TypeDeclOrTemplateWrapper[util.TemplateInterface]] =
      util.orderedDependencies(interface)
    val (templateIds, typeDeclsToGenerate): (
        Map[Identifier, util.TemplateInterface],
        List[ScopedDataType.FWT],
    ) = {

      /* Here we collect templates and the
       * [[TypeDecl]]s without generating code for them.
       */
      val templateIdOrTypeDecls
          : List[(Identifier, util.TemplateInterface) Either ScopedDataType.FWT] =
        orderedDependencies.deps.toList.flatMap {
          case (templateId, Node(TypeDeclWrapper(typeDecl), _, _)) =>
            Seq(Right(ScopedDataType.fromDefDataType(templateId, typeDecl)))
          case (templateId, Node(TemplateWrapper(templateInterface), _, _)) =>
            Seq(Left((templateId, templateInterface)))
        }

      templateIdOrTypeDecls.partitionMap(identity).leftMap(_.toMap)
    }

    // Each record/variant has Scala code generated for it individually, unless their names are related
    writeTemplatesAndTypes(util)(WriteParams(templateIds, typeDeclsToGenerate))

    logger.info(
      s"""Scala Codegen result:
          |Number of generated templates: ${templateIds.size}
          |Number of not generated templates: ${util
        .templateCount(interface) - templateIds.size}
          |Details: ${orderedDependencies.errors.map(_.msg).mkString("\n")}""".stripMargin
    )
  }

  private[codegen] def produceTemplateAndTypeFilesLF(
      wp: WriteParams[DefTemplateWithRecord.FWT],
      util: lf.LFUtil,
  ): IterableOnce[FilePlan] = {
    import wp._

    // New prep steps for LF codegen
    // 1. collect records, search variants and splat/filter
    val (unassociatedRecords, splattedVariants, enums) = splatVariants(definitions)

    // 2. put templates/types into single Namespace.fromHierarchy
    val treeified: Namespace[String, Option[lf.HierarchicalOutput.TemplateOrDatatype]] =
      Namespace.fromHierarchy {
        def widenDDT[R, V](iddt: Iterable[ScopedDataType.DT[R, V]]) = iddt
        import lf.DamlDataTypeGen.DataType
        type SrcV = DefTemplateWithRecord.FWT \/ DataType
        val ntdRights =
          (widenDDT(unassociatedRecords ++ enums) ++ splattedVariants)
            .map(sdt => (sdt.name, \/-(sdt): SrcV))
        val tmplLefts = templateIds.transform((_, v) => -\/(v): SrcV)

        (ntdRights ++ tmplLefts) map { case (ddtIdent @ Identifier(_, qualName), body) =>
          (qualName.module.segments.toList ++ qualName.name.segments.toList, (ddtIdent, body))
        }
      }

    // fold up the tree to discover the hierarchy's roots, each of which produces a file
    val (treeErrors, topFiles) = lf.HierarchicalOutput.discoverFiles(treeified, util)
    val filePlans = topFiles.map { case (fil, trees) =>
      \/-((None, fil, trees)): FilePlan
    } ++ treeErrors
      .map(e => -\/(e): FilePlan)

    // Finally we generate the "event decoder" and "package ID source"
    val specials =
      Seq(lf.EventDecoderGen.generate(util, templateIds.keySet), lf.PackageIDsGen.generate(util))

    val specialPlans = specials map { case (fp, t) => \/-((None, fp, t)): FilePlan }

    filePlans ++ specialPlans
  }

  private[this] def splitNTDs[RT, VT](definitions: List[ScopedDataType.DT[RT, VT]]): (
      List[ScopedDataType[Record[RT]]],
      List[ScopedDataType[Variant[VT]]],
      List[ScopedDataType[Enum]],
  ) = {

    val (recordAndVariants, enums) = definitions.partitionMap {
      case sdt @ ScopedDataType(_, _, ddt) =>
        ddt match {
          case r: Record[RT] =>
            Left(Left(sdt copy (dataType = r)))
          case v: Variant[VT] =>
            Left(Right(sdt copy (dataType = v)))
          case e: Enum =>
            Right(sdt copy (dataType = e))
          case _: Iface =>
            sys.error("Interfaces are not supported")
        }
    }

    val (records, variants) = recordAndVariants.partitionMap(identity)

    (records, variants, enums)
  }

  /** Replace every VT that refers to some apparently-nominalized record
    * type in the argument list with the fields of that record, and drop
    * those records that arose from nominalization.
    *
    * The nature of each variant data constructor ("field") can be
    * figured by examining the _2: left means splatted, right means
    * unchanged.
    */
  private[this] def splatVariants[RT <: iface.Type, VT <: iface.Type](
      definitions: List[ScopedDataType.DT[RT, VT]]
  ): (
      List[ScopedDataType[Record[RT]]],
      List[ScopedDataType[Variant[List[(Ref.Name, RT)] \/ VT]]],
      List[ScopedDataType[Enum]],
  ) = {
    type VariantField = List[(Ref.Name, RT)] \/ VT

    val (records, variants, enums) = splitNTDs(definitions)

    val recordMap: Map[(ScopedDataType.Name, List[Ref.Name]), ScopedDataType[Record[RT]]] =
      records.view.map { case ddt @ ScopedDataType(name, vars, _) =>
        (name -> vars.toList) -> ddt
      }.toMap

    val noDeletion = Set.empty[(Identifier, List[Ref.Name])]
    val (deletedRecords, newVariants) =
      variants.toList.traverse {
        case ScopedDataType(ident @ Identifier(packageId, qualName), vTypeVars, Variant(fields)) =>
          val typeVarDelegate = Util simplyDelegates vTypeVars
          val (deleted, sdt) = fields.traverse { case (vn, vt) =>
            val syntheticRecord = Identifier(
              packageId,
              qualName copy (name =
                DottedName.assertFromSegments(qualName.name.segments.slowSnoc(vn).toSeq)),
            )
            val key = (syntheticRecord, vTypeVars.toList)
            typeVarDelegate(vt)
              .filter((_: Identifier) == syntheticRecord)
              .flatMap(_ => recordMap get key)
              .cata(
                nr => (Set(key), (vn, -\/(nr.dataType.fields.toList): VariantField)),
                (noDeletion, (vn, \/-(vt): VariantField)),
              )
          }
          (deleted, ScopedDataType(ident, vTypeVars, Variant(sdt)))
      }

    ((recordMap -- deletedRecords).values.toList, newVariants, enums.toList)
  }

  private[this] def writeTemplatesAndTypes(
      util: Util
  )(wp: WriteParams[util.TemplateInterface]): Unit = {
    util.templateAndTypeFiles(wp).iterator.foreach {
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
