// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.implicits.toTraverseOps
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod.Empty
import com.daml.logging.LoggingContext
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, TestSalt}
import com.digitalasset.canton.ledger.api.validation.ValidateUpgradingPackageResolutions.ValidatedCommandPackageResolutionsSnapshot
import com.digitalasset.canton.ledger.api.validation.{
  CommandsValidator,
  ValidateDisclosedContracts,
  ValidateUpgradingPackageResolutions,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NoLogging}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.util.TestEngine.{InMemoryPackageStore, TxAndMeta}
import com.digitalasset.daml.lf.archive
import com.digitalasset.daml.lf.archive.DamlLf
import com.digitalasset.daml.lf.command.ReplayCommand
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{PackageId, ParticipantId, QualifiedName}
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.engine.*
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.transaction.*
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import io.grpc.StatusRuntimeException
import org.scalatest.{EitherValues, OptionValues}

import java.io.File
import java.time.{Duration, Instant}
import scala.annotation.tailrec

/** Allows API commands to be applied directly to the engine.
  */
class TestEngine(
    packagePaths: Seq[String],
    participantId: ParticipantId = ParticipantId.assertFromString("TestParticipantId"),
    userId: String = "TestUserId",
    commandId: String = "TestCmdId",
) extends EitherValues
    with OptionValues {

  private val validateUpgradingPackageResolutions = new ValidateUpgradingPackageResolutions {
    override def apply(rawUserPackageIdPreferences: Seq[String])(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Either[StatusRuntimeException, ValidatedCommandPackageResolutionsSnapshot] =
      Right(
        ValidatedCommandPackageResolutionsSnapshot(
          packageStore.packageMap,
          packageStore.packagePreference,
        )
      )
  }

  private val commandsValidator = new CommandsValidator(
    validateUpgradingPackageResolutions = validateUpgradingPackageResolutions,
    validateDisclosedContracts = ValidateDisclosedContracts.WithContractIdVerificationDisabled,
  )

  val packageStore: InMemoryPackageStore = packagePaths.foldLeft(InMemoryPackageStore()) { (s, p) =>
    s.withDarFile(new File(p)).value
  }

  private implicit val logger: ErrorLoggingContext = NoLogging
  private implicit val context: LoggingContext = LoggingContext.empty

  private val zeroHash = LfHash.assertFromByteArray(Array.ofDim[Byte](Hash.underlyingHashLength))
  val randomHash: () => LfHash = LfHash.secureRandom(zeroHash)

  private val nextSalt: () => Salt = {
    val it = Iterator.from(0)
    () => TestSalt.generateSalt(it.next())
  }

  val cryptoOps: HashOps & HmacOps = new SymbolicPureCrypto()

  val unicumGenerator = new UnicumGenerator(cryptoOps)

  private val testInstant = Instant.now
  private val testTimestamp = Time.Timestamp.assertFromInstant(testInstant)
  private val maxDeduplicationDuration = Duration.ZERO
  private val cantonContractIdVersion = AuthenticatedContractIdVersionV11

  val engine = new Engine(
    EngineConfig(allowedLanguageVersions = LanguageVersion.AllVersions(LanguageMajorVersion.V2))
  )

  private val valueEnricher = new Enricher(engine)

  def consume[T](
      initial: Result[T],
      contracts: Map[ContractId, FatContractInstance] = Map.empty,
  ): T = {
    @tailrec
    def go(need: Result[T]): T =
      need match {
        case ResultDone(result) => result
        case ResultPrefetch(_, _, resume) =>
          go(resume())
        case ResultNeedPackage(packageId, resume) =>
          go(resume(packageStore.getPackage(packageId)))
        case ResultNeedContract(acoid, resume) =>
          val instance: Option[FatContractInstance] = contracts.get(acoid)
          go(resume(ResultNeedContract.wrapLegacyResponse(instance)))
        case other => throw new IllegalStateException(s"Did not expect $other")
      }
    go(initial)
  }

  def validateCommand(
      command: com.daml.ledger.javaapi.data.Command,
      actAs: String,
      disclosedContracts: Seq[FatContractInstance] = Seq.empty,
  ): com.digitalasset.canton.ledger.api.Commands = {

    val protoCommand: com.daml.ledger.api.v2.commands.Command =
      com.daml.ledger.api.v2.commands.Command.fromJavaProto(command.toProtoCommand)

    val commands: com.daml.ledger.api.v2.commands.Commands =
      com.daml.ledger.api.v2.commands.Commands(
        workflowId = "",
        userId = userId,
        commandId = commandId,
        commands = Seq(protoCommand),
        deduplicationPeriod = Empty,
        minLedgerTimeAbs = None,
        minLedgerTimeRel = None,
        actAs = Seq(actAs),
        readAs = Nil,
        submissionId = "",
        disclosedContracts = disclosedContracts.map(disclose),
        synchronizerId = "",
        packageIdSelectionPreference = Nil,
        prefetchContractKeys = Nil,
      )

    val engineCommands: com.digitalasset.canton.ledger.api.Commands =
      commandsValidator
        .validateCommands(
          commands = commands,
          currentLedgerTime = testInstant,
          currentUtcTime = testInstant,
          maxDeduplicationDuration = maxDeduplicationDuration,
        )
        .value

    engineCommands
  }

  def submitAndConsume(
      command: com.daml.ledger.javaapi.data.Command,
      actAs: String,
      disclosedContracts: Seq[FatContractInstance] = Seq.empty,
      storedContracts: Seq[FatContractInstance] = Seq.empty,
  ): (SubmittedTransaction, Transaction.Metadata) = {

    val engineCommands = validateCommand(command, actAs, disclosedContracts)

    val result: Result[TxAndMeta] = engine.submit(
      packageMap = engineCommands.packageMap,
      packagePreference = engineCommands.packagePreferenceSet,
      submitters = Set(Ref.Party.assertFromString(actAs)),
      cmds = engineCommands.commands,
      disclosures = engineCommands.disclosedContracts.map(_.fatContractInstance),
      participantId = participantId,
      submissionSeed = randomHash(),
      readAs = Set.empty,
      prefetchKeys = Seq.empty,
    )

    val contractMap = storedContracts.map(c => c.contractId -> c).toMap

    consume(result, contracts = contractMap)

  }

  def suffix(create: Node.Create): LfFatContractInst = {

    val salt = nextSalt()

    val unicum = unicumGenerator
      .recomputeUnicum(
        contractSalt = salt,
        ledgerCreateTime = CreationTime.CreatedAt(testTimestamp),
        metadata = ContractMetadata.tryCreate(
          create.signatories,
          create.stakeholders,
          create.keyOpt.map(Versioned(create.version, _)),
        ),
        suffixedContractInstance = create.coinst,
        cantonContractIdVersion = cantonContractIdVersion,
      )
      .value

    val discriminator = create.coid.asInstanceOf[LfContractId.V1].discriminator

    val contractId = cantonContractIdVersion.fromDiscriminator(discriminator, unicum)

    val suffixed = create.mapCid(_ => contractId)

    val authenticationData =
      ContractAuthenticationDataV1(salt)(AuthenticatedContractIdVersionV11).toLfBytes

    FatContractInstance.fromCreateNode(
      suffixed,
      CreationTime.CreatedAt(testTimestamp),
      authenticationData,
    )
  }

  def recomputeUnicum(
      fat: FatContractInstance,
      recomputeIdVersion: CantonContractIdV1Version,
  ): Unicum =
    unicumGenerator.recomputeUnicum(fat, recomputeIdVersion).value

  def disclose(fat: FatContractInstance): com.daml.ledger.api.v2.commands.DisclosedContract = {
    val t = fat.templateId
    com.daml.ledger.api.v2.commands.DisclosedContract(
      templateId = Some(
        com.daml.ledger.api.v2.value.Identifier(
          t.packageId,
          t.qualifiedName.module.dottedName,
          t.qualifiedName.name.dottedName,
        )
      ),
      contractId = fat.contractId.coid,
      createdEventBlob = TransactionCoder.encodeFatContractInstance(fat).value,
      synchronizerId = "",
    )
  }

  def reinterpretAndConsume(
      submitters: Set[Ref.Party],
      command: ReplayCommand,
      nodeSeed: Hash,
      contracts: Map[ContractId, FatContractInstance] = Map.empty,
      packageResolution: Map[Ref.PackageName, Ref.PackageId] = Map.empty,
  ): TxAndMeta = {
    val result = engine.reinterpret(
      submitters = submitters,
      command = command,
      nodeSeed = Some(nodeSeed),
      preparationTime = testTimestamp,
      ledgerEffectiveTime = testTimestamp,
      packageResolution = packageResolution,
    )
    consume(result, contracts)
  }

  def reinterpretReplayNode(
      testNodeId: NodeId,
      tx: SubmittedTransaction,
      meta: Transaction.Metadata,
      contracts: Map[ContractId, FatContractInstance] = Map.empty,
  ): (SubmittedTransaction, Transaction.Metadata) = {

    val nodeSeeds = Map.from(meta.nodeSeeds.toList)
    val node = tx.nodes.get(testNodeId).value
    val (replayCommand, submitters) = node match {
      case create: Node.Create =>
        (
          ReplayCommand.Create(
            create.templateId,
            create.arg,
          ),
          create.requiredAuthorizers,
        )
      case ex: Node.Exercise =>
        (
          ReplayCommand.Exercise(
            templateId = ex.templateId,
            interfaceId = ex.interfaceId,
            contractId = ex.targetCoid,
            choiceId = ex.choiceId,
            argument = ex.chosenValue,
          ),
          ex.requiredAuthorizers,
        )
      case other => throw new UnsupportedOperationException(s"Do not support $other")
    }

    val nodeSeed = nodeSeeds(testNodeId)

    val packageResolution = tx.nodes.values
      .collect {
        case ex: Node.Exercise => ex.interfaceId.map(_ => ex.packageName -> ex.templateId.packageId)
        case _ => None
      }
      .flatten
      .toMap

    reinterpretAndConsume(
      submitters = submitters,
      command = replayCommand,
      nodeSeed = nodeSeed,
      contracts = contracts,
      packageResolution = packageResolution,
    )
  }

  def toRefIdentifier(i: com.daml.ledger.javaapi.data.Identifier): Ref.Identifier =
    Ref.Identifier(
      Ref.PackageId.assertFromString(i.getPackageId),
      QualifiedName(
        Ref.ModuleName.assertFromString(i.getModuleName),
        Ref.DottedName.assertFromString(i.getEntityName),
      ),
    )

  def enrichContract(identifier: com.daml.ledger.javaapi.data.Identifier, value: Value): Value =
    consume(valueEnricher.enrichContract(toRefIdentifier(identifier), value), Map.empty)

  def extractAuthenticationData(fat: FatContractInstance): ContractAuthenticationData = {
    val contractIdVersion = CantonContractIdVersion.tryCantonContractIdVersion(fat.contractId)
    ContractAuthenticationData.fromLfBytes(contractIdVersion, fat.authenticationData).value
  }

}

object TestEngine {

  private type TxAndMeta = (SubmittedTransaction, Transaction.Metadata)

  final case class InMemoryPackageStore(
      packages: Map[PackageId, (DamlLf.Archive, Ast.Package)] = Map.empty
  ) {

    val packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] =
      packages.view.mapValues { case (_, p) => (p.metadata.name, p.metadata.version) }.toMap

    val packagePreference: Set[PackageId] = packages.keySet

    def getPackage(packageId: PackageId): Option[Ast.Package] =
      packages.get(packageId).map(_._2)

    def getArchive(packageId: PackageId): Option[DamlLf.Archive] =
      packages.get(packageId).map(_._1)

    def withDarFile(file: File): Either[String, InMemoryPackageStore] =
      for {
        dar <- archive.DarParser
          .readArchiveFromFile(file)
          .left
          .map(t => s"Failed to parse DAR from $file: $t")
        packages <- addArchives(dar.all)
      } yield packages

    private def addArchives(archives: List[DamlLf.Archive]): Either[String, InMemoryPackageStore] =
      archives
        .traverse(proto =>
          try {
            Right((proto, archive.Decode.assertDecodeArchive(proto)._2))
          } catch {
            case err: archive.Error => Left(s"Could not parse archive ${proto.getHash}: $err")
          }
        )
        .map(pkgs =>
          pkgs.foldLeft(this) { case (store, (archive, pkg)) =>
            val pkgId = PackageId.assertFromString(archive.getHash)
            store.addPackage(pkgId, archive, pkg)
          }
        )

    private def addPackage(
        pkgId: PackageId,
        archive: DamlLf.Archive,
        pkg: Ast.Package,
    ): InMemoryPackageStore =
      InMemoryPackageStore(packages + (pkgId -> (archive, pkg)))

  }

}
