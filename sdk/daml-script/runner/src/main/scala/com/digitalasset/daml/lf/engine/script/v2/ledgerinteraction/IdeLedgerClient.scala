// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package script
package v2
package ledgerinteraction

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ledger.api._
import com.digitalasset.canton.ledger.localstore.InMemoryUserManagementStore
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.daml.lf.command.{ApiCommand, ApiContractKey}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.ScriptEngine.{
  ExtendedValueComputationMode,
  TraceLog,
  WarningLog,
  runExtendedValueComputation,
}
import com.digitalasset.daml.lf.interpretation.Error.ContractIdInContractKey
import com.digitalasset.daml.lf.language.Ast.PackageMetadata
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion, LookupError, Reference}
import com.digitalasset.daml.lf.script
import com.digitalasset.daml.lf.script.{IdeLedger, IdeLedgerRunner}
import com.digitalasset.daml.lf.speedy.{Pretty, SError}
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import org.apache.pekko.stream.Materializer
import scalaz.OneAnd
import scalaz.OneAnd._
import scalaz.std.set._
import scalaz.syntax.foldable._

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

// Client for the script service.
class IdeLedgerClient(
    val originalCompiledPackages: PureCompiledPackages,
    traceLog: TraceLog,
    warningLog: WarningLog,
    canceled: () => Boolean,
    namedLoggerFactory: NamedLoggerFactory,
) extends ScriptLedgerClient {
  override def transport = "script service"

  private val nextSeed: () => crypto.Hash =
    // We seeds to secureRandom with a fix seed to get deterministic sequences of seeds
    // across different runs of IdeLedgerClient.
    crypto.Hash.secureRandom(crypto.Hash.hashPrivateKey(s"script-service"))

  private var _currentSubmission: Option[IdeLedgerRunner.CurrentSubmission] = None

  def currentSubmission: Option[IdeLedgerRunner.CurrentSubmission] = _currentSubmission

  private[this] var compiledPackages = originalCompiledPackages

  private[this] var preprocessor = makePreprocessor

  private[this] var unvettedPackages: Set[PackageId] = Set.empty

  private[this] def makePreprocessor =
    new preprocessing.CommandPreprocessor(
      compiledPackages.pkgInterface,
      forbidLocalContractIds = true,
    )

  val enricher = new Enricher(
    compiledPackages = compiledPackages,
    // Cannot load packages in GrpcLedgerClient
    loadPackage = { (_: PackageId, _: Reference) => ResultDone(()) },
    addTypeInfo = true,
    addFieldNames = true,
    addTrailingNoneFields = true,
    forbidLocalContractIds = true,
  )

  // Given a set of disabled packages, filter out all definitions from those packages from the original compiled packages
  // Similar logic to Script-services' Context.scala, however here we make no changes on the module level, and never directly add new packages
  // We only maintain a subset of an original known package set.
  private[this] def updateCompiledPackages() = {
    compiledPackages =
      if (!unvettedPackages.isEmpty)
        new PureCompiledPackages(
          signatures = originalCompiledPackages.signatures -- unvettedPackages,
          definitions = originalCompiledPackages.definitions.filterNot { case (ref, _) =>
            unvettedPackages(ref.packageId)
          },
          compilerConfig = originalCompiledPackages.compilerConfig,
        )
      else
        originalCompiledPackages
    preprocessor = makePreprocessor
  }

  // Returns false if the package is unknown, leaving the packageId unmodified
  // so later computation can throw a better formulated error
  def packageSupportsUpgrades(packageId: PackageId): Boolean =
    compiledPackages.pkgInterface
      .lookupPackage(packageId)
      .fold(
        _ => false,
        pkgSig => LanguageVersion.featurePackageUpgrades.enabledIn(pkgSig.languageVersion),
      )

  private var _ledger: IdeLedger = IdeLedger.initialLedger(Time.Timestamp.Epoch)
  def ledger: IdeLedger = _ledger

  private var allocatedParties: Map[String, PartyDetails] = Map()

  private val userManagementStore =
    new InMemoryUserManagementStore(createAdmin = false, namedLoggerFactory)

  private[this] def blob(contract: FatContractInstance): Bytes =
    Bytes.fromByteString(TransactionCoder.encodeFatContractInstance(contract).toOption.get)

  private[this] def blob(create: Node.Create, createAt: Time.Timestamp): Bytes =
    blob(FatContractInstance.fromCreateNode(create, CreationTime.CreatedAt(createAt), Bytes.Empty))

  override def query(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Seq[ScriptLedgerClient.ActiveContract]] = {
    val acs = ledger.query(
      actAs = Set.empty,
      readAs = parties.toSet,
      effectiveAt = ledger.currentTime,
    )
    val filtered = acs.collect {
      case IdeLedger.LookupOk(contract)
          if contract.templateId == templateId && parties.any(contract.stakeholders.contains(_)) =>
        ScriptLedgerClient.ActiveContract(
          contract.templateId,
          contract.contractId,
          contract.createArg,
          blob(contract),
        )
    }
    Future.successful(filtered)
  }

  private def lookupContractInstance(
      parties: OneAnd[Set, Ref.Party],
      cid: ContractId,
  ): Option[FatContractInstance] = {

    ledger.lookupGlobalContract(
      actAs = Set.empty,
      readAs = parties.toSet,
      effectiveAt = ledger.currentTime,
      coid = cid,
    ) match {
      case IdeLedger.LookupOk(contract) if parties.any(contract.stakeholders.contains(_)) =>
        Some(contract)
      case _ =>
        // Note that contrary to `fetch` in a script, we do not
        // abort on any of the error cases. This makes sense if you
        // consider this a wrapper around the ACS endpoint where
        // we cannot differentiate between visibility errors
        // and the contract not being active.
        None
    }
  }

  // Maps a result exception to a converter exception and throws it
  def failResultAsConverterException[X](res: Result[X]): X = {
    import com.digitalasset.daml.lf.script.converter.ConverterException
    res.consume().fold(err => throw new ConverterException(err.toString), identity)
  }

  override def queryContractId(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      cid: ContractId,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[ScriptLedgerClient.ActiveContract]] =
    Future {
      lookupContractInstance(parties, cid) match {
        case None => None
        case Some(contract) => {
          val arg =
            failResultAsConverterException(enricher.enrichContract(templateId, contract.createArg))
          Some(ScriptLedgerClient.ActiveContract(templateId, cid, arg, blob(contract)))
        }
      }
    }

  private[this] def computeView(
      templateId: TypeConId,
      interfaceId: TypeConId,
      arg: Value,
  ): Option[Value] =
    runExtendedValueComputation(
      computationMode = ExtendedValueComputationMode.ByInterfaceView(templateId, interfaceId, arg),
      cancelled = () => None,
      compiledPackages = compiledPackages,
      iterationsBetweenInterruptions = 100000,
      traceLog = traceLog,
      warningLog = warningLog,
      convertLegacyExceptions = false,
    )(Script.DummyLoggingContext).toOption.map(ev =>
      Converter.castCommandExtendedValue(ev).toOption.get
    )

  private[this] def implements(templateId: TypeConId, interfaceId: TypeConId): Boolean = {
    compiledPackages.pkgInterface.lookupInterfaceInstance(interfaceId, templateId).isRight
  }

  override def queryInterface(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
      viewType: Ast.Type,
  )(implicit ec: ExecutionContext, mat: Materializer): Future[Seq[(ContractId, Option[Value])]] = {

    val acs: Seq[IdeLedger.LookupOk] = ledger.query(
      actAs = Set.empty,
      readAs = parties.toSet,
      effectiveAt = ledger.currentTime,
    )
    val reversePackageIdMap = getPackageIdReverseMap()
    val packageMap = calculatePackageMap(List(), reversePackageIdMap)
    val res = for {
      contract <- acs.collect {
        case IdeLedger.LookupOk(contract) if parties.any(contract.stakeholders.contains(_)) =>
          contract
      }
      preferredPkgId <- packageMap.get(PackageName.assertFromString(contract.packageName))
      upgradedTemplateId = contract.templateId.copy(pkg = preferredPkgId)
      if implements(upgradedTemplateId, interfaceId)
      enrichedCreateArg = failResultAsConverterException(
        enricher.enrichContract(upgradedTemplateId, contract.createArg)
      )
    } yield {
      val viewOpt = computeView(upgradedTemplateId, interfaceId, enrichedCreateArg)
      (contract.contractId, viewOpt)
    }
    Future.successful(res)
  }

  override def queryInterfaceContractId(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
      viewType: Ast.Type,
      cid: ContractId,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[Value]] = {

    lookupContractInstance(parties, cid) match {
      case None => Future.successful(None)
      case Some(contract) =>
        val reversePackageIdMap = getPackageIdReverseMap()
        val packageMap = calculatePackageMap(List(), reversePackageIdMap)
        Future.successful(
          for {
            preferredPkgId <- packageMap.get(PackageName.assertFromString(contract.packageName))
            upgradedTemplateId = contract.templateId.copy(pkg = preferredPkgId)
            enrichedCreateArg = failResultAsConverterException(
              enricher.enrichContract(upgradedTemplateId, contract.createArg)
            )
            view <- computeView(
              upgradedTemplateId,
              interfaceId,
              enrichedCreateArg,
            )
          } yield view
        )
    }
  }

  private[this] def preprocessKey(templateId: Identifier, key: Value)(implicit
      ec: ExecutionContext
  ): Future[GlobalKey] =
    Future(
      preprocessor.unsafePreprocessApiContractKey(Map.empty, ApiContractKey(templateId.toRef, key))
    )
      .recoverWith { case Error.Preprocessing.ContractIdInContractKey(key) =>
        Future.failed(
          new RuntimeException(
            Pretty.prettyDamlException(ContractIdInContractKey(key)).renderWideStream.mkString
          )
        )
      }

  override def queryContractKey(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      key: Value,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Option[ScriptLedgerClient.ActiveContract]] =
    for {
      gkey <- preprocessKey(templateId, key)
      res <- ledger.ledgerData.activeKeys.get(gkey) match {
        case None => Future.successful(None)
        case Some(cid) => queryContractId(parties, templateId, cid)
      }
    } yield res

  private def getTypeIdentifier(t: Ast.Type): Option[Identifier] =
    t match {
      case Ast.TTyCon(ty) => Some(ty)
      case _ => None
    }

  private def fromInterpretationError(err: interpretation.Error): SubmitError = {
    import interpretation.Error._
    err match {
      case ContractNotFound(cid) =>
        SubmitError.ContractNotFound(
          NonEmpty(Seq, cid),
          Some(SubmitError.ContractNotFound.AdditionalInfo.NotFound()),
        )
      case ContractKeyNotFound(key) => SubmitError.ContractKeyNotFound(key)
      case UnresolvedPackageName(packageName) => SubmitError.UnresolvedPackageName(packageName)
      case e: FailedAuthorization =>
        SubmitError.AuthorizationError(Pretty.prettyDamlException(e).renderWideStream.mkString)
      case ContractNotActive(cid, tid, _) =>
        SubmitError.ContractNotFound(
          NonEmpty(Seq, cid),
          Some(SubmitError.ContractNotFound.AdditionalInfo.NotActive(cid, tid)),
        )
      case e @ ContractHashingError(coid, dstTemplateId, createArg, _) =>
        SubmitError.ContractHashingError(
          coid,
          dstTemplateId,
          createArg,
          Pretty.prettyDamlException(e).renderWideStream.mkString,
        )
      case DisclosedContractKeyHashingError(cid, key, hash) =>
        SubmitError.DisclosedContractKeyHashingError(cid, key, hash.toString)
      case DuplicateContractKey(key) => SubmitError.DuplicateContractKey(Some(key))
      case InconsistentContractKey(key) => SubmitError.InconsistentContractKey(key)
      // Only pass on the error if the type is a TTyCon
      case UnhandledException(ty, v) =>
        SubmitError.UnhandledException(getTypeIdentifier(ty).map(tyId => (tyId, v)))
      case UserError(msg) => SubmitError.UserError(msg)
      case _: TemplatePreconditionViolated => SubmitError.TemplatePreconditionViolated()
      case CreateEmptyContractKeyMaintainers(tid, arg, _) =>
        SubmitError.CreateEmptyContractKeyMaintainers(tid, arg)
      case FetchEmptyContractKeyMaintainers(tid, keyValue, packageName) =>
        SubmitError.FetchEmptyContractKeyMaintainers(
          GlobalKey.assertBuild(
            tid,
            packageName,
            keyValue,
            // This GlobalKeyWithMaintainers is only used for rendering errors, and that rendering ignores the hash.
            crypto.Hash.hashPrivateKey("unused-dummy-key-hash"),
          )
        )
      case WronglyTypedContract(cid, exp, act) => SubmitError.WronglyTypedContract(cid, exp, act)
      case ContractDoesNotImplementInterface(iid, cid, tid) =>
        SubmitError.ContractDoesNotImplementInterface(cid, tid, iid)
      case ContractDoesNotImplementRequiringInterface(requiringIid, requiredIid, cid, tid) =>
        SubmitError.ContractDoesNotImplementRequiringInterface(cid, tid, requiredIid, requiringIid)
      case NonComparableValues => SubmitError.NonComparableValues()
      case ContractIdInContractKey(_) => SubmitError.ContractIdInContractKey()
      case ContractIdComparability(cid) => SubmitError.ContractIdComparability(cid.toString)
      case ValueNesting(limit) => SubmitError.ValueNesting(limit)
      case MalformedText(err) => SubmitError.MalformedText(err)
      case e: FailureStatus => SubmitError.FailureStatusError(e, None)
      case e @ Upgrade(innerError: Upgrade.ValidationFailed) =>
        SubmitError.UpgradeError.ValidationFailed(
          innerError.coid,
          innerError.srcTemplateId,
          innerError.dstTemplateId,
          innerError.srcPackageName,
          innerError.dstPackageName,
          innerError.originalSignatories,
          innerError.originalObservers,
          innerError.originalKeyOpt,
          innerError.recomputedSignatories,
          innerError.recomputedObservers,
          innerError.recomputedKeyOpt,
          Pretty.prettyDamlException(e).renderWideStream.mkString,
        )
      case e @ Upgrade(innerError: Upgrade.TranslationFailed) =>
        SubmitError.UpgradeError.TranslationFailed(
          innerError.coid,
          innerError.srcTemplateId,
          innerError.dstTemplateId,
          innerError.createArg,
          Pretty.prettyDamlException(e).renderWideStream.mkString,
        )
      case e @ Upgrade(innerError: Upgrade.AuthenticationFailed) =>
        SubmitError.UpgradeError.AuthenticationFailed(
          innerError.coid,
          innerError.srcTemplateId,
          innerError.dstTemplateId,
          innerError.createArg,
          Pretty.prettyDamlException(e).renderWideStream.mkString,
        )
      case e @ Crypto(innerError: Crypto.MalformedByteEncoding) =>
        SubmitError.CryptoError.MalformedByteEncoding(
          innerError.value,
          Pretty.prettyDamlException(e).renderWideStream.mkString,
        )
      case e @ Crypto(innerError: Crypto.MalformedKey) =>
        SubmitError.CryptoError.MalformedKey(
          innerError.key,
          Pretty.prettyDamlException(e).renderWideStream.mkString,
        )
      case e @ Crypto(innerError: Crypto.MalformedSignature) =>
        SubmitError.CryptoError.MalformedSignature(
          innerError.signature,
          Pretty.prettyDamlException(e).renderWideStream.mkString,
        )
      case e @ Dev(_, innerError) =>
        SubmitError.DevError(
          innerError.getClass.getSimpleName,
          Pretty.prettyDamlException(e).renderWideStream.mkString,
        )
    }
  }

  // Projects the ide-ledger submission error down to the script submission error
  private def fromIdeLedgerError(err: script.Error): SubmitError = err match {
    case script.Error.RunnerException(e: SError.SErrorCrash) =>
      SubmitError.UnknownError(e.toString)
    case script.Error.RunnerException(SError.SErrorDamlException(err)) =>
      fromInterpretationError(err)

    case script.Error.Internal(reason) => SubmitError.UnknownError(reason)
    case script.Error.Timeout(timeout) => SubmitError.UnknownError("Timeout: " + timeout)

    // We treat ineffective contracts (ie, ones that don't exist yet) as being not found
    case script.Error.ContractNotEffective(cid, tid, effectiveAt) =>
      SubmitError.ContractNotFound(
        NonEmpty(Seq, cid),
        Some(SubmitError.ContractNotFound.AdditionalInfo.NotEffective(cid, tid, effectiveAt)),
      )

    case script.Error.ContractNotActive(cid, tid, _) =>
      SubmitError.ContractNotFound(
        NonEmpty(Seq, cid),
        Some(SubmitError.ContractNotFound.AdditionalInfo.NotActive(cid, tid)),
      )

    // Similarly, we treat contracts that we can't see as not being found
    case script.Error.ContractNotVisible(cid, tid, actAs, readAs, observers) =>
      SubmitError.ContractNotFound(
        NonEmpty(Seq, cid),
        Some(
          SubmitError.ContractNotFound.AdditionalInfo.NotVisible(cid, tid, actAs, readAs, observers)
        ),
      )

    case script.Error.CommitError(
          IdeLedger.CommitError.UniqueKeyViolation(IdeLedger.UniqueKeyViolation(gk))
        ) =>
      SubmitError.DuplicateContractKey(Some(gk))

    case script.Error.LookupError(err, _, _) =>
      // TODO[SW]: Implement proper Lookup error throughout
      SubmitError.UnknownError("Lookup error: " + err.toString)

    // This covers MustFailSucceeded, InvalidPartyName, PartyAlreadyExists which should not be throwable by a command submission
    // It also covers PartiesNotAllocated.
    case err => SubmitError.UnknownError("Unexpected error type: " + err.toString)
  }
  // Build a SubmissionError with empty transaction
  private def makeEmptySubmissionError(err: script.Error): IdeLedgerRunner.SubmissionError =
    IdeLedgerRunner.SubmissionError(
      err,
      IncompleteTransaction(
        transaction = Transaction(Map.empty, ImmArray.empty),
        locationInfo = Map.empty,
      ),
    )

  private[this] def tyConRefToPkgId(tyCon: Ref.TypeConRef) =
    tyCon.pkg match {
      case PackageRef.Id(id) =>
        id
      case PackageRef.Name(_) =>
        // TODO: https://github.com/digital-asset/daml/issues/17995
        //  add support for package name
        throw new IllegalArgumentException("package name not support")
    }

  private def getReferencePackageId(ref: Reference): PackageId =
    ref match {
      // TODO: https://github.com/digital-asset/daml/issues/17995
      //  add support for package name
      case Reference.Package(PackageRef.Name(_)) =>
        throw new IllegalArgumentException("package name not support")
      case Reference.Package(PackageRef.Id(packageId)) => packageId
      case Reference.Module(packageId, _) => packageId
      case Reference.Definition(name) => name.packageId
      case Reference.TypeSyn(name) => name.packageId
      case Reference.DataType(name) => name.packageId
      case Reference.DataRecord(name) => name.packageId
      case Reference.DataRecordField(name, _) => name.packageId
      case Reference.DataVariant(name) => name.packageId
      case Reference.DataVariantConstructor(name, _) => name.packageId
      case Reference.DataEnum(name) => name.packageId
      case Reference.DataEnumConstructor(name, _) => name.packageId
      case Reference.Value(name) => name.packageId
      case Reference.Template(tyCon) => tyConRefToPkgId(tyCon)
      case Reference.Interface(name) => name.packageId
      case Reference.TemplateKey(name) => name.packageId
      case Reference.InterfaceInstance(_, name) => name.packageId
      case Reference.TemplateChoice(name, _) => name.packageId
      case Reference.InterfaceChoice(name, _) => name.packageId
      case Reference.InheritedChoice(name, _, _) => name.packageId
      case Reference.TemplateOrInterface(tyCon) => tyConRefToPkgId(tyCon)
      case Reference.Choice(name, _) => name.packageId
      case Reference.Method(name, _) => name.packageId
      case Reference.Exception(name) => name.packageId
    }

  private def getLookupErrorPackageId(err: LookupError): PackageId =
    err match {
      case LookupError.NotFound(notFound, _) => getReferencePackageId(notFound)
    }

  private def makeLookupError(
      err: LookupError
  ): IdeLedgerRunner.SubmissionError = {
    val packageId = getLookupErrorPackageId(err)
    val packageMetadata = getPackageIdReverseMap().lift(packageId).map {
      case ScriptLedgerClient.ReadablePackageId(packageName, packageVersion) =>
        PackageMetadata(packageName, packageVersion, None)
    }
    makeEmptySubmissionError(script.Error.LookupError(err, packageMetadata, packageId))
  }

  private def makePartiesNotAllocatedError(
      unallocatedSubmitters: Set[Party]
  ): IdeLedgerRunner.SubmissionError =
    makeEmptySubmissionError(script.Error.PartiesNotAllocated(unallocatedSubmitters))

  /* Given a daml-script CommandWithMeta, returns the corresponding IDE Ledger
   * ApiCommand. If the CommandWithMeta has an explicit package id, or is <LF1.17,
   * the ApiCommand will have the same PackageRef, otherwise it will be
   * replaced with PackageRef.Name, such that the preprocess will replace it
   * according to package resolution
   */
  private def toCommand(
      cmdWithMeta: ScriptLedgerClient.CommandWithMeta,
      // This map only contains packageIds >=LF1.17
      packageIdMap: PartialFunction[PackageId, ScriptLedgerClient.ReadablePackageId],
  ): ApiCommand = {
    def adjustTypeConRef(old: TypeConRef): TypeConRef =
      old match {
        case TypeConRef(PackageRef.Id(packageIdMap(nameVersion)), qName) =>
          TypeConRef(PackageRef.Name(nameVersion.name), qName)
        case ref => ref
      }

    val ScriptLedgerClient.CommandWithMeta(cmd, explicitPackageId) = cmdWithMeta

    cmd match {
      case _ if explicitPackageId => cmd
      case ApiCommand.Create(templateRef, argument) =>
        ApiCommand.Create(
          adjustTypeConRef(templateRef),
          argument,
        )
      case ApiCommand.Exercise(typeRef, contractId, choiceId, argument) =>
        ApiCommand.Exercise(
          adjustTypeConRef(typeRef),
          contractId,
          choiceId,
          argument,
        )
      case ApiCommand.ExerciseByKey(templateRef, contractKey, choiceId, argument) =>
        ApiCommand.ExerciseByKey(
          adjustTypeConRef(templateRef),
          contractKey,
          choiceId,
          argument,
        )
      case ApiCommand.CreateAndExercise(templateRef, createArgument, choiceId, choiceArgument) =>
        ApiCommand.CreateAndExercise(
          adjustTypeConRef(templateRef),
          createArgument,
          choiceId,
          choiceArgument,
        )
    }
  }

  // unsafe version of submit that does not clear the commit.
  private def unsafeSubmit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      disclosures: List[Disclosure],
      packagePreference: List[PackageId],
      commands: List[ScriptLedgerClient.CommandWithMeta],
      optLocation: Option[Location],
  ): Either[
    IdeLedgerRunner.SubmissionError,
    IdeLedgerRunner.Commit[IdeLedger.CommitResult],
  ] = {
    val unallocatedSubmitters: Set[Party] =
      (actAs.toSet union readAs) -- allocatedParties.values.map(_.party)
    if (unallocatedSubmitters.nonEmpty) {
      Left(makePartiesNotAllocatedError(unallocatedSubmitters))
    } else {
      val reversePackageIdMap = getPackageIdReverseMap()
      val packageMap = calculatePackageMap(packagePreference, reversePackageIdMap)
      @tailrec
      def loop(
          result: IdeLedgerRunner.SubmissionResult[IdeLedger.CommitResult]
      ): Either[
        IdeLedgerRunner.SubmissionError,
        IdeLedgerRunner.Commit[IdeLedger.CommitResult],
      ] =
        result match {
          case _ if canceled() =>
            throw engine.script.Runner.TimedOut
          case IdeLedgerRunner.Interruption(continue) =>
            loop(continue())
          case err: IdeLedgerRunner.SubmissionError => Left(err)
          case commit @ IdeLedgerRunner.Commit(result, _) =>
            val referencedParties: Set[Party] =
              result.richTransaction.blindingInfo.disclosure.values
                .fold(Set.empty[Party])(_ union _)
            val unallocatedParties = referencedParties -- allocatedParties.values.map(_.party)
            for {
              _ <- Either.cond(
                unallocatedParties.isEmpty,
                (),
                IdeLedgerRunner.SubmissionError(
                  script.Error.PartiesNotAllocated(unallocatedParties),
                  commit.tx,
                ),
              )
              // We look for inactive explicit disclosures
              activeContracts = ledger.ledgerData.activeContracts
              _ <- disclosures
                .collectFirst {
                  case Disclosure(tmplId, coid, _) if !activeContracts(coid) =>
                    IdeLedgerRunner.SubmissionError(
                      script.Error.ContractNotActive(coid, tmplId, None),
                      commit.tx,
                    )
                }
                .toLeft(())
            } yield commit
        }

      // We use try + unsafePreprocess here to avoid the addition template lookup logic in `preprocessApiCommands`
      val eitherSpeedyCommands =
        try {
          Right(
            preprocessor.unsafePreprocessApiCommands(
              packageMap,
              commands
                .map(toCommand(_, reversePackageIdMap.view.filterKeys(packageSupportsUpgrades(_))))
                .to(ImmArray),
            )
          )
        } catch {
          case Error.Preprocessing.Lookup(err) => Left(makeLookupError(err))
          // Expose type mismatches as unknown errors to match canton behaviour. Later this should be fully expressed as a SubmitError
          case Error.Preprocessing.TypeMismatch(_, _, msg) =>
            Left(
              makeEmptySubmissionError(
                script.Error.Internal("COMMAND_PREPROCESSING_FAILED(0, 00000000): " + msg)
              )
            )
        }

      val eitherSpeedyDisclosures
          : Either[script.IdeLedgerRunner.SubmissionError, List[FatContractInstance]] = {
        import cats.implicits._

        disclosures
          .traverse(b => TransactionCoder.decodeFatContractInstance(b.blob.toByteString))
          .left
          .map(err => makeEmptySubmissionError(script.Error.DisclosureDecoding(err.errorMessage)))
      }

      val ledgerApi = IdeLedgerRunner.ScriptLedgerApi(ledger)

      for {
        speedyCommands <- eitherSpeedyCommands
        speedyDisclosures <- eitherSpeedyDisclosures
        translated = compiledPackages.compiler.unsafeCompile(speedyCommands)
        result =
          IdeLedgerRunner.submit(
            compiledPackages,
            speedyDisclosures,
            ledgerApi,
            actAs.toSet,
            readAs,
            translated,
            optLocation,
            nextSeed(),
            packageMap,
            traceLog,
            warningLog,
          )(Script.DummyLoggingContext)
        res <- loop(result)
      } yield res
    }
  }

  override def submit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      disclosures: List[Disclosure],
      optPackagePreference: Option[List[PackageId]],
      commands: List[ScriptLedgerClient.CommandWithMeta],
      prefetchContractKeys: List[AnyContractKey],
      optLocation: Option[Location],
      languageVersionLookup: PackageId => Either[String, LanguageVersion],
      errorBehaviour: ScriptLedgerClient.SubmissionErrorBehaviour,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Either[
    ScriptLedgerClient.SubmitFailure,
    (Seq[ScriptLedgerClient.CommandResult], ScriptLedgerClient.TransactionTree),
  ]] = Future {
    synchronized {
      unsafeSubmit(
        actAs,
        readAs,
        disclosures,
        optPackagePreference.getOrElse(List()),
        commands,
        optLocation,
      ) match {
        case Right(IdeLedgerRunner.Commit(result, tx)) =>
          val commandResultPackageIds = commands.flatMap(toCommandPackageIds(_))
          _ledger = result.newLedger
          val transaction = result.richTransaction.transaction
          def convEvent(
              id: NodeId,
              oIntendedPackageId: Option[PackageId],
          ): Option[ScriptLedgerClient.TreeEvent] =
            transaction.nodes(id) match {
              case create: Node.Create =>
                val intendedTemplateId =
                  oIntendedPackageId
                    .fold(create.templateId)(intendedPackageId =>
                      create.templateId.copy(pkg = intendedPackageId)
                    )
                Some(
                  ScriptLedgerClient.Created(
                    intendedTemplateId,
                    create.coid,
                    failResultAsConverterException(
                      enricher.enrichContract(intendedTemplateId, create.arg)
                    ),
                    blob(create, result.richTransaction.effectiveAt),
                  )
                )
              case exercise: Node.Exercise =>
                val intendedTemplateId =
                  oIntendedPackageId
                    .fold(exercise.templateId)(intendedPackageId =>
                      exercise.templateId.copy(pkg = intendedPackageId)
                    )
                val enrichedArg = failResultAsConverterException(
                  enricher.enrichChoiceArgument(
                    intendedTemplateId,
                    exercise.interfaceId,
                    exercise.choiceId,
                    exercise.chosenValue,
                  )
                )
                val enrichedResult = failResultAsConverterException(
                  enricher.enrichChoiceResult(
                    intendedTemplateId,
                    exercise.interfaceId,
                    exercise.choiceId,
                    exercise.exerciseResult.get,
                  )
                )
                Some(
                  ScriptLedgerClient.Exercised(
                    intendedTemplateId,
                    exercise.interfaceId,
                    exercise.targetCoid,
                    exercise.choiceId,
                    enrichedArg,
                    enrichedResult,
                    exercise.children.collect(Function.unlift(convEvent(_, None))).toList,
                  )
                )
              case _: Node.Fetch | _: Node.LookupByKey | _: Node.Rollback => None
            }
          val tree = ScriptLedgerClient.TransactionTree(
            transaction.roots.toList
              .zip(commandResultPackageIds)
              .collect(Function.unlift { case (id, pkgId) => convEvent(id, Some(pkgId)) })
          )
          val results = ScriptLedgerClient.transactionTreeToCommandResults(tree)
          if (errorBehaviour == ScriptLedgerClient.SubmissionErrorBehaviour.MustFail)
            _currentSubmission = Some(IdeLedgerRunner.CurrentSubmission(optLocation, tx))
          else
            _currentSubmission = None
          Right((results, tree))
        case Left(IdeLedgerRunner.SubmissionError(err, tx)) =>
          import ScriptLedgerClient.SubmissionErrorBehaviour._
          // Some compatibility logic to keep the "steps" the same.
          // We may consider changing this to always insert SubmissionFailed, but this requires splitting the golden files in the integration tests
          errorBehaviour match {
            case MustSucceed =>
              _currentSubmission = Some(IdeLedgerRunner.CurrentSubmission(optLocation, tx))
            case MustFail =>
              _currentSubmission = None
              _ledger = ledger.insertAssertMustFail(actAs.toSet, readAs, optLocation)
            case Try =>
              _currentSubmission = None
              _ledger = ledger.insertSubmissionFailed(actAs.toSet, readAs, optLocation)
          }
          Left(ScriptLedgerClient.SubmitFailure(err, fromIdeLedgerError(err)))
      }
    }
  }

  // Note that CreateAndExerciseCommand gives two results, so we duplicate the package id
  private def toCommandPackageIds(cmd: ScriptLedgerClient.CommandWithMeta): List[PackageId] =
    cmd.command match {
      case command.CreateAndExerciseCommand(tmplRef, _, _, _) =>
        List(tmplRef.assertToTypeConId.packageId, tmplRef.assertToTypeConId.packageId)
      case cmd =>
        List(cmd.typeRef.assertToTypeConId.packageId)
    }

  override def allocateParty(partyIdHint: String)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ) = {
    val usedNames = allocatedParties.keySet
    Future.fromTry(for {
      name <-
        if (partyIdHint != "") {
          // Try to allocate the given hint as party name. Will fail if the name is already taken.
          if (usedNames contains partyIdHint) {
            Failure(script.Error.PartyAlreadyExists(partyIdHint))
          } else {
            Success(partyIdHint)
          }
        } else {
          val namePrefix = "party"
          val candidates = namePrefix #:: LazyList.from(1).map(namePrefix + _.toString())
          Success(candidates.find(s => !(usedNames contains s)).get)
        }
      party <- Ref.Party
        .fromString(name)
        .fold(msg => Failure(script.Error.InvalidPartyName(name, msg)), Success(_))

      // Create and store the new party.
      partyDetails = PartyDetails(
        party = party,
        isLocal = true,
        metadata = ObjectMeta.empty,
        identityProviderId = IdentityProviderId.Default,
      )
      _ = allocatedParties += (name -> partyDetails)
    } yield partyDetails.party)
  }

  override def listKnownParties()(implicit ec: ExecutionContext, mat: Materializer) = {
    Future.successful(allocatedParties.values.toList)
  }

  override def getStaticTime()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Time.Timestamp] = {
    Future.successful(ledger.currentTime)
  }

  override def setStaticTime(time: Time.Timestamp)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = {
    val diff = time.micros - ledger.currentTime.micros
    // IdeLedger only provides pass, so we have to calculate the diff.
    // Note that IdeLedger supports going backwards in time.
    _ledger = ledger.passTime(diff)
    Future.unit
  }

  override def createUser(
      user: User,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[Unit]] =
    userManagementStore
      .createUser(user, rights.toSet)(LoggingContextWithTrace.empty)
      .map(_.toOption.map(_ => ()))

  override def getUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[User]] =
    userManagementStore
      .getUser(id, IdentityProviderId.Default)(LoggingContextWithTrace.empty, implicitly)
      .map(_.toOption)

  override def deleteUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[Unit]] =
    userManagementStore
      .deleteUser(id, IdentityProviderId.Default)(LoggingContextWithTrace.empty)
      .map(_.toOption)

  override def listAllUsers()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[User]] =
    userManagementStore
      .listUsers(None, Int.MaxValue, IdentityProviderId.Default)(LoggingContextWithTrace.empty)
      .map(_.toOption.toList.flatMap(_.users))

  override def grantUserRights(
      id: UserId,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    userManagementStore
      .grantRights(id, rights.toSet, IdentityProviderId.Default)(LoggingContextWithTrace.empty)
      .map(_.toOption.map(_.toList))

  override def revokeUserRights(
      id: UserId,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    userManagementStore
      .revokeRights(id, rights.toSet, IdentityProviderId.Default)(LoggingContextWithTrace.empty)
      .map(_.toOption.map(_.toList))

  override def listUserRights(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    userManagementStore
      .listUserRights(id, IdentityProviderId.Default)(LoggingContextWithTrace.empty, implicitly)
      .map(_.toOption.map(_.toList))

  /* Generate a package name map based on package preference then highest version
   */
  def calculatePackageMap(
      packagePreference: List[PackageId],
      reverseMap: Map[PackageId, ScriptLedgerClient.ReadablePackageId],
  ): Map[PackageName, PackageId] = {
    // Map containing only elements of the package preference
    val pkgPrefMap: Map[PackageName, PackageId] = packagePreference
      .map(pkgId =>
        (
          reverseMap
            .getOrElse(pkgId, throw new IllegalArgumentException(s"No such PackageId $pkgId"))
            .name,
          if (packageSupportsUpgrades(pkgId)) pkgId
          else throw new IllegalArgumentException(s"Package $pkgId does not support Upgrades."),
        )
      )
      .toMap
    val ordering: Ordering[(PackageVersion, PackageId)] = Ordering[PackageVersion].on(_._1)
    // Map containing only highest versions of upgrades compatible packages, could be cached
    val highestVersionMap: Map[PackageName, PackageId] = getPackageIdMap()
      .filter { case (_, pkgId) => packageSupportsUpgrades(pkgId) }
      .groupMapReduce(_._1.name) { case (nameVersion, packageId) =>
        (nameVersion.version, packageId)
      }(ordering.max)
      .view
      .mapValues(_._2)
      .toMap

    // Preference overrides highest version
    highestVersionMap ++ pkgPrefMap
  }

  def getPackageIdMap(): Map[ScriptLedgerClient.ReadablePackageId, PackageId] =
    getPackageIdPairs().toMap

  def getPackageIdReverseMap(): Map[PackageId, ScriptLedgerClient.ReadablePackageId] =
    getPackageIdPairs().map(_.swap).toMap

  def getPackageIdPairs(): collection.View[(ScriptLedgerClient.ReadablePackageId, PackageId)] =
    (for {
      entry <- originalCompiledPackages.signatures.view
      (pkgId, pkg) = entry
      readablePackageId = ScriptLedgerClient.ReadablePackageId(
        pkg.metadata.name,
        pkg.metadata.version,
      )
    } yield (readablePackageId, pkgId))

  override def vetPackages(packages: List[ScriptLedgerClient.ReadablePackageId])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = Future {
    val packageMap = getPackageIdMap()
    val pkgIdsToVet = packages.map(pkg =>
      packageMap.getOrElse(pkg, throw new IllegalArgumentException(s"Unknown package $pkg"))
    )

    unvettedPackages = unvettedPackages -- pkgIdsToVet.toSet
    updateCompiledPackages()
  }

  override def waitUntilVettingVisible(
      packages: Iterable[ScriptLedgerClient.ReadablePackageId],
      onParticipantUid: String,
  )(implicit ec: ExecutionContext): Future[Unit] =
    Future.successful(())

  override def unvetPackages(packages: List[ScriptLedgerClient.ReadablePackageId])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = Future {
    val packageMap = getPackageIdMap()
    val pkgIdsToUnvet = packages.map(pkg =>
      packageMap.getOrElse(pkg, throw new IllegalArgumentException(s"Unknown package $pkg"))
    )

    unvettedPackages = unvettedPackages ++ pkgIdsToUnvet.toSet
    updateCompiledPackages()
  }

  override def waitUntilUnvettingVisible(
      packages: Iterable[ScriptLedgerClient.ReadablePackageId],
      onParticipantUid: String,
  )(implicit ec: ExecutionContext): Future[Unit] =
    Future.successful(())

  override def listVettedPackages()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[ScriptLedgerClient.ReadablePackageId]] =
    Future.successful(getPackageIdMap().filter(kv => !unvettedPackages(kv._2)).keys.toList)

  override def listAllPackages()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[ScriptLedgerClient.ReadablePackageId]] =
    Future.successful(getPackageIdMap().keys.toList)

  override def allocatePartyOnMultipleParticipants(
      party: Ref.Party,
      toParticipantIds: Iterable[String],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Unit] = Future.failed(
    new RuntimeException(
      "allocatePartyOnMultipleParticipants should not be called on IDE ledger, use aggregateAllocatePartyOnMultipleParticipants instead"
    )
  )

  override def aggregateAllocatePartyOnMultipleParticipants(
      clients: List[ScriptLedgerClient],
      partyHint: String,
      namespace: String,
      toParticipantIds: Iterable[String],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Ref.Party] = allocateParty(partyHint)

  override def waitUntilHostingVisible(
      party: Ref.Party,
      onParticipantUid: Iterable[String],
  ): Future[Unit] =
    Future.successful(())

  override def getParticipantUid: String = ""
}
