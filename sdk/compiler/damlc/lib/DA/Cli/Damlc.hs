-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ApplicativeDo       #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveAnyClass #-}

-- | Main entry-point of the Daml compiler
module DA.Cli.Damlc (main, Command (..), MultiPackageManifestEntry (..), fullParseArgs) where

import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import Control.Exception (bracket, catch, displayException, throwIO, handle, throw)
import Control.Exception.Safe (catchIO)
import Control.Monad (forM, forM_, unless, void, when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Extra (allM, mapMaybeM, whenM, whenJust)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import qualified Crypto.Hash as Hash
import DA.Bazel.Runfiles (Resource(..),
                          locateResource,
                          mainWorkspace,
                          resourcesPath,
                          runfilesPathPrefix,
                          setRunfilesEnv)
import qualified DA.Cli.Args as ParseArgs
import DA.Cli.Options (Debug(..),
                       EnableMultiPackage(..),
                       GenerateMultiPackageManifestOutput (..),
                       InitPkgDb(..),
                       MultiPackageBuildAll(..),
                       MultiPackageCleanAll(..),
                       MultiPackageLocation(..),
                       MultiPackageNoCache(..),
                       ProjectOpts(..),
                       Style(..),
                       Telemetry(..),
                       cliOptDetailLevel,
                       cliOptLogLevel,
                       debugOpt,
                       disabledDlintUsageParser,
                       enabledDlintUsageParser,
                       enableMultiPackageOpt,
                       enableScenarioServiceOpt,
                       generateMultiPackageManifestOutputOpt,
                       incrementalBuildOpt,
                       initPkgDbOpt,
                       inputDarOpt,
                       inputFileOpt,
                       inputFileOptWithExt,
                       multiPackageBuildAllOpt,
                       multiPackageCleanAllOpt,
                       multiPackageLocationOpt,
                       multiPackageNoCacheOpt,
                       optionalDlintUsageParser,
                       optionalOutputFileOpt,
                       optionsParser,
                       optPackageName,
                       outputFileOpt,
                       packageNameOpt,
                       projectOpts,
                       render,
                       studioAutorunAllScenariosOpt,
                       targetFileNameOpt,
                       telemetryOpt)
import DA.Cli.Damlc.BuildInfo (buildInfo)
import DA.Cli.Damlc.Command.MultiIde (runMultiIde)
import qualified DA.Daml.Dar.Reader as InspectDar
import qualified DA.Cli.Damlc.Command.Damldoc as Damldoc
import DA.Cli.Damlc.Packaging (createProjectPackageDb, mbErr)
import DA.Cli.Damlc.DependencyDb (installDependencies)
import DA.Cli.Damlc.Test (CoveragePaths(..),
                          LoadCoverageOnly(..),
                          RunAllTests(..),
                          ShowCoverage(..),
                          TableOutputPath(..),
                          TransactionsOutputPath(..),
                          UseColor(..),
                          execTest,
                          getRunAllTests,
                          loadAggregatePrintResults,
                          CoverageFilter(..))
import DA.Daml.Compiler.Dar (FromDalf(..),
                             breakAt72Bytes,
                             buildDar,
                             createDarFile,
                             damlFilesInDir,
                             getDamlRootFiles,
                             writeIfacesAndHie)
import DA.Daml.Compiler.Output (diagnosticsLogger, writeOutput, writeOutputBSL)
import DA.Daml.Project.Types
    ( UnresolvedReleaseVersion(..),
      unresolvedReleaseVersionToString,
      parseUnresolvedVersion,
      isHeadVersion,
      ConfigError(..),
      ProjectPath(..),
      ProjectConfig,
      unsafeResolveReleaseVersion)
import DA.Daml.Assistant.Version (resolveReleaseVersionUnsafe)
import DA.Daml.Assistant.Util (wrapErr)
import qualified DA.Daml.Compiler.Repl as Repl
import DA.Daml.Compiler.DocTest (docTest)
import DA.Daml.Desugar (desugar)
import DA.Daml.LF.ScenarioServiceClient (readScenarioServiceConfig, withScenarioService')
import qualified DA.Daml.LF.ReplClient as ReplClient
import DA.Daml.Compiler.Validate (validateDar)
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.Util (splitUnitId)
import qualified DA.Daml.LF.Proto3.Archive as Archive
import DA.Daml.LF.Reader (dalfPaths,
                          mainDalf,
                          mainDalfPath,
                          manifestPath,
                          readDalfManifest,
                          readDalfs,
                          readManifest)
import qualified DA.Daml.LF.Reader as Reader
import DA.Daml.LanguageServer (runLanguageServer)
import DA.Daml.Options (toCompileOpts)
import DA.Daml.Options.Types (EnableScenarioService(..),
                              Haddock(..),
                              IncrementalBuild (..),
                              Options(Options),
                              SkipScenarioValidation(..),
                              StudioAutorunAllScenarios,
                              UpgradeInfo(..),
                              damlArtifactDir,
                              distDir,
                              getLogger,
                              ifaceDir,
                              optDamlLfVersion,
                              optEnableOfInterestRule,
                              optEnableScenarios,
                              optHaddock,
                              optHideUnitId,
                              optIfaceDir,
                              optImportPath,
                              optIncrementalBuild,
                              optMbPackageConfigPath,
                              optMbPackageName,
                              optMbPackageVersion,
                              optPackageDbs,
                              optPackageImports,
                              optScenarioService,
                              optSkipScenarioValidation,
                              optThreads,
                              optUpgradeInfo,
                              pkgNameVersion,
                              projectPackageDatabase)
import DA.Daml.Package.Config (MultiPackageConfigFields(..),
                               PackageConfigFields(..),
                               checkPkgConfig,
                               findMultiPackageConfig,
                               withPackageConfig,
                               withMultiPackageConfig)
import DA.Daml.Project.Config (queryProjectConfig, queryProjectConfigRequired, readProjectConfig)
import DA.Daml.Project.Consts (ProjectCheck(..),
                               damlCacheEnvVar,
                               damlPathEnvVar,
                               getProjectPath,
                               getSdkVersion,
                               multiPackageConfigName,
                               projectConfigName,
                               sdkVersionEnvVar,
                               withExpectProjectRoot,
                               withProjectRoot,
                               damlAssistantIsSet)
import DA.Daml.Assistant.Env (getDamlEnv, getDamlPath, envUseCache)
import DA.Daml.Assistant.Types (LookForProjectPath(..))
import qualified DA.Pretty
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.GCP as Logger.GCP
import qualified DA.Service.Logger.Impl.IO as Logger.IO
import DA.Signals (installSignalHandlers)
import qualified Com.Daml.DamlLfDev.DamlLf as PLF
import Data.Aeson (FromJSON, ToJSON)
import qualified Data.Aeson.Encode.Pretty as Aeson.Pretty
import qualified Data.Aeson.Text as Aeson
import Data.Bifunctor (bimap, second)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC
import qualified Data.ByteString.UTF8 as BSUTF8
import Data.Either (partitionEithers)
import Data.FileEmbed (embedFile)
import qualified Data.HashSet as HashSet
import Data.List (isPrefixOf, isInfixOf)
import Data.List.Extra (elemIndices, nubOrd, nubSort, nubSortOn)
import qualified Data.List.Split as Split
import qualified Data.Map.Strict as Map
import Data.Maybe (catMaybes, fromMaybe, listToMaybe, mapMaybe)
import qualified Data.Text.Extended as T
import Data.Text.Encoding (encodeUtf8)
import qualified Data.Text.Lazy.IO as TL
import qualified Data.Text.IO as T
import Data.Traversable (for)
import Data.Typeable (Typeable)
import qualified Data.Yaml as Y
import Development.IDE.Core.API (getDalf, makeVFSHandle, runActionSync, setFilesOfInterest)
import Development.IDE.Core.Debouncer (newAsyncDebouncer, noopDebouncer)
import Development.IDE.Core.IdeState.Daml (getDamlIdeState,
                                           enabledPlugins,
                                           withDamlIdeState,
                                           toIdeLogger)
import Development.IDE.Core.Rules (transitiveModuleDeps)
import Development.IDE.Core.Rules.Daml (getDlintIdeas, getSpanInfo)
import Development.IDE.Core.Shake (Config(..),
                                   IdeResult,
                                   NotificationHandler(..),
                                   ShakeLspEnv(..),
                                   actionLogger,
                                   defineEarlyCutoffWithDefaultRunChanged,
                                   getDiagnostics,
                                   use,
                                   use_,
                                   uses,
                                   uses_)
import Development.IDE.GHC.Util (hscEnv, moduleImportPath)
import Development.IDE.Types.Location (NormalizedFilePath, fromNormalizedFilePath, toNormalizedFilePath')
import "ghc-lib-parser" DynFlags (DumpFlag(..),
                                  ModRenaming(..),
                                  PackageArg(..),
                                  PackageFlag(..))
import GHC.Conc (getNumProcessors)
import "ghc-lib-parser" Module (unitIdString, stringToUnitId)
import qualified Network.Socket as NS
import Options.Applicative.Extended (flagYesNoAuto, optionOnce, strOptionOnce)
import Options.Applicative ((<|>),
                            CommandFields,
                            Mod,
                            Parser,
                            ParserInfo(..),
                            ParserResult(..),
                            auto,
                            command,
                            defaultPrefs,
                            eitherReader,
                            execParserPure,
                            flag,
                            flag',
                            forwardOptions,
                            fullDesc,
                            handleParseResult,
                            headerDoc,
                            help,
                            helper,
                            info,
                            internal,
                            liftA2,
                            long,
                            many,
                            metavar,
                            optional,
                            prefs,
                            progDesc,
                            renderFailure,
                            short,
                            str,
                            strArgument,
                            subparser,
                            switch,
                            value)
import qualified Options.Applicative (option, strOption)
import qualified Options.Applicative.Types as Options.Applicative (readerAsk)
import qualified Proto3.Suite as PS
import qualified Proto3.Suite.JSONPB as Proto.JSONPB
import System.Directory.Extra
import System.Environment
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process (StdStream(..),
                      CreateProcess(..),
                      proc,
                      withCreateProcess,
                      waitForProcess,
                      )
import qualified Text.PrettyPrint.ANSI.Leijen as PP
import Development.IDE.Core.RuleTypes
import "ghc-lib-parser" ErrUtils
-- For dumps
import "ghc-lib" GHC
import "ghc-lib-parser" HsDumpAst
import "ghc-lib" HscStats
import "ghc-lib-parser" HscTypes
import qualified "ghc-lib-parser" Outputable as GHC
import qualified SdkVersion.Class
import "ghc-lib-parser" Util (looksLikePackageName)
import Text.Regex.TDFA

import qualified Development.IDE.Core.Service as IDE
import Control.DeepSeq
import Data.Binary
import Data.Hashable
import GHC.Generics (Generic)
import Development.Shake (Rules, RuleResult)
import Development.Shake.Rule (RunChanged (ChangedRecomputeDiff, ChangedRecomputeSame))
import qualified Development.IDE.Types.Logger as IDELogger

--------------------------------------------------------------------------------
-- Commands
--------------------------------------------------------------------------------

data CommandName =
    Build
  | Clean
  | Compile
  | DamlDoc
  | DebugIdeSpanInfo
  | Desugar
  | DocTest
  | Ide
  | Init
  | Inspect
  | InspectDar
  | ValidateDar
  | License
  | Lint
  | MergeDars
  | Package
  | Test
  | Repl
  | GenerateMultiPackageManifest
  | MultiIde
  deriving (Ord, Show, Eq)
data Command = Command CommandName (Maybe ProjectOpts) (IO ())

cmdMultiIde :: SdkVersion.Class.SdkVersioned => Int -> Mod CommandFields Command
cmdMultiIde _numProcessors =
    command "multi-ide" $ info (helper <*> cmd) $
       progDesc
        "Start the Daml Multi-IDE language server on standard input/output."
    <> fullDesc
    <> forwardOptions
  where
    cmd = fmap (Command MultiIde Nothing) $ runMultiIde
        <$> cliOptLogLevel
        <*> optional (strOptionOnce $ long "ide-identifier" <> help "Identifier string for this IDE")
        <*> many (strArgument mempty)

cmdIde :: SdkVersion.Class.SdkVersioned => Int -> Mod CommandFields Command
cmdIde numProcessors =
    command "ide" $ info (helper <*> cmd) $
       progDesc
        "Start the Daml language server on standard input/output."
    <> fullDesc
  where
    cmd = execIde
        <$> telemetryOpt
        <*> debugOpt
        <*> enableScenarioServiceOpt
        <*> studioAutorunAllScenariosOpt
        <*> optionsParser
              numProcessors
              (EnableScenarioService True)
              (pure Nothing)
              (optionalDlintUsageParser True)

cmdLicense :: Mod CommandFields Command
cmdLicense =
    command "license" $ info (helper <*> pure execLicense) $
       progDesc
        "License information for open-source projects included in Daml."
    <> fullDesc

cmdCompile :: SdkVersion.Class.SdkVersioned => Int -> Mod CommandFields Command
cmdCompile numProcessors =
    command "compile" $ info (helper <*> cmd) $
        progDesc "Compile the Daml program into a Core/Daml-LF archive."
    <> fullDesc
  where
    cmd = execCompile
        <$> inputFileOpt
        <*> outputFileOpt
        <*> optionsParser
              numProcessors
              (EnableScenarioService False)
              optPackageName
              disabledDlintUsageParser
        <*> optWriteIface
        <*> optional (strOptionOnce $ long "iface-dir" <> metavar "IFACE_DIR" <> help "Directory for interface files")

    optWriteIface =
        fmap WriteInterface $
        switch $
        help "Produce interface files. This is used for building the package db for daml-prim and daml-stdib" <>
        long "write-iface"

cmdDesugar :: SdkVersion.Class.SdkVersioned => Int -> Mod CommandFields Command
cmdDesugar numProcessors =
  command "desugar" $ info (helper <*> cmd) $
      progDesc "Show the desugared Daml program"
    <> fullDesc
  where
    cmd = execDesugar
      <$> inputFileOpt
      <*> outputFileOpt
      <*> optionsParser
            numProcessors
            (EnableScenarioService False)
            optPackageName
            disabledDlintUsageParser

cmdDebugIdeSpanInfo :: SdkVersion.Class.SdkVersioned => Int -> Mod CommandFields Command
cmdDebugIdeSpanInfo numProcessors =
  command "debug-ide-span-info" $ info (helper <*> cmd) $
      progDesc "Show the IDE span infos for the Daml program"
    <> fullDesc
  where
    cmd = execDebugIdeSpanInfo
      <$> inputFileOpt
      <*> outputFileOpt
      <*> optionsParser
            numProcessors
            (EnableScenarioService False)
            optPackageName
            disabledDlintUsageParser

cmdLint :: SdkVersion.Class.SdkVersioned => Int -> Mod CommandFields Command
cmdLint numProcessors =
    command "lint" $ info (helper <*> cmd) $
        progDesc "Lint the Daml program."
    <> fullDesc
  where
    cmd = execLint
        <$> many inputFileOpt
        <*> optionsParser
              numProcessors
              (EnableScenarioService False)
              optPackageName
              enabledDlintUsageParser

cmdTest :: SdkVersion.Class.SdkVersioned => Int -> Mod CommandFields Command
cmdTest numProcessors =
    command "test" $ info (helper <*> cmd) $
       progDesc progDoc
    <> fullDesc
  where
    progDoc = unlines
      [ "Test the current Daml project or the given files by running all test declarations."
      , "Must be in Daml project if --files is not set."
      ]
    cmd = runTestsInProjectOrFiles
      <$> projectOpts "daml test"
      <*> filesOpt
      <*> fmap RunAllTests runAllTests
      <*> fmap LoadCoverageOnly loadCoverageOnly
      <*> fmap ShowCoverage showCoverageOpt
      <*> fmap UseColor colorOutput
      <*> junitOutput
      <*> optionsParser
            numProcessors
            (EnableScenarioService True)
            optPackageName
            disabledDlintUsageParser
      <*> initPkgDbOpt
      <*> fmap TableOutputPath tableOutputPathOpt
      <*> fmap TransactionsOutputPath transactionsOutputPathOpt
      <*> coveragePathsOpt
      <*> many coverageFilterSkipOpt
    filesOpt = optional (flag' () (long "files" <> help filesDoc) *> many inputFileOpt)
    filesDoc = "Only run test declarations in the specified files."
    junitOutput = optional $ strOptionOnce $ long "junit" <> metavar "FILENAME" <> help "Filename of JUnit output file"
    colorOutput = switch $ long "color" <> help "Colored test results"
    showCoverageOpt = switch $ long "show-coverage" <> help "Show detailed test coverage"
    runAllTests = switch $ long "all" <> help "Run tests in current project as well as dependencies"
    tableOutputPathOpt = optional $ strOptionOnce $ long "table-output" <> help "Filename to which table should be output"
    transactionsOutputPathOpt = optional $ strOptionOnce $ long "transactions-output" <> help "Filename to which the transaction list should be output"
    coveragePathsOpt =
      let loadCoveragePaths = many $ Options.Applicative.strOption $ long "load-coverage" <> help "File to read prior coverage results from. Can be specified more than once."
          saveCoveragePath = optional $ strOptionOnce $ long "save-coverage" <> help "File to write final aggregated coverage results to."
      in
      CoveragePaths <$> loadCoveragePaths <*> saveCoveragePath
    loadCoverageOnly = switch $ long "load-coverage-only" <> help "Don't run any tests - only load coverage results from files and write the aggregate to a single file."
    coverageFilterSkipOpt =
      Options.Applicative.option (Options.Applicative.readerAsk >>= fmap CoverageFilter . makeRegexM) $
        long "coverage-ignore-choice" <> help "Remove choices matching a regex from the coverage report. The full name of a local choice takes the format '<module>:<template name>:<choice name>', preceded by '<package id>:' for nonlocal packages."

runTestsInProjectOrFiles ::
       SdkVersion.Class.SdkVersioned
    => ProjectOpts
    -> Maybe [FilePath]
    -> RunAllTests
    -> LoadCoverageOnly
    -> ShowCoverage
    -> UseColor
    -> Maybe FilePath
    -> Options
    -> InitPkgDb
    -> TableOutputPath
    -> TransactionsOutputPath
    -> CoveragePaths
    -> [CoverageFilter]
    -> Command
runTestsInProjectOrFiles projectOpts mbInFiles allTests (LoadCoverageOnly True) coverage _ _ _ _ _ _ coveragePaths coverageFilters = Command Test (Just projectOpts) effect
  where effect = do
          when (getRunAllTests allTests) $ do
            hPutStrLn stderr "Cannot specify --all and --load-coverage-only at the same time."
            exitFailure
          case mbInFiles of
            Just _ -> do
              hPutStrLn stderr "Cannot specify --all and --load-coverage-only at the same time."
              exitFailure
            Nothing -> do
              loadAggregatePrintResults coveragePaths coverageFilters coverage Nothing
runTestsInProjectOrFiles projectOpts Nothing allTests _ coverage color mbJUnitOutput cliOptions initPkgDb tableOutputPath transactionsOutputPath coveragePaths coverageFilters = Command Test (Just projectOpts) effect
  where effect = withExpectProjectRoot (projectRoot projectOpts) "daml test" $ \pPath relativize -> do
        installDepsAndInitPackageDb cliOptions initPkgDb
        mbJUnitOutput <- traverse relativize mbJUnitOutput
        withPackageConfig (ProjectPath pPath) $ \PackageConfigFields{..} -> do
            -- TODO: We set up one scenario service context per file that
            -- we pass to execTest and scenario contexts are quite expensive.
            -- Therefore we keep the behavior of only passing the root file
            -- if source points to a specific file.
            files <- getDamlRootFiles pSrc
            execTest files allTests coverage color mbJUnitOutput cliOptions tableOutputPath transactionsOutputPath coveragePaths coverageFilters
runTestsInProjectOrFiles projectOpts (Just inFiles) allTests _ coverage color mbJUnitOutput cliOptions initPkgDb tableOutputPath transactionsOutputPath coveragePaths coverageFilters = Command Test (Just projectOpts) effect
  where effect = withProjectRoot' projectOpts $ \relativize -> do
        installDepsAndInitPackageDb cliOptions initPkgDb
        mbJUnitOutput <- traverse relativize mbJUnitOutput
        inFiles' <- mapM (fmap toNormalizedFilePath' . relativize) inFiles
        execTest inFiles' allTests coverage color mbJUnitOutput cliOptions tableOutputPath transactionsOutputPath coveragePaths coverageFilters

cmdInspect :: Mod CommandFields Command
cmdInspect =
    command "inspect" $ info (helper <*> cmd)
      $ progDesc "Pretty print a DALF file or the main DALF of a DAR file"
    <> fullDesc
  where
    jsonOpt = switch $ long "json" <> help "Output the raw Protocol Buffer structures as JSON"
    cmd = execInspect <$> inputFileOptWithExt ".dalf or .dar" <*> outputFileOpt <*> jsonOpt <*> cliOptDetailLevel

cmdBuild :: SdkVersion.Class.SdkVersioned => Int -> Mod CommandFields Command
cmdBuild numProcessors =
    command "build" $
    info (helper <*> cmdBuildParser numProcessors) $
    progDesc "Initialize, build and package the Daml project" <> fullDesc

cmdBuildParser :: SdkVersion.Class.SdkVersioned => Int -> Parser Command
cmdBuildParser numProcessors =
    execBuild
        <$> projectOpts "daml build"
        <*> optionsParser
              numProcessors
              (EnableScenarioService False)
              (pure Nothing)
              disabledDlintUsageParser
        <*> optionalOutputFileOpt
        <*> incrementalBuildOpt
        <*> initPkgDbOpt
        <*> enableMultiPackageOpt
        <*> multiPackageBuildAllOpt
        <*> multiPackageNoCacheOpt
        <*> multiPackageLocationOpt

cmdRepl :: SdkVersion.Class.SdkVersioned => Int -> Mod CommandFields Command
cmdRepl numProcessors =
    command "repl" $ info (helper <*> cmd) $
    progDesc "Launch the Daml REPL." <>
    fullDesc
  where
    cmd =
        execRepl
            <$> many (strArgument (help "DAR to load in the repl" <> metavar "DAR"))
            <*> many packageImport
            <*> optional
                  ((,) <$> strOptionOnce (long "ledger-host" <> help "Host of the ledger API")
                       <*> strOptionOnce (long "ledger-port" <> help "Port of the ledger API")
                  )
            <*> accessTokenFileFlag
            <*> optional
                  (ReplClient.ApplicationId <$>
                     strOptionOnce
                       (long "application-id" <>
                        help "Application ID used for command submissions"
                       )
                  )
            <*> sslConfig
            <*> optional
                    (optionOnce auto $
                        long "max-inbound-message-size" <>
                        help "Optional max inbound message size in bytes."
                    )
            <*> timeModeFlag
            <*> projectOpts "daml repl"
            <*> optionsParser
                  numProcessors
                  (EnableScenarioService False)
                  (pure Nothing)
                  disabledDlintUsageParser
            <*> strOptionOnce (long "script-lib" <> value "daml-script" <> internal)
            -- This is useful for tests and `bazel run`.

    packageImport = Options.Applicative.option readPackage $
        long "import"
        <> short 'i'
        <> help "Import modules of these packages into the REPL"
        <> metavar "PACKAGE"
      where
        readPackage = eitherReader $ \s -> do
            let pkg@(name, _) = splitUnitId (stringToUnitId s)
                strName = T.unpack . LF.unPackageName $ name
            unless (looksLikePackageName strName) $
                Left $ "Illegal package name: " ++ strName
            pure pkg
    accessTokenFileFlag = optional . optionOnce str $
        long "access-token-file"
        <> metavar "TOKEN_PATH"
        <> help "Path to the token-file for ledger authorization"

    sslConfig :: Parser (Maybe ReplClient.ClientSSLConfig)
    sslConfig = do
        tls <- switch $ mconcat
            [ long "tls"
            , help "Enable TLS for the connection to the ledger. This is implied if --cacrt, --pem or --crt are passed"
            ]
        mbCACert <- optional $ strOptionOnce $ mconcat
            [ long "cacrt"
            , help "The crt file to be used as the trusted root CA."
            ]
        mbClientKeyCertPair <- optional $ liftA2 ReplClient.ClientSSLKeyCertPair
            (strOptionOnce $ mconcat
                 [ long "pem"
                 , help "The pem file to be used as the private key in mutual authentication."
                 ]
            )
            (strOptionOnce $ mconcat
                 [ long "crt"
                 , help "The crt file to be used as the cert chain in mutual authentication."
                 ]
            )
        return $ case (tls, mbCACert, mbClientKeyCertPair) of
            (False, Nothing, Nothing) -> Nothing
            (_, _, _) -> Just ReplClient.ClientSSLConfig
                { serverRootCert = mbCACert
                , clientSSLKeyCertPair = mbClientKeyCertPair
                , clientMetadataPlugin = Nothing
                }

    timeModeFlag :: Parser ReplClient.ReplTimeMode
    timeModeFlag =
        (flag' ReplClient.ReplWallClock $ mconcat
            [ short 'w'
            , long "wall-clock-time"
            , help "Use wall clock time (UTC). (this is the default)"
            ])
        <|> (flag ReplClient.ReplWallClock ReplClient.ReplStatic $ mconcat
            [ short 's'
            , long "static-time"
            , help "Use static time."
            ])


cmdClean :: Mod CommandFields Command
cmdClean =
    command "clean" $
    info (helper <*> cmd) $
    progDesc "Remove Daml project build artifacts" <> fullDesc
  where
    cmd = execClean
            <$> projectOpts "daml clean"
            <*> enableMultiPackageOpt
            <*> multiPackageLocationOpt
            <*> multiPackageCleanAllOpt

cmdInit :: SdkVersion.Class.SdkVersioned => Int -> Mod CommandFields Command
cmdInit numProcessors =
    command "init" $
    info (helper <*> cmd) $ progDesc "Initialize a Daml project" <> fullDesc
  where
    cmd = execInit
            <$> optionsParser
                  numProcessors
                  (EnableScenarioService False)
                  (pure Nothing)
                  disabledDlintUsageParser
            <*> projectOpts "daml damlc init"

cmdPackage :: SdkVersion.Class.SdkVersioned => Int -> Mod CommandFields Command
cmdPackage numProcessors =
    command "package" $ info (helper <*> cmd) $
       progDesc "Compile the Daml program into a DAR (deprecated)"
    <> fullDesc
  where
    cmd = execPackage
        <$> projectOpts "daml damlc package"
        <*> inputFileOpt
        <*> optionsParser
              numProcessors
              (EnableScenarioService False)
              (Just <$> packageNameOpt)
              disabledDlintUsageParser
        <*> optionalOutputFileOpt
        <*> optFromDalf

    optFromDalf :: Parser FromDalf
    optFromDalf = fmap FromDalf $
      switch $
      help "package an existing dalf file rather than compiling Daml sources" <>
      long "dalf" <>
      internal

cmdInspectDar :: Mod CommandFields Command
cmdInspectDar =
    command "inspect-dar" $
    info (helper <*> cmd) $ progDesc "Inspect a DAR archive" <> fullDesc
  where
    jsonOpt =
        flag InspectDar.PlainText InspectDar.Json $
        long "json" <> help "Output the information in JSON"
    cmd = execInspectDar
        <$> inputDarOpt
        <*> jsonOpt

cmdValidateDar :: Mod CommandFields Command
cmdValidateDar =
    command "validate-dar" $
    info (helper <*> cmd) $ progDesc "Validate a DAR archive" <> fullDesc
  where
    cmd = execValidateDar <$> inputDarOpt

cmdMergeDars :: Mod CommandFields Command
cmdMergeDars =
    command "merge-dars" $
    info (helper <*> cmd) $ progDesc "Merge two dar archives into one" <> fullDesc
  where
    cmd = execMergeDars <$> inputDarOpt <*> inputDarOpt <*> targetFileNameOpt

cmdDocTest :: SdkVersion.Class.SdkVersioned => Int -> Mod CommandFields Command
cmdDocTest numProcessors =
    command "doctest" $
    info (helper <*> cmd) $
    progDesc "Early Access (Labs). doc tests" <> fullDesc
  where
    cmd = execDocTest
        <$> optionsParser
              numProcessors
              (EnableScenarioService True)
              optPackageName
              disabledDlintUsageParser
        <*> strOptionOnce (long "script-lib" <> value "daml-script" <> internal)
            -- This is useful for tests and `bazel run`.
        <*> (ImportSource <$> flagYesNoAuto "import-source" True "Should source code directory be directly imported" internal)
            -- We need this when generating docs for stdlib, as we do not want to directly provide the source here,
            -- instead we use the already provided stdlib in the env. The reason for this is that Daml.Script relies on stdlib,
            -- and recompiling it alongside Daml.Script causes issues with missing interface files
        <*> many inputFileOpt

cmdGenerateMultiPackageManifest :: SdkVersion.Class.SdkVersioned => Mod CommandFields Command
cmdGenerateMultiPackageManifest =
    command "generate-multi-package-manifest" $
    info (helper <*> cmd) $
    progDesc "Generates the multi-package manifest file used by external build systems" <> fullDesc
  where
    cmd = execGenerateMultiPackageManifest
        <$> multiPackageLocationOpt
        <*> generateMultiPackageManifestOutputOpt

--------------------------------------------------------------------------------
-- Execution
--------------------------------------------------------------------------------

execLicense :: Command
execLicense =
  Command License Nothing effect
  where
    effect = B.putStr licenseData
    licenseData :: B.ByteString
    licenseData = $(embedFile "NOTICES")

execIde :: SdkVersion.Class.SdkVersioned
        => Telemetry
        -> Debug
        -> EnableScenarioService
        -> StudioAutorunAllScenarios
        -> Options
        -> Command
execIde telemetry (Debug debug) enableScenarioService autorunAllScenarios options =
    Command Ide Nothing effect
  where effect = NS.withSocketsDo $ do
          let threshold =
                  if debug
                  then Logger.Debug
                  else Logger.Info
          loggerH <- Logger.IO.newIOLogger
            stderr
            (Just 5000)
            -- NOTE(JM): ^ Limit the message length to 5000 characters as VSCode
            -- performance will be significatly impacted by large log output.
            threshold
            "LanguageServer"
          damlCacheM <- lookupEnv damlCacheEnvVar
          damlPathM <- lookupEnv damlPathEnvVar
          let gcpConfig = Logger.GCP.GCPConfig
                  { gcpConfigTag = "ide"
                  , gcpConfigCachePath = damlCacheM
                  , gcpConfigDamlPath = damlPathM
                  }
              withLogger f = case telemetry of
                  TelemetryOptedIn ->
                    let logOfInterest prio = prio `elem` [Logger.Telemetry, Logger.Warning, Logger.Error] in
                    Logger.GCP.withGcpLogger gcpConfig logOfInterest loggerH $ \gcpStateM loggerH' -> do
                      whenJust gcpStateM $ \gcpState -> do
                        Logger.GCP.setOptIn gcpState
                        Logger.GCP.logMetaData gcpState
                      f loggerH'
                  TelemetryOptedOut -> Logger.GCP.withGcpLogger gcpConfig (const False) loggerH $ \gcpStateM loggerH -> do
                      whenJust gcpStateM $ \gcpState -> Logger.GCP.logOptOut gcpState
                      f loggerH
                  TelemetryIgnored -> Logger.GCP.withGcpLogger gcpConfig (const False) loggerH $ \gcpStateM loggerH -> do
                      whenJust gcpStateM $ \gcpState -> Logger.GCP.logIgnored gcpState
                      f loggerH
                  TelemetryDisabled -> f loggerH
          pkgPath <- getCanonDefaultProjectPath
          mPkgConfig <- withMaybeConfig (withPackageConfig pkgPath) pure
          let pkgConfUpgradeDar = pUpgradeDar =<< mPkgConfig
          options <- updateUpgradePath "ide" pkgPath options pkgConfUpgradeDar
          options <- pure options
              { optScenarioService = enableScenarioService
              , optEnableOfInterestRule = True
              , optSkipScenarioValidation = SkipScenarioValidation True
              -- TODO(MH): The `optionsParser` does not provide a way to skip
              -- individual options. As a stopgap we ignore the argument to
              -- --jobs.
              , optThreads = 0
              , optMbPackageName = pName <$> mPkgConfig
              , optMbPackageVersion = mPkgConfig >>= pVersion
              , optMbPackageConfigPath = Just pkgPath
              -- IDE was built around not passing the package name and version, assuming the unit-id is "main" everywhere
              -- For upgrade metadata checking, the IDE now needs the package name and version
              -- This flag allows us to still hide the unit-id from ghc, so our assumption that the unit-id is "main" holds.
              , optHideUnitId = True
              }
          installDepsAndInitPackageDb options (InitPkgDb True)
          scenarioServiceConfig <- readScenarioServiceConfig
          withLogger $ \loggerH ->
              withScenarioService' enableScenarioService (optEnableScenarios options) (optDamlLfVersion options) loggerH scenarioServiceConfig $ \mbScenarioService -> do
                  sdkVersion <- getSdkVersion `catchIO` const (pure "Unknown (not started via the assistant)")
                  Logger.logInfo loggerH (T.pack $ "SDK version: " <> sdkVersion)
                  debouncer <- newAsyncDebouncer
                  runLanguageServer loggerH enabledPlugins Config $ \lspEnv vfs _ ->
                      getDamlIdeState options autorunAllScenarios mbScenarioService loggerH debouncer (RealLspEnv lspEnv) vfs


-- | Whether we should write interface files during `damlc compile`.
newtype WriteInterface = WriteInterface Bool

execCompile :: SdkVersion.Class.SdkVersioned => FilePath -> FilePath -> Options -> WriteInterface -> Maybe FilePath -> Command
execCompile inputFile outputFile opts (WriteInterface writeInterface) mbIfaceDir =
  Command Compile (Just projectOpts) effect
  where
    projectOpts = ProjectOpts Nothing (ProjectCheck "" False)
    effect = withProjectRoot' projectOpts $ \relativize -> do
      loggerH <- getLogger opts "compile"
      inputFile <- toNormalizedFilePath' <$> relativize inputFile
      opts <- pure opts { optIfaceDir = mbIfaceDir }
      withDamlIdeState opts loggerH diagnosticsLogger $ \ide -> do
          setFilesOfInterest ide (HashSet.singleton inputFile)
          runActionSync ide $ do
            -- Support for '-ddump-parsed', '-ddump-parsed-ast', '-dsource-stats'.
            dflags <- hsc_dflags . hscEnv <$> use_ GhcSession inputFile
            parsed <- pm_parsed_source <$> use_ GetParsedModule inputFile
            liftIO $ do
              ErrUtils.dumpIfSet_dyn dflags Opt_D_dump_parsed "Parser" $ GHC.ppr parsed
              ErrUtils.dumpIfSet_dyn dflags Opt_D_dump_parsed_ast "Parser AST" $ showAstData NoBlankSrcSpan parsed
              ErrUtils.dumpIfSet_dyn dflags Opt_D_source_stats "Source Statistics" $ ppSourceStats False parsed

            when writeInterface $ do
                files <- nubSort . concatMap transitiveModuleDeps <$> use GetDependencies inputFile
                mbIfaces <- writeIfacesAndHie (toNormalizedFilePath' $ fromMaybe ifaceDir $ optIfaceDir opts) files
                void $ liftIO $ mbErr "ERROR: Compilation failed." mbIfaces

            mbDalf <- getDalf inputFile
            dalf <- liftIO $ mbErr "ERROR: Compilation failed." mbDalf
            liftIO $ write dalf
    write bs
      | outputFile == "-" = putStrLn $ render Colored $ DA.Pretty.pretty bs
      | otherwise = do
        createDirectoryIfMissing True $ takeDirectory outputFile
        B.writeFile outputFile $ Archive.encodeArchive bs

execDesugar :: SdkVersion.Class.SdkVersioned => FilePath -> FilePath -> Options -> Command
execDesugar inputFile outputFile opts = Command Desugar (Just projectOpts) effect
  where
    projectOpts = ProjectOpts Nothing (ProjectCheck "" False)
    effect = withProjectRoot' projectOpts $ \relativize ->
      liftIO . write =<< desugar opts =<< relativize inputFile
    write s
      | outputFile == "-" = T.putStrLn s
      | otherwise = do
        createDirectoryIfMissing True $ takeDirectory outputFile
        T.writeFile outputFile s

execDebugIdeSpanInfo :: SdkVersion.Class.SdkVersioned => FilePath -> FilePath -> Options -> Command
execDebugIdeSpanInfo inputFile outputFile opts =
  Command DebugIdeSpanInfo (Just projectOpts) effect
  where
    projectOpts = ProjectOpts Nothing (ProjectCheck "" False)
    effect =
      withProjectRoot' projectOpts $ \relativize -> do
        loggerH <- getLogger opts "debug-ide-span-info"
        inputFile <- toNormalizedFilePath' <$> relativize inputFile
        spanInfo <- withDamlIdeState opts loggerH diagnosticsLogger $ \ide -> do
          setFilesOfInterest ide (HashSet.singleton inputFile)
          runActionSync ide $ do
            dflags <- hsc_dflags . hscEnv <$> use_ GhcSession inputFile
            getSpanInfo dflags inputFile
        write $ Aeson.encodeToLazyText spanInfo
    write s
      | outputFile == "-" = TL.putStrLn s
      | otherwise = do
        createDirectoryIfMissing True $ takeDirectory outputFile
        TL.writeFile outputFile s

execLint :: SdkVersion.Class.SdkVersioned => [FilePath] -> Options -> Command
execLint inputFiles opts =
  Command Lint (Just projectOpts) effect
  where
     projectOpts = ProjectOpts Nothing (ProjectCheck "" False)
     effect =
       withProjectRoot' projectOpts $ \relativize ->
       do
         loggerH <- getLogger opts "lint"
         withDamlIdeState opts loggerH diagnosticsLogger $ \ide -> do
             inputFiles <- getInputFiles relativize inputFiles
             setFilesOfInterest ide (HashSet.fromList inputFiles)
             diags <- forM inputFiles $ \inputFile -> do
               void $ runActionSync ide $ getDlintIdeas inputFile
               getDiagnostics ide
             if all null diags then
               hPutStrLn stderr "No hints"
             else
               exitFailure
     getInputFiles relativize = \case
       [] -> do
           withPackageConfig defaultProjectPath $ \PackageConfigFields {pSrc} -> do
           getDamlRootFiles pSrc
       fs -> forM fs $ fmap toNormalizedFilePath' . relativize

defaultProjectPath :: ProjectPath
defaultProjectPath = ProjectPath "."

getCanonDefaultProjectPath :: IO ProjectPath
getCanonDefaultProjectPath = fmap ProjectPath $ canonicalizePath $ unwrapProjectPath defaultProjectPath

-- | If we're in a daml project, read the daml.yaml field, install the dependencies and create the
-- project local package database. Otherwise do nothing.
execInit :: SdkVersion.Class.SdkVersioned => Options -> ProjectOpts -> Command
execInit opts projectOpts =
  Command Init (Just projectOpts) effect
  where effect = withProjectRoot' projectOpts $ \_relativize ->
          installDepsAndInitPackageDb
            opts
            (InitPkgDb True)

installDepsAndInitPackageDb :: SdkVersion.Class.SdkVersioned => Options -> InitPkgDb -> IO ()
installDepsAndInitPackageDb opts (InitPkgDb shouldInit) =
    when shouldInit $ do
        -- Rather than just checking that there is a daml.yaml file we check that it has a project configuration.
        -- This allows us to have a `daml.yaml` in the root of a multi-package project that just has an `sdk-version` field.
        -- Once the IDE handles `initPackageDb` properly in multi-package projects instead of simply calling it once on
        -- startup, this can be removed.
        isProject <- withPackageConfig defaultProjectPath (const $ pure True) `catch` (\(_ :: ConfigError) -> pure False)
        when isProject $ do
            projRoot <- getCurrentDirectory
            withPackageConfig defaultProjectPath $ \PackageConfigFields {..} -> do
              damlAssistantIsSet <- damlAssistantIsSet
              releaseVersion <- if damlAssistantIsSet
                  then do
                    damlPath <- getDamlPath
                    damlEnv <- getDamlEnv damlPath (LookForProjectPath False)
                    wrapErr "installing dependencies and initializing package database" $
                      resolveReleaseVersionUnsafe (envUseCache damlEnv) pSdkVersion
                  else pure (unsafeResolveReleaseVersion pSdkVersion)
              installDependencies
                  (toNormalizedFilePath' projRoot)
                  opts
                  releaseVersion
                  pDependencies
                  pDataDependencies
              createProjectPackageDb (toNormalizedFilePath' projRoot) opts pModulePrefixes

getMultiPackagePath :: MultiPackageLocation -> IO (Maybe ProjectPath)
getMultiPackagePath multiPackageLocation =
  case multiPackageLocation of
    MPLSearch -> findMultiPackageConfig defaultProjectPath
    MPLPath path -> do
      hasMultiPackage <- doesFileExist $ path </> multiPackageConfigName
      unless hasMultiPackage $ do
        hPutStrLn stderr $ (path </> multiPackageConfigName) <> " does not exist."
        exitFailure
      pure $ Just $ ProjectPath path

execBuild
  :: SdkVersion.Class.SdkVersioned
  => ProjectOpts
  -> Options
  -> Maybe FilePath
  -> IncrementalBuild
  -> InitPkgDb
  -> EnableMultiPackage
  -> MultiPackageBuildAll
  -> MultiPackageNoCache
  -> MultiPackageLocation
  -> Command
execBuild projectOpts opts mbOutFile incrementalBuild initPkgDb enableMultiPackage buildAll noCache multiPackageLocation =
  Command Build (Just projectOpts) $ evalContT $ do
    relativize <- ContT $ withProjectRoot' (projectOpts {projectCheck = ProjectCheck "" False})

    let buildSingle :: ProjectPath -> PackageConfigFields -> IO ()
        buildSingle pkgPath pkgConfig = void $ buildEffect relativize pkgPath pkgConfig opts mbOutFile incrementalBuild initPkgDb
        buildMulti :: ProjectPath -> Maybe PackageConfigFields -> ProjectPath -> IO ()
        buildMulti pkgPath mPkgConfig multiPackageConfigPath = do
          hPutStrLn stderr $ "Running multi-package build of "
            <> maybe ("all packages in " <> unwrapProjectPath multiPackageConfigPath) (T.unpack . LF.unPackageName . pName) mPkgConfig <> "."
          withMultiPackageConfig multiPackageConfigPath $ \multiPackageConfig ->
            multiPackageBuildEffect relativize pkgPath mPkgConfig multiPackageConfig opts mbOutFile incrementalBuild initPkgDb noCache

    pkgPath <- liftIO getCanonDefaultProjectPath
    mPkgConfig <- ContT $ withMaybeConfig $ withPackageConfig pkgPath
    liftIO $ if getEnableMultiPackage enableMultiPackage then do
      mMultiPackagePath <- getMultiPackagePath multiPackageLocation
      -- At this point, if mMultiPackagePath is Just, we know it points to a multi-package.yaml

      case (getMultiPackageBuildAll buildAll, mPkgConfig, mMultiPackagePath) of
        -- We're attempting to multi-package build --all, so we require that we have a multi-package.yaml, but do not care if we have a daml.yaml
        (True, _, Just multiPackagePath) ->
          -- TODO[SW]: Ideally we would error here if any of the flags that change `opts` has been set, as it won't be propagated
          -- Its unclear how to implement this.
          buildMulti pkgPath Nothing multiPackagePath

        -- We're attempting to multi-package build --all but we don't have a multi-package.yaml
        (True, _, Nothing) -> do
          hPutStrLn stderr
            "Attempted to build all packages, but could not find a multi-package.yaml at current or parent directory. Use --multi-package-path to specify its location."
          exitFailure

        -- We know the package we want and we have a multi-package.yaml
        (False, Just pkgConfig, Just multiPackagePath) -> buildMulti pkgPath (Just pkgConfig) multiPackagePath

        -- We know the package we want but we do not have a multi-package. The user has provided no reason they would want a multi-package build.
        (False, Just pkgConfig, Nothing) -> do
          hPutStrLn stderr $ "Running single package build of " <> T.unpack (LF.unPackageName $ pName pkgConfig) <> " as no multi-package.yaml was found."
          buildSingle pkgPath pkgConfig

        -- We have no package context, but we have found a multi package at the current directory
        (False, Nothing, Just _) -> do
          hPutStrLn stderr $ "No valid daml.yaml found, but a multi-package.yaml was found instead.\n"
            <> "If you intended to build everything within this multi-package.yaml, use the --all flag"
          exitFailure

        -- We have nothing, we're lost
        (False, Nothing, Nothing) -> do
          hPutStrLn stderr "No valid daml.yaml or multi-package.yaml could be found."
          exitFailure
    else
      case mPkgConfig of
        Nothing -> do
          hPutStrLn stderr "daml build: Not in project."
          exitFailure
        Just pkgConfig -> do
          let usedMultiPackageOption =
                getMultiPackageBuildAll buildAll
                  || getMultiPackageNoCache noCache
                  || multiPackageLocation /= MPLSearch
          if usedMultiPackageOption
            then do
              hPutStrLn stderr "Multi-package build option used with multi-package disabled - re-enable it by setting the --enable-multi-package=yes flag."
              exitFailure
            else buildSingle pkgPath pkgConfig

-- Takes the withPackageConfig style functions and changes the continuation
-- to give a Maybe, where Nothing signifies a missing file. Parse errors are still thrown
withMaybeConfig
  :: forall config x
  .  (forall y. (config -> IO y) -> IO y)
  -> (Maybe config -> IO x)
  -> IO x
withMaybeConfig withConfig handler = do
  mConfig <-
    handle (\case
      ConfigFileInvalid _ (Y.InvalidYaml (Just (Y.YamlException exc))) | "Yaml file not found: " `isPrefixOf` exc ->
        pure Nothing
      ConfigFileInvalid _ (Y.InvalidYaml (Just (Y.YamlException exc))) | "packageless daml.yaml" `isInfixOf` exc -> do
        hPutStrLn stderr "Found daml.yaml with only sdk-version, ignoring this file."
        pure Nothing
      e -> throwIO e
    ) (withConfig $ pure . Just)
  handler mConfig

buildEffect
  :: SdkVersion.Class.SdkVersioned
  => (FilePath -> IO FilePath)
  -> ProjectPath
  -> PackageConfigFields
  -> Options
  -> Maybe FilePath
  -> IncrementalBuild
  -> InitPkgDb
  -> IO (Maybe LF.PackageId)
buildEffect relativize pkgPath pkgConfig opts mbOutFile incrementalBuild initPkgDb = do
  (pkgConfig, opts) <- syncUpgradesField pkgPath pkgConfig opts
  let PackageConfigFields{..} = pkgConfig
  installDepsAndInitPackageDb opts initPkgDb
  loggerH <- getLogger opts "build"
  Logger.logInfo loggerH $ "Compiling " <> LF.unPackageName pName <> " to a DAR."
  let errors = checkPkgConfig pkgConfig
  unless (null errors) $ do
      mapM_ (Logger.logError loggerH) errors
      exitFailure
  withDamlIdeState
      opts
        { optMbPackageName = Just pName
        , optMbPackageVersion = pVersion
        , optMbPackageConfigPath = Just pkgPath
        , optIncrementalBuild = incrementalBuild
        }
      loggerH
      diagnosticsLogger $ \compilerH -> do
      fp <- targetFilePath relativize $ unitIdString (pkgNameVersion pName pVersion)
      mbDar <-
          buildDar
              compilerH
              pkgConfig
              (toNormalizedFilePath' $ fromMaybe ifaceDir $ optIfaceDir opts)
              (FromDalf False)
              (optUpgradeInfo opts)
      (dar, mPkgId) <- mbErr "ERROR: Creation of DAR file failed." mbDar
      createDarFile loggerH fp dar
      pure mPkgId
    where
        targetFilePath rel name =
          case mbOutFile of
            Nothing -> pure $ distDir </> name <.> "dar"
            Just out -> rel out

        syncUpgradesField :: ProjectPath -> PackageConfigFields -> Options -> IO (PackageConfigFields, Options)
        syncUpgradesField pkgPath pkgConf opts = do
          opts <- updateUpgradePath "build" pkgPath opts (pUpgradeDar pkgConf)
          pure (pkgConf { pUpgradeDar = uiUpgradedPackagePath (optUpgradeInfo opts) }, opts)

updateUpgradePath :: T.Text -> ProjectPath -> Options -> Maybe FilePath -> IO Options
updateUpgradePath context projectPath opts@Options{optUpgradeInfo} newPkgPath = do
  let projectFilePath = unwrapProjectPath projectPath
  optCanonPath <- traverse canonicalizePath $ uiUpgradedPackagePath optUpgradeInfo
  pkgCanonPath <- withCurrentDirectory projectFilePath $ traverse canonicalizePath newPkgPath
  -- optUpgradeInfo is normalised to current directory
  -- newPkgPath is normalised to daml.yaml location (or currentDirectory)
  uiUpgradedPackagePath <- case (pkgCanonPath, optCanonPath) of
    (Just damlYamlOption, Just buildFlagsOption) | damlYamlOption /= buildFlagsOption -> do
      loggerH <- getLogger opts context
      Logger.logError loggerH $ T.unlines
        [ "ERROR: Specified two different DARs to run upgrade checks against:"
        , "  Path specified in daml.yaml `upgrades:` field is '" <> T.pack (makeRelative projectFilePath damlYamlOption) <> "'"
        , "  Path specified in `--upgrades` build option is '" <> T.pack (makeRelative projectFilePath buildFlagsOption) <> "'"
        ]
      exitFailure
    (a, b) ->
      pure (a <|> b)
  pure $ opts { optUpgradeInfo = optUpgradeInfo { uiUpgradedPackagePath } }


{- | Multi Build! (the multi-package.yaml approach)
  This is currently separate to build, but won't be in future.
  Multi build is a light wrapper over the top of the existing build, and does not interact with the internals of the build process at all
  It spins up its own empty shake environment purely for the caching behaviour, and defers as soon as possible to the usual build process
  We strictly defer via the CLI interface, to support different versions of the SDK in future.

  The general idea is taken somewhat from cabal, in which we define some kind of project file (currently multi-package.yaml)
  which lists directories to packages (folders containing a daml.yaml) that are in our project, and we'd like to rebuild together.
  Then, any data-dependency on one of these packages will first ensure the dar artefact is up to date with the source, and rebuild if not.
  As such, any set of packages with these relations will continue to be buildable without the multi-package.yaml.
  Given we may want to build specific packages within our dependency multi-tree (be that a sub tree, or an alternate root),
  the multi-package.yaml is not required to be in the same directory as any given package. We only require it is at or above
  (in the directory tree) the package you are building. multi-build will first look for this file before attempting to build.

  The process is as follows:
  - Search for the multi-package.yaml file by traversing up the directory tree to root until we are at a directory containing the file.
  - Set up own shake environment, defer to it immediately with the project path
  - Within the shake rule:
  - Partially read the daml.yaml file, looking for data dependencies, and some other details needed for caching
  - Intercept the packages data dependencies with the packages we have listed in multi-package.yaml.
  - Call to shake to build all the above subset of data-dependencies, each will return data about the DAR that exists (or was built)
  - If we already have a DAR, check its staleness. Staleness is defined by any source files being different
      or any of the package ids of souce dependencies being different between what is currently built, and what is embedded in our dar.
  - If either are different, call to the SDK via CLI to build our own DAR.
  - Get the package ID of the DAR we have created, and return it to shake, which can use it as a caching key.

  Due to shakes caching behaviour, this will automatically discover the build order for free, by doing a depth first search over the tree then
    caching any "nodes" we've already seen entirely within Shake - our rule will not be called twice for any package within the tree for any given multi-build.

  We currently rely on shake to find cycles, how its errors includes too much information about internals, so we'll need to implement our own cycle detection.
-}
multiPackageBuildEffect
  :: SdkVersion.Class.SdkVersioned
  => (FilePath -> IO FilePath)
  -> ProjectPath
  -> Maybe PackageConfigFields -- Nothing signifies build all
  -> MultiPackageConfigFields
  -> Options
  -> Maybe FilePath
  -> IncrementalBuild
  -> InitPkgDb
  -> MultiPackageNoCache
  -> IO ()
multiPackageBuildEffect relativize pkgPath mPkgConfig multiPackageConfig opts mbOutFile incrementalBuild initPkgDb noCache = do
  vfs <- makeVFSHandle
  loggerH <- getLogger opts "multi-package build"
  assistantPath <- getEnv "DAML_ASSISTANT"
  -- Must drop DAML_PROJECT from env var so it can be repopulated based on `cwd`
  assistantEnv <- filter (flip notElem ["DAML_PROJECT", "DAML_SDK_VERSION", "DAML_SDK"] . fst) <$> getEnvironment

  buildableDataDepsMapping <- fmap Map.fromList $ for (mpPackagePaths multiPackageConfig) $ \path -> do
    darPath <- darPathFromDamlYaml path
    pure (darPath, path)

  let assistantRunner = AssistantRunner $ \location args ->
        withCreateProcess ((proc assistantPath args) {cwd = Just location, env = Just assistantEnv}) $ \_ _ _ p -> do
          exitCode <- waitForProcess p
          when (exitCode /= ExitSuccess) $ error $ "Failed to build package at " <> location <> "."

      buildableDataDeps = BuildableDataDeps $ flip Map.lookup buildableDataDepsMapping
      mRootPkgBuilder = flip fmap mPkgConfig $ \pkgConfig -> do
        mPkgId <- buildEffect relativize pkgPath pkgConfig opts mbOutFile incrementalBuild initPkgDb
        pure $ fromMaybe
          (error "Internal error: root package was built from dalf, giving no package-id. This is incompatible with multi-package")
          mPkgId
      mRootPkgData = (toNormalizedFilePath' $ unwrapProjectPath pkgPath,) <$> mRootPkgBuilder
      rule = buildMultiRule assistantRunner buildableDataDeps noCache mRootPkgData

  -- Set up a near empty shake environment, with just the buildMulti rule
  bracket
    (IDE.initialise rule (DummyLspEnv diagnosticsLogger) (toIdeLogger loggerH) noopDebouncer (toCompileOpts opts) vfs)
    IDE.shutdown
    $ \ideState -> runActionSync ideState
      $ case mRootPkgData of
          -- `uses_` tries to run in "parallel", but ends up just running backwards for multi-build.
          -- We use `reverse` here to keep the build order consistent with the multi-package.yaml
          Nothing -> void $ uses_ BuildMulti $ reverse $ toNormalizedFilePath' <$> mpPackagePaths multiPackageConfig
          Just (rootPkgPath, _) -> void $ use_ BuildMulti rootPkgPath

data AssistantRunner = AssistantRunner { runAssistant :: FilePath -> [String] -> IO ()}

-- Stores a mapping from dar path to project path
data BuildableDataDeps = BuildableDataDeps { getDataDepSource :: FilePath -> Maybe FilePath }

data BuildMulti = BuildMulti
    deriving (Eq, Show, Typeable, Generic)
instance Binary BuildMulti
instance Hashable BuildMulti
instance NFData BuildMulti

-- We return all the information needed to derive the dalf name
type instance RuleResult BuildMulti = (LF.PackageName, LF.PackageVersion, LF.PackageId)

-- Subset of PackageConfig needed for multi-package build deferring.
data BuildMultiPackageConfig = BuildMultiPackageConfig
  { bmSdkVersion :: UnresolvedReleaseVersion
  , bmName :: LF.PackageName
  , bmVersion :: LF.PackageVersion
  , bmDarDeps :: [FilePath]
      -- ^ not canonicalized, all dars that the package requires to build, including "upgrades"
  , bmSourceDaml :: FilePath
      -- ^ not canonicalized
  , bmOutput :: Maybe FilePath
      -- ^ not canonicalized
  }

-- Version of getDamlFiles from Dar.hs that assumes source is a folder, and runs in IO rather than action
-- Warns and gives an empty map when source is a file
getDamlFilesBuildMulti :: (String -> IO ()) -> FilePath -> FilePath -> IO (Map.Map FilePath BSL.ByteString)
getDamlFilesBuildMulti log packagePath srcDir = do
  let path = normalise $ packagePath </> srcDir
  exists <- doesDirectoryExist path
  if exists then do
    files <- fmap fromNormalizedFilePath <$> damlFilesInDir path
    Map.fromList <$> traverse (\file -> (makeRelative path file,) <$> BSL.readFile file) files
  else do
    log $ "WARNING: Multi-package caching does not support setting the daml.yaml source value to a daml file. This package will always rebuild.\n"
      <> "Change the source value in "
      <> (packagePath </> projectConfigName)
      <> " to a directory to allow correct caching.\n"
    pure Map.empty

-- Extract the name/version string and package ID of a given dalf in a dar.
entryToDalfData :: ZipArchive.Entry -> Maybe (String, LF.PackageId)
entryToDalfData entry = do
  let fileName = takeBaseName $ ZipArchive.eRelativePath entry
  lastDash <- listToMaybe $ reverse $ elemIndices '-' fileName
  pure $ second (LF.PackageId . T.pack . tail) $ splitAt lastDash fileName

dalfDataKey :: LF.PackageName -> LF.PackageVersion -> String
dalfDataKey name version = T.unpack (LF.unPackageName name <> "-" <> LF.unPackageVersion version)

-- Given an archive, returns the packagename-packageversion string with package ID for all dalfs, and the file path and file contents for all daml files.
readDarStalenessData :: ZipArchive.Archive -> (Map.Map String LF.PackageId, Map.Map FilePath BSL.ByteString)
readDarStalenessData archive =
  bimap Map.fromList Map.fromList . partitionEithers $
    flip mapMaybe (ZipArchive.zEntries archive) $ \entry ->
      case takeExtension $ ZipArchive.eRelativePath entry of
        ".dalf" -> Left <$> entryToDalfData entry
        ".daml" ->
          -- Drop the first directory in the path, as thats the folder containing the entire package contents.
          -- Previously structured this way to support multiple packages in one dar.
          let relPackagePath = joinPath $ tail $ splitPath $ ZipArchive.eRelativePath entry
           in Just $ Right (relPackagePath, ZipArchive.fromEntry entry)
        _ -> Nothing

getDarSdkVersion :: ZipArchive.Archive -> Either String String
getDarSdkVersion dar = Reader.sdkVersion <$> readDalfManifest dar

-- | Gets the package id of a Dar in the given project ONLY if it is not stale.
getPackageIdIfFresh :: IDELogger.Logger -> FilePath -> BuildMultiPackageConfig -> [(LF.PackageName, LF.PackageVersion, LF.PackageId)] -> IO (Maybe LF.PackageId)
getPackageIdIfFresh logger path BuildMultiPackageConfig {..} sourceDepPids = do
  let logDebug str = IDELogger.logDebug logger $ T.pack $ path <> ": " <> str
      logInfo str = IDELogger.logInfo logger $ T.pack str
  darPath <- deriveDarPath path bmName bmVersion bmOutput
  exists <- doesFileExist darPath
  if not exists
    then do
      logDebug "No DAR found, build is stale."
      pure Nothing
    else do
      logDebug "DAR found, checking staleness."
      -- Get the real source files we expect to be included in the dar
      sourceFiles <- getDamlFilesBuildMulti logInfo path bmSourceDaml

      dar <- ZipArchive.toArchive <$> BSL.readFile darPath

      -- Pull all information we need from the dar.
      let (archiveDalfPids, archiveSourceFiles) = readDarStalenessData dar
          archiveSdkVersion = getDarSdkVersion dar

          getArchiveDalfPid :: LF.PackageName -> LF.PackageVersion -> Maybe LF.PackageId
          getArchiveDalfPid name version = dalfDataKey name version `Map.lookup` archiveDalfPids
          -- We check source files by comparing the maps, any extra, missing or changed files will be covered by this, regardless of order
          sourceFilesCorrect = sourceFiles == archiveSourceFiles
          sdkVersionCorrect = archiveSdkVersion == Right (unresolvedReleaseVersionToString bmSdkVersion)

      -- Expanded `all` check that allows us to log specific dependencies being stale. Useful for debugging, maybe we can remove it.
      sourceDepsCorrect <-
        allM (\(name, version, pid) -> do
          let archiveDalfPid = getArchiveDalfPid name version
              valid = archiveDalfPid == Just pid

          when (not valid) $ logDebug $
            "Source dependency \"" <> dalfDataKey name version <> "\" is stale. Expected PackageId "
              <> T.unpack (LF.unPackageId pid) <> " but got " <> (show $ LF.unPackageId <$> archiveDalfPid) <> "."

          pure valid
        ) sourceDepPids

      logDebug $ "Source dependencies are " <> (if sourceDepsCorrect then "not " else "") <> "stale."
      logDebug $ "Source files are " <> (if sourceFilesCorrect then "not " else "") <> "stale."
      logDebug $ "Sdk version is " <> (if sdkVersionCorrect then "not " else "") <> "stale."
      pure $ if sourceDepsCorrect && sourceFilesCorrect && sdkVersionCorrect then getArchiveDalfPid bmName bmVersion else Nothing

damlMultiBuildVersion :: UnresolvedReleaseVersion
damlMultiBuildVersion = either throw id $ parseUnresolvedVersion "2.8.0-snapshot.20231018.0"

buildMultiRule
  :: AssistantRunner
  -> BuildableDataDeps
  -> MultiPackageNoCache
  -> Maybe (NormalizedFilePath, IO LF.PackageId)
  -> Rules ()
buildMultiRule assistantRunner buildableDataDeps (MultiPackageNoCache noCache) mRootPackage =
  defineEarlyCutoffWithDefaultRunChanged $ \BuildMulti path -> do
    logger <- actionLogger

    liftIO $ IDELogger.logDebug logger $ T.pack $ "Considering building: " <> fromNormalizedFilePath path

    let filePath = fromNormalizedFilePath path
    bmPkgConfig@BuildMultiPackageConfig {..} <- liftIO $ buildMultiPackageConfigFromDamlYaml filePath

    -- Wrap up a PackageId and changed flag into the expected Shake return structure
    let makeReturn :: LF.PackageId -> Bool -> (Maybe B.ByteString, RunChanged, IdeResult (LF.PackageName, LF.PackageVersion, LF.PackageId))
        makeReturn pid changed =
          ( Just $ encodeUtf8 $ LF.unPackageId pid
          , if changed then ChangedRecomputeDiff else ChangedRecomputeSame
          , ([], Just (bmName, bmVersion, pid))
          )

    toBuild <- liftIO $ withCurrentDirectory filePath $ flip mapMaybeM bmDarDeps $ \darPath -> do
      canonDarPath <- canonicalizePath darPath
      pure $ getDataDepSource buildableDataDeps canonDarPath

    -- Build all our deps!
    sourceDepsData <- uses_ BuildMulti $ toNormalizedFilePath' <$> toBuild

    liftIO $ case mRootPackage of
      -- We're building the root package, we'll envoke the normal builder directly with the Options already applied
      Just (pkgRootPath, builder) | path == pkgRootPath -> do
        IDELogger.logInfo logger $ T.pack $ "Building " <> filePath
        pkgId <- builder
        pure $ makeReturn pkgId True
      _ -> do
        -- Check our own staleness, if we're not stale, give the package ID to return to shake.
        -- If caching disabled, we fail this check pre-emtively
        ownValidPid <-
          if noCache
            then pure Nothing
            else getPackageIdIfFresh logger filePath bmPkgConfig sourceDepsData

        case ownValidPid of
          Just pid -> do
            IDELogger.logInfo logger $ T.pack $ filePath <> " is unchanged."
            pure $ makeReturn pid False
          Nothing -> do
            IDELogger.logInfo logger $ T.pack $ "Building " <> filePath

            -- Call build via daml assistant so it selects the correct SDK version.
            runAssistant assistantRunner filePath $
              ["build"] <> (["--enable-multi-package=no" | bmSdkVersion >= damlMultiBuildVersion || isHeadVersion bmSdkVersion])

            darPath <- deriveDarPath filePath bmName bmVersion bmOutput
            -- Extract the new package ID from the dar we just built, by reading the DAR and looking for the dalf that matches our package name/version
            archive <- ZipArchive.toArchive <$> BSL.readFile darPath
            let archiveDalfPids = Map.fromList $ mapMaybe entryToDalfData $ ZipArchive.zEntries archive
                mOwnPid = dalfDataKey bmName bmVersion `Map.lookup` archiveDalfPids
                ownPid = fromMaybe (error "Failed to get PID of own package - implies a DAR was built without its own dalf, something broke!") mOwnPid

            pure $ makeReturn ownPid True

execRepl
    :: SdkVersion.Class.SdkVersioned
    => [FilePath]
    -> [(LF.PackageName, Maybe LF.PackageVersion)]
    -> Maybe (String, String)
    -> Maybe FilePath
    -> Maybe ReplClient.ApplicationId
    -> Maybe ReplClient.ClientSSLConfig
    -> Maybe ReplClient.MaxInboundMessageSize
    -> ReplClient.ReplTimeMode
    -> ProjectOpts
    -> Options
    -> FilePath
    -> Command
execRepl dars importPkgs mbLedgerConfig mbAuthToken mbAppId mbSslConf mbMaxInboundMessageSize timeMode projectOpts opts scriptDar = Command Repl (Just projectOpts) effect
  where
        toPackageFlag (LF.PackageName name, Nothing) =
            ExposePackage "--import" (PackageArg $ T.unpack name) (ModRenaming True [])
        toPackageFlag (name, mbVer) =
            ExposePackage "--import" (UnitIdArg (pkgNameVersion name mbVer)) (ModRenaming True [])
        pkgFlags = [ toPackageFlag pkg | pkg <- importPkgs ]
        effect = do
            -- We change directory so make this absolute
            dars <- mapM makeAbsolute dars
            opts <- pure opts
                { optScenarioService = EnableScenarioService False
                , optPackageImports = optPackageImports opts ++ pkgFlags
                }
            logger <- getLogger opts "repl"
            jar <- locateResource Resource
                -- //compiler/repl-service/server:repl_service_jar
                { resourcesPath = "repl-service.jar"
                  -- In a packaged application, this is stored directly underneath
                  -- the resources directory because it's the target's only output.
                  -- See @bazel_tools/packaging/packaging.bzl@.
                , runfilesPathPrefix = mainWorkspace </> "compiler" </> "repl-service" </> "server"
                }
            let replClientOptions =
                    ReplClient.Options
                        jar
                        mbLedgerConfig
                        mbAuthToken
                        mbAppId
                        mbSslConf
                        mbMaxInboundMessageSize
                        timeMode
                        (LF.versionMajor (optDamlLfVersion opts))
                        Inherit
            ReplClient.withReplClient replClientOptions $ \replHandle ->
                withTempDir $ \dir ->
                withCurrentDirectory dir $ do
                sdkVer <- fromMaybe SdkVersion.Class.sdkVersion <$> lookupEnv sdkVersionEnvVar
                writeFileUTF8 "daml.yaml" $ unlines $
                    [ "sdk-version: " <> sdkVer
                    , "name: repl"
                    , "version: 0.0.1"
                    , "source: ."
                    , "dependencies:"
                    , "- daml-prim"
                    , "- daml-stdlib"
                    , "- " <> show scriptDar
                    , "data-dependencies:"
                    ] ++ ["- " <> show dar | dar <- dars]
                installDepsAndInitPackageDb opts (InitPkgDb True)
                replLogger <- Repl.newReplLogger
                withDamlIdeState opts logger (Repl.replEventLogger replLogger)
                    (Repl.runRepl importPkgs opts replHandle replLogger)

-- | Remove any build artifacts if they exist.
execClean :: ProjectOpts -> EnableMultiPackage -> MultiPackageLocation -> MultiPackageCleanAll -> Command
execClean projectOpts enableMultiPackage multiPackageLocation cleanAll =
  Command Clean (Just projectOpts) effect
  where
    effect
      | getEnableMultiPackage enableMultiPackage
      = do
        mMultiPackagePath <- getMultiPackagePath multiPackageLocation
        isProject <- withProjectRoot' (projectOpts {projectCheck = ProjectCheck "" False}) $ const $ doesFileExist projectConfigName
        case (mMultiPackagePath, isProject, getMultiPackageCleanAll cleanAll) of
          -- daml clean --all with no multi-package.yaml
          (Nothing, _, True) -> do
            hPutStrLn stderr "Couldn't find a multi-package.yaml at current or parent directory. Use --multi-package-path to specify its location."
            exitFailure
          -- daml clean --all with a multi-package.yaml
          (Just path, _, True) ->
            withMultiPackageConfig path $ \multiPackageConfig -> do
              hPutStrLn stderr $ "Running multi-package clean of all packages in " <> unwrapProjectPath path
              forM_ (mpPackagePaths multiPackageConfig) $ \p ->
                singleCleanEffect $ projectOpts {projectRoot = Just $ ProjectPath p}
              hPutStrLn stderr "Removed build artifacts."
          -- daml clean in a package
          (_, True, False) -> do
            singleCleanEffect projectOpts
            hPutStrLn stderr "Removed build artifacts."
          -- daml clean outside a package and no multi-package.yaml
          (Nothing, False, False) -> do
            hPutStrLn stderr "No valid daml.yaml or multi-package.yaml could be found."
            exitFailure
          -- daml clean outside a package with a multi-package.yaml
          (Just _, False, False) -> do
            hPutStrLn stderr $ "No valid daml.yaml found, but a multi-package.yaml was found instead.\n"
              <> "If you intended to clean everything within this multi-package.yaml, use the --all flag"
            exitFailure
      | getMultiPackageCleanAll cleanAll
      = do
        hPutStrLn stderr "Multi-package clean option used with multi-package disabled - re-enable it by setting the --enable-multi-package=yes flag."
        exitFailure
      | otherwise = do
        singleCleanEffect projectOpts
        hPutStrLn stderr "Removed build artifacts."

singleCleanEffect :: ProjectOpts -> IO ()
singleCleanEffect projectOpts =
  withProjectRoot' projectOpts $ \_relativize -> do
    isProject <- doesFileExist projectConfigName
    when isProject $ do
      canonDir <- getCurrentDirectory
      hPutStrLn stderr $ "Cleaning " <> canonDir
      whenM (doesPathExist damlArtifactDir) $
        removePathForcibly damlArtifactDir

execPackage :: SdkVersion.Class.SdkVersioned
            => ProjectOpts
            -> FilePath -- ^ input file
            -> Options
            -> Maybe FilePath
            -> FromDalf
            -> Command
execPackage projectOpts filePath opts mbOutFile dalfInput =
  Command Package (Just projectOpts) effect
  where
    effect = withProjectRoot' projectOpts $ \relativize -> do
      hPutStrLn stderr $ unlines
        [ "WARNING: The comannd"
        , ""
        , "    daml damlc package"
        , ""
        , "is deprecated. Please use"
        , ""
        , "    daml build"
        , ""
        , "instead."
        ]
      loggerH <- getLogger opts "package"
      filePath <- relativize filePath
      withDamlIdeState opts loggerH diagnosticsLogger $ \ide -> do
          -- We leave the sdk version blank and the list of exposed modules empty.
          -- This command is being removed anytime now and not present
          -- in the new daml assistant.
          mbDar <- buildDar ide
                            PackageConfigFields
                              { pName = fromMaybe (LF.PackageName $ T.pack $ takeBaseName filePath) $ optMbPackageName opts
                              , pSrc = filePath
                              , pExposedModules = Nothing
                              , pVersion = optMbPackageVersion opts
                              , pDependencies = []
                              , pDataDependencies = []
                              , pSdkVersion = SdkVersion.Class.unresolvedBuiltinSdkVersion
                              , pModulePrefixes = Map.empty
                              , pUpgradeDar = Nothing
                              -- execPackage is deprecated so it doesn't need to support upgrades
                              }
                            (toNormalizedFilePath' $ fromMaybe ifaceDir $ optIfaceDir opts)
                            dalfInput
                            (optUpgradeInfo opts)
          case mbDar of
            Nothing -> do
                hPutStrLn stderr "ERROR: Creation of DAR file failed."
                exitFailure
            Just (dar, _) -> createDarFile loggerH targetFilePath dar
    -- This is somewhat ugly but our CLI parser guarantees that this will always be present.
    -- We could parametrize CliOptions by whether the package name is optional
    -- but I don’t think that is worth the complexity of carrying around a type parameter.
    name = fromMaybe (error "Internal error: Package name was not present") (optMbPackageName opts)

    -- The default output filename is based on Maven coordinates if
    -- the package name is specified via them, otherwise we use the
    -- name.
    defaultDarFile =
      case Split.splitOn ":" (T.unpack $ LF.unPackageName name) of
        [_g, a, v] -> a <> "-" <> v <> ".dar"
        _otherwise -> (T.unpack $ LF.unPackageName name) <> ".dar"

    targetFilePath = fromMaybe defaultDarFile mbOutFile

-- | Given a path to a .dalf or a .dar return the bytes of either the .dalf file
-- or the main dalf from the .dar
-- In addition to the bytes, we also return the basename of the dalf file.
getDalfBytes :: FilePath -> IO (B.ByteString, FilePath)
getDalfBytes fp
  | "dar" `isExtensionOf` fp = do
        dar <- B.readFile fp
        let archive = ZipArchive.toArchive $ BSL.fromStrict dar
        manifest <- either fail pure $ readDalfManifest archive
        dalfs <- either fail pure $ readDalfs archive
        pure (BSL.toStrict $ mainDalf dalfs, takeBaseName $ mainDalfPath manifest)
  | otherwise = (, takeBaseName fp) <$> B.readFile fp

execInspect :: FilePath -> FilePath -> Bool -> DA.Pretty.PrettyLevel -> Command
execInspect inFile outFile jsonOutput lvl =
  Command Inspect Nothing effect
  where
    effect = do
      (bytes, _) <- getDalfBytes inFile
      if jsonOutput
      then do
        payloadBytes <- PLF.archivePayload <$> errorOnLeft "Cannot decode archive" (PS.fromByteString bytes)
        archive :: PLF.ArchivePayload <- errorOnLeft "Cannot decode archive payload" $ PS.fromByteString payloadBytes
        writeOutputBSL outFile
         $ Aeson.Pretty.encodePretty
         $ Proto.JSONPB.toAesonValue archive
      else do
        (pkgId, lfPkg) <- errorOnLeft "Cannot decode package" $
                   Archive.decodeArchive Archive.DecodeAsMain bytes
        writeOutput outFile $ render Plain $
          DA.Pretty.vcat
            [ DA.Pretty.keyword_ "package" DA.Pretty.<-> DA.Pretty.text (LF.unPackageId pkgId)
            , DA.Pretty.pPrintPrec lvl 0 lfPkg
            ]

errorOnLeft :: Show a => String -> Either a b -> IO b
errorOnLeft desc = \case
  Left err -> ioError $ userError $ unlines [ desc, show err ]
  Right x  -> return x

execInspectDar :: FilePath -> InspectDar.Format -> Command
execInspectDar inFile jsonOutput =
  Command InspectDar Nothing (InspectDar.inspectDar inFile jsonOutput)

execValidateDar :: FilePath -> Command
execValidateDar inFile =
  Command ValidateDar Nothing effect
  where
    effect = do
      n <- validateDar inFile -- errors if validation fails
      putStrLn $ "DAR is valid; contains " <> show n <> " packages."

-- | Merge two dars. The idea is that the second dar is a delta. Hence, we take the main in the
-- manifest from the first.
execMergeDars :: FilePath -> FilePath -> Maybe FilePath -> Command
execMergeDars darFp1 darFp2 mbOutFp =
  Command MergeDars Nothing effect
  where
    effect = do
      let outFp = fromMaybe darFp1 mbOutFp
      bytes1 <- B.readFile darFp1
      bytes2 <- B.readFile darFp2
      let dar1 = ZipArchive.toArchive $ BSL.fromStrict bytes1
      let dar2 = ZipArchive.toArchive $ BSL.fromStrict bytes2
      mf <- mergeManifests dar1 dar2
      let merged =
              ZipArchive.Archive
                  (nubSortOn ZipArchive.eRelativePath $ mf : ZipArchive.zEntries dar1 ++ ZipArchive.zEntries dar2)
                  -- nubSortOn keeps the first occurrence
                  Nothing
                  BSL.empty
      BSL.writeFile outFp $ ZipArchive.fromArchive merged
    mergeManifests dar1 dar2 = do
        manifest1 <- either fail pure $ readDalfManifest dar1
        manifest2 <- either fail pure $ readDalfManifest dar2
        let mergedDalfs = BSC.intercalate ", " $ map BSUTF8.fromString $ nubSort $ dalfPaths manifest1 ++ dalfPaths manifest2
        attrs1 <- either fail pure $ readManifest dar1
        attrs1 <- pure $ map (\(k, v) -> if k == "Dalfs" then (k, mergedDalfs) else (k, v)) attrs1
        pure $ ZipArchive.toEntry manifestPath 0 $ BSLC.unlines $
            map (\(k, v) -> breakAt72Bytes $ BSL.fromStrict $ k <> ": " <> v) attrs1

-- | Should source files for doc test be imported into the test project (default yes)
newtype ImportSource = ImportSource Bool

execDocTest :: SdkVersion.Class.SdkVersioned => Options -> FilePath -> ImportSource -> [FilePath] -> Command
execDocTest opts scriptDar (ImportSource importSource) files =
  Command DocTest Nothing effect
  where
    effect = do
      let files' = map toNormalizedFilePath' files
          packageFlag =
            ExposePackage
              ("--package daml-script-" <> SdkVersion.Class.sdkPackageVersion)
              (UnitIdArg $ stringToUnitId $ "daml-script-" <> SdkVersion.Class.sdkPackageVersion)
              (ModRenaming True [])

      logger <- getLogger opts "doctest"

      -- Install daml-script in their project and update the package db
      -- Note this installs directly to the users dependency database, giving this command side effects.
      -- An approach of copying out the deps into a temporary location to build/run the tests has been considered
      -- but the effort to build this, combined with the low number of users of this feature, as well as most projects
      -- already using daml-script has led us to leave this as is. We'll fix this if someone is affected and notifies us.
      damlAssistantIsSet <- damlAssistantIsSet
      releaseVersion <- if damlAssistantIsSet
          then do
            damlPath <- getDamlPath
            damlEnv <- getDamlEnv damlPath (LookForProjectPath False)
            wrapErr "running doc test" $
              resolveReleaseVersionUnsafe (envUseCache damlEnv) SdkVersion.Class.unresolvedBuiltinSdkVersion
          else pure (unsafeResolveReleaseVersion SdkVersion.Class.unresolvedBuiltinSdkVersion)
      installDependencies "." opts releaseVersion [scriptDar] []
      createProjectPackageDb "." opts mempty

      opts <- pure opts
        { optPackageDbs = projectPackageDatabase : optPackageDbs opts
        , optPackageImports = packageFlag : optPackageImports opts
        -- Drop version information to avoid package clashes (specifically when generating for internal packages)
        , optMbPackageVersion = Nothing
        }

      -- We don’t add a logger here since we will otherwise emit logging messages twice.
      importPaths <-
        if importSource
          then
            withDamlIdeState opts { optScenarioService = EnableScenarioService False }
              logger (NotificationHandler $ \_ _ -> pure ()) $
              \ideState -> runActionSync ideState $ do
                pmS <- catMaybes <$> uses GetParsedModule files'
                -- This is horrible but we do not have a way to change the import paths in a running
                -- IdeState at the moment.
                pure $ nubOrd $ mapMaybe (uncurry moduleImportPath) (zip files' pmS)
          else pure []
      opts <- pure opts
        { optImportPath = importPaths <> optImportPath opts
        , optHaddock = Haddock True
        }
      withDamlIdeState opts logger diagnosticsLogger $ \ideState ->
          docTest ideState files'

data MultiPackageManifestEntry = MultiPackageManifestEntry
  { packageName :: String
  , packageVersion :: String
  , packageDir :: FilePath
  , packageSrc :: FilePath
  , packageFileHashes :: Map.Map FilePath String
  , packageHash :: String
  , output :: FilePath
  , packageDeps :: [String]
  , darDeps :: [FilePath]
  }
  deriving (Show, Eq, Generic, ToJSON, FromJSON)

-- Map can't be a bifunctor because of this `Ord` constraint, but its a shame such a function isn't defined in Data.Map
bimapMap :: Ord k' => (k -> k') -> (v -> v') -> Map.Map k v -> Map.Map k' v'
bimapMap keyMap valueMap = fmap valueMap . Map.mapKeys keyMap

execGenerateMultiPackageManifest :: SdkVersion.Class.SdkVersioned => MultiPackageLocation -> GenerateMultiPackageManifestOutput -> Command
execGenerateMultiPackageManifest multiPackageLocation outputLocation =
    Command GenerateMultiPackageManifest Nothing effect
  where
    effect = do
      mMultiPackageConfigProjectPath <- getMultiPackagePath multiPackageLocation
      multiPackageConfigProjectPath <- case mMultiPackageConfigProjectPath of
        Nothing -> do
          hPutStrLn stderr "Couldn't find a multi-package.yaml at current or parent directory. Use --multi-package-path to specify its location."
          exitFailure
        Just path -> pure path
      let multiPackageConfigPath = unwrapProjectPath multiPackageConfigProjectPath
      withMultiPackageConfig multiPackageConfigProjectPath $ \multiPackageConfig -> do
        configs <- forM (mpPackagePaths multiPackageConfig) $ \path -> do
          config@BuildMultiPackageConfig {..} <- buildMultiPackageConfigFromDamlYaml path
          darPath <- deriveDarPath path bmName bmVersion bmOutput
          pure (path, darPath, config)

        -- Map from dar path to config, for classifying data deps
        let configMap = Map.fromList $ (\(_, darPath, config) -> (darPath, config)) <$> configs

        manifestEntries <- forM configs $ \(packagePath, darPath, BuildMultiPackageConfig {..}) -> do
          damlFilesMap <- bimapMap (bmSourceDaml </>) BSL.toStrict <$> getDamlFilesBuildMulti (hPutStrLn stderr) packagePath bmSourceDaml
          damlYamlContent <- B.readFile $ packagePath </> projectConfigName

          let manifestFileHash :: B.ByteString -> String
              manifestFileHash = show . Hash.hash @_ @Hash.SHA256
              makeRelativeToRoot :: FilePath -> FilePath
              makeRelativeToRoot path = makeRelative multiPackageConfigPath $ normalise $ packagePath </> path
              relativeDamlFilesMap = Map.mapKeys makeRelativeToRoot $ Map.insert ("." </> projectConfigName) damlYamlContent damlFilesMap
              relativeDamlFileHashes = manifestFileHash <$> relativeDamlFilesMap
              fullHash = manifestFileHash $ BSUTF8.fromString $ Map.foldMapWithKey (<>) relativeDamlFileHashes

          (packageDeps, darDeps) <- fmap partitionEithers $ withCurrentDirectory packagePath $ forM bmDarDeps $ \depPath -> do
            canonDepPath <- canonicalizePath depPath
            case Map.lookup canonDepPath configMap of
              Just BuildMultiPackageConfig {..} -> pure $ Left $ unitIdString $ pkgNameVersion bmName $ Just bmVersion
              Nothing -> pure $ Right $ makeRelative multiPackageConfigPath canonDepPath

          pure MultiPackageManifestEntry
            { packageName = T.unpack $ LF.unPackageName bmName
            , packageVersion = T.unpack $ LF.unPackageVersion bmVersion
            , packageDir = makeRelative multiPackageConfigPath packagePath
            , packageSrc = makeRelativeToRoot bmSourceDaml
            , packageFileHashes = relativeDamlFileHashes
            , packageHash = fullHash
            , output = makeRelative multiPackageConfigPath darPath
            , packageDeps = packageDeps
            , darDeps = darDeps
            }
        let encodedManifestEntries = Aeson.Pretty.encodePretty manifestEntries
        case getGenerateMultiPackageManifestOutput outputLocation of
          Nothing -> BSLC.putStrLn encodedManifestEntries
          Just path -> do
            BSL.writeFile path encodedManifestEntries
            putStrLn $ "Wrote manifest to " <> path

--------------------------------------------------------------------------------
-- main
--------------------------------------------------------------------------------

options :: SdkVersion.Class.SdkVersioned => Int -> Parser Command
options numProcessors =
    subparser
      (  cmdIde numProcessors
      <> cmdMultiIde numProcessors
      <> cmdLicense
      -- cmdPackage can go away once we kill the old assistant.
      <> cmdPackage numProcessors
      <> cmdBuild numProcessors
      <> cmdTest numProcessors
      <> Damldoc.cmd numProcessors (\cli -> Command DamlDoc Nothing $ Damldoc.exec cli)
      <> cmdInspectDar
      <> cmdValidateDar
      <> cmdDocTest numProcessors
      <> cmdLint numProcessors
      <> cmdRepl numProcessors
      )
    <|> subparser
      (internal -- internal commands
        <> cmdInspect
        <> cmdMergeDars
        <> cmdInit numProcessors
        <> cmdCompile numProcessors
        <> cmdDesugar numProcessors
        <> cmdDebugIdeSpanInfo numProcessors
        <> cmdClean
        <> cmdGenerateMultiPackageManifest
      )

parserInfo :: SdkVersion.Class.SdkVersioned => Int -> Bool -> ParserInfo Command
parserInfo numProcessors addBuildArgsBackupParser =
  info (backupParserWithBuildArgs addBuildArgsBackupParser $ helper <*> options numProcessors)
    (  fullDesc
    <> progDesc "Invoke the Daml compiler. Use -h for help."
    <> headerDoc (Just $ PP.vcat
        [ "damlc - Compiler and IDE backend for the Daml programming language"
        , buildInfo
        ])
    )

-- | Add the build parser as a backup for when we're adding the CLI args from daml.yaml, incase whatever command
-- we're running doesn't recognise all of the `build-options:` e.g. `daml test` cannot use `--output`
backupParserWithBuildArgs :: SdkVersion.Class.SdkVersioned => Bool -> Parser Command -> Parser Command
backupParserWithBuildArgs shouldBackup parser = if shouldBackup then parser <* cmdBuildParser 1 else parser

-- | Attempts to find the --output flag in the given build-options for a package
-- Given the many ways --output can be specified, we invoke the parser directly
-- Sadly, optparse-applicative gives no way to "ignore" flags it doesn't know about
-- so we must provide all the other flags we might expect.
-- Note: if we ever fully remove a flag in future, an "ignore" parser will need to be added here so
-- that old packages with that flag specified don't trigger an error.
scrapeOutputFlag :: [String] -> Maybe FilePath
scrapeOutputFlag args =
  case execParserPure (prefs mempty) (info mbOutFileParser mempty) args of
    Success mPath -> mPath
    Failure err -> error $ fst $ renderFailure err "daml build"
    CompletionInvoked _ -> error "Impossible internal completion invoked."
  where
    mbOutFileParser :: Parser (Maybe FilePath)
    mbOutFileParser = do
      void $ optionsParser
        0
        (EnableScenarioService False)
        (pure Nothing)
        disabledDlintUsageParser
      mbOutFile <- optionalOutputFileOpt
      void incrementalBuildOpt
      void initPkgDbOpt
      pure mbOutFile

-- | Query the optional `--output` flag from the daml.yaml build-options
queryProjectConfigBuildOutput :: ProjectConfig -> Either ConfigError (Maybe FilePath)
queryProjectConfigBuildOutput project = do
  mBuildOptions <- queryProjectConfig ["build-options"] project
  pure $ mBuildOptions >>= scrapeOutputFlag

-- | Calculates the canonical path to the DAR for a given package, overriding with a given mOutput if needed
deriveDarPath :: FilePath -> LF.PackageName -> LF.PackageVersion -> Maybe FilePath -> IO FilePath
deriveDarPath path name version mOutput = case mOutput of
  Just output -> withCurrentDirectory path $ canonicalizePath output
  Nothing -> pure $ path </> distDir </> unitIdString (pkgNameVersion name (Just version)) <> ".dar"

-- | Derive the dar path from the daml.yaml, using either the --output override, or the default path derived from name and version
darPathFromDamlYaml :: FilePath -> IO FilePath
darPathFromDamlYaml path = do
  (name, version, mOutput) <-
    onDamlYaml 
      (\e -> error $ "Failed to parse daml.yaml at " <> path <> " for multi-package build: " <> displayException e)
      (\project -> do
        name <- queryProjectConfigRequired ["name"] project
        version <- queryProjectConfigRequired ["version"] project
        mOutput <- queryProjectConfigBuildOutput project
        pure (name, version, mOutput)
      ) mbProjectOpts
  deriveDarPath path name version mOutput
  where
    mbProjectOpts = Just $ ProjectOpts (Just $ ProjectPath path) (ProjectCheck "" False)

-- | Subset of parseProjectConfig to get only what we need for deferring to the correct build call with multi-package build
buildMultiPackageConfigFromDamlYaml :: FilePath -> IO BuildMultiPackageConfig
buildMultiPackageConfigFromDamlYaml path =
  onDamlYaml
    (\e -> error $ "Failed to parse daml.yaml at " <> path <> " for multi-package build: " <> displayException e)
    (\project -> do
      bmSdkVersion <- queryProjectConfigRequired ["sdk-version"] project
      bmName <- queryProjectConfigRequired ["name"] project
      bmVersion <- queryProjectConfigRequired ["version"] project
      bmSourceDaml <- queryProjectConfigRequired ["source"] project
      dataDeps <- fromMaybe [] <$> queryProjectConfig ["data-dependencies"] project
      deps <- fromMaybe [] <$> queryProjectConfig ["dependencies"] project
      upgradesDar <- maybe [] pure <$> queryProjectConfig ["upgrades"] project
      let bmDarDeps = dataDeps <> filter (\dep -> takeExtension dep == ".dar") deps <> upgradesDar
      bmOutput <- queryProjectConfigBuildOutput project
      pure $ BuildMultiPackageConfig {..}
    )
    mbProjectOpts
  where
    mbProjectOpts = Just $ ProjectOpts (Just $ ProjectPath path) (ProjectCheck "" False)

-- | Extract some value from a daml.yaml via a projection function. Return a default value if the file doesn't exist
onDamlYaml :: (ConfigError -> t) -> (ProjectConfig -> Either ConfigError t) -> Maybe ProjectOpts -> IO t
onDamlYaml def f mbProjectOpts = do
    -- This is the same logic used in withProjectRoot but we don’t need to change CWD here
    -- and this is simple enough so we inline it here.
    mbEnvProjectPath <- fmap ProjectPath <$> getProjectPath
    let mbProjectPath = projectRoot =<< mbProjectOpts
    let projectPath = fromMaybe (ProjectPath ".") (mbProjectPath <|> mbEnvProjectPath)
    handle (\(e :: ConfigError) -> pure $ def e) $ do
        project <- readProjectConfig projectPath
        either throwIO pure $ f project

cliArgsFromDamlYaml :: Maybe ProjectOpts -> IO [String]
cliArgsFromDamlYaml =
  onDamlYaml (const []) $ \project -> Right $ case queryProjectConfigRequired ["build-options"] project of
    Left _ -> []
    Right xs -> xs

fullParseArgs :: SdkVersion.Class.SdkVersioned => Int -> [String] -> IO Command
fullParseArgs numProcessors cliArgs = do
    let parse :: [String] -> Maybe [String] -> ([String], ParserResult Command)
        parse args mBuildOptions =
          case mBuildOptions of
            Nothing -> ParseArgs.lax (parserInfo numProcessors False) args
            Just damlYamlArgs -> ([], execParserPure defaultPrefs (parserInfo numProcessors True) $ args ++ damlYamlArgs)

    let (_, tempParseResult) = parse cliArgs Nothing
    -- Note: need to parse given args first to decide whether we need to add
    -- args from daml.yaml.
    Command cmd mbProjectOpts _ <- handleParseResult tempParseResult

    (errMsgs, parseResult) <- if cmdUseDamlYamlArgs cmd
      then
        parse cliArgs . Just <$> cliArgsFromDamlYaml mbProjectOpts
      else
        pure $ parse cliArgs Nothing

    cmd <- handleParseResult parseResult
    forM_ errMsgs $ \msg -> do
        hPutStrLn stderr msg

    pure cmd

main :: SdkVersion.Class.SdkVersioned => IO ()
main = do
    -- We need this to ensure that logs are flushed on SIGTERM.
    installSignalHandlers
    -- Save the runfiles environment to work around
    -- https://gitlab.haskell.org/ghc/ghc/-/issues/18418.
    setRunfilesEnv
    numProcessors <- getNumProcessors
    cliArgs <- getArgs

    Command _ _ io <- fullParseArgs numProcessors cliArgs

    withProgName "damlc" io

-- | Commands for which we add the args from daml.yaml build-options.
cmdUseDamlYamlArgs :: CommandName -> Bool
cmdUseDamlYamlArgs = \case
  Build -> True
  Clean -> False -- don't need any flags to remove files
  Compile -> True
  DamlDoc -> True
  DebugIdeSpanInfo -> True
  Desugar -> True
  DocTest -> True
  Ide -> True
  Init -> True
  Inspect -> False -- just reads the dalf
  InspectDar -> False -- just reads the dar
  ValidateDar -> False -- just reads the dar
  License -> False -- just prints the license
  Lint -> True
  MergeDars -> False -- just reads the dars
  Package -> False -- deprecated
  Test -> True
  Repl -> True
  GenerateMultiPackageManifest -> False -- Just reads config files
  MultiIde -> False

withProjectRoot' :: ProjectOpts -> ((FilePath -> IO FilePath) -> IO a) -> IO a
withProjectRoot' ProjectOpts{..} act =
    withProjectRoot projectRoot projectCheck (const act)
