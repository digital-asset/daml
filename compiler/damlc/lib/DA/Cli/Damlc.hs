-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ApplicativeDo       #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE MultiWayIf #-}

-- | Main entry-point of the Daml compiler
module DA.Cli.Damlc (main) where

import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import Control.Exception
import Control.Exception.Safe (catchIO)
import Control.Monad.Except
import Control.Monad.Extra (whenM, whenJust)
import DA.Bazel.Runfiles
import qualified DA.Cli.Args as ParseArgs
import DA.Cli.Options
import DA.Cli.Damlc.BuildInfo
import qualified DA.Cli.Damlc.InspectDar as InspectDar
import qualified DA.Cli.Damlc.Command.Damldoc as Damldoc
import DA.Cli.Damlc.Packaging
import DA.Cli.Damlc.DependencyDb
import DA.Cli.Damlc.Test
import DA.Daml.Compiler.Dar
import DA.Daml.Compiler.Output
import qualified DA.Daml.Compiler.Repl as Repl
import DA.Daml.Compiler.DocTest
import DA.Daml.Desugar (desugar)
import DA.Daml.LF.ScenarioServiceClient (readScenarioServiceConfig, withScenarioService')
import qualified DA.Daml.LF.ReplClient as ReplClient
import DA.Daml.Compiler.Validate (validateDar)
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.Util (splitUnitId)
import qualified DA.Daml.LF.Proto3.Archive as Archive
import DA.Daml.LF.Reader
import DA.Daml.LanguageServer
import DA.Daml.Options.Types
import DA.Daml.Package.Config
import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import DA.Daml.Project.Types (ConfigError(..), ProjectPath(..))
import DA.Daml.Visual
import qualified DA.Pretty
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.GCP as Logger.GCP
import qualified DA.Service.Logger.Impl.IO as Logger.IO
import DA.Signals
import qualified Com.Daml.DamlLfDev.DamlLf as PLF
import qualified Data.Aeson.Encode.Pretty as Aeson.Pretty
import qualified Data.Aeson.Text as Aeson
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC
import qualified Data.ByteString.UTF8 as BSUTF8
import Data.FileEmbed (embedFile)
import qualified Data.HashSet as HashSet
import Data.List.Extra
import qualified Data.List.Split as Split
import qualified Data.Map.Strict as Map
import Data.Maybe
import qualified Data.Text.Extended as T
import qualified Data.Text.Lazy.IO as TL
import qualified Data.Text.IO as T
import Development.IDE.Core.API
import Development.IDE.Core.Debouncer
import Development.IDE.Core.IdeState.Daml
import Development.IDE.Core.Rules
import Development.IDE.Core.Rules.Daml (getDlintIdeas, getSpanInfo)
import Development.IDE.Core.Shake
import Development.IDE.GHC.Util (hscEnv, moduleImportPath)
import Development.IDE.Types.Location
import "ghc-lib-parser" DynFlags
import GHC.Conc
import "ghc-lib-parser" Module (unitIdString, stringToUnitId)
import qualified Network.Socket as NS
import Options.Applicative.Extended
import Options.Applicative hiding (option, strOption)
import qualified Options.Applicative (option)
import qualified Proto3.Suite as PS
import qualified Proto3.Suite.JSONPB as Proto.JSONPB
import System.Directory.Extra
import System.Environment
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process (StdStream(..))
import qualified Text.PrettyPrint.ANSI.Leijen as PP
import Development.IDE.Core.RuleTypes
import "ghc-lib-parser" ErrUtils
-- For dumps
import "ghc-lib" GHC
import "ghc-lib-parser" HsDumpAst
import "ghc-lib" HscStats
import "ghc-lib-parser" HscTypes
import qualified "ghc-lib-parser" Outputable as GHC
import qualified SdkVersion
import "ghc-lib-parser" Util (looksLikePackageName)

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
  | GenerateSrc
  | GenerateGenerics
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
  | Visual
  | Repl
  deriving (Ord, Show, Eq)
data Command = Command CommandName (Maybe ProjectOpts) (IO ())

cmdIde :: Int -> Mod CommandFields Command
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

cmdCompile :: Int -> Mod CommandFields Command
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

cmdDesugar :: Int -> Mod CommandFields Command
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

cmdDebugIdeSpanInfo :: Int -> Mod CommandFields Command
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

cmdLint :: Int -> Mod CommandFields Command
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

cmdTest :: Int -> Mod CommandFields Command
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
      <*> fmap ShowCoverage showCoverageOpt
      <*> fmap UseColor colorOutput
      <*> junitOutput
      <*> optionsParser
            numProcessors
            (EnableScenarioService True)
            optPackageName
            disabledDlintUsageParser
      <*> initPkgDbOpt
    filesOpt = optional (flag' () (long "files" <> help filesDoc) *> many inputFileOpt)
    filesDoc = "Only run test declarations in the specified files."
    junitOutput = optional $ strOptionOnce $ long "junit" <> metavar "FILENAME" <> help "Filename of JUnit output file"
    colorOutput = switch $ long "color" <> help "Colored test results"
    showCoverageOpt = switch $ long "show-coverage" <> help "Show detailed test coverage"
    runAllTests = switch $ long "all" <> help "Run tests in current project as well as dependencies"

runTestsInProjectOrFiles ::
       ProjectOpts
    -> Maybe [FilePath]
    -> RunAllTests
    -> ShowCoverage
    -> UseColor
    -> Maybe FilePath
    -> Options
    -> InitPkgDb
    -> Command
runTestsInProjectOrFiles projectOpts Nothing allTests coverage color mbJUnitOutput cliOptions initPkgDb = Command Test (Just projectOpts) effect
  where effect = withExpectProjectRoot (projectRoot projectOpts) "daml test" $ \pPath relativize -> do
        installDepsAndInitPackageDb cliOptions initPkgDb
        mbJUnitOutput <- traverse relativize mbJUnitOutput
        withPackageConfig (ProjectPath pPath) $ \PackageConfigFields{..} -> do
            -- TODO: We set up one scenario service context per file that
            -- we pass to execTest and scenario contexts are quite expensive.
            -- Therefore we keep the behavior of only passing the root file
            -- if source points to a specific file.
            files <- getDamlRootFiles pSrc
            execTest files allTests coverage color mbJUnitOutput cliOptions
runTestsInProjectOrFiles projectOpts (Just inFiles) allTests coverage color mbJUnitOutput cliOptions initPkgDb = Command Test (Just projectOpts) effect
  where effect = withProjectRoot' projectOpts $ \relativize -> do
        installDepsAndInitPackageDb cliOptions initPkgDb
        mbJUnitOutput <- traverse relativize mbJUnitOutput
        inFiles' <- mapM (fmap toNormalizedFilePath' . relativize) inFiles
        execTest inFiles' allTests coverage color mbJUnitOutput cliOptions

cmdInspect :: Mod CommandFields Command
cmdInspect =
    command "inspect" $ info (helper <*> cmd)
      $ progDesc "Pretty print a DALF file or the main DALF of a DAR file"
    <> fullDesc
  where
    jsonOpt = switch $ long "json" <> help "Output the raw Protocol Buffer structures as JSON"
    detailOpt =
        fmap (maybe DA.Pretty.prettyNormal DA.Pretty.PrettyLevel) $
            optional $ optionOnce auto $ long "detail" <> metavar "LEVEL" <> help "Detail level of the pretty printed output (default: 0)"
    cmd = execInspect <$> inputFileOptWithExt ".dalf or .dar" <*> outputFileOpt <*> jsonOpt <*> detailOpt

cmdVisual :: Mod CommandFields Command
cmdVisual =
    command "visual" $ info (helper <*> cmd) $ progDesc "Early Access (Labs). Generate visual from dar" <> fullDesc
    where
      cmd = vis <$> inputDarOpt <*> dotFileOpt
      vis a b = Command Visual Nothing $ execVisual a b

cmdVisualWeb :: Mod CommandFields Command
cmdVisualWeb =
    command "visual-web" $ info (helper <*> cmd) $ progDesc "Early Access (Labs). Generate D3-Web Visual from dar" <> fullDesc
    where
      cmd = vis <$> inputDarOpt <*> htmlOutFile <*> openBrowser
      vis a b browser = Command Visual Nothing $ execVisualHtml a b browser

cmdBuild :: Int -> Mod CommandFields Command
cmdBuild numProcessors =
    command "build" $
    info (helper <*> cmd) $
    progDesc "Initialize, build and package the Daml project" <> fullDesc
  where
    cmd =
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

cmdRepl :: Int -> Mod CommandFields Command
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
    cmd = execClean <$> projectOpts "daml clean"

cmdInit :: Int -> Mod CommandFields Command
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

cmdPackage :: Int -> Mod CommandFields Command
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

cmdDocTest :: Int -> Mod CommandFields Command
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
        <*> many inputFileOpt

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

execIde :: Telemetry
        -> Debug
        -> EnableScenarioService
        -> Options
        -> Command
execIde telemetry (Debug debug) enableScenarioService options =
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
          options <- pure options
              { optScenarioService = enableScenarioService
              , optEnableOfInterestRule = True
              , optSkipScenarioValidation = SkipScenarioValidation True
              -- TODO(MH): The `optionsParser` does not provide a way to skip
              -- individual options. As a stopgap we ignore the argument to
              -- --jobs.
              , optThreads = 0
              }
          installDepsAndInitPackageDb options (InitPkgDb True)
          scenarioServiceConfig <- readScenarioServiceConfig
          withLogger $ \loggerH ->
              withScenarioService' enableScenarioService (optEnableScenarios options) (optDamlLfVersion options) loggerH scenarioServiceConfig $ \mbScenarioService -> do
                  sdkVersion <- getSdkVersion `catchIO` const (pure "Unknown (not started via the assistant)")
                  Logger.logInfo loggerH (T.pack $ "SDK version: " <> sdkVersion)
                  debouncer <- newAsyncDebouncer
                  runLanguageServer loggerH enabledPlugins Config $ \lspEnv vfs _ ->
                      getDamlIdeState options mbScenarioService loggerH debouncer (RealLspEnv lspEnv) vfs


-- | Whether we should write interface files during `damlc compile`.
newtype WriteInterface = WriteInterface Bool

execCompile :: FilePath -> FilePath -> Options -> WriteInterface -> Maybe FilePath -> Command
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

execDesugar :: FilePath -> FilePath -> Options -> Command
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

execDebugIdeSpanInfo :: FilePath -> FilePath -> Options -> Command
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

execLint :: [FilePath] -> Options -> Command
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

-- | If we're in a daml project, read the daml.yaml field, install the dependencies and create the
-- project local package database. Otherwise do nothing.
execInit :: Options -> ProjectOpts -> Command
execInit opts projectOpts =
  Command Init (Just projectOpts) effect
  where effect = withProjectRoot' projectOpts $ \_relativize ->
          installDepsAndInitPackageDb
            opts
            (InitPkgDb True)

installDepsAndInitPackageDb :: Options -> InitPkgDb -> IO ()
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
              installDependencies
                  (toNormalizedFilePath' projRoot)
                  opts
                  pSdkVersion
                  pDependencies
                  pDataDependencies
              createProjectPackageDb (toNormalizedFilePath' projRoot) opts pModulePrefixes

execBuild :: ProjectOpts -> Options -> Maybe FilePath -> IncrementalBuild -> InitPkgDb -> Command
execBuild projectOpts opts mbOutFile incrementalBuild initPkgDb =
  Command Build (Just projectOpts) effect
  where effect = withProjectRoot' projectOpts $ \relativize -> do
            installDepsAndInitPackageDb opts initPkgDb
            withPackageConfig defaultProjectPath $ \pkgConfig@PackageConfigFields{..} -> do
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
                      , optIncrementalBuild = incrementalBuild
                      }
                    loggerH
                    diagnosticsLogger $ \compilerH -> do
                    mbDar <-
                        buildDar
                            compilerH
                            pkgConfig
                            (toNormalizedFilePath' $ fromMaybe ifaceDir $ optIfaceDir opts)
                            (FromDalf False)
                    dar <- mbErr "ERROR: Creation of DAR file failed." mbDar
                    fp <- targetFilePath relativize $ unitIdString (pkgNameVersion pName pVersion)
                    createDarFile loggerH fp dar
            where
                targetFilePath rel name =
                  case mbOutFile of
                    Nothing -> pure $ distDir </> name <.> "dar"
                    Just out -> rel out
execRepl
    :: [FilePath]
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
            ReplClient.withReplClient (ReplClient.Options jar mbLedgerConfig mbAuthToken mbAppId mbSslConf mbMaxInboundMessageSize timeMode Inherit) $ \replHandle ->
                withTempDir $ \dir ->
                withCurrentDirectory dir $ do
                sdkVer <- fromMaybe SdkVersion.sdkVersion <$> lookupEnv sdkVersionEnvVar
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
execClean :: ProjectOpts -> Command
execClean projectOpts =
  Command Clean (Just projectOpts) effect
  where effect = do
            withProjectRoot' projectOpts $ \_relativize -> do
                isProject <- doesFileExist projectConfigName
                when isProject $ do
                    let removeAndWarn path = do
                            whenM (doesDirectoryExist path) $ do
                                putStrLn ("Removing directory " <> path)
                                removePathForcibly path
                            whenM (doesFileExist path) $ do
                                putStrLn ("Removing file " <> path)
                                removePathForcibly path
                    removeAndWarn damlArtifactDir
                    putStrLn "Removed build artifacts."

execPackage :: ProjectOpts
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
                              , pSdkVersion = PackageSdkVersion SdkVersion.sdkVersion
                              , pModulePrefixes = Map.empty
                              }
                            (toNormalizedFilePath' $ fromMaybe ifaceDir $ optIfaceDir opts)
                            dalfInput
          case mbDar of
            Nothing -> do
                hPutStrLn stderr "ERROR: Creation of DAR file failed."
                exitFailure
            Just dar -> createDarFile loggerH targetFilePath dar
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

execDocTest :: Options -> [FilePath] -> Command
execDocTest opts files =
  Command DocTest Nothing effect
  where
    effect = do
      let files' = map toNormalizedFilePath' files
      logger <- getLogger opts "doctest"
      -- We don’t add a logger here since we will otherwise emit logging messages twice.
      importPaths <-
          withDamlIdeState opts { optScenarioService = EnableScenarioService False }
              logger (NotificationHandler $ \_ _ -> pure ()) $ \ideState -> runActionSync ideState $ do
          pmS <- catMaybes <$> uses GetParsedModule files'
          -- This is horrible but we do not have a way to change the import paths in a running
          -- IdeState at the moment.
          pure $ nubOrd $ mapMaybe (uncurry moduleImportPath) (zip files' pmS)
      opts <- pure opts
        { optImportPath = importPaths <> optImportPath opts
        , optHaddock = Haddock True
        , optEnableScenarios = EnableScenarios True }
      withDamlIdeState opts logger diagnosticsLogger $ \ideState ->
          docTest ideState files'

--------------------------------------------------------------------------------
-- main
--------------------------------------------------------------------------------

options :: Int -> Parser Command
options numProcessors =
    subparser
      (  cmdIde numProcessors
      <> cmdLicense
      -- cmdPackage can go away once we kill the old assistant.
      <> cmdPackage numProcessors
      <> cmdBuild numProcessors
      <> cmdTest numProcessors
      <> Damldoc.cmd numProcessors (\cli -> Command DamlDoc Nothing $ Damldoc.exec cli)
      <> cmdVisual
      <> cmdVisualWeb
      <> cmdInspectDar
      <> cmdValidateDar
      <> cmdDocTest numProcessors
      <> cmdLint numProcessors
      <> cmdRepl numProcessors
      )
    <|> subparser
      (internal -- internal commands
        <> cmdInspect
        <> cmdVisual
        <> cmdVisualWeb
        <> cmdMergeDars
        <> cmdInit numProcessors
        <> cmdCompile numProcessors
        <> cmdDesugar numProcessors
        <> cmdDebugIdeSpanInfo numProcessors
        <> cmdClean
      )

parserInfo :: Int -> ParserInfo Command
parserInfo numProcessors =
  info (helper <*> options numProcessors)
    (  fullDesc
    <> progDesc "Invoke the Daml compiler. Use -h for help."
    <> headerDoc (Just $ PP.vcat
        [ "damlc - Compiler and IDE backend for the Daml programming language"
        , buildInfo
        ])
    )

cliArgsFromDamlYaml :: Maybe ProjectOpts -> IO [String]
cliArgsFromDamlYaml mbProjectOpts = do
    -- This is the same logic used in withProjectRoot but we don’t need to change CWD here
    -- and this is simple enough so we inline it here.
    mbEnvProjectPath <- fmap ProjectPath <$> getProjectPath
    let mbProjectPath = projectRoot =<< mbProjectOpts
    let projectPath = fromMaybe (ProjectPath ".") (mbProjectPath <|> mbEnvProjectPath)
    handle (\(_ :: ConfigError) -> return []) $ do
        project <- readProjectConfig projectPath
        case queryProjectConfigRequired ["build-options"] project of
            Left _ -> pure []
            Right xs -> pure xs

main :: IO ()
main = do
    -- We need this to ensure that logs are flushed on SIGTERM.
    installSignalHandlers
    -- Save the runfiles environment to work around
    -- https://gitlab.haskell.org/ghc/ghc/-/issues/18418.
    setRunfilesEnv
    numProcessors <- getNumProcessors
    let parse = ParseArgs.lax (parserInfo numProcessors)
    cliArgs <- getArgs
    let (_, tempParseResult) = parse cliArgs
    -- Note: need to parse given args first to decide whether we need to add
    -- args from daml.yaml.
    Command cmd mbProjectOpts _ <- handleParseResult tempParseResult
    damlYamlArgs <- cliArgsFromDamlYaml mbProjectOpts
    let args = if cmd `elem` [Build, Compile, Desugar, Ide, DebugIdeSpanInfo, Test, DamlDoc]
               then cliArgs ++ damlYamlArgs
               else cliArgs
        (errMsgs, parseResult) = parse args
    Command _ _ io <- handleParseResult parseResult
    forM_ errMsgs $ \msg -> do
        hPutStrLn stderr msg
    withProgName "damlc" io

withProjectRoot' :: ProjectOpts -> ((FilePath -> IO FilePath) -> IO a) -> IO a
withProjectRoot' ProjectOpts{..} act =
    withProjectRoot projectRoot projectCheck (const act)
