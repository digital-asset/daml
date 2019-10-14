-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ApplicativeDo       #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE MultiWayIf #-}

-- | Main entry-point of the DAML compiler
module DA.Cli.Damlc (main) where

import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import qualified "zip" Codec.Archive.Zip as Zip
import Control.Exception
import Control.Exception.Safe (catchIO, handleIO)
import Control.Lens (toListOf)
import Control.Monad.Except
import Control.Monad.Extra (whenM)
import DA.Bazel.Runfiles
import qualified DA.Cli.Args as ParseArgs
import DA.Cli.Damlc.Base
import DA.Cli.Damlc.BuildInfo
import qualified DA.Cli.Damlc.Command.Damldoc as Damldoc
import DA.Cli.Damlc.IdeState
import DA.Cli.Damlc.Test
import DA.Daml.Compiler.Dar
import DA.Daml.Compiler.DocTest
import DA.Daml.Compiler.Scenario
import DA.Daml.Compiler.Upgrade
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Proto3.Archive as Archive
import DA.Daml.LF.Reader
import DA.Daml.LanguageServer
import DA.Daml.Options.Types
import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import DA.Daml.Project.Types (ConfigError, ProjectPath(..))
import DA.Daml.Visual
import qualified DA.Pretty
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.GCP as Logger.GCP
import qualified DA.Service.Logger.Impl.IO as Logger.IO
import DA.Signals
import qualified Com.Digitalasset.DamlLfDev.DamlLf as PLF
import qualified Data.Aeson.Encode.Pretty as Aeson.Pretty
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC
import qualified Data.ByteString.UTF8 as BSUTF8
import Data.FileEmbed (embedFile)
import Data.Graph
import Data.List.Extra
import qualified Data.List.Split as Split
import qualified Data.Map.Strict as MS
import Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Set as Set
import qualified Data.Text.Extended as T
import Development.IDE.Core.API
import Development.IDE.Core.RuleTypes.Daml (GetParsedModule(..))
import Development.IDE.Core.Rules
import Development.IDE.Core.Rules.Daml (getDalf, getDlintIdeas)
import Development.IDE.Core.Service (runAction)
import Development.IDE.Core.Shake
import Development.IDE.GHC.Util (hscEnv, moduleImportPath)
import Development.IDE.Types.Location
import Development.IDE.Types.Options (clientSupportsProgress)
import "ghc-lib-parser" DynFlags
import GHC.Conc
import "ghc-lib-parser" Module
import qualified Network.Socket as NS
import Options.Applicative.Extended
import qualified Proto3.Suite as PS
import qualified Proto3.Suite.JSONPB as Proto.JSONPB
import System.Directory.Extra
import System.Environment
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Info.Extra
import System.Process (callProcess)
import qualified Text.PrettyPrint.ANSI.Leijen as PP
import DA.Daml.LF.Ast.Optics
import Development.IDE.Core.RuleTypes
import "ghc-lib-parser" ErrUtils
-- For dumps
import "ghc-lib" GHC
import "ghc-lib" HsDumpAst
import "ghc-lib" HscStats
import "ghc-lib-parser" HscTypes
import qualified "ghc-lib-parser" Outputable as GHC

--------------------------------------------------------------------------------
-- Commands
--------------------------------------------------------------------------------

data CommandName =
    Build
  | Clean
  | Compile
  | DamlDoc
  | DocTest
  | GenerateSrc
  | Ide
  | Init
  | Inspect
  | InspectDar
  | License
  | Lint
  | MergeDars
  | Migrate
  | Package
  | Test
  | Visual
  deriving (Ord, Show, Eq)
data Command = Command CommandName (IO ())

cmdIde :: Mod CommandFields Command
cmdIde =
    command "ide" $ info (helper <*> cmd) $
       progDesc
        "Start the DAML language server on standard input/output."
    <> fullDesc
  where
    cmd = execIde
        <$> telemetryOpt
        <*> debugOpt
        <*> enableScenarioOpt
        <*> optGhcCustomOptions
        <*> shakeProfilingOpt
        <*> optional lfVersionOpt

cmdLicense :: Mod CommandFields Command
cmdLicense =
    command "license" $ info (helper <*> pure execLicense) $
       progDesc
        "Show the licensing information!"
    <> fullDesc

cmdCompile :: Int -> Mod CommandFields Command
cmdCompile numProcessors =
    command "compile" $ info (helper <*> cmd) $
        progDesc "Compile the DAML program into a Core/DAML-LF archive."
    <> fullDesc
  where
    cmd = execCompile
        <$> inputFileOpt
        <*> outputFileOpt
        <*> optionsParser numProcessors (EnableScenarioService False) optPackageName
        <*> optional (strOption $ long "iface-dir" <> metavar "IFACE_DIR" <> help "Directory for interface files")

cmdLint :: Int -> Mod CommandFields Command
cmdLint numProcessors =
    command "lint" $ info (helper <*> cmd) $
        progDesc "Lint the DAML program."
    <> fullDesc
  where
    cmd = execLint
        <$> inputFileOpt
        <*> optionsParser numProcessors (EnableScenarioService False) optPackageName

cmdTest :: Int -> Mod CommandFields Command
cmdTest numProcessors =
    command "test" $ info (helper <*> cmd) $
       progDesc progDoc
    <> fullDesc
  where
    progDoc = unlines
      [ "Test the current DAML project or the given files by running all test declarations."
      , "Must be in DAML project if --files is not set."
      ]
    cmd = runTestsInProjectOrFiles
      <$> projectOpts "daml test"
      <*> filesOpt
      <*> fmap UseColor colorOutput
      <*> junitOutput
      <*> optionsParser numProcessors (EnableScenarioService True) optPackageName
    filesOpt = optional (flag' () (long "files" <> help filesDoc) *> many inputFileOpt)
    filesDoc = "Only run test declarations in the specified files."
    junitOutput = optional $ strOption $ long "junit" <> metavar "FILENAME" <> help "Filename of JUnit output file"
    colorOutput = switch $ long "color" <> help "Colored test results"

runTestsInProjectOrFiles :: ProjectOpts -> Maybe [FilePath] -> UseColor -> Maybe FilePath -> Options -> Command
runTestsInProjectOrFiles projectOpts Nothing color mbJUnitOutput cliOptions = Command Test effect
  where effect = withExpectProjectRoot (projectRoot projectOpts) "daml test" $ \pPath _ -> do
        project <- readProjectConfig $ ProjectPath pPath
        case parseProjectConfig project of
            Left err -> throwIO err
            Right PackageConfigFields {..} -> do
              -- TODO: We set up one scenario service context per file that
              -- we pass to execTest and scenario cnotexts are quite expensive.
              -- Therefore we keep the behavior of only passing the root file
              -- if source points to a specific file.
              files <- getDamlRootFiles pSrc
              execTest files color mbJUnitOutput cliOptions
runTestsInProjectOrFiles projectOpts (Just inFiles) color mbJUnitOutput cliOptions = Command Test effect
  where effect = withProjectRoot' projectOpts $ \relativize -> do
        inFiles' <- mapM (fmap toNormalizedFilePath . relativize) inFiles
        execTest inFiles' color mbJUnitOutput cliOptions

cmdInspect :: Mod CommandFields Command
cmdInspect =
    command "inspect" $ info (helper <*> cmd)
      $ progDesc "Pretty print a DALF file or the main DALF of a DAR file"
    <> fullDesc
  where
    jsonOpt = switch $ long "json" <> help "Output the raw Protocol Buffer structures as JSON"
    detailOpt =
        fmap (maybe DA.Pretty.prettyNormal DA.Pretty.PrettyLevel) $
            optional $ option auto $ long "detail" <> metavar "LEVEL" <> help "Detail level of the pretty printed output (default: 0)"
    cmd = execInspect <$> inputFileOptWithExt ".dalf or .dar" <*> outputFileOpt <*> jsonOpt <*> detailOpt

cmdVisual :: Mod CommandFields Command
cmdVisual =
    command "visual" $ info (helper <*> cmd) $ progDesc "Generate visual from dar" <> fullDesc
    where
      cmd = vis <$> inputDarOpt <*> dotFileOpt
      vis a b = Command Visual $ execVisual a b

cmdVisualWeb :: Mod CommandFields Command
cmdVisualWeb =
    command "visual-web" $ info (helper <*> cmd) $ progDesc "Generate D3-Web Visual from dar" <> fullDesc
    where
      cmd = vis <$> inputDarOpt <*> htmlOutFile <*> openBrowser
      vis a b browser = Command Visual $ execVisualHtml a b browser

cmdBuild :: Int -> Mod CommandFields Command
cmdBuild numProcessors =
    command "build" $
    info (helper <*> cmd) $
    progDesc "Initialize, build and package the DAML project" <> fullDesc
  where
    cmd =
        execBuild
            <$> projectOpts "daml build"
            <*> optionsParser numProcessors (EnableScenarioService False) (pure Nothing)
            <*> optionalOutputFileOpt
            <*> initPkgDbOpt

cmdClean :: Mod CommandFields Command
cmdClean =
    command "clean" $
    info (helper <*> cmd) $
    progDesc "Remove DAML project build artifacts" <> fullDesc
  where
    cmd = execClean <$> projectOpts "daml clean"

cmdInit :: Int -> Mod CommandFields Command
cmdInit numProcessors =
    command "init" $
    info (helper <*> cmd) $ progDesc "Initialize a DAML project" <> fullDesc
  where
    cmd = execInit <$> optionsParser numProcessors (EnableScenarioService False) (pure Nothing) <*> projectOpts "daml damlc init"

cmdPackage :: Int -> Mod CommandFields Command
cmdPackage numProcessors =
    command "package" $ info (helper <*> cmd) $
       progDesc "Compile the DAML program into a DAML Archive (DAR)"
    <> fullDesc
  where
    cmd = execPackage
        <$> projectOpts "daml damlc package"
        <*> inputFileOpt
        <*> optionsParser numProcessors (EnableScenarioService False) (Just <$> packageNameOpt)
        <*> optionalOutputFileOpt
        <*> optFromDalf

    optFromDalf :: Parser FromDalf
    optFromDalf = fmap FromDalf $
      switch $
      help "package an existing dalf file rather than compiling DAML sources" <>
      long "dalf" <>
      internal

cmdInspectDar :: Mod CommandFields Command
cmdInspectDar =
    command "inspect-dar" $
    info (helper <*> cmd) $ progDesc "Inspect a DAR archive" <> fullDesc
  where
    cmd = execInspectDar <$> inputDarOpt

cmdMigrate :: Mod CommandFields Command
cmdMigrate =
    command "migrate" $
    info (helper <*> cmd) $
    progDesc "Generate a migration package to upgrade the ledger" <> fullDesc
  where
    cmd =
        execMigrate
        <$> projectOpts "daml damlc migrate"
        <*> inputDarOpt
        <*> inputDarOpt
        <*> targetSrcDirOpt

cmdMergeDars :: Mod CommandFields Command
cmdMergeDars =
    command "merge-dars" $
    info (helper <*> cmd) $ progDesc "Merge two dar archives into one" <> fullDesc
  where
    cmd = execMergeDars <$> inputDarOpt <*> inputDarOpt <*> targetFileNameOpt

cmdGenerateSrc :: Mod CommandFields Command
cmdGenerateSrc =
    command "generate-src" $
    info (helper <*> cmd) $
    progDesc "Generate DAML source code from a dalf package" <> fullDesc
  where
    cmd = execGenerateSrc <$> inputDalfOpt

cmdDocTest :: Int -> Mod CommandFields Command
cmdDocTest numProcessors =
    command "doctest" $
    info (helper <*> cmd) $
    progDesc "doc tests" <> fullDesc
  where
    cmd = execDocTest
        <$> optionsParser numProcessors (EnableScenarioService True) optPackageName
        <*> many inputFileOpt

--------------------------------------------------------------------------------
-- Execution
--------------------------------------------------------------------------------

execLicense :: Command
execLicense =
  Command License effect
  where
    effect = B.putStr licenseData
    licenseData :: B.ByteString
    licenseData = $(embedFile "compiler/daml-licenses/licenses/licensing.md")

execIde :: Telemetry
        -> Debug
        -> EnableScenarioService
        -> [String]
        -> Maybe FilePath
        -> Maybe LF.Version
        -> Command
execIde telemetry (Debug debug) enableScenarioService ghcOpts mbProfileDir (fromMaybe LF.versionDefault -> lfVersion) =
    Command Ide effect
  where effect = NS.withSocketsDo $ do
          let threshold =
                  if debug
                  then Logger.Debug
                  -- info is used pretty extensively for debug messages in our code base so
                  -- I've set the no debug threshold at warning
                  else Logger.Warning
          loggerH <- Logger.IO.newIOLogger
            stderr
            (Just 5000)
            -- NOTE(JM): ^ Limit the message length to 5000 characters as VSCode
            -- performance will be significatly impacted by large log output.
            threshold
            "LanguageServer"
          let withLogger f = case telemetry of
                  OptedIn -> Logger.GCP.withGcpLogger (>= Logger.Warning) loggerH $ \gcpState loggerH' -> do
                      Logger.GCP.logMetaData gcpState
                      f loggerH'
                  OptedOut -> Logger.GCP.withGcpLogger (const False) loggerH $ \gcpState loggerH -> do
                      Logger.GCP.logOptOut gcpState
                      f loggerH
                  Undecided -> f loggerH
          opts <- defaultOptionsIO (Just lfVersion)
          initPackageDb opts (InitPkgDb True)
          dlintDataDir <-locateRunfiles $ mainWorkspace </> "compiler/damlc/daml-ide-core"
          opts <- pure $ opts
              { optScenarioService = enableScenarioService
              , optSkipScenarioValidation = SkipScenarioValidation True
              , optShakeProfiling = mbProfileDir
              , optThreads = 0
              , optDlintUsage = DlintEnabled dlintDataDir True
              , optGhcCustomOpts = ghcOpts
              }
          scenarioServiceConfig <- readScenarioServiceConfig
          withLogger $ \loggerH ->
              withScenarioService' enableScenarioService loggerH scenarioServiceConfig $ \mbScenarioService -> do
                  sdkVersion <- getSdkVersion `catchIO` const (pure "Unknown (not started via the assistant)")
                  Logger.logInfo loggerH (T.pack $ "SDK version: " <> sdkVersion)
                  runLanguageServer $ \sendMsg vfs caps ->
                      getDamlIdeState opts mbScenarioService loggerH sendMsg vfs (clientSupportsProgress caps)


execCompile :: FilePath -> FilePath -> Options -> Maybe FilePath -> Command
execCompile inputFile outputFile opts mbIfaceDir =
  Command Compile effect
  where
    effect = withProjectRoot' (ProjectOpts Nothing (ProjectCheck "" False)) $ \relativize -> do
      loggerH <- getLogger opts "compile"
      inputFile <- toNormalizedFilePath <$> relativize inputFile
      opts' <- mkOptions opts { optIfaceDir = mbIfaceDir }
      withDamlIdeState opts' loggerH diagnosticsLogger $ \ide -> do
          setFilesOfInterest ide (Set.singleton inputFile)
          runAction ide $ do
            -- Support for '-ddump-parsed', '-ddump-parsed-ast', '-dsource-stats'.
            dflags <- hsc_dflags . hscEnv <$> use_ GhcSession inputFile
            parsed <- pm_parsed_source <$> use_ GetParsedModule inputFile
            liftIO $ do
              ErrUtils.dumpIfSet_dyn dflags Opt_D_dump_parsed "Parser" $ GHC.ppr parsed
              ErrUtils.dumpIfSet_dyn dflags Opt_D_dump_parsed_ast "Parser AST" $ showAstData NoBlankSrcSpan parsed
              ErrUtils.dumpIfSet_dyn dflags Opt_D_source_stats "Source Statistics" $ ppSourceStats False parsed

            when (optWriteInterface opts') $ do
                files <- nubSort . concatMap transitiveModuleDeps <$> use GetDependencies inputFile
                mbIfaces <- writeIfacesAndHie (toNormalizedFilePath $ fromMaybe ifaceDir $ optIfaceDir opts') files
                void $ liftIO $ mbErr "ERROR: Compilation failed." mbIfaces

            mbDalf <- getDalf inputFile
            dalf <- liftIO $ mbErr "ERROR: Compilation failed." mbDalf
            liftIO $ write dalf
    write bs
      | outputFile == "-" = putStrLn $ render Colored $ DA.Pretty.pretty bs
      | otherwise = do
        createDirectoryIfMissing True $ takeDirectory outputFile
        B.writeFile outputFile $ Archive.encodeArchive bs

execLint :: FilePath -> Options -> Command
execLint inputFile opts =
  Command Lint effect
  where
     effect =
       withProjectRoot' (ProjectOpts Nothing (ProjectCheck "" False)) $ \relativize ->
       do
         loggerH <- getLogger opts "lint"
         inputFile <- toNormalizedFilePath <$> relativize inputFile
         opts <- (setDlintDataDir <=< mkOptions) opts
         withDamlIdeState opts loggerH diagnosticsLogger $ \ide -> do
             setFilesOfInterest ide (Set.singleton inputFile)
             runAction ide $ getDlintIdeas inputFile
             diags <- getDiagnostics ide
             if null diags then
               hPutStrLn stderr "No hints"
             else
               exitFailure
     setDlintDataDir :: Options -> IO Options
     setDlintDataDir opts = do
       defaultDir <-locateRunfiles $
         mainWorkspace </> "compiler/damlc/daml-ide-core"
       return $ case optDlintUsage opts of
         DlintEnabled _ _ -> opts
         DlintDisabled  -> opts{optDlintUsage=DlintEnabled defaultDir True}

-- | Parse the daml.yaml for package specific config fields.
parseProjectConfig :: ProjectConfig -> Either ConfigError PackageConfigFields
parseProjectConfig project = do
    name <- queryProjectConfigRequired ["name"] project
    main <- queryProjectConfigRequired ["source"] project
    exposedModules <- queryProjectConfig ["exposed-modules"] project
    version <- queryProjectConfigRequired ["version"] project
    dependencies <-
        queryProjectConfigRequired ["dependencies"] project
    sdkVersion <- queryProjectConfigRequired ["sdk-version"] project
    cliOpts <- queryProjectConfig ["build-options"] project
    Right $ PackageConfigFields name main exposedModules version dependencies sdkVersion cliOpts

-- | We assume that this is only called within `withProjectRoot`.
withPackageConfig :: (PackageConfigFields -> IO a) -> IO a
withPackageConfig f = do
    project <- readProjectConfig $ ProjectPath "."
    case parseProjectConfig project of
        Left err -> throwIO err
        Right pkgConfig -> f pkgConfig

-- | If we're in a daml project, read the daml.yaml field and create the project local package
-- database. Otherwise do nothing.
execInit :: Options -> ProjectOpts -> Command
execInit opts projectOpts =
  Command Init effect
  where effect = withProjectRoot' projectOpts $ \_relativize ->
          initPackageDb
            opts
            (InitPkgDb True)

initPackageDb :: Options -> InitPkgDb -> IO ()
initPackageDb opts (InitPkgDb shouldInit) =
    when shouldInit $ do
        isProject <- doesFileExist projectConfigName
        when isProject $ do
          project <- readProjectConfig $ ProjectPath "."
          case parseProjectConfig project of
              Left err -> throwIO err
              Right PackageConfigFields {..} -> do
                  createProjectPackageDb opts pDependencies

-- | Create the project package database containing the given dar packages.
createProjectPackageDb ::
       Options -> [FilePath] -> IO ()
createProjectPackageDb opts fps = do
    let dbPath = projectPackageDatabase </> (lfVersionString $ optDamlLfVersion opts)
    createDirectoryIfMissing True $ dbPath </> "package.conf.d"
    -- Expand SDK package dependencies using the SDK root path.
    -- E.g. `daml-trigger` --> `$DAML_SDK/daml-libs/daml-trigger.dar`
    -- Or, fail if not run from DAML assistant.
    mbSdkPath <- handleIO (\_ -> pure Nothing) $ Just <$> getSdkPath
    let isSdkPackage fp = takeExtension fp /= ".dar"
        handleSdkPackages :: [FilePath] -> IO [FilePath]
        handleSdkPackages =
          let expand fp
                | isSdkPackage fp
                = case mbSdkPath of
                    Just sdkPath -> pure $! sdkPath </> "daml-libs" </> fp <.> "dar"
                    Nothing -> fail $ "Cannot resolve SDK dependency '" ++ fp ++ "'. Use daml-assistant."
                | otherwise
                = pure fp
          in mapM expand
    fps0 <- handleSdkPackages $ filter (`notElem` basePackages) fps
    (sdkVersions, confFiles, dalfs, srcs) <-
        fmap mconcat $ forM fps0 $ \fp -> do
            bs <- BSL.readFile fp
            let archive = ZipArchive.toArchive bs
            manifest <- getEntry manifestPath archive
            sdkVersion <- case parseManifestFile $ BSL.toStrict $ ZipArchive.fromEntry manifest of
                Left err -> fail err
                Right manifest -> case lookup "Sdk-Version" manifest of
                    Nothing -> fail "No Sdk-Version entry in manifest"
                    Just version -> pure $! trim $ BSUTF8.toString version
            let confFiles =
                    [ e
                    | e <- ZipArchive.zEntries archive
                    , ".conf" `isExtensionOf` ZipArchive.eRelativePath e
                    ]
            let dalfs =
                    [ e
                    | e <- ZipArchive.zEntries archive
                    , ".dalf" `isExtensionOf` ZipArchive.eRelativePath e
                    ]
            let srcs =
                    [ e
                    | e <- ZipArchive.zEntries archive
                    , takeExtension (ZipArchive.eRelativePath e) `elem`
                          [".daml", ".hie", ".hi"]
                    ]
            pure ([sdkVersion], confFiles, dalfs, srcs)
    let uniqSdkVersions = nubSort sdkVersions
    if | length uniqSdkVersions <= 1 ->
           do forM_ dalfs $ \dalf -> do
                  let path = dbPath </> ZipArchive.eRelativePath dalf
                  createDirectoryIfMissing True (takeDirectory path)
                  BSL.writeFile path (ZipArchive.fromEntry dalf)
              forM_ confFiles $ \conf ->
                  BSL.writeFile
                      (dbPath </> "package.conf.d" </>
                       (takeFileName $ ZipArchive.eRelativePath conf))
                      (ZipArchive.fromEntry conf)
              forM_ srcs $ \src -> do
                  let path = dbPath </> ZipArchive.eRelativePath src
                  write path (ZipArchive.fromEntry src)
              ghcPkgPath <- getGhcPkgPath
              callProcess
                  (ghcPkgPath </> exe "ghc-pkg")
                  [ "recache"
                  -- ghc-pkg insists on using a global package db and will try
                  -- to find one automatically if we don’t specify it here.
                  , "--global-package-db=" ++ (dbPath </> "package.conf.d")
                  , "--expand-pkgroot"
                  ]
       | length uniqSdkVersions > 1 && optAllowDifferentSdks opts ->
            -- when we compile packages with different sdk versions, we need to generate the interface files
           do loggerH <- getLogger opts "generate interface files"
              pkgs <-
                  forM dalfs $ \dalf -> do
                      let dalfBS = BSL.toStrict $ ZipArchive.fromEntry dalf
                      (pkgId, package) <-
                          either (fail . DA.Pretty.renderPretty) pure $
                          Archive.decodeArchive dalfBS
                      pure
                          ( LF.rewriteSelfReferences pkgId package
                          , stringToUnitId $
                            dropExtension $
                            takeFileName $ ZipArchive.eRelativePath dalf)
              let pkgMap =
                      MS.fromList
                          [ (pkgId, unitId)
                          | (LF.ExternalPackage pkgId _pkg, unitId) <- pkgs
                          ]
              -- order the packages in topological order
              let (depGraph, vertexToNode, _keyToVertex) =
                      graphFromEdges $ do
                          (LF.ExternalPackage pkgId dalf, unitId) <- pkgs
                          let pkgRefs =
                                  [ pid
                                  | LF.PRImport pid <- toListOf packageRefs dalf
                                  ]
                          let src = generateSrcPkgFromLf pkgId pkgMap dalf
                          pure ((src, unitId, dalf), pkgId, pkgRefs)
              let pkgIdsTopoSorted = reverse $ topSort depGraph
              ghcPkgPath <- getGhcPkgPath
              dbPathAbs <- makeAbsolute dbPath
              projectPackageDatabaseAbs <- makeAbsolute projectPackageDatabase
              forM_ pkgIdsTopoSorted $ \vertex -> do
                  let ((src, uid, dalf), pkgId, _) = vertexToNode vertex
                  when (uid /= primUnitId) $ do
                      let unitIdStr = unitIdString uid
                      let pkgIdStr = T.unpack $ LF.unPackageId pkgId
                      let workDir = dbPath </> unitIdStr <> "-" <> pkgIdStr
                      createDirectoryIfMissing True workDir
                      -- write the dalf package
                      B.writeFile (workDir </> unitIdStr <.> "dalf") $
                          Archive.encodeArchive dalf
                      -- we change the working dir so that we get correct file paths for the
                      -- interface files.
                      withCurrentDirectory workDir $
                       -- typecheck and generate interface files
                       do
                          forM_ src $ \(fp, content) -> do
                              let path = fromNormalizedFilePath fp
                              createDirectoryIfMissing True $ takeDirectory path
                              writeFileUTF8 path content
                          opts' <-
                              mkOptions $
                              opts
                                  { optWriteInterface = True
                                  , optPackageDbs =
                                        projectPackageDatabaseAbs :
                                        optPackageDbs opts
                                  , optIfaceDir = Just "./"
                                  , optIsGenerated = True
                                  , optDflagCheck = False
                                  , optMbPackageName = Just unitIdStr
                                  , optHideAllPkgs = False
                                  , optPackageImports = []
                                  , optGhcCustomOpts = []
                                  }
                          withDamlIdeState opts' loggerH diagnosticsLogger $ \ide ->
                              runAction ide $
                              writeIfacesAndHie
                                  (toNormalizedFilePath "./")
                                  [fp | (fp, _content) <- src]
                      let deps =
                              [ unitIdString uId <.> "dalf"
                              | ((_src, uId, _dalf), pId, _) <-
                                    map vertexToNode $ reachable depGraph vertex
                              , pkgId /= pId
                              ]
                      -- write the conf file and refresh the package cache
                      let (pkgName, pkgVersion) =
                              fromMaybe (unitIdStr, "") $ stripInfixEnd "-" unitIdStr
                      let (cfPath, cfBs) =
                              mkConfFile
                                  PackageConfigFields
                                      { pName = pkgName
                                      , pSrc = "" -- not used
                                      , pExposedModules = Nothing
                                      , pVersion = pkgVersion
                                      , pDependencies = deps
                                      , pSdkVersion = "unknown"
                                      , cliOpts = Nothing
                                      }
                                  (map T.unpack $ LF.packageModuleNames dalf)
                                  pkgIdStr
                      B.writeFile (dbPathAbs </> "package.conf.d" </> cfPath) cfBs
                      callProcess
                          (ghcPkgPath </> exe "ghc-pkg")
                          [ "recache"
                          -- ghc-pkg insists on using a global package db and will try
                          -- to find one automatically if we don’t specify it here.
                          , "--global-package-db=" ++
                            (dbPathAbs </> "package.conf.d")
                          , "--expand-pkgroot"
                          ]
       | otherwise ->
           fail $
           "Package dependencies from different SDK versions: " ++
           intercalate ", " uniqSdkVersions
  where
    write fp bs = createDirectoryIfMissing True (takeDirectory fp) >> BSL.writeFile fp bs
    getGhcPkgPath =
        if isWindows
            then locateRunfiles "rules_haskell_ghc_windows_amd64/bin"
            else locateRunfiles "ghc_nix/lib/ghc-8.6.5/bin"


-- | Fail with an exit failure and errror message when Nothing is returned.
mbErr :: String -> Maybe a -> IO a
mbErr err = maybe (hPutStrLn stderr err >> exitFailure) pure

execBuild :: ProjectOpts -> Options -> Maybe FilePath -> InitPkgDb -> Command
execBuild projectOpts options mbOutFile initPkgDb =
  Command Build effect
  where effect = withProjectRoot' projectOpts $ \_relativize -> do
            initPackageDb options initPkgDb
            withPackageConfig $ \pkgConfig@PackageConfigFields{..} -> do
                putStrLn $ "Compiling " <> pName <> " to a DAR."
                opts <- mkOptions options
                loggerH <- getLogger opts "package"
                withDamlIdeState
                    opts {optMbPackageName = Just $ pkgNameVersion pName pVersion}
                    loggerH
                    diagnosticsLogger $ \compilerH -> do
                    mbDar <-
                        buildDar
                            compilerH
                            pkgConfig
                            (toNormalizedFilePath $ fromMaybe ifaceDir $ optIfaceDir opts)
                            (FromDalf False)
                    dar <- mbErr "ERROR: Creation of DAR file failed." mbDar
                    let fp = targetFilePath $ pkgNameVersion pName pVersion
                    createDirectoryIfMissing True $ takeDirectory fp
                    Zip.createArchive fp dar
                    putStrLn $ "Created " <> fp <> "."
            where
                targetFilePath name = fromMaybe (distDir </> name <.> "dar") mbOutFile

-- | Remove any build artifacts if they exist.
execClean :: ProjectOpts -> Command
execClean projectOpts =
  Command Clean effect
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

lfVersionString :: LF.Version -> String
lfVersionString = DA.Pretty.renderPretty

execPackage:: ProjectOpts
            -> FilePath -- ^ input file
            -> Options
            -> Maybe FilePath
            -> FromDalf
            -> Command
execPackage projectOpts filePath opts mbOutFile dalfInput =
  Command Package effect
  where
    effect = withProjectRoot' projectOpts $ \relativize -> do
      loggerH <- getLogger opts "package"
      filePath <- relativize filePath
      opts' <- mkOptions opts
      withDamlIdeState opts' loggerH diagnosticsLogger $ \ide -> do
          -- We leave the sdk version blank and the list of exposed modules empty.
          -- This command is being removed anytime now and not present
          -- in the new daml assistant.
          mbDar <- buildDar ide
                            PackageConfigFields
                              { pName = fromMaybe (takeBaseName filePath) $ optMbPackageName opts
                              , pSrc = filePath
                              , pExposedModules = Nothing
                              , pVersion = ""
                              , pDependencies = []
                              , pSdkVersion = ""
                              , cliOpts = Nothing
                              }
                            (toNormalizedFilePath $ fromMaybe ifaceDir $ optIfaceDir opts')
                            dalfInput
          case mbDar of
            Nothing -> do
                hPutStrLn stderr "ERROR: Creation of DAR file failed."
                exitFailure
            Just dar -> do
              createDirectoryIfMissing True $ takeDirectory targetFilePath
              Zip.createArchive targetFilePath dar
              putStrLn $ "Created " <> targetFilePath <> "."
    -- This is somewhat ugly but our CLI parser guarantees that this will always be present.
    -- We could parametrize CliOptions by whether the package name is optional
    -- but I don’t think that is worth the complexity of carrying around a type parameter.
    name = fromMaybe (error "Internal error: Package name was not present") (optMbPackageName opts)

    -- The default output filename is based on Maven coordinates if
    -- the package name is specified via them, otherwise we use the
    -- name.
    defaultDarFile =
      case Split.splitOn ":" name of
        [_g, a, v] -> a <> "-" <> v <> ".dar"
        _otherwise -> name <> ".dar"

    targetFilePath = fromMaybe defaultDarFile mbOutFile

execInspect :: FilePath -> FilePath -> Bool -> DA.Pretty.PrettyLevel -> Command
execInspect inFile outFile jsonOutput lvl =
  Command Inspect effect
  where
    effect = do
      bytes <-
          if "dar" `isExtensionOf` inFile
              then do
                  dar <- B.readFile inFile
                  dalfs <- either fail pure $ readDalfs $ ZipArchive.toArchive $ BSL.fromStrict dar
                  pure $! BSL.toStrict $ mainDalf dalfs
              else B.readFile inFile

      if jsonOutput
      then do
        archive :: PLF.ArchivePayload <- errorOnLeft "Cannot decode archive" (PS.fromByteString bytes)
        writeOutputBSL outFile
         $ Aeson.Pretty.encodePretty
         $ Proto.JSONPB.toAesonValue archive
      else do
        (pkgId, lfPkg) <- errorOnLeft "Cannot decode package" $
                   Archive.decodeArchive bytes
        writeOutput outFile $ render Plain $
          DA.Pretty.vsep
            [ DA.Pretty.keyword_ "package" DA.Pretty.<-> DA.Pretty.text (LF.unPackageId pkgId) DA.Pretty.<-> DA.Pretty.keyword_ "where"
            , DA.Pretty.nest 2 (DA.Pretty.pPrintPrec lvl 0 lfPkg)
            ]

errorOnLeft :: Show a => String -> Either a b -> IO b
errorOnLeft desc = \case
  Left err -> ioError $ userError $ unlines [ desc, show err ]
  Right x  -> return x

execInspectDar :: FilePath -> Command
execInspectDar inFile =
  Command InspectDar effect
  where
    effect = do
      bytes <- B.readFile inFile

      putStrLn "DAR archive contains the following files: \n"
      let dar = ZipArchive.toArchive $ BSL.fromStrict bytes
      let files = [ZipArchive.eRelativePath e | e <- ZipArchive.zEntries dar]
      mapM_ putStrLn files

      putStrLn "\nDAR archive contains the following packages: \n"
      let dalfEntries =
              [e | e <- ZipArchive.zEntries dar, ".dalf" `isExtensionOf` ZipArchive.eRelativePath e]
      forM_ dalfEntries $ \dalfEntry -> do
          let dalf = BSL.toStrict $ ZipArchive.fromEntry dalfEntry
          (pkgId, _lfPkg) <-
              errorOnLeft
                  ("Cannot decode package " <> ZipArchive.eRelativePath dalfEntry)
                  (Archive.decodeArchive dalf)
          putStrLn $
              (dropExtension $ takeFileName $ ZipArchive.eRelativePath dalfEntry) <> " " <>
              show (LF.unPackageId pkgId)

execMigrate ::
       ProjectOpts
    -> FilePath
    -> FilePath
    -> Maybe FilePath
    -> Command
execMigrate projectOpts inFile1_ inFile2_ mbDir =
  Command Migrate effect
  where
    effect = do
      inFile1 <- makeAbsolute inFile1_
      inFile2 <- makeAbsolute inFile2_
      withProjectRoot' projectOpts $ \_relativize
       -> do
          -- get the package name and the lf-package
          [(pkgName1, pkgId1, lfPkg1, depMap1), (pkgName2, pkgId2, lfPkg2, depMap2)] <-
              forM [inFile1, inFile2] $ \inFile -> do
                  bytes <- B.readFile inFile
                  let dar = ZipArchive.toArchive $ BSL.fromStrict bytes
                  -- get the main pkg
                  dalfManifest <- either fail pure $ readDalfManifest dar
                  let pkgName = takeBaseName $ mainDalfPath dalfManifest
                  let dalfs = dalfPaths dalfManifest
                  deps <-
                      forM dalfs $ \dalfPath -> do
                          dalf <- getEntry dalfPath dar
                          (pkgId, _pkg) <- decode $ BSL.toStrict $ ZipArchive.fromEntry dalf
                          pure (pkgId, stringToUnitId $ takeBaseName dalfPath)
                  mainDalfEntry <- getEntry (mainDalfPath dalfManifest) dar
                  (mainPkgId, mainLfPkg) <- decode $ BSL.toStrict $ ZipArchive.fromEntry mainDalfEntry
                  pure (pkgName, mainPkgId, mainLfPkg, MS.fromList deps)
          let pkgMap = MS.union depMap1 depMap2
          -- generate upgrade modules and instances modules
          let eqModNames =
                  (NM.names $ LF.packageModules lfPkg1) `intersect`
                  (NM.names $ LF.packageModules lfPkg2)
          let eqModNamesStr = map (T.unpack . LF.moduleNameString) eqModNames
          let buildOptions =
                  [ "--allow-different-sdks"
                  , "'--package=" <> show (pkgName1, [(m, m ++ "A") | m <- eqModNamesStr]) <> "'"
                  , "'--package=" <> show (pkgName2, [(m, m ++ "B") | m <- eqModNamesStr]) <> "'"
                  , "--ghc-option=-Wno-unrecognised-pragmas"
                  ]
          forM_ eqModNames $ \m@(LF.ModuleName modName) -> do
              [genSrc1, genSrc2] <-
                  forM [(pkgId1, lfPkg1), (pkgId2, lfPkg2)] $ \(pkgId, pkg) -> do
                      generateSrcFromLf (Qualify False) pkgId pkgMap <$> getModule m pkg
              let upgradeModPath =
                      (joinPath $ fromMaybe "" mbDir : map T.unpack modName) <>
                      ".daml"
              let instancesModPath1 =
                      replaceBaseName upgradeModPath $
                      takeBaseName upgradeModPath <> "AInstances"
              let instancesModPath2 =
                      replaceBaseName upgradeModPath $
                      takeBaseName upgradeModPath <> "BInstances"
              templateNames <-
                  map (T.unpack . T.intercalate "." . LF.unTypeConName) .
                  NM.names . LF.moduleTemplates <$>
                  getModule m lfPkg1
              let generatedUpgradeMod =
                      generateUpgradeModule
                          templateNames
                          (T.unpack $ LF.moduleNameString m)
                          "A"
                          "B"
              let generatedInstancesMod1 =
                      generateGenInstancesModule "A" (pkgName1, genSrc1)
              let generatedInstancesMod2 =
                      generateGenInstancesModule "B" (pkgName2, genSrc2)
              forM_
                  [ (upgradeModPath, generatedUpgradeMod)
                  , (instancesModPath1, generatedInstancesMod1)
                  , (instancesModPath2, generatedInstancesMod2)
                  ] $ \(path, mod) -> do
                  createDirectoryIfMissing True $ takeDirectory path
                  writeFile path mod
          oldDamlYaml <- T.readFileUtf8 "daml.yaml"
          let newDamlYaml = T.unlines $
                T.lines oldDamlYaml ++
                ["build-options:"] ++
                map (\opt -> T.pack $ "- " <> opt) buildOptions
          T.writeFileUtf8 "daml.yaml" newDamlYaml
          putStrLn "Generation of migration project complete."
    decode dalf =
        errorOnLeft
            "Cannot decode daml-lf archive"
            (Archive.decodeArchive dalf)
    getModule modName pkg =
        maybe
            (fail $ T.unpack $ "Can't find module" <> LF.moduleNameString modName)
            pure $
        NM.lookup modName $ LF.packageModules pkg

-- | Get an entry from a dar or fail.
getEntry :: FilePath -> ZipArchive.Archive -> IO ZipArchive.Entry
getEntry fp dar =
    maybe (fail $ "Package does not contain " <> fp) pure $
    ZipArchive.findEntryByPath fp dar

-- | Merge two dars. The idea is that the second dar is a delta. Hence, we take the main in the
-- manifest from the first.
execMergeDars :: FilePath -> FilePath -> Maybe FilePath -> Command
execMergeDars darFp1 darFp2 mbOutFp =
  Command MergeDars effect
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
                  -- nubSortOn keeps the first occurence
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

-- | Generate daml source files from a dalf package.
execGenerateSrc :: FilePath -> Command
execGenerateSrc dalfFp = Command GenerateSrc effect
  where
    effect = do
        bytes <- B.readFile dalfFp
        case Archive.decodeArchive bytes of
            Left err -> fail $ DA.Pretty.renderPretty err
            Right (pkgId, pkg) -> do
                let genSrcs =
                        generateSrcPkgFromLf
                            pkgId
                            (MS.singleton
                                 pkgId
                                 (stringToUnitId $ takeBaseName dalfFp))
                            pkg
                forM_ genSrcs $ \(path, src) -> do
                    let fp = fromNormalizedFilePath path
                    createDirectoryIfMissing True $ takeDirectory fp
                    writeFileUTF8 fp src

execDocTest :: Options -> [FilePath] -> Command
execDocTest opts files =
  Command DocTest effect
  where
    effect = do
      let files' = map toNormalizedFilePath files
      logger <- getLogger opts "doctest"
      -- We don’t add a logger here since we will otherwise emit logging messages twice.
      importPaths <-
          withDamlIdeState opts { optScenarioService = EnableScenarioService False }
              logger (const $ pure ()) $ \ideState -> runAction ideState $ do
          pmS <- catMaybes <$> uses GetParsedModule files'
          -- This is horrible but we do not have a way to change the import paths in a running
          -- IdeState at the moment.
          pure $ nubOrd $ mapMaybe (uncurry moduleImportPath) (zip files' pmS)
      opts <- mkOptions opts { optImportPath = importPaths <> optImportPath opts, optHaddock = Haddock True }
      withDamlIdeState opts logger diagnosticsLogger $ \ideState ->
          docTest ideState files'

--------------------------------------------------------------------------------
-- main
--------------------------------------------------------------------------------

options :: Int -> Parser Command
options numProcessors =
    subparser
      (  cmdIde
      <> cmdLicense
      -- cmdPackage can go away once we kill the old assistant.
      <> cmdPackage numProcessors
      <> cmdBuild numProcessors
      <> cmdTest numProcessors
      <> Damldoc.cmd numProcessors (\cli -> Command DamlDoc $ Damldoc.exec cli)
      <> cmdVisual
      <> cmdVisualWeb
      <> cmdInspectDar
      <> cmdDocTest numProcessors
      <> cmdLint numProcessors
      )
    <|> subparser
      (internal -- internal commands
        <> cmdInspect
        <> cmdVisual
        <> cmdVisualWeb
        <> cmdMigrate
        <> cmdMergeDars
        <> cmdInit numProcessors
        <> cmdCompile numProcessors
        <> cmdClean
        <> cmdGenerateSrc
      )

parserInfo :: Int -> ParserInfo Command
parserInfo numProcessors =
  info (helper <*> options numProcessors)
    (  fullDesc
    <> progDesc "Invoke the DAML compiler. Use -h for help."
    <> headerDoc (Just $ PP.vcat
        [ "damlc - Compiler and IDE backend for the Digital Asset Modelling Language"
        , buildInfo
        ])
    )

cliArgsFromDamlYaml :: IO [String]
cliArgsFromDamlYaml = do
    handle (\(_ :: ConfigError) -> return [])
           $ do
               project <- readProjectConfig $ ProjectPath "."
               case parseProjectConfig project of
                   Left _ -> return []
                   Right pkgConfig -> case cliOpts pkgConfig of
                       Nothing -> return []
                       Just xs -> return xs

main :: IO ()
main = do
    -- We need this to ensure that logs are flushed on SIGTERM.
    installSignalHandlers
    numProcessors <- getNumProcessors
    let parse = ParseArgs.lax (parserInfo numProcessors)
    cliArgs <- getArgs
    damlYamlArgs <- cliArgsFromDamlYaml
    let (_, tempParseResult) = parse cliArgs
    -- Note: need to parse given args first to decide whether we need to add
    -- args from daml.yaml.
    Command cmd _ <- handleParseResult tempParseResult
    let args = if cmd `elem` [Build, Compile, Ide]
               then cliArgs ++ damlYamlArgs
               else cliArgs
        (errMsgs, parseResult) = parse args
    Command _ io <- handleParseResult parseResult
    forM_ errMsgs $ \msg -> do
        hPutStrLn stderr msg
    withProgName "damlc" io

withProjectRoot' :: ProjectOpts -> ((FilePath -> IO FilePath) -> IO a) -> IO a
withProjectRoot' ProjectOpts{..} act =
    withProjectRoot projectRoot projectCheck (const act)
