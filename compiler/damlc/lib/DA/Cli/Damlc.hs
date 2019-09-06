-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ApplicativeDo       #-}
{-# LANGUAGE CPP #-}

-- | Main entry-point of the DAML compiler
module DA.Cli.Damlc (main) where

import qualified "zip-archive" Codec.Archive.Zip as ZipArchive
import qualified "zip" Codec.Archive.Zip as Zip
import Control.Exception
import Control.Exception.Safe (catchIO)
import Control.Monad.Except
import Control.Monad.Extra (whenM)
import DA.Bazel.Runfiles
import DA.Cli.Args
import DA.Cli.Damlc.Base
import DA.Cli.Damlc.BuildInfo
import DA.Cli.Damlc.Command.Damldoc (cmdDamlDoc)
import DA.Cli.Damlc.IdeState
import DA.Cli.Damlc.Test
import DA.Daml.Visual
import DA.Daml.Compiler.Dar
import DA.Daml.Compiler.DocTest
import DA.Daml.Compiler.Scenario
import DA.Daml.Compiler.Upgrade
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Proto3.Archive as Archive
import DA.Daml.LF.Reader
import DA.Daml.LanguageServer
import DA.Daml.Options
import DA.Daml.Options.Types
import qualified DA.Pretty
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.GCP as Logger.GCP
import qualified DA.Service.Logger.Impl.IO as Logger.IO
import DA.Signals
import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import DA.Daml.Project.Types (ConfigError, ProjectPath(..))
import qualified Da.DamlLf as PLF
import qualified Data.Aeson.Encode.Pretty as Aeson.Pretty
import qualified Data.ByteString as B
import qualified Data.ByteString.UTF8 as BSUTF8
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC
import Data.FileEmbed (embedFile)
import Data.Graph
import qualified Data.Set as Set
import Data.List.Extra
import qualified Data.List.Split as Split
import qualified Data.Map.Strict as MS
import Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Text as T
import Development.IDE.Core.API
import Development.IDE.Core.Service (runAction)
import Development.IDE.Core.Shake
import Development.IDE.Core.Rules
import Development.IDE.Core.Rules.Daml (getDalf, getDlintIdeas)
import Development.IDE.Core.RuleTypes.Daml (DalfPackage(..), GetParsedModule(..))
import Development.IDE.GHC.Util (fakeDynFlags, moduleImportPaths)
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import Development.IDE.Types.Options (clientSupportsProgress)
import "ghc-lib-parser" DynFlags
import GHC.Conc
import "ghc-lib-parser" Module
import qualified Network.Socket as NS
import Options.Applicative.Extended
import "ghc-lib-parser" Packages
import qualified Proto3.Suite as PS
import qualified Proto3.Suite.JSONPB as Proto.JSONPB
import System.Directory.Extra
import System.Environment
import System.Exit
import System.FilePath
import System.Info.Extra
import System.IO.Extra
#ifndef mingw32_HOST_OS
import System.Posix.Files
#endif
import System.Process (callProcess)
import qualified Text.PrettyPrint.ANSI.Leijen      as PP

--------------------------------------------------------------------------------
-- Commands
--------------------------------------------------------------------------------

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

runTestsInProjectOrFiles :: ProjectOpts -> Maybe [FilePath] -> UseColor -> Maybe FilePath -> Options -> IO ()
runTestsInProjectOrFiles projectOpts Nothing color mbJUnitOutput cliOptions =
    withExpectProjectRoot (projectRoot projectOpts) "daml test" $ \pPath _ -> do
        project <- readProjectConfig $ ProjectPath pPath
        case parseProjectConfig project of
            Left err -> throwIO err
            Right PackageConfigFields {..} -> do
              files <- getDamlFiles pSrc
              execTest files color mbJUnitOutput cliOptions
runTestsInProjectOrFiles projectOpts (Just inFiles) color mbJUnitOutput cliOptions =
    withProjectRoot' projectOpts $ \relativize -> do
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
    cmd = execInspect <$> inputFileOpt <*> outputFileOpt <*> jsonOpt <*> detailOpt

cmdVisual :: Mod CommandFields Command
cmdVisual =
    command "visual" $ info (helper <*> cmd) $ progDesc "Generate visual from dar" <> fullDesc
    where
      cmd = execVisual <$> inputDarOpt <*> dotFileOpt


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

cmdInit :: Mod CommandFields Command
cmdInit =
    command "init" $
    info (helper <*> cmd) $ progDesc "Initialize a DAML project" <> fullDesc
  where
    cmd = execInit <$> lfVersionOpt <*> projectOpts "daml damlc init"

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

cmdMigrate :: Int -> Mod CommandFields Command
cmdMigrate numProcessors =
    command "migrate" $
    info (helper <*> cmd) $
    progDesc "Generate a migration package to upgrade the ledger" <> fullDesc
  where
    cmd =
        execMigrate
        <$> projectOpts "daml damlc migrate"
        <*> optionsParser
            numProcessors
            (EnableScenarioService False)
            (Just <$> packageNameOpt)
        <*> inputDarOpt
        <*> inputDarOpt
        <*> targetSrcDirOpt

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
    progDesc "doc tests" <> fullDesc
  where
    cmd = execDocTest
        <$> optionsParser numProcessors (EnableScenarioService True) optPackageName
        <*> many inputFileOpt

--------------------------------------------------------------------------------
-- Execution
--------------------------------------------------------------------------------

execLicense :: Command
execLicense = B.putStr licenseData
  where
    licenseData :: B.ByteString
    licenseData = $(embedFile "compiler/daml-licenses/licenses/licensing.md")

execIde :: Telemetry
        -> Debug
        -> EnableScenarioService
        -> [String]
        -> Maybe FilePath
        -> Command
execIde telemetry (Debug debug) enableScenarioService ghcOpts mbProfileDir = NS.withSocketsDo $ do
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
    -- TODO we should allow different LF versions in the IDE.
    initPackageDb LF.versionDefault (InitPkgDb True) (AllowDifferentSdkVersions False)
    dlintDataDir <-locateRunfiles $ mainWorkspace </> "compiler/damlc/daml-ide-core"
    opts <- defaultOptionsIO Nothing
    opts <- pure $ opts
        { optScenarioService = enableScenarioService
        , optScenarioValidation = ScenarioValidationLight
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


execCompile :: FilePath -> FilePath -> Options -> Command
execCompile inputFile outputFile opts = withProjectRoot' (ProjectOpts Nothing (ProjectCheck "" False)) $ \relativize -> do
    loggerH <- getLogger opts "compile"
    inputFile <- toNormalizedFilePath <$> relativize inputFile
    opts' <- mkOptions opts
    withDamlIdeState opts' loggerH diagnosticsLogger $ \ide -> do
        setFilesOfInterest ide (Set.singleton inputFile)
        runAction ide $ do
          when (optWriteInterface opts') $ do
              files <- nubSort . concatMap transitiveModuleDeps <$> use GetDependencies inputFile
              mbIfaces <- writeIfacesAndHie (toNormalizedFilePath $ fromMaybe ifaceDir $ optIfaceDir opts') files
              void $ liftIO $ mbErr "ERROR: Compilation failed." mbIfaces
          mbDalf <- getDalf inputFile
          dalf <- liftIO $ mbErr "ERROR: Compilation failed." mbDalf
          liftIO $ write dalf
  where
    write bs
      | outputFile == "-" = putStrLn $ render Colored $ DA.Pretty.pretty bs
      | otherwise = do
        createDirectoryIfMissing True $ takeDirectory outputFile
        B.writeFile outputFile $ Archive.encodeArchive bs

execLint :: FilePath -> Options -> Command
execLint inputFile opts =
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
  where
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
    Right $ PackageConfigFields name main exposedModules version dependencies sdkVersion

-- | We assume that this is only called within `withProjectRoot`.
withPackageConfig :: (PackageConfigFields -> IO a) -> IO a
withPackageConfig f = do
    project <- readProjectConfig $ ProjectPath "."
    case parseProjectConfig project of
        Left err -> throwIO err
        Right pkgConfig -> f pkgConfig

-- | If we're in a daml project, read the daml.yaml field and create the project local package
-- database. Otherwise do nothing.
execInit :: LF.Version -> ProjectOpts -> IO ()
execInit lfVersion projectOpts =
    withProjectRoot' projectOpts $ \_relativize ->
        initPackageDb
            lfVersion
            (InitPkgDb True)
            (AllowDifferentSdkVersions False)

initPackageDb :: LF.Version -> InitPkgDb -> AllowDifferentSdkVersions -> IO ()
initPackageDb lfVersion (InitPkgDb shouldInit) allowDiffSdkVersions =
    when shouldInit $ do
        isProject <- doesFileExist projectConfigName
        when isProject $ do
          project <- readProjectConfig $ ProjectPath "."
          case parseProjectConfig project of
              Left err -> throwIO err
              Right PackageConfigFields {..} -> do
                  createProjectPackageDb allowDiffSdkVersions lfVersion pDependencies

newtype AllowDifferentSdkVersions = AllowDifferentSdkVersions Bool

-- | Create the project package database containing the given dar packages.
createProjectPackageDb ::
       AllowDifferentSdkVersions -> LF.Version -> [FilePath] -> IO ()
createProjectPackageDb (AllowDifferentSdkVersions allowDiffSdkVersions) lfVersion fps = do
    let dbPath = projectPackageDatabase </> lfVersionString lfVersion
    createDirectoryIfMissing True $ dbPath </> "package.conf.d"
    let fps0 = filter (`notElem` basePackages) fps
    sdkVersions <-
        forM fps0 $ \fp -> do
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
            forM_ dalfs $ \dalf -> do
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
            pure sdkVersion
    let uniqSdkVersions = nubSort sdkVersions
    -- if there are no package dependencies, sdkVersions will be empty
    unless (length uniqSdkVersions <= 1 || allowDiffSdkVersions) $
        fail $
        "Package dependencies from different SDK versions: " ++
        intercalate ", " uniqSdkVersions
    ghcPkgPath <-
        if isWindows
          then locateRunfiles "rules_haskell_ghc_windows_amd64/bin"
          else locateRunfiles "ghc_nix/lib/ghc-8.6.5/bin"
    callProcess
        (ghcPkgPath </> exe "ghc-pkg")
        [ "recache"
        -- ghc-pkg insists on using a global package db and will try
        -- to find one automatically if we don’t specify it here.
        , "--global-package-db=" ++ (dbPath </> "package.conf.d")
        , "--expand-pkgroot"
        ]
  where
    write fp bs = createDirectoryIfMissing True (takeDirectory fp) >> BSL.writeFile fp bs


-- | Fail with an exit failure and errror message when Nothing is returned.
mbErr :: String -> Maybe a -> IO a
mbErr err = maybe (hPutStrLn stderr err >> exitFailure) pure

execBuild :: ProjectOpts -> Options -> Maybe FilePath -> InitPkgDb -> IO ()
execBuild projectOpts options mbOutFile initPkgDb = withProjectRoot' projectOpts $ \_relativize -> do
    initPackageDb (optDamlLfVersion options) initPkgDb (AllowDifferentSdkVersions False)
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
execClean :: ProjectOpts -> IO ()
execClean projectOpts = do
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
            -> IO ()
execPackage projectOpts filePath opts mbOutFile dalfInput = withProjectRoot' projectOpts $ \relativize -> do
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
  where
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
execInspect inFile outFile jsonOutput lvl = do
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
execInspectDar inFile = do
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
    -> Options
    -> FilePath
    -> FilePath
    -> Maybe FilePath
    -> Command
execMigrate projectOpts opts0 inFile1_ inFile2_ mbDir = do
    opts <- mkOptions opts0
    inFile1 <- makeAbsolute inFile1_
    inFile2 <- makeAbsolute inFile2_
    loggerH <- getLogger opts "migrate"
    withProjectRoot' projectOpts $ \_relativize
     -> do
        -- initialise the package database
        initPackageDb (optDamlLfVersion opts) (InitPkgDb True) (AllowDifferentSdkVersions True)
        -- for all contained dalfs, generate source, typecheck and generate interface files and
        -- overwrite the existing ones.
        dbPath <- makeAbsolute $
                projectPackageDatabase </>
                lfVersionString (optDamlLfVersion opts)
        (diags, pkgMap) <- generatePackageMap [dbPath]
        unless (null diags) $ Logger.logWarning loggerH $ showDiagnostics diags
        let pkgMap0 = MS.map dalfPackageId pkgMap
        let genSrcs =
                [ ( unitId
                  , generateSrcPkgFromLf (dalfPackageId dalfPkg) pkgMap0 dalf)
                | (unitId, dalfPkg) <- MS.toList pkgMap
                , LF.ExternalPackage _ dalf <- [dalfPackagePkg dalfPkg]
                ]
        -- order the packages in topological order
        packageState <-
            generatePackageState (dbPath : optPackageDbs opts) False []
        let (depGraph, vertexToNode, _keyToVertex) =
                graphFromEdges $ do
                    (uid, src) <- genSrcs
                    let iuid = toInstalledUnitId uid
                    Just pkgInfo <-
                        [ lookupInstalledPackage
                              (fakeDynFlags
                                   {pkgState = pdfPkgState packageState})
                              iuid
                        ]
                    pure (src, iuid, depends pkgInfo)
        let unitIdsTopoSorted = reverse $ topSort depGraph
        projectPkgDb <- makeAbsolute projectPackageDatabase
        forM_ unitIdsTopoSorted $ \vertex -> do
            let (src, iuid, _) = vertexToNode vertex
            let iuidString = installedUnitIdString iuid
            let workDir = genDir </> iuidString
            createDirectoryIfMissing True workDir
            -- we change the working dir so that we get correct file paths for the interface files.
            withCurrentDirectory workDir $
             -- typecheck and generate interface files
             do
                forM_ src $ \(fp, content) -> do
                    let path = fromNormalizedFilePath fp
                    createDirectoryIfMissing True $ takeDirectory path
                    writeFile path content
                opts' <-
                    mkOptions $
                    opts
                        { optWriteInterface = True
                        , optPackageDbs = [projectPkgDb]
                        , optIfaceDir = Just (dbPath </> installedUnitIdString iuid)
                        , optIsGenerated = True
                        , optDflagCheck = False
                        , optMbPackageName = Just $ installedUnitIdString iuid
                        }
                withDamlIdeState opts' loggerH diagnosticsLogger $ \ide ->
                    forM_ src $ \(fp, _content) -> do
                        mbCore <- runAction ide $ getGhcCore fp
                        when (isNothing mbCore) $
                            fail $
                            "Compilation of generated source for " <>
                            installedUnitIdString iuid <>
                            " failed."
        -- get the package name and the lf-package
        [(pkgName1, pkgId1, lfPkg1), (pkgName2, pkgId2, lfPkg2)] <-
            forM [inFile1, inFile2] $ \inFile -> do
                bytes <- B.readFile inFile
                let pkgName = takeBaseName inFile
                let dar = ZipArchive.toArchive $ BSL.fromStrict bytes
                -- get the main pkg
                dalfManifest <- either fail pure $ readDalfManifest dar
                mainDalfEntry <- getEntry (mainDalfPath dalfManifest) dar
                (mainPkgId, mainLfPkg) <- decode $ BSL.toStrict $ ZipArchive.fromEntry mainDalfEntry
                pure (pkgName, mainPkgId, mainLfPkg)
        -- generate upgrade modules and instances modules
        let eqModNames =
                (NM.names $ LF.packageModules lfPkg1) `intersect`
                (NM.names $ LF.packageModules lfPkg2)
        let eqModNamesStr = map (T.unpack . LF.moduleNameString) eqModNames
        let buildCmd escape =
                    "daml build --init-package-db=no" <> " --package " <>
                    escape (show (pkgName1, [(m, m ++ "A") | m <- eqModNamesStr])) <>
                    " --package " <>
                    escape (show (pkgName2, [(m, m ++ "B") | m <- eqModNamesStr])) <>
                    " --ghc-option -Wno-unrecognised-pragmas"
        forM_ eqModNames $ \m@(LF.ModuleName modName) -> do
            [genSrc1, genSrc2] <-
                forM [(pkgId1, lfPkg1), (pkgId2, lfPkg2)] $ \(pkgId, pkg) -> do
                    generateSrcFromLf (Qualify False) pkgId pkgMap0 <$> getModule m pkg
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
                escapeUnix arg = "'" <> arg <> "'"
                escapeWindows arg = T.unpack $ "\"" <> T.replace "\"" "\\\"" (T.pack arg) <> "\""
            forM_
                [ (upgradeModPath, generatedUpgradeMod)
                , (instancesModPath1, generatedInstancesMod1)
                , (instancesModPath2, generatedInstancesMod2)
                , ("build.sh", "#!/bin/sh\n" ++ buildCmd escapeUnix)
                , ("build.cmd", buildCmd escapeWindows)
                ] $ \(path, mod) -> do
                createDirectoryIfMissing True $ takeDirectory path
                writeFile path mod
#ifndef mingw32_HOST_OS
        setFileMode "build.sh" $ stdFileMode `unionFileModes` ownerExecuteMode
#endif

        putStrLn "Generation of migration project complete."
  where
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
execMergeDars :: FilePath -> FilePath -> Maybe FilePath -> IO ()
execMergeDars darFp1 darFp2 mbOutFp = do
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
  where
    mergeManifests dar1 dar2 = do
        manifest1 <- either fail pure $ readDalfManifest dar1
        manifest2 <- either fail pure $ readDalfManifest dar2
        let mergedDalfs = BSC.intercalate ", " $ map BSUTF8.fromString $ nubSort $ dalfPaths manifest1 ++ dalfPaths manifest2
        attrs1 <- either fail pure $ readManifest dar1
        attrs1 <- pure $ map (\(k, v) -> if k == "Dalfs" then (k, mergedDalfs) else (k, v)) attrs1
        pure $ ZipArchive.toEntry manifestPath 0 $ BSLC.unlines $
            map (\(k, v) -> breakAt72Bytes $ BSL.fromStrict $ k <> ": " <> v) attrs1

execDocTest :: Options -> [FilePath] -> IO ()
execDocTest opts files = do
    let files' = map toNormalizedFilePath files
    logger <- getLogger opts "doctest"
    -- We don’t add a logger here since we will otherwise emit logging messages twice.
    importPaths <-
        withDamlIdeState opts { optScenarioService = EnableScenarioService False }
            logger (const $ pure ()) $ \ideState -> runAction ideState $ do
        pmS <- catMaybes <$> uses GetParsedModule files'
        -- This is horrible but we do not have a way to change the import paths in a running
        -- IdeState at the moment.
        pure $ nubOrd $ mapMaybe moduleImportPaths pmS
    opts <- mkOptions opts { optImportPath = importPaths <> optImportPath opts, optHaddock = Haddock True }
    withDamlIdeState opts logger diagnosticsLogger $ \ideState ->
        docTest ideState files'

--------------------------------------------------------------------------------
-- main
--------------------------------------------------------------------------------

optDebugLog :: Parser Bool
optDebugLog = switch $ help "Enable debug output" <> long "debug"

optPackageName :: Parser (Maybe String)
optPackageName = optional $ strOption $
       metavar "PACKAGE-NAME"
    <> help "create package artifacts for the given package name"
    <> long "package-name"

-- | Parametrized by the type of pkgname parser since we want that to be different for
-- "package".
optionsParser :: Int -> EnableScenarioService -> Parser (Maybe String) -> Parser Options
optionsParser numProcessors enableScenarioService parsePkgName = Options
    <$> optImportPath
    <*> optPackageDir
    <*> parsePkgName
    <*> optWriteIface
    <*> pure Nothing
    <*> optHideAllPackages
    <*> many optPackage
    <*> shakeProfilingOpt
    <*> optShakeThreads
    <*> lfVersionOpt
    <*> optDebugLog
    <*> optGhcCustomOptions
    <*> pure enableScenarioService
    <*> pure (optScenarioValidation $ defaultOptions Nothing)
    <*> dlintUsageOpt
    <*> pure False
    <*> optNoDflagCheck
    <*> pure False
    <*> pure (Haddock False)
  where
    optImportPath :: Parser [FilePath]
    optImportPath =
        many $
        strOption $
        metavar "INCLUDE-PATH" <>
        help "Path to an additional source directory to be included" <>
        long "include"

    optPackageDir :: Parser [FilePath]
    optPackageDir = many $ strOption $ metavar "LOC-OF-PACKAGE-DB"
                      <> help "use package database in the given location"
                      <> long "package-db"

    optWriteIface :: Parser Bool
    optWriteIface =
        switch $
          help "Whether to write interface files during type checking, required for building a package such as daml-prim" <>
          long "write-iface"

    optPackage :: Parser (String, [(String, String)])
    optPackage =
      option auto $
      metavar "PACKAGE" <>
      help "explicit import of a package with optional renaming of modules" <>
      long "package" <>
      internal

    optHideAllPackages :: Parser Bool
    optHideAllPackages =
      switch $
      help "hide all packages, use -package for explicit import" <>
      long "hide-all-packages" <>
      internal

    -- optparse-applicative does not provide a nice way
    -- to make the argument for -j optional, see
    -- https://github.com/pcapriotti/optparse-applicative/issues/243
    optShakeThreads :: Parser Int
    optShakeThreads =
        flag' numProcessors
          (short 'j' <>
           internal) <|>
        option auto
          (long "jobs" <>
           metavar "THREADS" <>
           help threadsHelp <>
           value 1)
    threadsHelp =
        unlines
            [ "The number of threads to run in parallel."
            , "When -j is not passed, 1 thread is used."
            , "If -j is passed, the number of threads defaults to the number of processors."
            , "Use --jobs=N to explicitely set the number of threads to N."
            , "Note that the output is not deterministic for > 1 job."
            ]


    optNoDflagCheck :: Parser Bool
    optNoDflagCheck =
      flag True False $
      help "Dont check generated GHC DynFlags for errors." <>
      long "no-dflags-check" <>
      internal



optGhcCustomOptions :: Parser [String]
optGhcCustomOptions =
    fmap concat $ many $
    option (stringsSepBy ' ') $
    long "ghc-option" <>
    metavar "OPTION" <>
    help "Options to pass to the underlying GHC"

shakeProfilingOpt :: Parser (Maybe FilePath)
shakeProfilingOpt = optional $ strOption $
       metavar "PROFILING-REPORT"
    <> help "Directory for Shake profiling reports"
    <> long "shake-profiling"

options :: Int -> Parser Command
options numProcessors =
    subparser
      (  cmdIde
      <> cmdLicense
      -- cmdPackage can go away once we kill the old assistant.
      <> cmdPackage numProcessors
      <> cmdBuild numProcessors
      <> cmdTest numProcessors
      <> cmdDamlDoc
      <> cmdVisual
      <> cmdInspectDar
      <> cmdDocTest numProcessors
      <> cmdLint numProcessors
      )
    <|> subparser
      (internal -- internal commands
        <> cmdInspect
        <> cmdVisual
        <> cmdMigrate numProcessors
        <> cmdMergeDars
        <> cmdInit
        <> cmdCompile numProcessors
        <> cmdClean
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

main :: IO ()
main = do
    -- We need this to ensure that logs are flushed on SIGTERM.
    installSignalHandlers
    numProcessors <- getNumProcessors
    withProgName "damlc" $ join $ execParserLax (parserInfo numProcessors)

withProjectRoot' :: ProjectOpts -> ((FilePath -> IO FilePath) -> IO a) -> IO a
withProjectRoot' ProjectOpts{..} act =
    withProjectRoot projectRoot projectCheck (const act)
