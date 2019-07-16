-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ApplicativeDo       #-}

-- | Main entry-point of the DAML compiler
module DA.Cli.Damlc (main) where

import Codec.Archive.Zip
import Control.Exception
import Control.Exception.Safe (catchIO)
import Control.Monad.Except
import Control.Monad.Extra (whenM)
import qualified "cryptonite" Crypto.Hash as Crypto
import DA.Bazel.Runfiles
import DA.Cli.Args
import DA.Cli.Damlc.Base
import DA.Cli.Damlc.BuildInfo
import DA.Cli.Damlc.Command.Damldoc (cmdDamlDoc)
import DA.Cli.Damlc.IdeState
import DA.Cli.Damlc.Test
import DA.Cli.Visual
import DA.Daml.Compiler.Dar
import DA.Daml.Compiler.DocTest
import DA.Daml.Compiler.Scenario
import DA.Daml.Compiler.Upgrade
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Proto3.Archive as Archive
import DA.Daml.LanguageServer
import DA.Daml.Options
import DA.Daml.Options.Types
import qualified DA.Pretty
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.GCP as Logger.GCP
import qualified DA.Service.Logger.Impl.IO as Logger.IO
import DA.Signals
import DAML.Project.Config
import DAML.Project.Consts
import DAML.Project.Types (ConfigError, ProjectPath(..))
import qualified Da.DamlLf as PLF
import qualified Data.Aeson.Encode.Pretty as Aeson.Pretty
import Data.ByteArray.Encoding (Base(Base16), convertToBase)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL
import Data.FileEmbed (embedFile)
import Data.Graph
import qualified Data.Set as Set
import Data.List.Extra
import qualified Data.List.Split as Split
import qualified Data.Map.Strict as MS
import Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Development.IDE.Core.API
import Development.IDE.Core.Service (runAction)
import Development.IDE.Core.Shake
import Development.IDE.Core.Rules
import Development.IDE.Core.Rules.Daml (getDalf)
import Development.IDE.Core.RuleTypes.Daml (DalfPackage(..), GetParsedModule(..))
import Development.IDE.GHC.Util (fakeDynFlags, moduleImportPaths)
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import "ghc-lib-parser" DynFlags
import GHC.Conc
import "ghc-lib-parser" Module
import qualified Network.Socket as NS
import Options.Applicative.Extended
import "ghc-lib-parser" Packages
import qualified Proto3.Suite as PS
import qualified Proto3.Suite.JSONPB as Proto.JSONPB
import Safe
import System.Directory
import System.Environment
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process(callCommand)
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
    cmd = execIde <$> telemetryOpt <*> debugOpt <*> enableScenarioOpt

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
            Right PackageConfigFields {..} -> execTest [toNormalizedFilePath pMain] color mbJUnitOutput cliOptions
runTestsInProjectOrFiles projectOpts (Just inFiles) color mbJUnitOutput cliOptions =
    withProjectRoot' projectOpts $ \relativize -> do
        inFiles' <- mapM (fmap toNormalizedFilePath . relativize) inFiles
        execTest inFiles' color mbJUnitOutput cliOptions

cmdInspect :: Mod CommandFields Command
cmdInspect =
    command "inspect" $ info (helper <*> cmd)
      $ progDesc "Inspect a DAML-LF Archive."
    <> fullDesc
  where
    jsonOpt = switch $ long "json" <> help "Output the raw Protocol Buffer structures as JSON"
    cmd = execInspect <$> inputFileOpt <*> outputFileOpt <*> jsonOpt

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
    cmd = execInit <$> lfVersionOpt <*> projectOpts "daml damlc init" <*> pure (InitPkgDb True)

cmdPackage :: Int -> Mod CommandFields Command
cmdPackage numProcessors =
    command "package" $ info (helper <*> cmd) $
       progDesc "Compile the DAML program into a DAML Archive (DAR)"
    <> fullDesc
  where
    dumpPom = fmap DumpPom $ switch $ help "Write out pom and sha256 files" <> long "dump-pom"
    cmd = execPackage
        <$> projectOpts "daml damlc package"
        <*> inputFileOpt
        <*> optionsParser numProcessors (EnableScenarioService False) (Just <$> packageNameOpt)
        <*> optionalOutputFileOpt
        <*> dumpPom
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
        -> Command
execIde telemetry (Debug debug) enableScenarioService = NS.withSocketsDo $ do
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
    opts <- defaultOptionsIO Nothing
    opts <- pure $ opts
        { optScenarioService = enableScenarioService
        , optScenarioValidation = ScenarioValidationLight
        , optThreads = 0
        }
    scenarioServiceConfig <- readScenarioServiceConfig
    withLogger $ \loggerH ->
        withScenarioService' enableScenarioService loggerH scenarioServiceConfig $ \mbScenarioService -> do
            -- TODO we should allow different LF versions in the IDE.
            execInit LF.versionDefault (ProjectOpts Nothing (ProjectCheck "" False)) (InitPkgDb True)
            sdkVersion <- getSdkVersion `catchIO` const (pure "Unknown (not started via the assistant)")
            Logger.logInfo loggerH (T.pack $ "SDK version: " <> sdkVersion)
            runLanguageServer
                (getDamlIdeState opts mbScenarioService loggerH)

execCompile :: FilePath -> FilePath -> Options -> Command
execCompile inputFile outputFile opts = withProjectRoot' (ProjectOpts Nothing (ProjectCheck "" False)) $ \relativize -> do
    loggerH <- getLogger opts "compile"
    inputFile <- toNormalizedFilePath <$> relativize inputFile
    opts' <- mkOptions opts
    withDamlIdeState opts' loggerH diagnosticsLogger $ \ide -> do
        setFilesOfInterest ide (Set.singleton inputFile)
        res <- runAction ide $ getDalf inputFile
        case res of
            Nothing -> do
                hPutStrLn stderr "ERROR: Compilation failed."
                exitFailure
            Just dalf -> write dalf
  where
    write bs
      | outputFile == "-" = putStrLn $ render Colored $ DA.Pretty.pretty bs
      | otherwise = do
        createDirectoryIfMissing True $ takeDirectory outputFile
        B.writeFile outputFile $ Archive.encodeArchive bs

newtype DumpPom = DumpPom{unDumpPom :: Bool}

-- | daml.yaml config fields specific to packaging.
data PackageConfigFields = PackageConfigFields
    { pName :: String
    , pMain :: String
    , pExposedModules :: Maybe [String]
    , pVersion :: String
    , pDependencies :: [String]
    , pSdkVersion :: String
    }

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
execInit :: LF.Version -> ProjectOpts -> InitPkgDb -> IO ()
execInit lfVersion projectOpts (InitPkgDb shouldInit) =
    when shouldInit $
    withProjectRoot' projectOpts $ \_relativize -> do
        isProject <- doesFileExist projectConfigName
        when isProject $ do
          project <- readProjectConfig $ ProjectPath "."
          case parseProjectConfig project of
              Left err -> throwIO err
              Right PackageConfigFields {..} -> do
                  createProjectPackageDb lfVersion pDependencies

-- | Create the project package database containing the given dar packages.
createProjectPackageDb :: LF.Version -> [FilePath] -> IO ()
createProjectPackageDb lfVersion fps = do
    let dbPath = projectPackageDatabase </> lfVersionString lfVersion
    createDirectoryIfMissing True $ dbPath </> "package.conf.d"
    let fps0 = filter (`notElem` basePackages) fps
    forM_ fps0 $ \fp -> do
        bs <- BSL.readFile fp
        let archive = toArchive bs
        let confFiles =
                [ e
                | e <- zEntries archive
                , ".conf" `isExtensionOf` eRelativePath e
                ]
        let dalfs =
                [ e
                | e <- zEntries archive
                , ".dalf" `isExtensionOf` eRelativePath e
                ]
        let srcs =
                [ e
                | e <- zEntries archive
                , takeExtension (eRelativePath e) `elem` [".daml", ".hie", ".hi"]
                ]
        forM_ dalfs $ \dalf ->
            BSL.writeFile (dbPath </> eRelativePath dalf) (fromEntry dalf)
        forM_ confFiles $ \conf ->
            BSL.writeFile
                (dbPath </> "package.conf.d" </> (takeFileName $ eRelativePath conf))
                (fromEntry conf)
        forM_ srcs $ \src -> do
            let path = dbPath </> eRelativePath src
            createDirectoryIfMissing True $ takeDirectory path
            BSL.writeFile path (fromEntry src)
    ghcPkgPath <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "ghc-pkg")
    callCommand $
        unwords
            [ ghcPkgPath </> "ghc-pkg"
            , "recache"
            -- ghc-pkg insists on using a global package db and will trie
            -- to find one automatically if we don’t specify it here.
            , "--global-package-db=" ++ (dbPath </> "package.conf.d")
            , "--expand-pkgroot"
            ]

execBuild :: ProjectOpts -> Options -> Maybe FilePath -> InitPkgDb -> IO ()
execBuild projectOpts options mbOutFile initPkgDb = withProjectRoot' projectOpts $ \_relativize -> do
    execInit (optDamlLfVersion options) projectOpts initPkgDb
    withPackageConfig $ \PackageConfigFields {..} -> do
        putStrLn $ "Compiling " <> pMain <> " to a DAR."
        options' <- mkOptions options
        let opts = options'
                    { optMbPackageName = Just pName
                    , optWriteInterface = True
                    }
        loggerH <- getLogger opts "package"
        let confFile =
                mkConfFile
                    pName
                    pVersion
                    pExposedModules
                    pDependencies
        withDamlIdeState opts loggerH diagnosticsLogger $ \compilerH -> do
            mbDar <-
                buildDar
                    compilerH
                    (toNormalizedFilePath pMain)
                    pExposedModules
                    pName
                    pSdkVersion
                    (\pkg -> [confFile pkg])
                    (FromDalf False)
            case mbDar of
                Nothing -> do
                    hPutStrLn stderr "ERROR: Creation of DAR file failed."
                    exitFailure
                Just dar -> do
                    let fp = targetFilePath pName
                    createDirectoryIfMissing True $ takeDirectory fp
                    B.writeFile fp dar
                    putStrLn $ "Created " <> fp <> "."
    where
        -- The default output filename is based on Maven coordinates if
        -- the package name is specified via them, otherwise we use the
        -- name.
        defaultDarFile name =
            case Split.splitOn ":" name of
                [_g, a, v] -> a <> "-" <> v <> ".dar"
                _otherwise -> name <> ".dar"
        targetFilePath name = fromMaybe (distDir </> defaultDarFile name) mbOutFile
        mkConfFile ::
               String
            -> String
            -> Maybe [String]
            -> [FilePath]
            -> LF.Package
            -> (String, B.ByteString)
        mkConfFile name version exposedMods deps pkg = (confName, bs)
          where
            confName = name ++ ".conf"
            bs =
                BSC.pack $
                unlines
                    [ "name: " ++ name
                    , "id: " ++ name
                    , "key: " ++ name
                    , "version: " ++ version
                    , "exposed: True"
                    , "exposed-modules: " ++ unwords (fromMaybe (map T.unpack $ LF.packageModuleNames pkg) exposedMods)
                    , "import-dirs: ${pkgroot}" </> name
                    , "library-dirs: ${pkgroot}" </> name
                    , "data-dir: ${pkgroot}" </> name
                    , "depends: " ++
                      unwords [dropExtension $ takeFileName dep | dep <- deps]
                    ]

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
            -> DumpPom
            -> FromDalf
            -> IO ()
execPackage projectOpts filePath opts mbOutFile dumpPom dalfInput = withProjectRoot' projectOpts $ \relativize -> do
    loggerH <- getLogger opts "package"
    filePath <- relativize filePath
    opts' <- mkOptions opts
    withDamlIdeState opts' loggerH diagnosticsLogger $ \ide -> do
        let path = toNormalizedFilePath filePath
        -- We leave the sdk version blank and the list of exposed modules empty.
        -- This command is being removed anytime now and not present
        -- in the new daml assistant.
        mbDar <- buildDar ide path Nothing name "" (const []) dalfInput
        case mbDar of
          Nothing -> do
              hPutStrLn stderr "ERROR: Creation of DAR file failed."
              exitFailure
          Just dar -> do
            createDirectoryIfMissing True $ takeDirectory targetFilePath
            B.writeFile targetFilePath dar
            putStrLn $ "Created " <> targetFilePath <> "."
            when (unDumpPom dumpPom) $ createPomAndSHA256 dar
  where
    -- This is somewhat ugly but our CLI parser guarantees that this will always be present.
    -- We could parametrize CliOptions by whether the package name is optional
    -- but I don’t think that is worth the complexity of carrying around a type parameter.
    name = fromMaybe (error "Internal error: Package name was not present") (optMbPackageName opts)
    pomContents groupId artifactId version =
        TE.encodeUtf8 $ T.pack $
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
          \<project xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd\"\
          \ xmlns=\"http://maven.apache.org/POM/4.0.0\"\
          \ xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n\
          \  <modelVersion>4.0.0</modelVersion>\n\
          \  <groupId>" <> groupId <> "</groupId>\n\
          \  <artifactId>" <> artifactId <> "</artifactId>\n\
          \  <version>" <> version <> "</version>\n\
          \  <name>" <> artifactId <> "</name>\n\
          \  <type>dar</type>\n\
          \  <description>A Digital Asset DAML package</description>\n\
          \</project>"

    -- The default output filename is based on Maven coordinates if
    -- the package name is specified via them, otherwise we use the
    -- name.
    defaultDarFile =
      case Split.splitOn ":" name of
        [_g, a, v] -> a <> "-" <> v <> ".dar"
        _otherwise -> name <> ".dar"

    targetFilePath = fromMaybe defaultDarFile mbOutFile
    targetDir = takeDirectory targetFilePath

    createPomAndSHA256 darContent = do
      case Split.splitOn ":" name of
          [g, a, v] -> do
            let basePath = targetDir </> a <> "-" <> v
            let pomPath = basePath <.> "pom"
            let pomContent = pomContents g a v
            let writeAndAnnounce path content = do
                  B.writeFile path content
                  putStrLn $ "Created " <> path <> "."

            writeAndAnnounce pomPath pomContent
            writeAndAnnounce (basePath <.> "dar" <.> "sha256")
              (convertToBase Base16 $ Crypto.hashWith Crypto.SHA256 darContent)
            writeAndAnnounce (basePath <.> "pom" <.> "sha256")
              (convertToBase Base16 $ Crypto.hashWith Crypto.SHA256 pomContent)
          _ -> do
            putErrLn $ "ERROR: Not creating pom file as package name '" <> name <> "' is not a valid Maven coordinate (expected '<groupId>:<artifactId>:<version>')"
            exitFailure

    putErrLn = hPutStrLn stderr

execInspect :: FilePath -> FilePath -> Bool -> Command
execInspect inFile outFile jsonOutput = do
    bytes <- B.readFile inFile
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
          , DA.Pretty.nest 2 (DA.Pretty.pretty lfPkg)
          ]

errorOnLeft :: Show a => String -> Either a b -> IO b
errorOnLeft desc = \case
  Left err -> ioError $ userError $ unlines [ desc, show err ]
  Right x  -> return x

execInspectDar :: FilePath -> Command
execInspectDar inFile = do
    bytes <- B.readFile inFile

    putStrLn "DAR archive contains the following files: \n"
    let dar = toArchive $ BSL.fromStrict bytes
    let files = [eRelativePath e | e <- zEntries dar]
    mapM_ putStrLn files

    putStrLn "\nDAR archive contains the following packages: \n"
    let dalfEntries =
            [e | e <- zEntries dar, ".dalf" `isExtensionOf` eRelativePath e]
    forM_ dalfEntries $ \dalfEntry -> do
        let dalf = BSL.toStrict $ fromEntry dalfEntry
        (pkgId, _lfPkg) <-
            errorOnLeft
                ("Cannot decode package " <> eRelativePath dalfEntry)
                (Archive.decodeArchive dalf)
        putStrLn $
            (dropExtension $ takeFileName $ eRelativePath dalfEntry) <> " " <>
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
    -- initialise the package database
    execInit (optDamlLfVersion opts) projectOpts (InitPkgDb True)
    withProjectRoot' projectOpts $ \_relativize
     -> do
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
        createDirectoryIfMissing True genDir
        projectPkgDb <- makeAbsolute projectPackageDatabase
        forM_ unitIdsTopoSorted $ \vertex -> withCurrentDirectory genDir $ do
            let (src, iuid, _) = vertexToNode vertex
            unless
                -- TODO (drsk) remove this filter
                (iuid `elem`
                 map stringToInstalledUnitId ["daml-prim", "daml-stdlib"]) $ do
                  -- typecheck and generate interface files
                  forM_ src $ \(fp, content) -> do
                      let path = fromNormalizedFilePath fp
                      createDirectoryIfMissing True $ takeDirectory path
                      writeFile path content
                  opts' <-
                      mkOptions $
                      opts
                          { optWriteInterface = True
                          , optPackageDbs = [projectPkgDb]
                          , optIfaceDir =
                                Just
                                    (dbPath </> installedUnitIdString iuid)
                          , optIsGenerated = True
                          , optMbPackageName =
                                Just $ installedUnitIdString iuid
                          }
                  withDamlIdeState opts' loggerH diagnosticsLogger $ \ide ->
                      forM_ src $ \(fp, _content) -> do
                          mbCore <-
                              runAction ide $
                              getGhcCore fp
                          case mbCore of
                              Nothing ->
                                  ioError $
                                  userError $
                                  "Compilation of generated source for " <>
                                  installedUnitIdString iuid <>
                                  " failed."
                              Just _core -> pure ()
        -- get the package name and the lf-package
        [(pkgName1, pkgId1, lfPkg1), (pkgName2, pkgId2, lfPkg2)] <-
            forM [inFile1, inFile2] $ \inFile -> do
                bytes <- B.readFile inFile
                let pkgName = takeBaseName inFile
                let dar = toArchive $ BSL.fromStrict bytes
                -- get the main pkg
                manifest <- getEntry "META-INF/MANIFEST.MF" dar
                let mainDalfPath =
                        headNote
                            "Missing Main-Dalf entry in META-INF/MANIFEST.MF"
                            [ main
                            | l <- lines $ BSC.unpack $ BSL.toStrict $ fromEntry manifest
                            , Just main <- [stripPrefix "Main-Dalf: " l]
                            ]
                mainDalfEntry <- getEntry mainDalfPath dar
                (mainPkgId, mainLfPkg) <- decode $ BSL.toStrict $ fromEntry mainDalfEntry
                pure (pkgName, mainPkgId, mainLfPkg)
        -- generate upgrade modules and instances modules
        let eqModNames =
                (NM.names $ LF.packageModules lfPkg1) `intersect`
                (NM.names $ LF.packageModules lfPkg2)
        forM_ eqModNames $ \m@(LF.ModuleName modName) -> do
            [genSrc1, genSrc2] <-
                forM [(pkgId1, lfPkg1), (pkgId2, lfPkg2)] $ \(pkgId, pkg) -> do
                    generateSrcFromLf pkgId pkgMap0 <$> getModule m pkg
            let upgradeModPath =
                    (joinPath $ fromMaybe "" mbDir : map T.unpack modName) <>
                    ".daml"
            let instancesModPath1 =
                    replaceBaseName upgradeModPath $
                    takeBaseName upgradeModPath <> "InstancesA"
            let instancesModPath2 =
                    replaceBaseName upgradeModPath $
                    takeBaseName upgradeModPath <> "InstancesB"
            templateNames <-
                map (T.unpack . T.intercalate "." . LF.unTypeConName) .
                NM.names . LF.moduleTemplates <$>
                getModule m lfPkg1
            let generatedUpgradeMod =
                    generateUpgradeModule
                        templateNames
                        (T.unpack $ LF.moduleNameString m)
                        pkgName1
                        pkgName2
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
  where
    decode dalf =
        errorOnLeft
            "Cannot decode daml-lf archive"
            (Archive.decodeArchive dalf)
    getEntry fp dar =
        maybe (ioError $ userError $ "Package does not contain " <> fp) pure $
        findEntryByPath fp dar
    getModule modName pkg =
        maybe
            (ioError $
             userError $
             T.unpack $ "Can't find module" <> LF.moduleNameString modName)
            pure $
        NM.lookup modName $ LF.packageModules pkg


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
    opts <- mkOptions opts { optImportPath = importPaths <> optImportPath opts}
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
    <*> optShakeProfiling
    <*> optShakeThreads
    <*> lfVersionOpt
    <*> optDebugLog
    <*> (concat <$> many optGhcCustomOptions)
    <*> pure enableScenarioService
    <*> pure (optScenarioValidation $ defaultOptions Nothing)
    <*> hlintUsageOpt
    <*> pure False
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

    optShakeProfiling :: Parser (Maybe FilePath)
    optShakeProfiling = optional $ strOption $
           metavar "PROFILING-REPORT"
        <> help "path to Shake profiling report"
        <> long "shake-profiling"

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

    optGhcCustomOptions :: Parser [String]
    optGhcCustomOptions =
        option (stringsSepBy ' ') $
        long "ghc-option" <>
        metavar "OPTION" <>
        help "Options to pass to the underlying GHC"

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
      )
    <|> subparser
      (internal -- internal commands
        <> cmdInspect
        <> cmdVisual
        <> cmdMigrate numProcessors
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
