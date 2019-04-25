-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ApplicativeDo       #-}

-- | Main entry-point of the DAML compiler
module DA.Cli.Damlc (main) where

import Control.Monad.Except
import qualified Control.Monad.Managed             as Managed
import DA.Cli.Damlc.Base
import Control.Exception (throwIO)
import qualified "cryptonite" Crypto.Hash as Crypto
import Codec.Archive.Zip
import qualified Da.DamlLf as PLF
import           DA.Cli.Damlc.BuildInfo
import           DA.Cli.Damlc.Command.Damldoc      (cmdDamlDoc)
import           DA.Cli.Args
import qualified DA.Pretty
import           DA.Service.Daml.Compiler.Impl.Handle as Compiler
import DA.Daml.GHC.Compiler.Options (projectPackageDatabase, basePackages)
import qualified DA.Service.Daml.LanguageServer    as Daml.LanguageServer
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified DA.Service.Logger                 as Logger
import qualified DA.Service.Logger.Impl.IO         as Logger.IO
import qualified DA.Service.Logger.Impl.GCP        as Logger.GCP
import DAML.Project.Consts
import DAML.Project.Config
import DAML.Project.Types (ProjectPath(..), ConfigError)
import qualified Data.Aeson.Encode.Pretty as Aeson.Pretty
import Data.ByteArray.Encoding (Base (Base16), convertToBase)
import qualified Data.ByteString                   as B
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Char8 as BSC
import Data.FileEmbed (embedFile)
import Data.Functor
import qualified Data.Set as Set
import qualified Data.List.Split as Split
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Prettyprint.Doc.Syntax as Pretty
import Development.IDE.Types.Diagnostics
import GHC.Conc
import qualified Network.Socket                    as NS
import           Options.Applicative
import qualified Proto3.Suite as PS
import qualified Proto3.Suite.JSONPB as Proto.JSONPB
import System.Directory (createDirectoryIfMissing)
import System.Environment (withProgName)
import System.Exit (exitFailure)
import System.FilePath (takeDirectory, (<.>), (</>), isExtensionOf, takeFileName, dropExtension, takeBaseName)
import System.Process(callCommand)
import           System.IO                         (stderr, hPutStrLn)
import qualified Text.PrettyPrint.ANSI.Leijen      as PP
import DA.Cli.Damlc.Test

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
    cmd = execIde <$> telemetryOpt <*> debugOpt


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
        <*> optionsParser numProcessors optPackageName

cmdTest :: Int -> Mod CommandFields Command
cmdTest numProcessors =
    command "test" $ info (helper <*> cmd) $
       progDesc "Test the given DAML file by running all test declarations."
    <> fullDesc
  where
    cmd = execTest
      <$> many inputFileOpt
      <*> junitOutput
      <*> optionsParser numProcessors optPackageName
    junitOutput = optional $ strOption $
        long "junit" <>
        metavar "FILENAME" <>
        help "Filename of JUnit output file"

cmdInspect :: Mod CommandFields Command
cmdInspect =
    command "inspect" $ info (helper <*> cmd)
      $ progDesc "Inspect a DAML-LF Archive."
    <> fullDesc
  where
    jsonOpt = switch $ long "json" <> help "Output the raw Protocol Buffer structures as JSON"
    cmd = execInspect <$> inputFileOpt <*> outputFileOpt <*> jsonOpt


cmdBuild :: Int -> Mod CommandFields Command
cmdBuild numProcessors =
    command "build" $
    info (helper <*> cmd) $
    progDesc "Initialize, build and package the DAML project" <> fullDesc
  where
    cmd = execBuild numProcessors <$> optionalOutputFileOpt

cmdPackageNew :: Int -> Mod CommandFields Command
cmdPackageNew numProcessors =
    command "package-new" $
    info (helper <*> cmd) $
    progDesc "Compile the DAML project into a DAML Archive (DAR)" <> fullDesc
  where
    cmd = execPackageNew numProcessors <$> optionalOutputFileOpt

cmdInit :: Mod CommandFields Command
cmdInit =
    command "init" $
    info (helper <*> cmd) $ progDesc "Initialize a DAML project" <> fullDesc
  where
    cmd = pure execInit

cmdPackage :: Int -> Mod CommandFields Command
cmdPackage numProcessors =
    command "package" $ info (helper <*> cmd) $
       progDesc "Compile the DAML program into a DAML Archive (DAR)"
    <> fullDesc
  where
    dumpPom = fmap DumpPom $ switch $ help "Write out pom and sha256 files" <> long "dump-pom"
    cmd = execPackage
        <$> inputFileOpt
        <*> optionsParser numProcessors (Just <$> packageNameOpt)
        <*> optionalOutputFileOpt
        <*> dumpPom
        <*> optUseDalf

    optUseDalf :: Parser Compiler.UseDalf
    optUseDalf = fmap UseDalf $
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

--------------------------------------------------------------------------------
-- Execution
--------------------------------------------------------------------------------

execLicense :: Command
execLicense = B.putStr licenseData
  where
    licenseData :: B.ByteString
    licenseData = $(embedFile "daml-foundations/daml-tools/docs/daml-licenses/licenses/licensing.md")

execIde :: Telemetry
        -> Debug
        -> Command
execIde telemetry (Debug debug) = NS.withSocketsDo $ Managed.runManaged $ do
    let threshold =
            if debug
            then Logger.Debug
            -- info is used pretty extensively for debug messages in our code base so
            -- I've set the no debug threshold at warning
            else Logger.Warning
    loggerH <- Managed.liftIO $ Logger.IO.newIOLogger
      stderr
      (Just 5000)
      -- NOTE(JM): ^ Limit the message length to 5000 characters as VSCode
      -- performance will be significatly impacted by large log output.
      threshold
      "LanguageServer"
    loggerH <-
      case telemetry of
          OptedIn -> Logger.GCP.gcpLogger (>= Logger.Warning) loggerH
          OptedOut -> liftIO $ Logger.GCP.logOptOut $> loggerH
          Undecided -> pure loggerH

    opts <- liftIO $ defaultOptionsIO Nothing

    Managed.liftIO $
      Daml.LanguageServer.runLanguageServer
      (Compiler.newIdeState opts) loggerH


execCompile :: FilePath -> FilePath -> Compiler.Options -> Command
execCompile inputFile outputFile opts = withProjectRoot $ \relativize -> do
    loggerH <- getLogger opts "compile"
    inputFile <- relativize inputFile
    opts' <- Compiler.mkOptions opts
    Managed.with (Compiler.newIdeState opts' Nothing loggerH) $ \hDamlGhc -> do
        errOrDalf <- runExceptT $ Compiler.compileFile hDamlGhc inputFile
        either (reportErr "DAML-1.2 to LF compilation failed") write errOrDalf
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
    , pExposedModules :: [String]
    , pVersion :: String
    , pDependencies :: [String]
    }

-- | Parse the daml.yaml for package specific config fields.
parseProjectConfig :: ProjectConfig -> Either ConfigError PackageConfigFields
parseProjectConfig project = do
    name <- queryProjectConfigRequired ["name"] project
    main <- queryProjectConfigRequired ["source"] project
    exposedModules <-
        queryProjectConfigRequired ["exposed-modules"] project
    version <- queryProjectConfigRequired ["version"] project
    dependencies <-
        queryProjectConfigRequired ["dependencies"] project
    Right $ PackageConfigFields name main exposedModules version dependencies

-- | Package command that takes all arguments of daml.yaml.
execPackageNew :: Int -> Maybe FilePath -> IO ()
execPackageNew numProcessors mbOutFile =
    withProjectRoot $ \_relativize -> do
        project <- readProjectConfig $ ProjectPath "."
        case parseProjectConfig project of
            Left err -> throwIO err
            Right PackageConfigFields {..} -> do
                defaultOpts <- Compiler.defaultOptionsIO Nothing
                let opts =
                        defaultOpts
                            { optMbPackageName = Just pName
                            , optThreads = numProcessors
                            , optWriteInterface = True
                            }
                loggerH <- getLogger opts "package"
                let confFile =
                        mkConfFile
                            pName
                            pVersion
                            LF.versionDefault
                            pExposedModules
                            pDependencies
                Managed.with (Compiler.newIdeState opts Nothing loggerH) $ \compilerH -> do
                    darOrErr <-
                        runExceptT $
                        Compiler.buildDar
                            compilerH
                            pMain
                            pName
                            [confFile]
                            (UseDalf False)
                    case darOrErr of
                        Left errs ->
                            ioError $
                            userError $
                            unlines
                                [ "Creation of DAR file failed:"
                                , T.unpack $
                                  Pretty.renderColored $
                                  Pretty.vcat $ map prettyDiagnostic errs
                                ]
                        Right dar -> do
                            let fp = targetFilePath pName
                            createDirectoryIfMissing True $ takeDirectory fp
                            B.writeFile fp dar
                            putStrLn $ "Created " <> fp <> "."
  where
    mkConfFile ::
           String
        -> String
        -> LF.Version
        -> [String]
        -> [FilePath]
        -> (String, B.ByteString)
    mkConfFile name version lfVersion exposedMods deps = (confName, bs)
      where
        confName = name ++ ".conf"
        lfVersionStr = lfVersionString lfVersion
        bs =
            BSC.pack $
            unlines
                [ "name: " ++ name
                , "id: " ++ name
                , "key: " ++ name
                , "version: " ++ version
                , "exposed: True"
                , "exposed-modules: " ++ unwords exposedMods
                , "import-dirs: ${pkgroot}" </> lfVersionStr </> name
                , "library-dirs: ${pkgroot}" </> lfVersionStr </> name
                , "data-dir: ${pkgroot}" </> lfVersionStr </> name
                , "depends: " ++
                  unwords [dropExtension $ takeFileName dep | dep <- deps]
                ]
    -- The default output filename is based on Maven coordinates if
    -- the package name is specified via them, otherwise we use the
    -- name.
    defaultDarFile name =
        case Split.splitOn ":" name of
            [_g, a, v] -> a <> "-" <> v <> ".dar"
            _otherwise -> name <> ".dar"
    targetFilePath name = fromMaybe (defaultOutDir </> defaultDarFile name) mbOutFile

    defaultOutDir = "dist"

-- | Read the daml.yaml field and create the project local package database.
execInit :: IO ()
execInit =
    withProjectRoot $ \_relativize -> do
        project <- readProjectConfig $ ProjectPath "."
        case parseProjectConfig project of
            Left err -> throwIO err
            Right PackageConfigFields {..} -> do
                createProjectPackageDb LF.versionDefault pDependencies

-- | Create the project package database containing the given dar packages.
createProjectPackageDb :: LF.Version -> [FilePath] -> IO ()
createProjectPackageDb lfVersion fps = do
    let dbPath = projectPackageDatabase </> lfVersionString lfVersion
    createDirectoryIfMissing True dbPath
    let fps0 = filter (`notElem` basePackages) fps
    forM_ fps0 $ \fp -> do
        bs <- BSL.readFile fp
        let pkgName = takeBaseName fp
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
                , pkgName `isPrefixOf` eRelativePath e
                ]
        forM_ dalfs $ \dalf ->
            BSL.writeFile (dbPath </> eRelativePath dalf) (fromEntry dalf)
        forM_ confFiles $ \conf ->
            BSL.writeFile
                (dbPath </> (takeFileName $ eRelativePath conf))
                (fromEntry conf)
        createDirectoryIfMissing True $ dbPath </> pkgName
        forM_ srcs $ \src ->
            BSL.writeFile (dbPath </> eRelativePath src) (fromEntry src)
    sdkRoot <- getSdkPath
    callCommand $
        unwords
            [ sdkRoot </> "damlc/resources/ghc-pkg"
            , "recache"
            , "--package-db=" ++ dbPath
            , "--expand-pkgroot"
            ]

execBuild :: Int -> Maybe FilePath -> IO ()
execBuild numProcessors mbOutFile = do
  execInit
  execPackageNew numProcessors mbOutFile

lfVersionString :: LF.Version -> String
lfVersionString lfVersion =
    case lfVersion of
        LF.VDev _ -> "dev"
        _ -> DA.Pretty.renderPretty lfVersion

execPackage:: FilePath -- ^ input file
            -> Compiler.Options
            -> Maybe FilePath
            -> DumpPom
            -> Compiler.UseDalf
            -> IO ()
execPackage filePath opts mbOutFile dumpPom dalfInput = withProjectRoot $ \relativize -> do
    loggerH <- getLogger opts "package"
    filePath <- relativize filePath
    opts' <- Compiler.mkOptions opts
    Managed.with (Compiler.newIdeState opts' Nothing loggerH) $
      buildDar filePath
  where
    -- This is somewhat ugly but our CLI parser guarantees that this will always be present.
    -- We could parametrize CliOptions by whether the package name is optional
    -- but I don’t think that is worth the complexity of carrying around a type parameter.
    name = fromMaybe (error "Internal error: Package name was not present") (Compiler.optMbPackageName opts)
    buildDar path compilerH = do
        darOrErr <- runExceptT $ Compiler.buildDar compilerH path name [] dalfInput
        case darOrErr of
          Left errs
           -> ioError $ userError $ unlines
                [ "Creation of DAR file failed:"
                , T.unpack $ Pretty.renderColored
                    $ Pretty.vcat
                    $ map prettyDiagnostic
                    $ Set.toList $ Set.fromList errs ]
          Right dar -> do
            createDirectoryIfMissing True $ takeDirectory targetFilePath
            B.writeFile targetFilePath dar
            putStrLn $ "Created " <> targetFilePath <> "."

            when (unDumpPom dumpPom) $ createPomAndSHA256 dar

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

    putErrLn = hPutStrLn System.IO.stderr

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
          [ DA.Pretty.keyword_ "package" DA.Pretty.<-> DA.Pretty.text (unTagged pkgId) DA.Pretty.<-> DA.Pretty.keyword_ "where"
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
            show (untag pkgId)

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
-- "package"
optionsParser :: Int -> Parser (Maybe String) -> Parser Compiler.Options
optionsParser numProcessors parsePkgName = Compiler.Options
    <$> optImportPath
    <*> optPackageDir
    <*> parsePkgName
    <*> optWriteIface
    <*> optHideAllPackages
    <*> many optPackage
    <*> optShakeProfiling
    <*> optShakeThreads
    <*> lfVersionOpt
    <*> optDebugLog
    <*> (concat <$> many optGhcCustomOptions)
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
      <> cmdCompile numProcessors
      <> cmdPackage numProcessors
      <> cmdTest numProcessors
      <> cmdDamlDoc
      <> cmdInspectDar
      )
    <|> subparser
      (internal -- internal commands
        <> cmdInspect
        <> cmdPackageNew numProcessors
        <> cmdInit
        <> cmdBuild numProcessors
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
    numProcessors <- getNumProcessors
    withProgName "damlc" $ join $ execParserLax (parserInfo numProcessors)
