-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ApplicativeDo       #-}

-- | Main entry-point of the DAML compiler
module DA.Cli.Damlc (main, execTest) where

import Control.Monad.Except
import qualified Control.Monad.Managed             as Managed
import qualified "cryptonite" Crypto.Hash as Crypto
import Codec.Archive.Zip
import qualified Da.DamlLf as PLF
import           DA.Cli.Damli.BuildInfo
import           DA.Cli.Damli.Command.Damldoc      (cmdDamlDoc)
import           DA.Cli.Options
import DA.Cli.Output
import           DA.Cli.Args
import           DA.Prelude
import qualified DA.Pretty
import           DA.Service.Daml.Compiler.Impl.Handle as Compiler
import qualified DA.Service.Daml.LanguageServer    as Daml.LanguageServer
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified DA.Daml.LF.PrettyScenario as SS
import qualified DA.Daml.LF.ScenarioServiceClient as SSC
import qualified DA.Service.Logger                 as Logger
import qualified DA.Service.Logger.Impl.IO         as Logger.IO
import qualified DA.Service.Logger.Impl.GCP        as Logger.GCP
import qualified DA.Service.Logger.Impl.Pure as Logger.Pure
import DAML.Project.Config
import qualified Data.Aeson.Encode.Pretty as Aeson.Pretty
import Data.ByteArray.Encoding (Base (Base16), convertToBase)
import qualified Data.ByteString                   as B
import qualified Data.ByteString.Lazy as BSL
import Data.FileEmbed (embedFile)
import qualified Data.Set as Set
import qualified Data.List.Split as Split
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Prettyprint.Doc.Syntax as Pretty
import qualified Data.Vector as V
import qualified Development.Shake as Shake
import qualified Development.IDE.State.API as CompilerService
import qualified Development.IDE.State.Rules.Daml as CompilerService
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.LSP
import GHC.Conc
import qualified Network.Socket                    as NS
import           Options.Applicative
import qualified Proto3.Suite as PS
import qualified Proto3.Suite.JSONPB as Proto.JSONPB
import qualified ScenarioService as SS
import System.Directory (createDirectoryIfMissing)
import System.Environment (withProgName)
import System.Exit (exitFailure)
import System.FilePath (takeDirectory, (<.>), (</>), isExtensionOf, takeFileName, dropExtension)
import           System.IO                         (stderr, hPutStrLn)
import qualified Text.PrettyPrint.ANSI.Leijen      as PP
import qualified Text.XML.Light as XML

--------------------------------------------------------------------------------
-- Commands
--------------------------------------------------------------------------------

type Command = IO ()

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

cmdCompile :: FilePath -> Int -> Mod CommandFields Command
cmdCompile baseDir numProcessors =
    command "compile" $ info (helper <*> cmd) $
        progDesc "Compile the DAML program into a Core/DAML-LF archive."
    <> fullDesc
  where
    cmd = execCompile
        <$> inputFileOpt
        <*> outputFileOpt
        <*> optionsParser baseDir numProcessors optPackageName

cmdTest :: FilePath -> Int -> Mod CommandFields Command
cmdTest baseDir numProcessors =
    command "test" $ info (helper <*> cmd) $
       progDesc "Test the given DAML file by running all test declarations."
    <> fullDesc
  where
    cmd = execTest
      <$> inputFileOpt
      <*> junitOutput
      <*> optionsParser baseDir numProcessors optPackageName
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


cmdPackage :: FilePath -> Int -> Mod CommandFields Command
cmdPackage baseDir numProcessors =
    command "package" $ info (helper <*> cmd) $
       progDesc "Compile the DAML program into a DAML ARchive (DAR)"
    <> fullDesc
  where
    dumpPom = fmap DumpPom $ switch $ help "Write out pom and sha256 files" <> long "dump-pom"
    cmd = execPackage
        <$> inputFileOpt
        <*> optionsParser baseDir numProcessors (Just <$> packageNameOpt)
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
execIde (Telemetry telemetry) (Debug debug) = NS.withSocketsDo $ Managed.runManaged $ do
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
      if telemetry
      then Logger.GCP.gcpLogger (>= Logger.Warning) loggerH
      else pure loggerH

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

execPackage :: FilePath -- ^ input file
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
        darOrErr <- runExceptT $ Compiler.buildDar compilerH path name dalfInput
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


-- | Test a DAML file.
execTest :: FilePath -> Maybe FilePath -> Compiler.Options -> IO ()
execTest inFile mbJUnitOutput cliOptions = do
    loggerH <- getLogger cliOptions "test"
    opts <- Compiler.mkOptions cliOptions
    -- TODO (MK): For now the scenario service is only started if we have an event logger
    -- so we insert a dummy event logger.
    let eventLogger _ = pure ()
    Managed.with (Compiler.newIdeState opts (Just eventLogger) loggerH) $ \hDamlGhc -> do
        liftIO $ Compiler.setFilesOfInterest hDamlGhc [inFile]
        mbDeps <- liftIO $ CompilerService.runAction hDamlGhc (CompilerService.getDependencies inFile)
        depFiles <- maybe (reportDiagnostics hDamlGhc "Failed get dependencies") pure mbDeps
        let files = inFile : depFiles
        let lfVersion = Compiler.optDamlLfVersion cliOptions
        case mbJUnitOutput of
            Nothing -> testStdio lfVersion hDamlGhc files
            Just junitOutput -> testJUnit lfVersion hDamlGhc files junitOutput

prettyErr :: LF.Version -> SSC.Error -> DA.Pretty.Doc Pretty.SyntaxClass
prettyErr lfVersion err = case err of
    SSC.BackendError berr ->
        DA.Pretty.string (show berr)
    SSC.ScenarioError serr ->
        SS.prettyBriefScenarioError
          (LF.emptyWorld lfVersion)
          serr
    SSC.ExceptionError e -> DA.Pretty.string $ show e
prettyResult :: LF.Version -> Either SSC.Error SS.ScenarioResult -> DA.Pretty.Doc Pretty.SyntaxClass
prettyResult lfVersion errOrResult = case errOrResult of
  Left err ->
      DA.Pretty.error_ "fail. " DA.Pretty.$$
      DA.Pretty.nest 2 (prettyErr lfVersion err)
  Right result ->
    let nTx = length (SS.scenarioResultScenarioSteps result)
        isActive node =
          case SS.nodeNode node of
            Just SS.NodeNodeCreate{} ->
              isNothing (SS.nodeConsumedBy node)
            _otherwise -> False
        nActive = length $ filter isActive (V.toList (SS.scenarioResultNodes result))
    in DA.Pretty.typeDoc_ "ok, "
    <> DA.Pretty.int nActive <> " active contracts, "
    <> DA.Pretty.int nTx <> " transactions."

testStdio :: LF.Version -> IdeState -> [FilePath] -> IO ()
testStdio lfVersion hDamlGhc files =
    CompilerService.runAction hDamlGhc $
        void $ Shake.forP files $ \file -> do
            mbScenarioResults <- CompilerService.runScenarios file
            scenarioResults <- liftIO $ maybe (reportDiagnostics hDamlGhc "Failed to run scenarios") pure mbScenarioResults
            liftIO $ forM_ scenarioResults $ \(VRScenario vrFile vrName, result) -> do
                let doc = prettyResult lfVersion result
                let name = DA.Pretty.string vrFile <> ":" <> DA.Pretty.pretty vrName
                putStrLn $ DA.Pretty.renderPlain (name <> ": " <> doc)

testJUnit :: LF.Version -> IdeState -> [FilePath] -> FilePath -> IO ()
testJUnit lfVersion hDamlGhc files junitOutput =
    CompilerService.runAction hDamlGhc $ do
        results <- Shake.forP files $ \file -> do
            scenarios <- CompilerService.getScenarios file
            mbScenarioResults <- CompilerService.runScenarios file
            results <- case mbScenarioResults of
                Nothing -> do
                    -- If we don’t get scenario results, we use the diagnostics
                    -- as the error message for each scenario.
                    diagnostics <- liftIO $ CompilerService.getDiagnostics hDamlGhc
                    let errMsg = T.unlines (map (Pretty.renderPlain . prettyDiagnostic) diagnostics)
                    pure $ map (, Just errMsg) scenarios
                Just scenarioResults -> pure $
                    map (\(vr, res) -> (vr, either (Just . T.pack . DA.Pretty.renderPlainOneLine . prettyErr lfVersion) (const Nothing) res))
                        scenarioResults
            pure (file, results)
        liftIO $ do
            createDirectoryIfMissing True $ takeDirectory junitOutput
            writeFile junitOutput $ XML.showTopElement $ toJUnit results


toJUnit :: [(FilePath, [(VirtualResource, Maybe T.Text)])] -> XML.Element
toJUnit results =
    XML.node
        (XML.unqual "testsuites")
        ([ XML.Attr (XML.unqual "errors") "0"
           -- For now we only have successful tests and falures
         , XML.Attr (XML.unqual "failures") (show failures)
         , XML.Attr (XML.unqual "tests") (show tests)
         ],
         map handleFile results)
    where
        tests = length $ concatMap snd results
        failures = length $ concatMap (mapMaybe snd . snd) results
        handleFile :: (FilePath, [(VirtualResource, Maybe T.Text)]) -> XML.Element
        handleFile (f, vrs) =
            XML.node
                (XML.unqual "testsuite")
                ([ XML.Attr (XML.unqual "name") f
                 , XML.Attr (XML.unqual "tests") (show $ length vrs)
                 ],
                 map (handleVR f) vrs)
        handleVR :: FilePath -> (VirtualResource, Maybe T.Text) -> XML.Element
        handleVR f (vr, mbErr) =
            XML.node
                (XML.unqual "testcase")
                ([ XML.Attr (XML.unqual "name") (T.unpack $ vrScenarioName vr)
                 , XML.Attr (XML.unqual "classname") f
                 ],
                 maybe [] (\err -> [XML.node (XML.unqual "failure") (T.unpack err)]) mbErr
                )


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

getLogger :: Compiler.Options -> T.Text -> IO (Logger.Handle IO)
getLogger Compiler.Options {optDebug} name =
    if optDebug
        then Logger.IO.newStderrLogger name
        else pure Logger.Pure.makeNopHandle

optDebugLog :: Parser Bool
optDebugLog = switch $ help "Enable debug output" <> long "debug"

optPackageName :: Parser (Maybe String)
optPackageName = optional $ strOption $
       metavar "PACKAGE-NAME"
    <> help "create package artifacts for the given package name"
    <> long "package-name"

-- | Parametrized by the type of pkgname parser since we want that to be different for
-- "package"
optionsParser :: FilePath -> Int -> Parser (Maybe String) -> Parser Compiler.Options
optionsParser baseDir numProcessors parsePkgName = Compiler.Options
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
  where
    optImportPath :: Parser [FilePath]
    optImportPath = fmap (pure . fromMaybe (baseDir </> "daml-stdlib-src")) optStdLib
    optStdLib :: Parser (Maybe FilePath)
    optStdLib = optional $ strOption $
           metavar "LOC-OF-STDLIB"
        <> help "use std. library location from a custom location"
        <> long "stdlib"

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

options :: FilePath -> Int -> Parser Command
options baseDir numProcessors =
    subparser
      (  cmdIde
      <> cmdLicense
      <> cmdCompile baseDir numProcessors
      <> cmdPackage baseDir numProcessors
      <> cmdTest baseDir numProcessors
      <> cmdDamlDoc
      <> cmdInspectDar
      )
    <|> subparser
      (internal -- internal commands
        <> cmdInspect
      )

parserInfo :: FilePath -> Int -> ParserInfo Command
parserInfo baseDir numProcessors =
  info (helper <*> options baseDir numProcessors)
    (  fullDesc
    <> progDesc "Invoke the DAML compiler. Use -h for help."
    <> headerDoc (Just $ PP.vcat
        [ "damlc - Compiler and IDE backend for the Digital Asset Modelling Language"
        , buildInfo
        ])
    )

main :: IO ()
main = do
    baseDir <- getBaseDir
    numProcessors <- getNumProcessors
    withProgName "damlc" $ join $ execParserLax (parserInfo baseDir numProcessors)

------------------
-- Error reporting
------------------

reportDiagnostics :: CompilerService.IdeState -> String -> IO a
reportDiagnostics service err = do
    diagnostic <- CompilerService.getDiagnostics service
    reportErr err diagnostic

reportErr :: String -> [Diagnostic] -> IO a
reportErr msg errs =
  ioError $
  userError $
  unlines
    [ msg
    , T.unpack $
      Pretty.renderColored $
      Pretty.vcat $ map prettyDiagnostic $ Set.toList $ Set.fromList errs
    ]
