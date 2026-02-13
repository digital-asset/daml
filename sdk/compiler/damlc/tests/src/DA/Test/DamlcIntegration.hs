-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE CPP #-}

-- | Test driver for Daml-GHC CompilerService.
-- For each file, compile it with GHC, convert it,
-- typecheck with LF, test it.  Test annotations are documented as 'Ann'.
module DA.Test.DamlcIntegration
  ( main
  , withDamlScriptDep
  , ScriptPackageData
  ) where

{- HLINT ignore "locateRunfiles/package_app" -}

import           DA.Bazel.Runfiles
import           DA.Cli.Options (explicitSerializable)
import           DA.Daml.Options
import           DA.Daml.Options.Types
import           DA.Test.Util (standardizeQuotes)

import           DA.Daml.LF.Ast as LF
import           "ghc-lib-parser" UniqSupply
import           "ghc-lib-parser" Unique

import           Control.Concurrent.Extra
import           Control.DeepSeq
import           Control.Exception.Extra
import           Control.Monad
import           Control.Monad.IO.Class
import           DA.Daml.LF.PrettyScript (prettyScriptError, prettyScriptResult)
import           DA.Daml.LF.Proto3.Encode
import qualified DA.Daml.LF.Proto3.Archive.Encode as Archive
import           DA.Pretty hiding (first)
import qualified DA.Daml.LF.ScriptServiceClient as SS
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.IO as Logger
import Development.IDE.Core.Compile
import Development.IDE.Core.Debouncer
import Development.IDE.Core.Shake (ShakeLspEnv(..), NotificationHandler(..))
import qualified Development.IDE.Types.Logger as IdeLogger
import Development.IDE.Types.Location
import qualified Data.Aeson.Encode.Pretty as A
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy.Char8 as BSL
import           Data.Char
import qualified Data.DList as DList
import           Data.Either.Extra (eitherToMaybe)
import Data.Foldable
import           Data.List.Extra
import Data.IORef
import Data.Proxy
import           Development.IDE.Types.Diagnostics
import           Data.Maybe
import           Development.Shake hiding (cmd, withResource, withTempDir, doesFileExist)
import           System.Directory.Extra
import           System.Environment.Blank (setEnv)
import           System.FilePath
import           System.Process (readProcess)
import           System.Random.Shuffle (shuffleM)
import           System.IO
import           System.IO.Extra
import           System.Info.Extra (isWindows)
import           Text.Read
import qualified Text.Regex.PCRE.ByteString.Utils as PCRE
import qualified Data.Map.Strict as MS
import qualified Data.HashMap.Strict as HashMap
import qualified Data.HashSet as HashSet
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import           System.Time.Extra
import Development.IDE.Core.API
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.RuleTypes.Daml (ScriptName (..))
import qualified Development.IDE.Types.Diagnostics as D
import Development.IDE.GHC.Util
import           Data.Tagged                  (Tagged (..))
import qualified GHC
import Options.Applicative (execParser, forwardOptions, info, many, strArgument)
import qualified Options.Applicative
import Outputable (ppr, showSDoc)
import qualified Proto3.Suite.JSONPB as JSONPB
import DA.Daml.Project.Types (unsafeResolveReleaseVersion, parseUnresolvedVersion)
import qualified DA.Daml.LF.TypeChecker.Error.WarningFlags as WarningFlags
import DA.Daml.LF.TypeChecker.Error (upgradeInterfacesFlagSpec, upgradeExceptionsFlagSpec, templateInterfaceDependsOnScriptFlagSpec)


import Test.Tasty
import Test.Tasty.Golden (goldenVsStringDiff)
import qualified Test.Tasty.HUnit as HUnit
import Test.Tasty.HUnit ((@?=))
import Test.Tasty.Options
import Test.Tasty.Providers
import Test.Tasty.Runners (Result(..))

import DA.Cli.Damlc.Packaging (setupPackageDb)
import Module (stringToUnitId)
import SdkVersion (SdkVersioned, withSdkVersions, sdkVersion, sdkPackageVersion)

-- Newtype to avoid mixing up the loging function and the one for registering TODOs.
newtype TODO = TODO String

newtype LfVersionOpt = LfVersionOpt Version
  deriving (Eq)

instance IsOption LfVersionOpt where
  defaultValue = LfVersionOpt defaultLfVersion
  -- Tasty seems to force the value somewhere so we cannot just set this
  -- to `error`. However, this will always be set.
  parseValue = fmap LfVersionOpt . parseVersion
  optionName = Tagged "daml-lf-version"
  optionHelp = Tagged "Daml-LF version to test"

type ScriptPackageData = (FilePath, [PackageFlag])

-- | Creates a temp directory with daml script v1 installed, gives the database db path and package flag
withDamlScriptDep :: SdkVersioned => Maybe Version -> (ScriptPackageData -> IO a) -> IO a
withDamlScriptDep mLfVer = withDamlScriptDep' mLfVer []

-- | Creates a temp directory with daml script v1 installed, gives the database db path and package flag
withDamlScriptDep' :: SdkVersioned => Maybe Version -> [(String, String, String)] -> (ScriptPackageData -> IO a) -> IO a
withDamlScriptDep' mLfVer extraPackages =
  let
    lfVerStr = maybe "" (\lfVer -> "-" <> renderVersion lfVer) mLfVer
    darPath = "daml-script" </> "daml" </> "daml-script" <> lfVerStr <> ".dar"
  in withVersionedDamlScriptDep ("daml-script-" <> sdkPackageVersion) darPath mLfVer extraPackages

-- External dars for testing upgrades.
-- dar name, package name and package version
externalPackages :: Version -> [(String, String, String)]
externalPackages version =
  [ ("package-vetting-package-a-" <> LF.renderVersion version, "package-vetting-package-a", "1.0.0")
  , ("package-vetting-package-b-" <> LF.renderVersion version, "package-vetting-package-b", "1.0.0")
  ]

-- | Takes the bazel namespace, dar suffix (used for lf versions in v1) and lf version, installs relevant daml script and gives
-- database db path and package flag
withVersionedDamlScriptDep :: SdkVersioned => String -> String -> Maybe Version -> [(String, String, String)] -> (ScriptPackageData -> IO a) -> IO a
withVersionedDamlScriptDep packageFlagName darPath mLfVer extraPackages cont = do
  withTempDir $ \dir -> do
    withCurrentDirectory dir $ do
      let projDir = toNormalizedFilePath' dir
          -- Bring in daml-script as previously installed by withDamlScriptDep, must include package db
          -- daml-script use the sdkPackageVersion for their versioning
          mkPackageFlag flagName = ExposePackage ("--package " <> flagName) (UnitIdArg $ stringToUnitId flagName) (ModRenaming True [])
          toPackageName (_, name, version) = name <> "-" <> version
          packageFlags = mkPackageFlag <$> packageFlagName : (toPackageName <$> extraPackages)

      scriptDar <- locateRunfiles $ mainWorkspace </> darPath

      extraDars <- traverse (\(darName, _, _) -> locateRunfiles $ mainWorkspace </> "compiler" </> "damlc" </> "tests" </> darName <> ".dar") extraPackages

      setupPackageDb
        projDir
        (defaultOptions mLfVer)
        (unsafeResolveReleaseVersion (either throw id (parseUnresolvedVersion (T.pack sdkVersion))))
        ["daml-prim", "daml-stdlib", scriptDar]
        extraDars
        mempty

      cont (dir </> packageDatabasePath, packageFlags)

main :: IO ()
main = withSdkVersions $ do
  -- This is a bit hacky, we want the LF version before we hand over to
  -- tasty. To achieve that we first pass with optparse-applicative ignoring
  -- everything apart from the LF version.
  LfVersionOpt lfVer <- do
      let parser = optionCLParser
                   <* many (strArgument @String mempty)
      execParser (info parser forwardOptions)

  scriptLogger <- Logger.newStderrLogger Logger.Warning "script"

  let scriptConf = SS.defaultScriptServiceConfig
                       { SS.cnfJvmOptions = ["-Xmx200M"]
                       , SS.cnfEvaluationTimeout = Just 3
                       }

  withDamlScriptDep' (Just lfVer) (externalPackages lfVer) $ \scriptPackageData ->
    SS.withScriptService lfVer scriptLogger scriptConf Nothing $ \scriptService -> do
      hSetEncoding stdout utf8
      setEnv "TASTY_NUM_THREADS" "1" True
      todoRef <- newIORef DList.empty
      let registerTODO (TODO s) = modifyIORef todoRef (`DList.snoc` ("TODO: " ++ s))
      integrationTests <- getIntegrationTests registerTODO scriptService scriptPackageData
      let tests = testGroup "All" [parseRenderRangeTest, uniqueUniques, integrationTests]
      defaultMainWithIngredients ingredients tests
        `finally` (do
        todos <- readIORef todoRef
        putStr (unlines (DList.toList todos)))
      where
        ingredients = includingOptions [Option (Proxy @LfVersionOpt)] : defaultIngredients

parseRenderRangeTest :: TestTree
parseRenderRangeTest =
    let s = "1:1-1:1"
        r = parseRange s
    in
    testGroup "parseRange roundtrips with..."
        [ HUnit.testCase "renderRange" $ renderRange r @?= s
        , HUnit.testCase "showDiagnostics" $ do
            let d = D.Diagnostic r Nothing Nothing Nothing "" Nothing Nothing
            let x = showDiagnostics [("", D.ShowDiag, d)]
            unless (T.pack s `T.isInfixOf` x) $
              HUnit.assertFailure $ "Cannot find range " ++ s ++ " in diagnostic:\n" ++ T.unpack x

        ]

uniqueUniques :: TestTree
uniqueUniques = HUnit.testCase "Uniques" $
    withNumCapabilities 100 $ do
        setNumCapabilities 100
        var <- mkSplitUniqSupply 'x'
        ans <- forM (take 100 $ listSplitUniqSupply var) $ \u -> onceFork $ do
            evaluate $ force $ map getKey $ take 100 $ uniqsFromSupply u
        results <- sequence ans
        let n = length $ nubOrd $ concat results
        n @?= 10000

data DamlTestInput = DamlTestInput
  { name :: String
  , path :: FilePath
  , anns :: [Ann]
  }

getDamlTestFiles :: FilePath -> IO [DamlTestInput]
getDamlTestFiles location = do
    -- test files are declared as data in BUILD.bazel
    testsLocation <- locateRunfiles $ mainWorkspace </> location
    files <- filter (".daml" `isExtensionOf`) <$> listFiles testsLocation
    forM files $ \file -> do
        anns <- readFileAnns file
        pure DamlTestInput
            { name = makeRelative testsLocation file
            , path = file
            , anns
            }

getBondTradingTestFiles :: IO [DamlTestInput]
getBondTradingTestFiles = do
    -- only run Test.daml (see https://github.com/digital-asset/daml/issues/726)
    bondTradingLocation <- locateRunfiles $ mainWorkspace </> "compiler/damlc/tests/bond-trading"
    let file = bondTradingLocation </> "Test.daml"
    anns <- readFileAnns file
    pure
        [ DamlTestInput
            { name = "bond-trading/Test.daml"
            , path = file
            , anns
            }
        ]

getCantSkipPreprocessorTestFiles :: IO [DamlTestInput]
getCantSkipPreprocessorTestFiles = do
    cantSkipPreprocessorLocation <- locateRunfiles $ mainWorkspace </> "compiler/damlc/tests/cant-skip-preprocessor"
    let file = cantSkipPreprocessorLocation </> "DA" </> "Internal" </> "Hack.daml"
    anns <- readFileAnns file
    pure
        [ DamlTestInput
            { name = "cant-skip-preprocessor/DA/Internal/Hack.daml"
            , path = file
            , anns
            }
        ]

getIntegrationTests :: SdkVersioned => (TODO -> IO ()) -> SS.Handle -> ScriptPackageData -> IO TestTree
getIntegrationTests registerTODO scriptService (packageDbPath, packageFlags) = do
    putStrLn $ "rtsSupportsBoundThreads: " ++ show rtsSupportsBoundThreads
    do n <- getNumCapabilities; putStrLn $ "getNumCapabilities: " ++ show n

    damlTests <-
        mconcat @(IO [DamlTestInput])
            [ getDamlTestFiles "compiler/damlc/tests/daml-test-files"
            , getBondTradingTestFiles
            , getCantSkipPreprocessorTestFiles
            ]
        >>= shuffleM  -- Make sure we can run the tests in any order.

    let outdir = "compiler/damlc/output"
    createDirectoryIfMissing True outdir

    -- We need to start a different compiler service for each test that has
    -- different compiler options.  Fortunately there are not that many options
    -- that we allow toggling in tests, so the size of this map should be small
    -- (< 20).
    let damlTestsByExtraOpts :: MS.Map (S.Set String) [DamlTestInput]
        damlTestsByExtraOpts = fmap reverse $ MS.fromListWith (++) $ do
            damlTest <- damlTests
            let opts = S.fromList [opt | BuildOption opt <- anns damlTest]
            pure (opts, [damlTest])

    -- initialise the compiler service
    vfs <- makeVFSHandle
    -- We use a separate service for generated files so that we can test files containing internal imports.
    let tree :: TestTree
        tree = askOption $ \(LfVersionOpt version) ->
          let opts0 = defaultOptions (Just version)
              opts = opts0
                { optPackageDbs = [packageDbPath]
                , optThreads = 0
                , optCoreLinting = True
                , optDlintUsage = DlintEnabled DlintOptions
                    { dlintRulesFile = DefaultDlintRulesFile
                    , dlintHintFiles = NoDlintHintFiles
                    }
                , optPackageImports = packageFlags
                , optDetailLevel = PrettyLevel (-1)
                , optEnableInterfaces = EnableInterfaces True
                , optTypecheckerWarningFlags =
                    WarningFlags.addWarningFlags
                      [ WarningFlags.specToFlag upgradeInterfacesFlagSpec WarningFlags.AsWarning
                      , WarningFlags.specToFlag upgradeExceptionsFlagSpec WarningFlags.AsWarning
                      , -- Almost every test will report this, so we disable it at root
                        WarningFlags.specToFlag templateInterfaceDependsOnScriptFlagSpec WarningFlags.Hidden
                      ]
                      (optTypecheckerWarningFlags opts0)
                }

              mkIde options = do
                damlEnv <- mkDamlEnv options (StudioAutorunAllScripts True) (Just scriptService)
                initialise
                  (mainRule options)
                  (DummyLspEnv $ NotificationHandler $ \_ _ -> pure ())
                  IdeLogger.noLogging
                  noopDebouncer
                  damlEnv
                  (toCompileOpts options)
                  vfs
          in
          testGroup ("Tests for Daml-LF " ++ renderPretty version) $ do
            (optsSet, tests) <- MS.toList damlTestsByExtraOpts
            let extraOpts = S.toList optsSet
                customOpts = parseBuildOptions extraOpts opts
            pure $ withResource
              (mkIde customOpts)
              shutdown
              $ \service ->
              testGroup (if null extraOpts then "default opts" else unwords extraOpts) $
                map (damlFileTestTree version service outdir registerTODO) tests

    pure tree

newtype TestCase = TestCase ((String -> IO ()) -> IO Result)

instance IsTest TestCase where
  run _ (TestCase r) _ = do
    (res, log) <- withLog \log -> do
      res <- r log
      log (resultDescription res)
      pure res
    pure res { resultDescription = log }
  testOptions = Tagged []

-- | 'withLog' takes a function 'f :: (String -> IO ()) -> IO a'
-- (that is, a function such that given a logger function ':: String -> IO ()'
-- returns (in an IO context) a value of type 'a'), so that 'withLog f' is an
-- IO action that performs all the actions of 'f' and returns the
-- final value of type 'a' together with the final log.
withLog :: ((String -> IO ()) -> IO a) -> IO (a, String)
withLog action = do
  logger <- newIORef DList.empty
  let log msg = unless (null msg) $ modifyIORef logger (`DList.snoc` msg)
  r <- action log
  msgs <- readIORef logger
  pure (r, unlines (DList.toList msgs))

data DamlOutput = DamlOutput
  { diagnostics :: [D.FileDiagnostic]
  , jsonPackagePath :: Maybe FilePath
  , buildLog :: String
  , scriptResults :: HashMap.HashMap ScriptName T.Text
  }

stripPartySuffix :: T.Text -> T.Text
stripPartySuffix = replace "'([a-zA-Z0-9]+)-[a-z0-9]{8}'" "'\\1'"
  where
    replace :: BS.ByteString -> BS.ByteString -> T.Text -> T.Text
    replace patt replacement input = case PCRE.substituteCompile' patt (TE.encodeUtf8 input) replacement of
      Right result -> TE.decodeUtf8 result
      Left err -> error $ "Failed to strip party suffix within " <> show input <> " because " <> err

testSetup :: IO IdeState -> FilePath -> FilePath -> IO DamlOutput
testSetup getService outdir path = do
  service <- getService
  (ex, buildLog) <- withLog \log ->
    try @SomeException $ mainProj service outdir log (toNormalizedFilePath' path)
  diags0 <- getDiagnostics service
  pure DamlOutput
    { diagnostics =
        [ ideErrorText "" $ T.pack $ show e
        | Left e <- [ex]
        , not $ "_IGNORE_" `isInfixOf` show e
        ] ++ diags0
    , jsonPackagePath = (\(_, x, _) -> x) <$> eitherToMaybe ex
    , scriptResults = either (const HashMap.empty) (\(_, _, x) -> x) ex
    , buildLog
    }

damlFileTestTree :: LF.Version -> IO IdeState -> FilePath -> (TODO -> IO ()) -> DamlTestInput -> TestTree
damlFileTestTree version getService outdir registerTODO input
  | any (ignoreVersion version) anns =
    singleTest name $ TestCase \_ ->
      pure (testPassed "") { resultShortDescription = "IGNORE" }
  | otherwise =
    withResource
      (testSetup getService outdir path)
      (\_ ->
        -- FIXME: Use of unsafeClearDiagnostics is only because we don't naturally lose them when we change setFilesOfInterest
        getService >>= unsafeClearDiagnostics
      )
      \getDamlOutput -> do
        testGroup name
          [ singleTest "Build log" $ TestCase \_ -> do
              for_ [path ++ ", " ++ x | Todo x <- anns] (registerTODO . TODO)
              testPassed . buildLog <$> getDamlOutput
          , singleTest "Check diagnostics" $ TestCase \log -> do
              diags <- diagnostics <$> getDamlOutput
              let strippedDiags = [ (x, y, diag { _message = stripPartySuffix (_message diag) }) | (x, y, diag) <- diags ]
              resDiag <- checkDiagnostics version log [fields | DiagnosticFields fields <- anns] strippedDiags
              pure $ maybe (testPassed "") testFailed resDiag
          , testGroup "jq Queries"
              [ singleTest ("#" <> show @Integer ix) $ TestCase \log -> do
                  mJsonFile <- jsonPackagePath <$> getDamlOutput
                  r <- runJqQuery log mJsonFile q isStream
                  pure $ maybe (testPassed "") testFailed r
              | (ix, QueryLF q isStream) <- zip [1..] anns
              ]
          , testGroup "Ledger expectation tests"
              [ goldenVsStringDiff
                  ("Script: " <> scriptName <> ", file: " <> expectedFile)
                  diff
                  (takeDirectory path </> expectedFile)
                  do
                    scriptResult <-
                      fromMaybe (error $ "Ledger expectation test failure: the script '" <> scriptName <> "' did not run")
                      . HashMap.lookup (ScriptName $ T.pack scriptName)
                      . scriptResults
                      <$> getDamlOutput
                    pure $ BSL.fromStrict $ TE.encodeUtf8 (stripPartySuffix scriptResult)
              | Ledger scriptName expectedFile <- anns
              ]
          ]
  where
    DamlTestInput { name, path, anns } = input
    ignoreVersion version = \case
      Ignore -> True
      SinceLF versionBounds -> not (versionBounds `containsVersion` version)
      UntilLF versionBounds -> not (versionBounds `extendsVersion` version)
      SupportsFeature featureName -> not (version `supports` nameToFeature featureName)
      DoesNotSupportFeature featureName -> version `supports` nameToFeature featureName
      _ -> False
    diff ref new = [POSIX_DIFF, "-w", "--strip-trailing-cr", ref, new]

containsVersion :: MS.Map LF.MajorVersion LF.MinorVersion -> LF.Version -> Bool
containsVersion bounds (Version major minor) =
  case major `MS.lookup` bounds of
    Nothing -> False
    Just minorMinBound -> minor >= minorMinBound

extendsVersion :: MS.Map LF.MajorVersion LF.MinorVersion -> LF.Version -> Bool
extendsVersion bounds (Version major minor) =
  case major `MS.lookup` bounds of
    Nothing -> False
    Just minorMinBound -> minor <= minorMinBound

runJqQuery :: (String -> IO ()) -> Maybe FilePath -> String -> Bool -> IO (Maybe String)
runJqQuery log mJsonFile q isStream = do
  case mJsonFile of
    Just jsonPath -> do
      log $ "running jq query: " ++ q
      let jqKey = "external" </> "jq_dev_env" </> "bin" </> if isWindows then "jq.exe" else "jq"
      jq <- locateRunfiles $ mainWorkspace </> jqKey
      queryLfDir <- locateRunfiles $ mainWorkspace </> "compiler/damlc/tests/src"
      let fullQuery = "import \"./query-lf\" as lf; inputs as $pkg | " ++ q
      out <- readProcess jq (["--stream" | isStream] <> ["-n", "-L", queryLfDir, fullQuery, jsonPath]) ""
      case trim out of
        "true" -> pure Nothing
        other -> pure $ Just $ "jq query failed: got " ++ other
    _ | otherwise -> do
      log "jq query failed to run as test errored"
      pure (Just "Couldn't run jq")

data DiagnosticField
  = DFilePath !FilePath
  | DRange !Range
  | DSeverity !DiagnosticSeverity
  | DSource !String
  | DMessage !String
  | DFeatureCondition !String

renderRange :: Range -> String
renderRange r = p (_start r) ++ "-" ++ p (_end r)
  where
    p x = show (_line x + 1) ++ ":" ++ show (_character x + 1)

renderDiagnosticField :: DiagnosticField -> String
renderDiagnosticField f = case f of
    DFilePath fp -> "file=" ++ fp ++ ";"
    DRange r -> "range=" ++ renderRange r ++ ";"
    DSeverity s -> case s of
        DsError -> "@ERROR"
        DsWarning -> "@WARN"
        DsInfo -> "@INFO"
        DsHint -> "@HINT"
    DSource s -> "source=" ++ s ++ ";"
    DMessage m -> m
    DFeatureCondition f -> "if=" ++ f

renderDiagnosticFields :: [DiagnosticField] -> String
renderDiagnosticFields fs = unwords ("--" : map renderDiagnosticField fs)

checkDiagnostics :: LF.Version -> (String -> IO ()) -> [[DiagnosticField]] -> [D.FileDiagnostic] -> IO (Maybe String)
checkDiagnostics version log expected' got
    -- you require the same number of diagnostics as expected
    -- and each diagnostic is at least partially expected
    | length expected /= length got = do
        logDiags
        pure $ Just $ "Wrong number of diagnostics, expected " ++ show (length expected) ++ ", but got " ++ show (length got)
    | notNull bad = do
        logDiags
        pure $ Just $ unlines ("Could not find matching diagnostics:" : map renderDiagnosticFields bad)
    | otherwise = pure Nothing
    where checkField :: D.FileDiagnostic -> DiagnosticField -> Bool
          checkField (fp, _, D.Diagnostic{..}) f = case f of
            DFilePath p -> toNormalizedFilePath' p == fp
            DRange r -> r == _range
            DSeverity s -> Just s == _severity
            DSource s -> Just (T.pack s) == _source
            DMessage m ->
              standardizeQuotes (T.pack m)
                  `T.isInfixOf`
                      standardizeQuotes (T.unwords (T.words _message))
            DFeatureCondition _ -> True
          expected :: [[DiagnosticField]]
          expected = filter (not . any shouldDropExpected) expected'
          shouldDropExpected :: DiagnosticField -> Bool
          shouldDropExpected (DFeatureCondition featureName) = not $ version `supports` nameToFeature (T.pack featureName)
          shouldDropExpected _ = False
          logDiags = log $ T.unpack $ showDiagnostics got
          bad = filter
            (\expFields -> not $ any (\diag -> all (checkField diag) expFields) got)
            expected

------------------------------------------------------------
-- functionality
data Ann
    = Ignore
      -- ^ Don't run this test at all
    | SinceLF (MS.Map LF.MajorVersion LF.MinorVersion)
      -- ^ Only run this test if the target Daml-LF version can depend on the
      -- given Daml-LF version with the same major version as the target LF
      -- version.
    | UntilLF (MS.Map LF.MajorVersion LF.MinorVersion)
      -- ^ Only run this test if the target Daml-LF version can depend on the
      -- given Daml-LF version with the same major version as the target LF
      -- version.
    | SupportsFeature T.Text
      -- ^ Only run this test if the given feature is supported by the target Daml-LF version
    | DoesNotSupportFeature T.Text
      -- ^ Only run this test if the given feature is not supported by the target Daml-LF version
    | DiagnosticFields [DiagnosticField]
      -- ^ I expect a diagnostic that has the given fields
    | QueryLF String Bool
      -- ^ The jq query against the produced Daml-LF returns "true". Includes a boolean for is stream
    | Todo String
      -- ^ Just a note that is printed out
    | Ledger String FilePath
      -- ^ I expect the output of running the script named <first argument> to match the golden file <second argument>.
      -- The path of the golden file is relative to the `.daml` test file.
    | BuildOption String
      -- ^ Extra build option passed to damlc.  Note that this only supports a
      -- subset of real options, currently:
      --
      -- * explicit-serializable
      -- * toggling InlineDamlCustomWarnings

readFileAnns :: FilePath -> IO [Ann]
readFileAnns file = do
    enc <- mkTextEncoding "UTF-8//IGNORE" -- ignore unknown characters
    mapMaybe f . lines <$> readFileEncoding' enc file
    where
        f :: String -> Maybe Ann
        f (stripPrefix "-- @" . trim -> Just x) = case word1 $ trim x of
            ("IGNORE",_) -> Just Ignore
            ("SINCE-LF", x) -> Just $ SinceLF $ parseMaybeVersions x
            ("UNTIL-LF", x) -> Just $ UntilLF $ parseMaybeVersions x
            ("SUPPORTS-LF-FEATURE", x) -> Just $ SupportsFeature $ T.pack $ trim x
            ("DOES-NOT-SUPPORT-LF-FEATURE", x) -> Just $ DoesNotSupportFeature $ T.pack $ trim x
            ("ERROR",x) -> Just (DiagnosticFields (DSeverity DsError : parseFields x))
            ("WARN",x) -> Just (DiagnosticFields (DSeverity DsWarning : parseFields x))
            ("INFO",x) -> Just (DiagnosticFields (DSeverity DsInfo : parseFields x))
            ("QUERY-LF", x) -> Just $ QueryLF x False
            ("QUERY-LF-STREAM", x) -> Just $ QueryLF x True
            ("TODO",x) -> Just $ Todo x
            ("LEDGER", words -> [script, path]) -> Just $ Ledger script path
            ("BUILD-OPTION", x) -> Just $ BuildOption $ trim x
            _ -> error $ "Can't understand test annotation in " ++ show file ++ ", got " ++ show x
        f _ = Nothing

parseMaybeVersions :: String -> MS.Map LF.MajorVersion LF.MinorVersion
parseMaybeVersions str = MS.fromList (pairUp sortedMajorVersions parsedMaybeVersions)
  where
    sortedMajorVersions :: [LF.MajorVersion]
    sortedMajorVersions = [minBound .. maxBound]

    parsedMaybeVersions :: [Maybe LF.Version]
    parsedMaybeVersions = map (parseMaybeVersion . trim) (words str)

    pairUp :: [LF.MajorVersion] -> [Maybe LF.Version] -> [(LF.MajorVersion, LF.MinorVersion)]
    pairUp (_ : majors) (Nothing : versions) = pairUp majors versions
    pairUp (major : majors) (Just version : versions)
        | major /= versionMajor version =
            error $
                "expected a version with major version "
                    <> LF.renderMajorVersion major
                    <> ", got "
                    <> LF.renderVersion version
        | otherwise = (major, versionMinor version) : pairUp majors versions
    pairUp majors@(_ : _) [] =
        error $
            "missing version bounds for major versions "
                <> intercalate ", " (map LF.renderMajorVersion majors)
    pairUp [] (_ : _) = error "too many version bounds provided"
    pairUp [] [] = []

parseMaybeVersion :: String -> Maybe LF.Version
parseMaybeVersion "None" = Nothing
parseMaybeVersion s =
  case LF.parseVersion s of
    Just v -> Just v
    Nothing -> error ("couldn't parse version: " <> show s)

parseFields :: String -> [DiagnosticField]
parseFields = map (parseField . trim) . wordsBy (==';')

parseField :: String -> DiagnosticField
parseField s =
  case stripInfix "=" s of
    Nothing -> DMessage s
    Just (name, val) ->
      case trim (map toLower name) of
        "file" -> DFilePath val
        "range" -> DRange (parseRange val)
        "source" -> DSource val
        "message" -> DMessage val
        "if" -> DFeatureCondition val
        -- We do not parse severity fields as they are already
        -- specified by using @FAIL or @WARN.
        _ -> DMessage s

parseRange :: String -> Range
parseRange s =
  case traverse readMaybe (wordsBy (`elem` (":-" :: String)) s) of
    Just [rowStart, colStart, rowEnd, colEnd] ->
      Range
        -- NOTE(MH): The locations/ranges in dagnostics are zero-based
        -- internally but are pretty-printed one-based, both by our command
        -- line tools and in vscode. In our test files, we want to specify them
        -- the way they are printed to be able to just copy & paste them. Thus,
        -- we need to subtract 1 from all coordinates here.
        (Position (rowStart - 1) (colStart - 1))
        (Position (rowEnd - 1) (colEnd - 1))
    _ -> error $ "Failed to parse range, got " ++ s

parseBuildOptions :: [String] -> (Options -> Options)
parseBuildOptions args = case result of
  Options.Applicative.Failure failure ->
    error $ fst $ Options.Applicative.renderFailure failure ""
  Options.Applicative.Success f -> f
  Options.Applicative.CompletionInvoked _ ->
    error "Options.Applicative.CompletionInvoked for test extra option"
  where
    result = Options.Applicative.execParserPure prefs info args
    prefs = Options.Applicative.prefs mempty
    info = flip Options.Applicative.info mempty $ fmap (foldr (.) id) $ sequenceA
      -- NOTE(jaspervdj): Can add more option parsers here if we need them for
      -- tests. We should not let this list become too big, as we currently
      -- create a new IDE instance for each unique combination of options in
      -- the tests.
      [ (\seri opts -> opts {optExplicitSerializable = seri}) <$>
              explicitSerializable
      , (\wf opts -> opts
              { optInlineDamlCustomWarningFlags = WarningFlags.addWarningFlags
                  (WarningFlags.wfFlags wf)
                  (optInlineDamlCustomWarningFlags opts)
              }) <$>
              WarningFlags.runParser warningFlagParserInlineDamlCustom
      ]

mainProj :: IdeState -> FilePath -> (String -> IO ()) -> NormalizedFilePath -> IO (LF.Package, FilePath, HashMap.HashMap ScriptName T.Text)
mainProj service outdir log file = do
    let proj = takeBaseName (fromNormalizedFilePath file)

    -- NOTE (MK): For some reason ghcide’s `prettyPrint` seems to fall over on Windows with `commitBuffer: invalid argument`.
    -- With `fakeDynFlags` things seem to work out fine.
    let corePrettyPrint = timed log "Core pretty-printing" . liftIO . writeFile (outdir </> proj <.> "core") . showSDoc fakeDynFlags . ppr
    let lfSave = timed log "LF saving" . liftIO . BS.writeFile (outdir </> proj <.> "dalf") . Archive.encodeArchive
    let lfPrettyPrint = timed log "LF pretty-printing" . liftIO . writeFile (outdir </> proj <.> "pdalf") . renderPretty
    let jsonPath = outdir </> proj <.> "json"
    let jsonSave pkg =
            let json = A.encodePretty $ JSONPB.toJSONPB (encodePackage pkg) JSONPB.jsonPBOptions
            in timed log "JSON saving" . liftIO . BSL.writeFile jsonPath $ json

    setFilesOfInterest service (HashSet.singleton file)
    runActionSync service $ do
            dlint log file
            lf <- lfConvert log file
            lfPrettyPrint lf
            core <- ghcCompile log file
            corePrettyPrint core
            lf <- lfTypeCheck log file
            lfSave lf
            scriptResults <- lfRunScripts log file
            jsonSave lf
            pure (lf, jsonPath, scriptResults)

unjust :: Action (Maybe b) -> Action b
unjust act = do
    res <- act
    case res of
      Nothing -> fail "_IGNORE_"
      Just v -> return v

dlint :: (String -> IO ()) -> NormalizedFilePath -> Action ()
dlint log file = timed log "DLint" $ unjust $ getDlintIdeas file

ghcCompile :: (String -> IO ()) -> NormalizedFilePath -> Action GHC.CoreModule
ghcCompile log file = timed log "GHC compile" $ do
    (_, Just (safeMode, guts, details)) <- generateCore (RunSimplifier False) file
    pure $ cgGutsToCoreModule safeMode guts details

lfConvert :: (String -> IO ()) -> NormalizedFilePath -> Action LF.Package
lfConvert log file = timed log "LF convert" $ unjust $ getRawDalf file

lfTypeCheck :: (String -> IO ()) -> NormalizedFilePath -> Action LF.Package
lfTypeCheck log file = timed log "LF type check" $ unjust $ getDalf file

lfRunScripts :: (String -> IO ()) -> NormalizedFilePath -> Action (HashMap.HashMap ScriptName T.Text)
lfRunScripts log file = timed log "LF scripts execution" $ do
    world <- worldForFile file
    results <- unjust $ runScripts file
    pure $ HashMap.fromList
        [ (scriptName, format world res)
        | (scriptName, res) <- results
        ]
    where
        format world
          = renderPlain
          . ($$ text "") -- add a newline at the end to appease git
          . \case
              Right res ->
                let activeContracts = S.fromList (V.toList (SS.scriptResultActiveContracts res))
                in prettyScriptResult lvl world activeContracts res
              Left (SS.ScriptError err) ->
                prettyScriptError lvl world err
              Left e ->
                shown e
        lvl =
          -- hide package ids
          PrettyLevel (-1)

timed :: MonadIO m => (String -> IO ()) -> String -> m a -> m a
timed log msg act = do
    time <- liftIO offsetTime
    res <- act
    time <- liftIO time
    liftIO $ log $ "Time: " ++ msg ++ " = " ++ showDuration time
    return res
