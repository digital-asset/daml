-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Test driver for DAML-GHC CompilerService.
-- For each file, compile it with GHC, convert it,
-- typecheck with LF, test it.  Test annotations are documented as 'Ann'.
module DA.Test.DamlcIntegration
  ( main
  ) where

import           DA.Bazel.Runfiles
import           DA.Daml.Options
import           DA.Daml.Options.Types
import           DA.Daml.LFConversion.UtilLF
import           DA.Test.Util (standardizeQuotes)

import           DA.Daml.LF.Ast as LF hiding (IsTest)
import           "ghc-lib-parser" UniqSupply
import           "ghc-lib-parser" Unique

import           Control.Concurrent.Extra
import           Control.DeepSeq
import           Control.Exception.Extra
import           Control.Monad
import           Control.Monad.IO.Class
import           DA.Daml.LF.Proto3.EncodeV1
import           DA.Pretty hiding (first)
import qualified DA.Daml.LF.ScenarioServiceClient as SS
import qualified DA.Service.Logger.Impl.Pure as Logger
import Development.IDE.Core.Compile
import Development.IDE.Core.Debouncer
import qualified Development.IDE.Types.Logger as IdeLogger
import Development.IDE.Types.Location
import Development.IDE.Types.Options(IdeReportProgress(..))
import qualified Data.Aeson.Encode.Pretty as A
import qualified Data.ByteString.Lazy.Char8 as BSL
import           Data.Char
import Data.Default
import qualified Data.DList as DList
import Data.Foldable
import           Data.List.Extra
import Data.IORef
import Data.Proxy
import           Development.IDE.Types.Diagnostics
import           Data.Maybe
import           Development.Shake hiding (cmd, withResource)
import qualified Language.Haskell.LSP.Types as LSP
import           System.Directory.Extra
import           System.Environment.Blank (setEnv)
import           System.FilePath
import           System.Process (readProcess)
import           System.IO
import           System.IO.Extra
import           System.Info.Extra (isWindows)
import           Text.Read
import qualified Data.HashSet as HashSet
import qualified Data.Text as T
import           System.Time.Extra
import Development.IDE.Core.API
import Development.IDE.Core.Rules.Daml
import qualified Development.IDE.Types.Diagnostics as D
import Development.IDE.GHC.Util
import           Data.Tagged                  (Tagged (..))
import qualified GHC
import Options.Applicative (execParser, forwardOptions, info, many, strArgument)
import Outputable (ppr, showSDoc)
import qualified Proto3.Suite.JSONPB as JSONPB

import Test.Tasty
import qualified Test.Tasty.HUnit as HUnit
import Test.Tasty.HUnit ((@?=))
import Test.Tasty.Options
import Test.Tasty.Providers
import Test.Tasty.Runners (Outcome(..), Result(..))

-- Newtype to avoid mixing up the loging function and the one for registering TODOs.
newtype TODO = TODO String

newtype LfVersionOpt = LfVersionOpt Version
  deriving (Eq)

instance IsOption LfVersionOpt where
  defaultValue = LfVersionOpt versionDefault
  -- Tasty seems to force the value somewhere so we cannot just set this
  -- to `error`. However, this will always be set.
  parseValue = fmap LfVersionOpt . parseVersion
  optionName = Tagged "daml-lf-version"
  optionHelp = Tagged "DAML-LF version to test"

main :: IO ()
main = do
 let scenarioConf = SS.defaultScenarioServiceConfig { SS.cnfJvmOptions = ["-Xmx200M"] }
 -- This is a bit hacky, we want the LF version before we hand over to
 -- tasty. To achieve that we first pass with optparse-applicative ignoring
 -- everything apart from the LF version.
 LfVersionOpt lfVer <- do
     let parser = optionCLParser <* many (strArgument @String mempty)
     execParser (info parser forwardOptions)
 SS.withScenarioService lfVer Logger.makeNopHandle scenarioConf $ \scenarioService -> do
  hSetEncoding stdout utf8
  setEnv "TASTY_NUM_THREADS" "1" True
  todoRef <- newIORef DList.empty
  let registerTODO (TODO s) = modifyIORef todoRef (`DList.snoc` ("TODO: " ++ s))
  integrationTests <- getIntegrationTests registerTODO scenarioService
  let tests = testGroup "All" [parseRenderRangeTest, uniqueUniques, integrationTests]
  defaultMainWithIngredients ingredients tests
    `finally` (do
    todos <- readIORef todoRef
    putStr (unlines (DList.toList todos)))
  where ingredients =
          includingOptions [Option (Proxy :: Proxy PackageDb), Option (Proxy @LfVersionOpt)] :
          defaultIngredients

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

getIntegrationTests :: (TODO -> IO ()) -> SS.Handle -> IO TestTree
getIntegrationTests registerTODO scenarioService = do
    putStrLn $ "rtsSupportsBoundThreads: " ++ show rtsSupportsBoundThreads
    do n <- getNumCapabilities; putStrLn $ "getNumCapabilities: " ++ show n

    -- test files are declared as data in BUILD.bazel
    testsLocation <- locateRunfiles $ mainWorkspace </> "compiler/damlc/tests/daml-test-files"
    damlTestFiles <-
        map (\f -> (makeRelative testsLocation f, f)) .
        filter (".daml" `isExtensionOf`) <$> listFiles testsLocation
    -- only run Test.daml (see https://github.com/digital-asset/daml/issues/726)
    bondTradingLocation <- locateRunfiles $ mainWorkspace </> "compiler/damlc/tests/bond-trading"
    let allTestFiles = damlTestFiles ++ [("bond-trading/Test.daml", bondTradingLocation </> "Test.daml")]
    let (generatedFiles, nongeneratedFiles) = partition (\(f, _) -> takeFileName f == "ProposalDesugared.daml") allTestFiles

    let outdir = "compiler/damlc/output"
    createDirectoryIfMissing True outdir

    dlintDataDir <- locateRunfiles $ mainWorkspace </> "compiler/damlc/daml-ide-core"

    -- initialise the compiler service
    vfs <- makeVFSHandle
    -- We use a separate service for generated files so that we can test files containing internal imports.
    let tree :: TestTree
        tree = askOption $ \(LfVersionOpt version) ->
          let opts = (defaultOptions (Just version))
                { optThreads = 0
                , optCoreLinting = True
                , optDlintUsage = DlintEnabled dlintDataDir False
                }
          in
          withResource (mkDamlEnv opts (Just scenarioService)) (\_damlEnv -> pure ()) $ \getDamlEnv ->
          withResource
          (getDamlEnv >>= \damlEnv -> initialise def (mainRule opts) (pure $ LSP.IdInt 0) (const $ pure ()) IdeLogger.noLogging noopDebouncer damlEnv (toCompileOpts opts (IdeReportProgress False)) vfs)
          shutdown $ \service ->
          withResource
          (getDamlEnv >>= \damlEnv -> initialise def (mainRule opts) (pure $ LSP.IdInt 0) (const $ pure ()) IdeLogger.noLogging noopDebouncer damlEnv (toCompileOpts opts { optIsGenerated = True } (IdeReportProgress False)) vfs)
          shutdown $ \serviceGenerated ->
          withTestArguments $ \args -> testGroup ("Tests for DAML-LF " ++ renderPretty version) $
            map (testCase args version service outdir registerTODO) nongeneratedFiles <>
            map (testCase args version serviceGenerated outdir registerTODO) generatedFiles

    pure tree

newtype TestCase = TestCase ((String -> IO ()) -> IO Result)

instance IsTest TestCase where
  run _ (TestCase r) _ = do
    logger <- newIORef DList.empty
    let log msg = modifyIORef logger (`DList.snoc` msg)
    res <- r log
    msgs <- readIORef logger
    let desc
          | null (resultDescription res) = unlines (DList.toList msgs)
          | otherwise = unlines (DList.toList (msgs `DList.snoc` resultDescription res))
    pure $ res { resultDescription = desc }
  testOptions = Tagged []

testCase :: TestArguments -> LF.Version -> IO IdeState -> FilePath -> (TODO -> IO ()) -> (String, FilePath) -> TestTree
testCase args version getService outdir registerTODO (name, file) = singleTest name . TestCase $ \log -> do
  service <- getService
  anns <- readFileAnns file
  if any (`notElem` supportedOutputVersions) [v | UntilLF v <- anns] then
    pure (testFailed "Unsupported DAML-LF version in UNTIL-LF annotation")
  else if any (ignoreVersion version) anns
    then pure $ Result
      { resultOutcome = Success
      , resultDescription = ""
      , resultShortDescription = "IGNORE"
      , resultTime = 0
      }
    else do
      -- FIXME: Use of unsafeClearDiagnostics is only because we don't naturally lose them when we change setFilesOfInterest
      unsafeClearDiagnostics service
      ex <- try $ mainProj args service outdir log (toNormalizedFilePath' file) :: IO (Either SomeException Package)
      diags <- getDiagnostics service
      for_ [file ++ ", " ++ x | Todo x <- anns] (registerTODO . TODO)
      resDiag <- checkDiagnostics log [fields | DiagnosticFields fields <- anns] $
        [ideErrorText "" $ T.pack $ show e | Left e <- [ex], not $ "_IGNORE_" `isInfixOf` show e] ++ diags
      resQueries <- runJqQuery log version outdir file [q | QueryLF q <- anns]
      let failures = catMaybes $ resDiag : resQueries
      case failures of
        err : _others -> pure $ testFailed err
        [] -> pure $ testPassed ""
  where
    ignoreVersion version = \case
      Ignore -> True
      SinceLF minVersion -> version < minVersion
      UntilLF maxVersion -> version > maxVersion
      _ -> False

runJqQuery :: (String -> IO ()) -> LF.Version -> FilePath -> FilePath -> [String] -> IO [Maybe String]
runJqQuery log version outdir file qs = do
  let proj = takeBaseName file
  forM qs $ \q -> do
    log $ "running jq query: " ++ q
    let jqKey = "external" </> "jq_dev_env" </> "bin" </> if isWindows then "jq.exe" else "jq"
    jq <- locateRunfiles $ mainWorkspace </> jqKey
    queryLfDir <- locateRunfiles $ mainWorkspace </> "compiler/damlc/tests/src"
    let queryLfMod
            | version `supports` featureStringInterning = "query-lf-interned"
            | otherwise = "query-lf-non-interned"
    let fullQuery = "import \"./" ++ queryLfMod ++ "\" as lf; . as $pkg | " ++ q
    out <- readProcess jq ["-L", queryLfDir, fullQuery, outdir </> proj <.> "json"] ""
    case trim out of
      "true" -> pure Nothing
      other -> pure $ Just $ "jq query failed: got " ++ other


data DiagnosticField
  = DFilePath !FilePath
  | DRange !Range
  | DSeverity !DiagnosticSeverity
  | DSource !String
  | DMessage !String

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

renderDiagnosticFields :: [DiagnosticField] -> String
renderDiagnosticFields fs = unwords ("--" : map renderDiagnosticField fs)

checkDiagnostics :: (String -> IO ()) -> [[DiagnosticField]] -> [D.FileDiagnostic] -> IO (Maybe String)
checkDiagnostics log expected got
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
            DMessage m -> standardizeQuotes(T.pack m) `T.isInfixOf` standardizeQuotes(T.unwords (T.words _message))
          logDiags = log $ T.unpack $ showDiagnostics got
          bad = filter
            (\expFields -> not $ any (\diag -> all (checkField diag) expFields) got)
            expected

------------------------------------------------------------
-- CLI argument handling

newtype PackageDb = PackageDb String

instance IsOption PackageDb where
  defaultValue = PackageDb "bazel-bin/compiler/damlc/pkg-db"
  parseValue = Just . PackageDb
  optionName = Tagged "package-db"
  optionHelp = Tagged "Path to the package database"

data TestArguments = TestArguments
    {package_db  :: FilePath
    }

withTestArguments :: (TestArguments -> TestTree) -> TestTree
withTestArguments f =
  askOption $ \(PackageDb packageDb) -> f (TestArguments packageDb)

------------------------------------------------------------
-- functionality
data Ann
    = Ignore                             -- Don't run this test at all
    | SinceLF LF.Version                 -- Only run this test since the given DAML-LF version
    | UntilLF LF.Version                 -- Only run this test until the given DAML-LF version
    | DiagnosticFields [DiagnosticField] -- I expect a diagnostic that has the given fields
    | QueryLF String                       -- The jq query against the produced DAML-LF returns "true"
    | Todo String                        -- Just a note that is printed out


readFileAnns :: FilePath -> IO [Ann]
readFileAnns file = do
    enc <- mkTextEncoding "UTF-8//IGNORE" -- ignore unknown characters
    mapMaybe f . lines <$> readFileEncoding' enc file
    where
        f :: String -> Maybe Ann
        f (stripPrefix "-- @" . trim -> Just x) = case word1 $ trim x of
            ("IGNORE",_) -> Just Ignore
            ("SINCE-LF", x) -> Just $ SinceLF $ fromJust $ LF.parseVersion $ trim x
            ("UNTIL-LF", x) -> Just $ UntilLF $ fromJust $ LF.parseVersion $ trim x
            ("ERROR",x) -> Just (DiagnosticFields (DSeverity DsError : parseFields x))
            ("WARN",x) -> Just (DiagnosticFields (DSeverity DsWarning : parseFields x))
            ("INFO",x) -> Just (DiagnosticFields (DSeverity DsInfo : parseFields x))
            ("QUERY-LF", x) -> Just $ QueryLF x
            ("TODO",x) -> Just $ Todo x
            _ -> error $ "Can't understand test annotation in " ++ show file ++ ", got " ++ show x
        f _ = Nothing

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

mainProj :: TestArguments -> IdeState -> FilePath -> (String -> IO ()) -> NormalizedFilePath -> IO LF.Package
mainProj TestArguments{..} service outdir log file = do
    let proj = takeBaseName (fromNormalizedFilePath file)

    -- NOTE (MK): For some reason ghcide’s `prettyPrint` seems to fall over on Windows with `commitBuffer: invalid argument`.
    -- With `fakeDynFlags` things seem to work out fine.
    let corePrettyPrint = timed log "Core pretty-printing" . liftIO . writeFile (outdir </> proj <.> "core") . showSDoc fakeDynFlags . ppr
    let lfSave = timed log "LF saving" . liftIO . writeFileLf (outdir </> proj <.> "dalf")
    let lfPrettyPrint = timed log "LF pretty-printing" . liftIO . writeFile (outdir </> proj <.> "pdalf") . renderPretty
    let jsonSave pkg =
            let json = A.encodePretty $ JSONPB.toJSONPB (encodePackage pkg) JSONPB.jsonPBOptions
            in timed log "JSON saving" . liftIO . BSL.writeFile (outdir </> proj <.> "json") $ json

    setFilesOfInterest service (HashSet.singleton file)
    runActionSync service $ do
            dlint log file
            lf <- lfConvert log file
            lfPrettyPrint lf
            core <- ghcCompile log file
            corePrettyPrint core
            lf <- lfTypeCheck log file
            lfSave lf
            lfRunScenarios log file
            jsonSave lf
            pure lf

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

lfRunScenarios :: (String -> IO ()) -> NormalizedFilePath -> Action ()
lfRunScenarios log file = timed log "LF execution" $ void $ unjust $ runScenarios file

timed :: MonadIO m => (String -> IO ()) -> String -> m a -> m a
timed log msg act = do
    time <- liftIO offsetTime
    res <- act
    time <- liftIO time
    liftIO $ log $ "Time: " ++ msg ++ " = " ++ showDuration time
    return res
