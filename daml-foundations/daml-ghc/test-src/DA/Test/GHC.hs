-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Test driver for DAML-GHC CompilerService.
-- For each file, compile it with GHC, convert it,
-- typecheck with LF, test it.  Test annotations are documented as 'Ann'.
module DA.Test.GHC
  ( main
  , mainAll
  ) where

import           DA.Bazel.Runfiles
import           DA.Daml.GHC.Compiler.Options
import           DA.Daml.GHC.Compiler.UtilLF
import           DA.Test.Util (standardizeQuotes)

import           DA.Daml.LF.Ast as LF hiding (IsTest)
import           "ghc-lib-parser" UniqSupply
import           "ghc-lib-parser" Unique

import           Control.Lens.Plated (transformOn)
import           Control.Concurrent.Extra
import           Control.DeepSeq
import           Control.Exception.Extra
import           Control.Monad
import           Control.Monad.IO.Class
import           DA.Daml.LF.Proto3.EncodeV1
import           DA.Pretty hiding (first)
import qualified DA.Service.Daml.Compiler.Impl.Scenario as SS
import qualified DA.Service.Logger.Impl.Pure as Logger
import qualified Development.IDE.Types.Logger as IdeLogger
import Development.IDE.Types.Location
import qualified Data.Aeson as A
import qualified Data.Aeson.Lens as A
import           Data.ByteString.Lazy.Char8 (unpack)
import           Data.Char
import qualified Data.DList as DList
import Data.Foldable
import           Data.List.Extra
import Data.IORef
import Data.Proxy
import           Development.IDE.Types.Diagnostics
import           Data.Maybe
import           Development.Shake hiding (cmd, withResource)
import           System.Directory.Extra
import           System.Environment.Blank (setEnv)
import           System.FilePath
import           System.Process (readProcess)
import           System.IO
import           System.IO.Extra
import           System.Info.Extra (isWindows)
import           Text.Read
import qualified Data.Set as Set
import qualified Data.Text as T
import           System.Time.Extra
import qualified Development.IDE.State.API as Compile
import qualified Development.IDE.State.Rules.Daml as Compile
import qualified Development.IDE.Types.Diagnostics as D
import Development.IDE.GHC.Util
import           Data.Tagged                  (Tagged (..))
import qualified GHC
import qualified Proto3.Suite.JSONPB as JSONPB

import Test.Tasty
import qualified Test.Tasty.HUnit as HUnit
import Test.Tasty.HUnit ((@?=))
import Test.Tasty.Options
import Test.Tasty.Providers
import Test.Tasty.Runners

-- Newtype to avoid mixing up the loging function and the one for registering TODOs.
newtype TODO = TODO String

main :: IO ()
main = mainWithVersions [versionDev]

mainAll :: IO ()
mainAll = mainWithVersions (delete versionDev supportedInputVersions)

mainWithVersions :: [Version] -> IO ()
mainWithVersions versions = SS.withScenarioService Logger.makeNopHandle $ \scenarioService -> do
  hSetEncoding stdout utf8
  setEnv "TASTY_NUM_THREADS" "1" True
  todoRef <- newIORef DList.empty
  let registerTODO (TODO s) = modifyIORef todoRef (`DList.snoc` ("TODO: " ++ s))
  integrationTests <- mapM (getIntegrationTests registerTODO scenarioService) versions
  let tests = testGroup "All" $ uniqueUniques : integrationTests
  defaultMainWithIngredients ingredients tests
    `finally` (do
    todos <- readIORef todoRef
    putStr (unlines (DList.toList todos)))
  where ingredients =
          includingOptions [Option (Proxy :: Proxy PackageDb)] :
          defaultIngredients

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

getIntegrationTests :: (TODO -> IO ()) -> SS.Handle -> Version -> IO TestTree
getIntegrationTests registerTODO scenarioService version = do
    putStrLn $ "rtsSupportsBoundThreads: " ++ show rtsSupportsBoundThreads
    do n <- getNumCapabilities; putStrLn $ "getNumCapabilities: " ++ show n

    -- test files are declared as data in BUILD.bazel
    testsLocation <- locateRunfiles $ mainWorkspace </> "daml-foundations/daml-ghc/tests"
    damlTestFiles <- filter (".daml" `isExtensionOf`) <$> listFiles testsLocation
    -- only run Test.daml (see https://github.com/digital-asset/daml/issues/726)
    bondTradingLocation <- locateRunfiles $ mainWorkspace </> "daml-foundations/daml-ghc/bond-trading"
    let allTestFiles = damlTestFiles ++ [bondTradingLocation </> "Test.daml"]

    let outdir = "daml-foundations/daml-ghc/output"
    createDirectoryIfMissing True outdir

    opts <- defaultOptionsIO (Just version)
    opts <- pure $ opts
        { optThreads = 0
        , optScenarioValidation = ScenarioValidationFull
        }

    -- initialise the compiler service
    vfs <- Compile.makeVFSHandle
    pure $
      withResource
      (Compile.initialise (Compile.mainRule opts) (const $ pure ()) IdeLogger.makeNopHandle opts vfs (Just scenarioService))
      Compile.shutdown $ \service ->
      withTestArguments $ \args -> testGroup ("Tests for DAML-LF " ++ renderPretty version) $
        map (testCase args version service outdir registerTODO) allTestFiles

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

testCase :: TestArguments -> LF.Version -> IO Compile.IdeState -> FilePath -> (TODO -> IO ()) -> FilePath -> TestTree
testCase args version getService outdir registerTODO file = singleTest file . TestCase $ \log -> do
  service <- getService
  anns <- readFileAnns file
  if any (ignoreVersion version) anns
    then pure $ Result
      { resultOutcome = Success
      , resultDescription = ""
      , resultShortDescription = "IGNORE"
      , resultTime = 0
      }
    else do
      Compile.unsafeClearDiagnostics service
      ex <- try $ mainProj args service outdir log (toNormalizedFilePath file) :: IO (Either SomeException Package)
      diags <- Compile.getDiagnostics service
      for_ [file ++ ", " ++ x | Todo x <- anns] (registerTODO . TODO)
      resDiag <- checkDiagnostics log [fields | DiagnosticFields fields <- anns] $
        [ideErrorText "" $ T.pack $ show e | Left e <- [ex], not $ "_IGNORE_" `isInfixOf` show e] ++ diags
      resQueries <- runJqQuery log [(pkg, q) | Right pkg <- [ex], QueryLF q <- anns]
      let failures = catMaybes $ resDiag : resQueries
      case failures of
        err : _others -> pure $ testFailed err
        [] -> pure $ testPassed ""
  where
    ignoreVersion version = \case
      Ignore -> True
      SinceLF minVersion -> version < minVersion
      _ -> False

runJqQuery :: (String -> IO ()) -> [(LF.Package, String)] -> IO [Maybe String]
runJqQuery log qs = do
  forM qs $ \(pkg, q) -> do
    log $ "running jq query: " ++ q
    let json = unpack $ A.encode $ transformOn A._Value numToString $ JSONPB.toJSONPB (encodePackage pkg) JSONPB.jsonPBOptions
    let jqKey = "external" </> "jq_dev_env" </> "bin" </> if isWindows then "jq.exe" else "jq"
    jq <- locateRunfiles $ mainWorkspace </> jqKey
    out <- readProcess jq [q] json
    case trim out of
      "true" -> pure Nothing
      other -> pure $ Just $ "jq query failed: got " ++ other
  where
    numToString :: A.Value -> A.Value
    numToString = \case
      A.Number x -> A.String $ T.pack $ show x
      other -> other


data DiagnosticField
  = DFilePath !FilePath
  | DRange !Range
  | DSeverity !DiagnosticSeverity
  | DSource !String
  | DMessage !String
  deriving (Eq, Show)

checkDiagnostics :: (String -> IO ()) -> [[DiagnosticField]] -> [D.FileDiagnostic] -> IO (Maybe String)
checkDiagnostics log expected got = do
    when (got /= []) $
        log $ T.unpack $ showDiagnostics got

    -- you require the same number of diagnostics as expected
    -- and each diagnostic is at least partially expected
    let bad = filter
            (\expFields -> not $ any (\diag -> all (checkField diag) expFields) got)
            expected
    pure $ if
      | length expected /= length got -> Just $ "Wrong number of diagnostics, expected " ++ show (length expected) ++ ", but got " ++ show (length got)
      | null bad -> Nothing
      | otherwise -> Just $ unlines ("Could not find matching diagnostics:" : map show bad)
    where checkField :: D.FileDiagnostic -> DiagnosticField -> Bool
          checkField (fp, D.Diagnostic{..}) f = case f of
            DFilePath p -> toNormalizedFilePath p == fp
            DRange r -> r == _range
            DSeverity s -> Just s == _severity
            DSource s -> Just (T.pack s) == _source
            DMessage m -> standardizeQuotes(T.pack m) `T.isInfixOf` standardizeQuotes(T.unwords (T.words _message))

------------------------------------------------------------
-- CLI argument handling

newtype PackageDb = PackageDb String

instance IsOption PackageDb where
  defaultValue = PackageDb "bazel-bin/daml-foundations/daml-ghc/package-database"
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
    | DiagnosticFields [DiagnosticField] -- I expect a diagnostic that has the given fields
    | QueryLF String                       -- The jq query against the produced DAML-LF returns "true"
    | Todo String                        -- Just a note that is printed out
      deriving Eq


readFileAnns :: FilePath -> IO [Ann]
readFileAnns file = do
    enc <- mkTextEncoding "UTF-8//IGNORE" -- ignore unknown characters
    mapMaybe f . lines <$> readFileEncoding' enc file
    where
        f :: String -> Maybe Ann
        f (stripPrefix "-- @" . trim -> Just x) = case word1 $ trim x of
            ("IGNORE",_) -> Just Ignore
            ("SINCE-LF", x) -> Just $ SinceLF $ fromJust $ LF.parseVersion $ trim x
            ("ERROR",x) -> Just (DiagnosticFields (DSeverity DsError : parseFields x))
            ("WARN",x) -> Just (DiagnosticFields (DSeverity DsWarning : parseFields x))
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
            (Position (rowStart - 1) (colStart - 1))
            (Position (rowEnd - 1) (colEnd - 1))
    _ -> error $ "Failed to parse range, got " ++ s

mainProj :: TestArguments -> Compile.IdeState -> FilePath -> (String -> IO ()) -> NormalizedFilePath -> IO LF.Package
mainProj TestArguments{..} service outdir log file = do
    writeFile <- return $ \a b -> length b `seq` writeFile a b
    let proj = takeBaseName (fromNormalizedFilePath file)

    let corePrettyPrint = timed log "Core pretty-printing" . liftIO . writeFile (outdir </> proj <.> "core") . unlines . map prettyPrint
    let lfSave = timed log "LF saving" . liftIO . writeFileLf (outdir </> proj <.> "dalf")
    let lfPrettyPrint = timed log "LF pretty-printing" . liftIO . writeFile (outdir </> proj <.> "pdalf") . renderPretty

    Compile.setFilesOfInterest service (Set.singleton file)
    Compile.runActionSync service $ do
            cores <- ghcCompile log file
            corePrettyPrint cores
            lf <- lfConvert log file
            lfPrettyPrint lf
            lf <- lfTypeCheck log file
            lfSave lf
            lfRunScenarios log file
            pure lf

unjust :: Action (Maybe b) -> Action b
unjust act = do
    res <- act
    case res of
      Nothing -> fail "_IGNORE_"
      Just v -> return v

ghcCompile :: (String -> IO ()) -> NormalizedFilePath -> Action [GHC.CoreModule]
ghcCompile log file = timed log "GHC compile" $ unjust $ Compile.getGhcCore file

lfConvert :: (String -> IO ()) -> NormalizedFilePath -> Action LF.Package
lfConvert log file = timed log "LF convert" $ unjust $ Compile.getRawDalf file

lfTypeCheck :: (String -> IO ()) -> NormalizedFilePath -> Action LF.Package
lfTypeCheck log file = timed log "LF type check" $ unjust $ Compile.getDalf file

lfRunScenarios :: (String -> IO ()) -> NormalizedFilePath -> Action ()
lfRunScenarios log file = timed log "LF execution" $ void $ unjust $ Compile.runScenarios file

timed :: MonadIO m => (String -> IO ()) -> String -> m a -> m a
timed log msg act = do
    time <- liftIO offsetTime
    res <- act
    time <- liftIO time
    liftIO $ log $ "Time: " ++ msg ++ " = " ++ showDuration time
    return res
