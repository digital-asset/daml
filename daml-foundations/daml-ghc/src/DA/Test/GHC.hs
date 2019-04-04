-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf #-}

-- | Test driver for DAML-GHC CompilerService.
-- For each file, compile it with GHC, convert it,
-- typecheck with LF, test it.  Test annotations are documented as 'Ann'.
module DA.Test.GHC
  ( main
  , mainVersionNewest
  , mainVersionDefault
  ) where

import DA.Daml.GHC.Compiler.Options
import           DA.Daml.GHC.Compiler.UtilLF

import qualified Data.Text.Prettyprint.Doc.Syntax as Pretty

import           DA.Daml.LF.Ast as LF hiding (IsTest)
import           "ghc-lib-parser" UniqSupply
import           "ghc-lib-parser" Unique

import           Control.Lens.Plated (transformOn)
import           Control.Concurrent.Extra
import           Control.DeepSeq
import           Control.Exception.Extra
import           Control.Monad
import           Control.Monad.IO.Class
import Control.Monad.Managed
import           DA.Pretty hiding (first)
import qualified DA.Service.Daml.Compiler.Impl.Scenario as SS
import qualified DA.Service.Logger.Impl.Pure as Logger
import qualified Development.IDE.Logger as IdeLogger
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
import           System.Environment (setEnv)
import           System.FilePath
import           System.Process (readProcess)
import           System.IO.Extra
import           Text.Read
import qualified Data.Set as Set
import qualified Data.Text as T
import           System.Time.Extra
import qualified Development.IDE.State.API as Compile
import qualified Development.IDE.State.Rules.Daml as Compile
import qualified Development.IDE.Types.Diagnostics as D
import Development.IDE.UtilGHC
import           Data.Tagged                  (Tagged (..))
import qualified GHC

import Test.Tasty
import qualified Test.Tasty.HUnit as HUnit
import Test.Tasty.HUnit ((@?=))
import Test.Tasty.Options
import Test.Tasty.Providers
import Test.Tasty.Runners

-- Newtype to avoid mixing up the loging function and the one for registering TODOs.
newtype TODO = TODO String

main :: IO ()
main = mainVersionNewest

mainVersionNewest :: IO ()
mainVersionNewest = mainWithVersion versionNewest

mainVersionDefault :: IO ()
mainVersionDefault = mainWithVersion versionDefault

mainWithVersion :: Version -> IO ()
mainWithVersion version =
  with (SS.startScenarioService (\_ -> pure ()) Logger.makeNopHandle) $ \scenarioService -> do
  setEnv "TASTY_NUM_THREADS" "1"
  todoRef <- newIORef DList.empty
  let registerTODO (TODO s) = modifyIORef todoRef (`DList.snoc` ("TODO: " ++ s))
  integrationTest <- getIntegrationTests registerTODO scenarioService version
  let tests = testGroup "All" [uniqueUniques, integrationTest]
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

    -- test files are declared as data in BUILD.bazel and are copied over relative to the current run file tree
    -- this is equivalent to PWD env variable
    files1 <- filter (".daml" `isExtensionOf`) <$> listFiles "daml-foundations/daml-ghc/tests"
    let files2 = ["daml-foundations/daml-ghc/bond-trading/Test.daml"] -- only run Test.daml (see https://github.com/digital-asset/daml/issues/726)
    let files = files1 ++ files2

    let outdir = "daml-foundations/daml-ghc/output"
    createDirectoryIfMissing True outdir

    opts <- fmap (\opts -> opts { optDamlLfVersion = version, optThreads = 0 } ) defaultOptionsIO

    -- initialise the compiler service
    pure $
      withResource
      (Compile.initialise Compile.mainRule (Just (\_ -> pure ())) IdeLogger.makeNopHandle opts (Just scenarioService))
      Compile.shutdown $ \service ->
      withTestArguments $ \args -> testGroup ("Tests for DAML-LF " ++ renderPretty version) $
        map (testCase args version service outdir registerTODO) files

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
  if any (`elem` [Ignore, IgnoreLfVersion (renderPretty version)]) anns
    then pure $ Result
      { resultOutcome = Success
      , resultDescription = ""
      , resultShortDescription = "IGNORE"
      , resultTime = 0
      }
    else do
      Compile.unsafeClearDiagnostics service
      ex <- try $ mainProj args service outdir log file :: IO (Either SomeException Package)
      diags <- Compile.getDiagnostics service
      for_ [file ++ ", " ++ x | Todo x <- anns] (registerTODO . TODO)
      resDiag <- checkDiagnostics log [fields | DiagnosticFields fields <- anns] $
        [ideErrorText "" $ T.pack $ show e | Left e <- [ex], not $ "_IGNORE_" `isInfixOf` show e] ++ diags
      resQueries <- runJqQuery log [(pkg, q) | Right pkg <- [ex], QueryLF q <- anns]
      let failures = catMaybes $ resDiag : resQueries
      case failures of
        err : _others -> pure $ testFailed err
        [] -> pure $ testPassed ""

runJqQuery :: (String -> IO ()) -> [(LF.Package, String)] -> IO [Maybe String]
runJqQuery log qs = do
  forM qs $ \(pkg, q) -> do
    log $ "running jq query: " ++ q

    let jq = "external" </> "jq" </> "bin" </> "jq"
    let json = unpack $ A.encode $ transformOn A._Value numToString $ A.toJSON pkg
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
  | DSeverity !Severity
  | DSource !String
  | DMessage !String
  deriving (Eq, Show)

checkDiagnostics :: (String -> IO ()) -> [[DiagnosticField]] -> [D.Diagnostic] -> IO (Maybe String)
checkDiagnostics log expected got = do
    when (got /= []) $
        log $ T.unpack $ Pretty.renderPlain $ Pretty.vcat $ map prettyDiagnostic got

    -- you require the same number of diagnostics as expected
    -- and each diagnostic is at least partially expected
    let bad = filter
            (\expFields -> not $ any (\diag -> all (checkField diag) expFields) got)
            expected
    pure $ if
      | length expected /= length got -> Just $ "Wrong number of diagnostics, expected " ++ show (length expected)
      | null bad -> Nothing
      | otherwise -> Just $ unlines ("Could not find matching diagnostics:" : map show bad)
    where checkField :: D.Diagnostic -> DiagnosticField -> Bool
          checkField D.Diagnostic{..} f = case f of
            DFilePath p -> p == dFilePath
            DRange r -> r == dRange
            DSeverity s -> s == dSeverity
            DSource s -> T.pack s == dSource
            DMessage m -> T.pack m `T.isInfixOf` T.unwords (T.words dMessage)

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
    | IgnoreLfVersion String             -- Don't run this test when testing the given DAML-LF version
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
            ("IGNORE-LF", x) -> Just (IgnoreLfVersion (trim x))
            ("ERROR",x) -> Just (DiagnosticFields (DSeverity Error : parseFields x))
            ("WARN",x) -> Just (DiagnosticFields (DSeverity Warning : parseFields x))
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
    _ -> noRange

mainProj :: TestArguments -> Compile.IdeState -> FilePath -> (String -> IO ()) -> FilePath -> IO LF.Package
mainProj TestArguments{..} service outdir log file = do
    writeFile <- return $ \a b -> length b `seq` writeFile a b
    let proj = takeBaseName file

    let corePrettyPrint = timed log "Core pretty-printing" . liftIO . writeFile (outdir </> proj <.> "core") . unlines . map prettyPrint
    let lfSave = timed log "LF saving" . liftIO . writeFileLf (outdir </> proj <.> "dalf")
    let lfPrettyPrint = timed log "LF pretty-printing" . liftIO . writeFile (outdir </> proj <.> "pdalf") . renderPretty

    Compile.setFilesOfInterest service (Set.singleton file)
    pkg <- Compile.runAction service $ lfTypeCheck log file
    void $ Compile.runActions service
        [do
            cores <- ghcCompile log file
            corePrettyPrint cores
        ,do
            lf <- lfConvert log file
            lfPrettyPrint lf
        ,do
            lf <- lfTypeCheck log file
            lfSave lf
            lfRunScenarios log file
        ]
    pure pkg

unjust :: Action (Maybe b) -> Action b
unjust act = do
    res <- act
    case res of
      Nothing -> fail "_IGNORE_"
      Just v -> return v

ghcCompile :: (String -> IO ()) -> FilePath -> Action [GHC.CoreModule]
ghcCompile log file = timed log "GHC compile" $ unjust $ Compile.getGhcCore file

lfConvert :: (String -> IO ()) -> FilePath -> Action LF.Package
lfConvert log file = timed log "LF convert" $ unjust $ Compile.getRawDalf file

lfTypeCheck :: (String -> IO ()) -> FilePath -> Action LF.Package
lfTypeCheck log file = timed log "LF type check" $ unjust $ Compile.getDalf file

lfRunScenarios :: (String -> IO ()) -> FilePath -> Action ()
lfRunScenarios log file = timed log "LF execution" $ void $ unjust $ Compile.runScenarios file

timed :: MonadIO m => (String -> IO ()) -> String -> m a -> m a
timed log msg act = do
    time <- liftIO offsetTime
    res <- act
    time <- liftIO time
    liftIO $ log $ "Time: " ++ msg ++ " = " ++ showDuration time
    return res
