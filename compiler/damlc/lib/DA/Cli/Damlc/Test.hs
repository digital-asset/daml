-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}

-- | Main entry-point of the Daml compiler
module DA.Cli.Damlc.Test (
    execTest
    , UseColor(..)
    , ShowCoverage(..)
    , RunAllTests(..)
    , TableOutputPath(..)
    , TransactionsOutputPath(..)
    , CoveragePaths(..)
    , LoadCoverageOnly(..)
    , CoverageFilter(..)
    , loadAggregatePrintResults
    -- , Summarize(..)
    ) where

import Control.Exception
import Control.Monad.Except
import Control.Monad.Extra
import DA.Cli.Damlc.Test.TestResults qualified as TR
import DA.Daml.Compiler.Output
import DA.Daml.LF.Ast qualified as LF
import DA.Daml.LF.PrettyScenario qualified as SS
import DA.Daml.LF.ScenarioServiceClient qualified as SSC
import DA.Daml.Options.Types
import DA.Daml.Project.Consts (sdkPathEnvVar)
import DA.Pretty (PrettyLevel)
import DA.Pretty qualified
import DA.Pretty qualified as Pretty
import Data.Foldable (fold)
import Data.HashSet qualified as HashSet
import Data.List.Extra
import Data.Maybe
import Data.Text qualified as T
import Data.Text.IO qualified as TIO
import Data.Text.Lazy qualified as TL
import Data.Tuple.Extra
import Data.Vector qualified as V
import Development.IDE.Core.API
import Development.IDE.Core.IdeState.Daml
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Service.Daml
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import Development.Shake qualified as Shake
import Safe
import ScenarioService qualified as SS
import System.Console.ANSI (SGR(..), setSGRCode, Underlining(..), ConsoleIntensity(..))
import System.Directory (createDirectoryIfMissing)
import System.Environment.Blank
import System.Exit (exitFailure)
import System.FilePath
import System.IO (hPutStrLn, stderr)
import System.IO.Error (isPermissionError, isAlreadyExistsError, isDoesNotExistError)
import Text.Blaze.Html.Renderer.Text qualified as Blaze
import Text.Blaze.Html4.Strict qualified as Blaze
import Text.Regex.TDFA
import Text.XML.Light qualified as XML

newtype UseColor = UseColor {getUseColor :: Bool}
newtype ShowCoverage = ShowCoverage {getShowCoverage :: Bool}
newtype CoverageFilter = CoverageFilter {getCoverageFilter :: Regex}
newtype RunAllTests = RunAllTests {getRunAllTests :: Bool}
newtype TableOutputPath = TableOutputPath {getTableOutputPath :: Maybe String}
newtype TransactionsOutputPath = TransactionsOutputPath {getTransactionsOutputPath :: Maybe String}
data CoveragePaths = CoveragePaths
    { loadCoveragePaths :: [String]
    , saveCoveragePath :: Maybe String
    }
newtype LoadCoverageOnly = LoadCoverageOnly {getLoadCoverageOnly :: Bool}

-- | Test a Daml file.
execTest :: [NormalizedFilePath] -> RunAllTests -> ShowCoverage -> UseColor -> Maybe FilePath -> Options -> TableOutputPath -> TransactionsOutputPath -> CoveragePaths -> [CoverageFilter] -> IO ()
execTest inFiles runAllTests coverage color mbJUnitOutput opts tableOutputPath transactionsOutputPath resultsIO coverageFilters = do
    loggerH <- getLogger opts "test"
    withDamlIdeState opts loggerH diagnosticsLogger $ \h -> do
        testRun h inFiles (optDetailLevel opts) (optDamlLfVersion opts) runAllTests coverage color mbJUnitOutput tableOutputPath transactionsOutputPath resultsIO coverageFilters
        diags <- getDiagnostics h
        when (any (\(_, _, diag) -> Just DsError == _severity diag) diags) exitFailure

loadAggregatePrintResults :: CoveragePaths -> [CoverageFilter] -> ShowCoverage -> Maybe TR.TestResults -> IO ()
loadAggregatePrintResults resultsIO coverageFilters coverage mbNewTestResults = do
    loadedTestResults <- forM (loadCoveragePaths resultsIO) $ \trPath -> do
        let np = NamedPath ("Input test result '" ++ trPath ++ "'") trPath
        tryWithPath TR.loadTestResults np
    let aggregatedTestResults = fold mbNewTestResults <> fold (catMaybes (catMaybes loadedTestResults))

    -- print total test coverage
    TR.printTestCoverageWithFilters
        (getShowCoverage coverage)
        (map getCoverageFilter coverageFilters)
        aggregatedTestResults

    case saveCoveragePath resultsIO of
      Just saveCoveragePath -> do
          let np = NamedPath ("Results output path from --save-coverage '" ++ saveCoveragePath ++ "'") saveCoveragePath
          _ <- tryWithPath (flip TR.saveTestResults aggregatedTestResults) np
          pure ()
      _ -> pure ()

testRun ::
       IdeState
    -> [NormalizedFilePath]
    -> PrettyLevel
    -> LF.Version
    -> RunAllTests
    -> ShowCoverage
    -> UseColor
    -> Maybe FilePath
    -> TableOutputPath
    -> TransactionsOutputPath
    -> CoveragePaths
    -> [CoverageFilter]
    -> IO ()
testRun h inFiles lvl lfVersion (RunAllTests runAllTests) coverage color mbJUnitOutput tableOutputPath transactionsOutputPath resultsIO coverageFilters = do
    -- make sure none of the files disappear
    liftIO $ setFilesOfInterest h (HashSet.fromList inFiles)

    -- take the transitive closure of all imports and run on all of them
    -- If some dependencies can't be resolved we'll get a Diagnostic out anyway, so don't worry
    deps <- runActionSync h $ mapM getDependencies inFiles
    let files = nubOrd $ concat $ inFiles : catMaybes deps

    -- get all external dependencies
    extPkgs <- fmap (nubSortOn LF.extPackageId . concat) $ runActionSync h $
      Shake.forP files $ \file -> getExternalPackages file

    results <- runActionSync h $ do
        Shake.forP files $ \file -> do
            world <- worldForFile file
            mod <- moduleForScenario file
            mbScenarioResults <- runScenarios file
            mbScriptResults <- runScripts file
            let mbResults = liftM2 (++) mbScenarioResults mbScriptResults
            return (world, file, mod, mbResults)

    extResults <-
        if runAllTests
        then case headMay inFiles of
                 Nothing -> pure [] -- nothing to test
                 Just file ->
                     runActionSync h $
                     forM extPkgs $ \pkg -> do
                         (_fileDiagnostics, mbResults) <- runScenariosScriptsPkg file pkg extPkgs
                         pure (pkg, mbResults)
        else pure []

    let -- All Packages / Modules mentioned somehow
        allPackages :: [TR.LocalOrExternal]
        allPackages = [TR.Local mod | (_, _, mod, _) <- results] ++ map TR.External extPkgs

        -- All results: subset of packages / modules that actually got scenarios run
        allResults :: [(TR.LocalOrExternal, [(VirtualResource, Either SSC.Error SS.ScenarioResult)])]
        allResults =
            [(TR.Local mod, result) | (_world, _file, mod, Just result) <- results]
            ++ [(TR.External pkg, result) | (pkg, Just result) <- extResults]

    -- print test summary after all tests have run
    printSummary color (concatMap snd allResults)

    let newTestResults = TR.scenarioResultsToTestResults allPackages allResults
    loadAggregatePrintResults resultsIO coverageFilters coverage (Just newTestResults)

    mbSdkPath <- getEnv sdkPathEnvVar
    let doesOutputTablesOrTransactions =
            isJust (getTableOutputPath tableOutputPath) ||
            isJust (getTransactionsOutputPath transactionsOutputPath)
    when doesOutputTablesOrTransactions $
        case mbSdkPath of
          Nothing -> pure ()
          Just sdkPath -> do
            let cssPath = sdkPath </> "studio/webview-stylesheet.css"
            extensionCss <-
                catchJust
                    (guard . isDoesNotExistError)
                    (Just <$> readFile cssPath)
                    (const $ do
                        hPutStrLn stderr $ "Warning: Could not open stylesheet '" <> cssPath <> "' for tables and transactions, will style plainly"
                        pure Nothing)
            outputTables lvl extensionCss tableOutputPath results
            outputTransactions lvl extensionCss transactionsOutputPath results

    whenJust mbJUnitOutput $ \junitOutput -> do
        createDirectoryIfMissing True $ takeDirectory junitOutput
        res <- forM results $ \(_world, file, _mod, resultM) -> do
            case resultM of
                Nothing -> fmap (file, ) $ runActionSync h $ failedTestOutput h file
                Just scenarioResults -> do
                    let render =
                            either
                                (Just . T.pack . DA.Pretty.renderPlainOneLine . prettyErr lvl lfVersion)
                                (const Nothing)
                    pure (file, map (second render) scenarioResults)
        writeFile junitOutput $ XML.showTopElement $ toJUnit res

data NamedPath = NamedPath { np_name :: String, np_path :: FilePath }
    deriving (Show, Eq, Ord)

tryWithPath :: (FilePath -> IO a) -> NamedPath -> IO (Maybe a)
tryWithPath action NamedPath {..} =
    handleJust select printer (Just <$> action np_path)
    where
        printer :: String -> IO (Maybe a)
        printer msg = do
            hPutStrLn stderr msg
            pure Nothing

        select :: IOError -> Maybe String
        select err
          | isPermissionError err =
              Just $ np_name ++ " cannot be created because of unsufficient permissions."
          | isAlreadyExistsError err =
              Just $ np_name ++ " cannot be created because it already exists."
          | isDoesNotExistError err =
              Just $ np_name ++ " cannot be created because its parent directory does not exist."
          | otherwise =
              Nothing

outputUnderDir :: NamedPath -> [(NamedPath, T.Text)] -> IO ()
outputUnderDir dir paths = do
    dirSuccess <- tryWithPath (createDirectoryIfMissing True) dir
    case dirSuccess of
      Nothing -> pure ()
      Just _ ->
          forM_ paths $ \(file, content) -> do
            _ <- tryWithPath (flip TIO.writeFile content) file
            pure ()

outputTables :: PrettyLevel -> Maybe String -> TableOutputPath -> [(LF.World, NormalizedFilePath, LF.Module, Maybe [(VirtualResource, Either SSC.Error SS.ScenarioResult)])] -> IO ()
outputTables lvl cssSource (TableOutputPath (Just path)) results =
    let outputs :: [(NamedPath, T.Text)]
        outputs = do
            (world, _, _, Just results) <- results
            (vr, Right result) <- results
            let activeContracts = SS.activeContractsFromScenarioResult result
                tableView = SS.renderTableView lvl world activeContracts (SS.scenarioResultNodes result)
                tableSource = TL.toStrict $ Blaze.renderHtml $ do
                    foldMap (Blaze.style . Blaze.preEscapedToHtml) cssSource
                    fold tableView
                outputFile = path </> ("table-" <> T.unpack (vrScenarioName vr) <> ".html")
                outputFileName = "Test table output file '" <> outputFile <> "'"
            pure (NamedPath outputFileName outputFile, tableSource)
    in
    outputUnderDir
        (NamedPath ("Test table output directory '" ++ path ++ "'") path)
        outputs
outputTables _ _ _ _ = pure ()

outputTransactions :: PrettyLevel -> Maybe String -> TransactionsOutputPath -> [(LF.World, NormalizedFilePath, LF.Module, Maybe [(VirtualResource, Either SSC.Error SS.ScenarioResult)])] -> IO ()
outputTransactions lvl cssSource (TransactionsOutputPath (Just path)) results =
    let outputs :: [(NamedPath, T.Text)]
        outputs = do
            (world, _, _, Just results) <- results
            (vr, Right result) <- results
            let activeContracts = SS.activeContractsFromScenarioResult result
                transView = SS.renderTransactionView lvl world activeContracts result
                transSource = TL.toStrict $ Blaze.renderHtml $ do
                    foldMap (Blaze.style . Blaze.preEscapedToHtml) cssSource
                    transView
                outputFile = path </> ("transaction-" <> T.unpack (vrScenarioName vr) <> ".html")
                outputFileName = "Test transaction output file '" <> outputFile <> "'"
            pure (NamedPath outputFileName outputFile, transSource)
    in
    outputUnderDir
        (NamedPath ("Test transaction output directory '" ++ path ++ "'") path)
        outputs
outputTransactions _ _ _ _ = pure ()

-- We didn't get scenario results, so we use the diagnostics as the error message for each scenario.
failedTestOutput :: IdeState -> NormalizedFilePath -> Action [(VirtualResource, Maybe T.Text)]
failedTestOutput h file = do
    mbScenarioNames <- getScenarioNames file
    diagnostics <- liftIO $ getDiagnostics h
    let errMsg = showDiagnostics diagnostics
    pure $ map (, Just errMsg) $ fromMaybe [VRScenario file "Unknown"] mbScenarioNames


printSummary :: UseColor -> [(VirtualResource, Either SSC.Error SSC.ScenarioResult)] -> IO ()
printSummary color res =
  liftIO $ do
    putStrLn $
      unlines
        [ setSGRCode [SetUnderlining SingleUnderline, SetConsoleIntensity BoldIntensity]
        , "Test Summary" <> setSGRCode []
        ]
    printScenarioResults color res

printScenarioResults :: UseColor -> [(VirtualResource, Either SSC.Error SS.ScenarioResult)] -> IO ()
printScenarioResults color results = do
    liftIO $ forM_ results $ \(VRScenario vrFile vrName, resultOrErr) -> do
      let name = DA.Pretty.string (fromNormalizedFilePath vrFile) <> ":" <> DA.Pretty.pretty vrName
      let stringStyleToRender = if getUseColor color then DA.Pretty.renderColored else DA.Pretty.renderPlain
      putStrLn $ stringStyleToRender $
        case resultOrErr of
          Left _err -> name <> ": " <> DA.Pretty.error_ "failed"
          Right result -> name <> ": " <> prettyResult result


prettyErr :: PrettyLevel -> LF.Version -> SSC.Error -> DA.Pretty.Doc Pretty.SyntaxClass
prettyErr lvl lfVersion err = case err of
    SSC.BackendError berr ->
        DA.Pretty.string (show berr)
    SSC.ScenarioError serr ->
        SS.prettyBriefScenarioError
          lvl
          (LF.initWorld [] lfVersion)
          serr
    SSC.ExceptionError e -> DA.Pretty.string $ show e


prettyResult :: SS.ScenarioResult -> DA.Pretty.Doc Pretty.SyntaxClass
prettyResult result =
    let nTx = length (SS.scenarioResultScenarioSteps result)
        nActive = length $ filter (SS.isActive (SS.activeContractsFromScenarioResult result)) (V.toList (SS.scenarioResultNodes result))
    in DA.Pretty.typeDoc_ "ok, "
    <> DA.Pretty.int nActive <> DA.Pretty.typeDoc_ " active contracts, "
    <> DA.Pretty.int nTx <> DA.Pretty.typeDoc_ " transactions."


toJUnit :: [(NormalizedFilePath, [(VirtualResource, Maybe T.Text)])] -> XML.Element
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
        handleFile :: (NormalizedFilePath, [(VirtualResource, Maybe T.Text)]) -> XML.Element
        handleFile (f, vrs) =
            XML.node
                (XML.unqual "testsuite")
                ([ XML.Attr (XML.unqual "name") (fromNormalizedFilePath f)
                 , XML.Attr (XML.unqual "tests") (show $ length vrs)
                 ],
                 map (handleVR f) vrs)
        handleVR :: NormalizedFilePath -> (VirtualResource, Maybe T.Text) -> XML.Element
        handleVR f (vr, mbErr) =
            XML.node
                (XML.unqual "testcase")
                ([ XML.Attr (XML.unqual "name") (T.unpack $ vrScenarioName vr)
                 , XML.Attr (XML.unqual "classname") (fromNormalizedFilePath f)
                 ],
                 maybe [] (\err -> [XML.node (XML.unqual "failure") (T.unpack err)]) mbErr
                )
