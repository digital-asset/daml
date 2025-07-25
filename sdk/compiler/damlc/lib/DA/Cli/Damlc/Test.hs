-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}

-- | Main entry-point of the Daml compiler
module DA.Cli.Damlc.Test (
    execTest
    , UseColor(..)
    , ShowCoverage(..)
    , RunAllOption(..)
    , TableOutputPath(..)
    , TransactionsOutputPath(..)
    , CoveragePaths(..)
    , LoadCoverageOnly(..)
    , CoverageFilter(..)
    , loadAggregatePrintResults
    -- , Summarize(..)
    -- Exposed for testing
    , runAllScripts
    ) where

import Control.Exception
import Control.Monad.Except
import Control.Monad.Extra
import DA.Daml.Compiler.Output
import DA.Daml.Package.Config
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.PrettyScript as SS
import qualified DA.Daml.LF.ScriptServiceClient as SSC
import DA.Daml.Options.Types
import DA.Daml.Project.Consts (sdkPathEnvVar)
import DA.Pretty (PrettyLevel)
import qualified DA.Pretty
import qualified DA.Pretty as Pretty
import Data.Foldable (fold)
import qualified Data.HashSet as HashSet
import Data.List.Extra
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Text.Lazy as TL
import Data.Tuple.Extra
import qualified Data.Vector as V
import Development.IDE.Core.API
import Development.IDE.Core.IdeState.Daml
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Rules.Daml
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import qualified Development.Shake as Shake
import Safe
import qualified ScriptService as SS
import qualified DA.Cli.Damlc.Test.TestResults as TR
import System.Console.ANSI (SGR(..), setSGRCode, Underlining(..), ConsoleIntensity(..))
import System.Directory (createDirectoryIfMissing)
import System.Environment.Blank
import System.Exit (exitFailure)
import System.FilePath
import System.IO (hPutStrLn, stderr)
import System.IO.Error (isPermissionError, isAlreadyExistsError, isDoesNotExistError)
import qualified Text.XML.Light as XML
import qualified Text.Blaze.Html.Renderer.Text as Blaze
import qualified Text.Blaze.Html4.Strict as Blaze
import Text.Regex.TDFA

import SdkVersion.Class (SdkVersioned)

newtype UseColor = UseColor {getUseColor :: Bool}
newtype ShowCoverage = ShowCoverage {getShowCoverage :: Bool}
newtype CoverageFilter = CoverageFilter {getCoverageFilter :: Regex}
newtype RunAllOption = RunAllOption {getRunAllTests :: Bool}
newtype TableOutputPath = TableOutputPath {getTableOutputPath :: Maybe String}
newtype TransactionsOutputPath = TransactionsOutputPath {getTransactionsOutputPath :: Maybe String}
data CoveragePaths = CoveragePaths
    { loadCoveragePaths :: [String]
    , saveCoveragePath :: Maybe String
    }
newtype LoadCoverageOnly = LoadCoverageOnly {getLoadCoverageOnly :: Bool}

-- | Test a Daml file.
execTest
    :: SdkVersioned
    => [NormalizedFilePath]
    -> RunAllOption
    -> ShowCoverage
    -> UseColor
    -> Maybe FilePath
    -> Maybe PackageConfigFields
    -> Options
    -> TableOutputPath
    -> TransactionsOutputPath
    -> CoveragePaths
    -> [CoverageFilter]
    -> IO ()
execTest inFiles runAllOption coverage color mbJUnitOutput mPkgConfig opts tableOutputPath transactionsOutputPath resultsIO coverageFilters = do
    loggerH <- getLogger opts "test"
    let optsWithPkg = case mPkgConfig of
            Just PackageConfigFields{..} -> opts { optMbPackageName = Just pName, optMbPackageVersion = pVersion }
            Nothing -> opts
    withDamlIdeState optsWithPkg loggerH diagnosticsLogger $ \h -> do
        runAndReport h inFiles (optDetailLevel opts) (optDamlLfVersion opts) runAllOption coverage color mbJUnitOutput tableOutputPath transactionsOutputPath resultsIO coverageFilters
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


runAndReport ::
       IdeState
    -> [NormalizedFilePath]
    -> PrettyLevel
    -> LF.Version
    -> RunAllOption
    -> ShowCoverage
    -> UseColor
    -> Maybe FilePath
    -> TableOutputPath
    -> TransactionsOutputPath
    -> CoveragePaths
    -> [CoverageFilter]
    -> IO ()
runAndReport ideState inFiles lvl lfVersion runAllOption coverage color mbJUnitOutput tableOutputPath transactionsOutputPath resultsIO coverageFilters = do
    (localResults, extResults) <- runAllScripts ideState inFiles runAllOption
    let allResults = localResults ++ extResults
    let allPackages = [loe | TR.ScriptResults loe _ _ <- allResults]
    -- print test summary after all tests have run
    printSummary color [(loe, scriptName, res) | TR.ScriptResults loe _ (Just results) <- allResults, (scriptName, res) <- results]

    let newTestResults = TR.scriptResultsToTestResults allPackages allResults
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
            outputTables lvl extensionCss tableOutputPath localResults
            outputTransactions lvl extensionCss transactionsOutputPath localResults

    whenJust mbJUnitOutput $ \junitOutput -> do
        createDirectoryIfMissing True $ takeDirectory junitOutput
        res <- sequence 
            [ case resultM of
                Nothing -> fmap (file, ) $ runActionSync ideState $ failedTestOutput ideState file
                Just scriptResults -> do
                    let render =
                            either
                                (Just . T.pack . DA.Pretty.renderPlainOneLine . prettyErr lvl lfVersion)
                                (const Nothing)
                    pure (file, map (second render) scriptResults)
            | TR.ScriptResults (TR.Local file _) _ resultM <- localResults
            ]
        writeFile junitOutput $ XML.showTopElement $ toJUnit res

runAllScripts :: IdeState -> [NormalizedFilePath] -> RunAllOption -> IO ([TR.ScriptResults], [TR.ScriptResults])
runAllScripts h inFiles (RunAllOption runAllOption) = do
    -- make sure none of the files disappear
    liftIO $ setFilesOfInterest h (HashSet.fromList inFiles)

    -- take the transitive closure of all imports and run on all of them
    -- If some dependencies can't be resolved we'll get a Diagnostic out anyway, so don't worry
    deps <- runActionSync h $ mapM getDependencies inFiles
    let files = nubOrd $ concat $ inFiles : catMaybes deps

    -- get all external dependencies
    extPkgs <- fmap (nubSortOn LF.extPackageId . concat) $ runActionSync h $
      Shake.forP files $ \file -> getExternalPackages file

    localResults <- runActionSync h $ do
        Shake.forP files $ \file -> do
            world <- worldForFile file
            mod <- moduleForScript file
            mbResults <- runScripts file
            return $ TR.ScriptResults (TR.Local file mod) world mbResults

    extResults <-
        if runAllOption
        then case headMay inFiles of
                Nothing -> pure [] -- nothing to test
                Just file ->
                    runActionSync h $
                    forM extPkgs $ \pkg -> do
                        mbResults <- runScriptsPkg file pkg
                        let world = LF.initWorldSelf extPkgs (LF.extPackagePkg pkg)
                        return $ TR.ScriptResults (TR.External pkg) world mbResults
        else pure []
    pure (localResults, extResults)

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

outputTables :: PrettyLevel -> Maybe String -> TableOutputPath -> [TR.ScriptResults] -> IO ()
outputTables lvl cssSource (TableOutputPath (Just path)) results =
    let outputs :: [(NamedPath, T.Text)]
        outputs = do
            TR.ScriptResults _ world (Just results) <- results
            (ScriptName scriptName, Right result) <- results
            let activeContracts = SS.activeContractsFromScriptResult result
                tableView = SS.renderTableView lvl world activeContracts (SS.scriptResultNodes result)
                tableSource = TL.toStrict $ Blaze.renderHtml $ do
                    foldMap (Blaze.style . Blaze.preEscapedToHtml) cssSource
                    fold tableView
                outputFile = path </> ("table-" <> T.unpack scriptName <> ".html")
                outputFileName = "Test table output file '" <> outputFile <> "'"
            pure (NamedPath outputFileName outputFile, tableSource)
    in
    outputUnderDir
        (NamedPath ("Test table output directory '" ++ path ++ "'") path)
        outputs
outputTables _ _ _ _ = pure ()

outputTransactions :: PrettyLevel -> Maybe String -> TransactionsOutputPath -> [TR.ScriptResults] -> IO ()
outputTransactions lvl cssSource (TransactionsOutputPath (Just path)) results =
    let outputs :: [(NamedPath, T.Text)]
        outputs = do
            TR.ScriptResults _ world (Just results) <- results
            (ScriptName scriptName, Right result) <- results
            let activeContracts = SS.activeContractsFromScriptResult result
                transView = SS.renderTransactionView lvl world activeContracts result
                transSource = TL.toStrict $ Blaze.renderHtml $ do
                    foldMap (Blaze.style . Blaze.preEscapedToHtml) cssSource
                    transView
                outputFile = path </> ("transaction-" <> T.unpack scriptName <> ".html")
                outputFileName = "Test transaction output file '" <> outputFile <> "'"
            pure (NamedPath outputFileName outputFile, transSource)
    in
    outputUnderDir
        (NamedPath ("Test transaction output directory '" ++ path ++ "'") path)
        outputs
outputTransactions _ _ _ _ = pure ()

-- We didn't get script results, so we use the diagnostics as the error message for each script.
failedTestOutput :: IdeState -> NormalizedFilePath -> Action [(ScriptName, Maybe T.Text)]
failedTestOutput h file = do
    scriptNames <- getScripts file
    diagnostics <- liftIO $ getDiagnostics h
    let errMsg = showDiagnostics diagnostics
    pure $ map (, Just errMsg) scriptNames


printSummary :: UseColor -> [(TR.LocalOrExternal, ScriptName, Either SSC.Error SSC.ScriptResult)] -> IO ()
printSummary color res =
  liftIO $ do
    putStrLn $
      unlines
        [ setSGRCode [SetUnderlining SingleUnderline, SetConsoleIntensity BoldIntensity]
        , "Test Summary" <> setSGRCode []
        ]
    printScriptResults color res

printScriptResults :: UseColor -> [(TR.LocalOrExternal, ScriptName, Either SSC.Error SS.ScriptResult)] -> IO ()
printScriptResults color results = do
    liftIO $ forM_ results $ \(loe, ScriptName scriptName, resultOrErr) -> do
      let name = DA.Pretty.pretty (TR.localOrExternalName loe) <> ":" <> DA.Pretty.pretty scriptName
      let stringStyleToRender = if getUseColor color then DA.Pretty.renderColored else DA.Pretty.renderPlain
      putStrLn $ stringStyleToRender $
        case resultOrErr of
          Left _err -> name <> ": " <> DA.Pretty.error_ "failed"
          Right result -> name <> ": " <> prettyResult result


prettyErr :: PrettyLevel -> LF.Version -> SSC.Error -> DA.Pretty.Doc Pretty.SyntaxClass
prettyErr lvl lfVersion err = case err of
    SSC.BackendError berr ->
        DA.Pretty.string (show berr)
    SSC.ScriptError serr ->
        SS.prettyBriefScriptError
          lvl
          (LF.initWorld [] lfVersion)
          serr
    SSC.ExceptionError e -> DA.Pretty.string $ show e


prettyResult :: SS.ScriptResult -> DA.Pretty.Doc Pretty.SyntaxClass
prettyResult result =
    let nTx = length (SS.scriptResultScriptSteps result)
        nActive = length $ filter (SS.isActive (SS.activeContractsFromScriptResult result)) (V.toList (SS.scriptResultNodes result))
    in DA.Pretty.typeDoc_ "ok, "
    <> DA.Pretty.int nActive <> DA.Pretty.typeDoc_ " active contracts, "
    <> DA.Pretty.int nTx <> DA.Pretty.typeDoc_ " transactions."


toJUnit :: [(NormalizedFilePath, [(ScriptName, Maybe T.Text)])] -> XML.Element
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
        handleFile :: (NormalizedFilePath, [(ScriptName, Maybe T.Text)]) -> XML.Element
        handleFile (f, res) =
            XML.node
                (XML.unqual "testsuite")
                ([ XML.Attr (XML.unqual "name") (fromNormalizedFilePath f)
                 , XML.Attr (XML.unqual "tests") (show $ length res)
                 ],
                 map (handleScript f) res)
        handleScript :: NormalizedFilePath -> (ScriptName, Maybe T.Text) -> XML.Element
        handleScript f (ScriptName scriptName, mbErr) =
            XML.node
                (XML.unqual "testcase")
                ([ XML.Attr (XML.unqual "name") (T.unpack scriptName)
                 , XML.Attr (XML.unqual "classname") (fromNormalizedFilePath f)
                 ],
                 maybe [] (\err -> [XML.node (XML.unqual "failure") (T.unpack err)]) mbErr
                )
