-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}

-- | Main entry-point of the Daml compiler
module DA.Cli.Damlc.Test (
    execTest
    , UseColor(..)
    , ShowCoverage(..)
    , Verbose(..)
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
import DA.Bazel.Runfiles
import DA.Daml.Compiler.Output
import DA.Daml.Package.Config
import Data.IORef
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.PrettyScript as SS
import qualified DA.Daml.LF.ScriptServiceClient as SSC
import DA.Daml.Options.Types
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
import System.Console.ANSI (SGR(..), setSGRCode, Underlining(..), ConsoleIntensity(..), Color(..), ColorIntensity(..), ConsoleLayer(..))
import System.Directory (createDirectoryIfMissing, getCurrentDirectory)
import System.Exit (exitFailure)
import System.FilePath
import System.IO (hFlush, hIsTerminalDevice, hPutStrLn, stderr, stdout)
import System.IO.Error (isPermissionError, isAlreadyExistsError, isDoesNotExistError)
import qualified Text.XML.Light as XML
import qualified Text.Blaze.Html.Renderer.Text as Blaze
import qualified Text.Blaze.Html4.Strict as Blaze
import Text.Regex.TDFA

import ComponentVersion.Class (ComponentVersioned)

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
newtype Verbose = Verbose {getVerbose :: Bool}

-- | Test a Daml file.
execTest
    :: ComponentVersioned
    => [NormalizedFilePath]
    -> RunAllOption
    -> ShowCoverage
    -> UseColor
    -> Verbose
    -> Maybe FilePath
    -> Maybe PackageConfigFields
    -> Options
    -> TableOutputPath
    -> TransactionsOutputPath
    -> CoveragePaths
    -> [CoverageFilter]
    -> Maybe FilePath
    -> IO ()
execTest inFiles runAllOption coverage color verbose mbJUnitOutput mPkgConfig opts tableOutputPath transactionsOutputPath resultsIO coverageFilters mbProjectPath = do
    loggerH <- getLogger opts "test"
    color <- if getUseColor color then pure color else do
        isTTY <- hIsTerminalDevice stdout
        pure $ UseColor isTTY
    let optsWithPkg = case mPkgConfig of
            Just PackageConfigFields{..} -> opts { optMbPackageName = Just pName, optMbPackageVersion = pVersion }
            Nothing -> opts
    diagsRef <- newIORef []
    let bufferedLogger = bufferingDiagnosticsLogger diagsRef
    printTestSuiteBegin color mbProjectPath
    withDamlIdeState optsWithPkg loggerH bufferedLogger $ \h -> do
        summaryResults <- runAndReport h inFiles (optDetailLevel opts) (optDamlLfVersion opts) runAllOption coverage mbJUnitOutput tableOutputPath transactionsOutputPath resultsIO coverageFilters
        hFlush stdout
        flushDiagnostics diagsRef
        diags <- getDiagnostics h
        printSummary color verbose mbProjectPath summaryResults
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
    -> Maybe FilePath
    -> TableOutputPath
    -> TransactionsOutputPath
    -> CoveragePaths
    -> [CoverageFilter]
    -> IO [(TR.LocalOrExternal, ScriptName, Either SSC.Error SSC.ScriptResult)]
runAndReport ideState inFiles lvl lfVersion runAllOption coverage mbJUnitOutput tableOutputPath transactionsOutputPath resultsIO coverageFilters = do
    (localResults, extResults) <- runAllScripts ideState inFiles runAllOption
    let allResults = localResults ++ extResults
    let allPackages = [loe | TR.ScriptResults loe _ _ <- allResults]
    let summaryResults = [(loe, scriptName, res) | TR.ScriptResults loe _ (Just results) <- allResults, (scriptName, res) <- results]

    let newTestResults = TR.scriptResultsToTestResults allPackages allResults
    loadAggregatePrintResults resultsIO coverageFilters coverage (Just newTestResults)

    let doesOutputTablesOrTransactions =
            isJust (getTableOutputPath tableOutputPath) ||
            isJust (getTransactionsOutputPath transactionsOutputPath)
    when doesOutputTablesOrTransactions $ do
      extensionCss <-
        locateResource Resource
          -- //compiler/daml-extension:webview-stylesheet.css
          { resourcesPath = "webview-stylesheet.css"
            -- In a packaged application, this is stored directly underneath the
            -- resources directory because it's the target's only output.
            -- See @bazel_tools/packaging/packaging.bzl@.
          , runfilesPathPrefix = mainWorkspace </> "compiler"
          }
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

    pure summaryResults

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

outputTables :: PrettyLevel -> String -> TableOutputPath -> [TR.ScriptResults] -> IO ()
outputTables lvl cssSource (TableOutputPath (Just path)) results =
    let outputs :: [(NamedPath, T.Text)]
        outputs = do
            TR.ScriptResults _ world (Just results) <- results
            (ScriptName scriptName, Right result) <- results
            let activeContracts = SS.activeContractsFromScriptResult result
                tableView = SS.renderTableView lvl world activeContracts (SS.scriptResultNodes result)
                tableSource = TL.toStrict $ Blaze.renderHtml $ do
                    Blaze.style $ Blaze.preEscapedToHtml cssSource
                    fold tableView
                outputFile = path </> ("table-" <> T.unpack scriptName <> ".html")
                outputFileName = "Test table output file '" <> outputFile <> "'"
            pure (NamedPath outputFileName outputFile, tableSource)
    in
    outputUnderDir
        (NamedPath ("Test table output directory '" ++ path ++ "'") path)
        outputs
outputTables _ _ _ _ = pure ()

outputTransactions :: PrettyLevel -> String -> TransactionsOutputPath -> [TR.ScriptResults] -> IO ()
outputTransactions lvl cssSource (TransactionsOutputPath (Just path)) results =
    let outputs :: [(NamedPath, T.Text)]
        outputs = do
            TR.ScriptResults _ world (Just results) <- results
            (ScriptName scriptName, Right result) <- results
            let activeContracts = SS.activeContractsFromScriptResult result
                transView = SS.renderTransactionView lvl world activeContracts result
                transSource = TL.toStrict $ Blaze.renderHtml $ do
                    Blaze.style $ Blaze.preEscapedToHtml cssSource
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


projectPathSuffix :: Maybe FilePath -> String
projectPathSuffix = maybe "" (\p -> " for \"" <> p <> "\"")

makeRelativePath :: FilePath -> IO FilePath
makeRelativePath path = do
    cwd <- getCurrentDirectory
    pure $ makeRelative cwd path

printTestSuiteBegin :: UseColor -> Maybe FilePath -> IO ()
printTestSuiteBegin color mbProjectPath =
    whenJust mbProjectPath $ \projectPath -> do
      relativeProjectPath <- makeRelativePath projectPath
      let colored = getUseColor color
      putStrLn $
        (if colored then setSGRCode [SetConsoleIntensity BoldIntensity] else "")
        <> "Running tests (" <> relativeProjectPath <> ") ..."
        <> (if colored then setSGRCode [] else "")

printSummary :: UseColor -> Verbose -> Maybe FilePath -> [(TR.LocalOrExternal, ScriptName, Either SSC.Error SSC.ScriptResult)] -> IO ()
printSummary color verbose mbProjectPath res =
  liftIO $ do
    let nFailed = length [() | (_, _, Left _) <- res]
        nPassed = length res - nFailed
        colored = getUseColor color
        countLine
          | nFailed > 0 =
              (if colored then setSGRCode [SetColor Foreground Vivid Red] else "")
              <> show nFailed <> " failed"
              <> (if colored then setSGRCode [] else "")
              <> ", " <> show nPassed <> " passed"
          | otherwise =
              (if colored then setSGRCode [SetColor Foreground Vivid Green] else "")
              <> show nPassed <> " passed"
              <> (if colored then setSGRCode [] else "")
        resultsToShow
          | getVerbose verbose || nFailed == 0 = res
          | otherwise = [r | r@(_, _, Left _) <- res]

    -- Print combined header line
    pathToShow <- case mbProjectPath of
        Just p -> Just <$> makeRelativePath p
        Nothing -> pure Nothing
    putStrLn $
      (if colored then setSGRCode [SetUnderlining SingleUnderline, SetConsoleIntensity BoldIntensity] else "")
      <> "Test summary" <> projectPathSuffix pathToShow <> ": "
      <> (if colored then setSGRCode [] else "")
      <> countLine

    -- Print failing tests section if there are failures
    when (nFailed > 0) $ do
      putStrLn ""
      putStrLn $
        (if colored then setSGRCode [SetConsoleIntensity BoldIntensity] else "")
        <> "Failing tests:"
        <> (if colored then setSGRCode [] else "")

    printScriptResults color mbProjectPath resultsToShow

printScriptResults :: UseColor -> Maybe FilePath -> [(TR.LocalOrExternal, ScriptName, Either SSC.Error SS.ScriptResult)] -> IO ()
printScriptResults color mbProjectPath results = do
    liftIO $ forM_ results $ \(loe, ScriptName scriptName, resultOrErr) -> do
      loeName <- case loe of
        TR.Local file _ -> do
          let absPath = fromNormalizedFilePath file
          -- Make path relative to project path if available, otherwise relative to CWD
          relativePath <- case mbProjectPath of
            Just projectPath -> pure $ makeRelative projectPath absPath
            Nothing -> makeRelativePath absPath
          pure $ T.pack relativePath
        TR.External _ -> pure $ TR.localOrExternalName loe
      let name = DA.Pretty.pretty loeName <> ":" <> DA.Pretty.pretty scriptName
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
