-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Main entry-point of the DAML compiler
module DA.Cli.Damlc.Test (
    execTest
    , UseColor(..)
    , ShowCoverage(..)
    ) where

import Control.Monad.Except
import Control.Monad.Extra
import DA.Cli.Damlc.Base
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.PrettyScenario as SS
import qualified DA.Daml.LF.ScenarioServiceClient as SSC
import DA.Daml.Options.Types
import qualified DA.Pretty
import qualified DA.Pretty as Pretty
import Data.Either
import qualified Data.HashSet as HashSet
import Data.List.Extra
import Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import Data.Tuple.Extra
import qualified Data.Vector as V
import Development.IDE.Core.API
import Development.IDE.Core.IdeState.Daml
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Service.Daml
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import qualified Development.Shake as Shake
import qualified ScenarioService as SS
import System.Directory (createDirectoryIfMissing)
import System.Exit (exitFailure)
import System.FilePath
import qualified Text.XML.Light as XML


newtype UseColor = UseColor {getUseColor :: Bool}
newtype ShowCoverage = ShowCoverage {getShowCoverage :: Bool}

-- | Test a DAML file.
execTest :: [NormalizedFilePath] -> ShowCoverage -> UseColor -> Maybe FilePath -> Options -> IO ()
execTest inFiles coverage color mbJUnitOutput opts = do
    loggerH <- getLogger opts "test"
    withDamlIdeState opts loggerH diagnosticsLogger $ \h -> do
        testRun h inFiles (optDamlLfVersion opts) coverage color mbJUnitOutput
        diags <- getDiagnostics h
        when (any (\(_, _, diag) -> Just DsError == _severity diag) diags) exitFailure


testRun ::
       IdeState
    -> [NormalizedFilePath]
    -> LF.Version
    -> ShowCoverage
    -> UseColor
    -> Maybe FilePath
    -> IO ()
testRun h inFiles lfVersion coverage color mbJUnitOutput  = do
    -- make sure none of the files disappear
    liftIO $ setFilesOfInterest h (HashSet.fromList inFiles)

    -- take the transitive closure of all imports and run on all of them
    -- If some dependencies can't be resolved we'll get a Diagnostic out anyway, so don't worry
    deps <- runActionSync h $ mapM getDependencies inFiles
    let files = nubOrd $ concat $ inFiles : catMaybes deps

    results <- runActionSync h $ do
        dalfs <- Shake.forP files dalfForScenario
        Shake.forP files $ \file -> do
            mbScenarioResults <- runScenarios file
            mbScriptResults <- runScripts file
            results <- case liftM2 (++) mbScenarioResults mbScriptResults of
                Nothing -> failedTestOutput h file
                Just scenarioResults -> do
                    -- failures are printed out through diagnostics, so just print the sucesses
                    let results' = [(v, r) | (v, Right r) <- scenarioResults]
                    liftIO $ printScenarioResults results' color
                    liftIO $ printTestCoverage coverage dalfs scenarioResults
                    let f = either (Just . T.pack . DA.Pretty.renderPlainOneLine . prettyErr lfVersion) (const Nothing)
                    pure $ map (second f) scenarioResults
            pure (file, results)

    whenJust mbJUnitOutput $ \junitOutput -> do
        createDirectoryIfMissing True $ takeDirectory junitOutput
        writeFile junitOutput $ XML.showTopElement $ toJUnit results


-- We didn't get scenario results, so we use the diagnostics as the error message for each scenario.
failedTestOutput :: IdeState -> NormalizedFilePath -> Action [(VirtualResource, Maybe T.Text)]
failedTestOutput h file = do
    mbScenarioNames <- getScenarioNames file
    diagnostics <- liftIO $ getDiagnostics h
    let errMsg = showDiagnostics diagnostics
    pure $ map (, Just errMsg) $ fromMaybe [VRScenario file "Unknown"] mbScenarioNames


printTestCoverage ::
       ShowCoverage
    -> [LF.Module]
    -> [(VirtualResource, Either SSC.Error SS.ScenarioResult)]
    -> IO ()
printTestCoverage ShowCoverage {getShowCoverage} dalfs results
  | any (\(_, errOrRes) -> isLeft errOrRes) results = pure ()
  | otherwise = do
      putStrLn $
          unwords
              [ "test coverage: templates"
              , percentage coveredNrOfTemplates nrOfTemplates <> ","
              , "choices"
              , percentage coveredNrOfChoices nrOfChoices
              ]
      when getShowCoverage $ do
          putStrLn $
              unlines $
              ["templates never created:"] <> map T.unpack missingTemplates <>
              ["choices never executed:"] <>
              [T.unpack t <> ":" <> T.unpack c | (t, c) <- missingChoices]
  where
    templates = [(m, t) | m <- dalfs , t <- NM.toList $ LF.moduleTemplates m]
    choices = [(m, t, n) | (m, t) <- templates, n <- NM.names $ LF.tplChoices t]
    percentage i j
      | j > 0 = show (round @Double $ 100.0 * (fromIntegral i / fromIntegral j) :: Int) <> "%"
      | otherwise = "100%"
    allScenarioNodes = [n | (_vr, Right res) <- results, n <- V.toList $ SS.scenarioResultNodes res]
    coveredTemplates =
        nubSort $
        [ SS.contractInstanceTemplateId contractInstance
        | n <- allScenarioNodes
        , Just (SS.NodeNodeCreate SS.Node_Create {SS.node_CreateContractInstance}) <-
              [SS.nodeNode n]
        , Just contractInstance <- [node_CreateContractInstance]
        ]
    missingTemplates =
        [ (LF.moduleNameString $ LF.moduleName m) <> ":" <>
        (T.intercalate "." $ LF.unTypeConName $ LF.tplTypeCon t)
        | (m, t) <- templates
        ] \\
        [TL.toStrict $ SS.identifierName tId | Just tId <- coveredTemplates]
    coveredChoices =
        nubSort $
        [ (templateId, node_ExerciseChoiceId)
        | n <- allScenarioNodes
        , Just (SS.NodeNodeExercise SS.Node_Exercise { SS.node_ExerciseTemplateId
                                                     , SS.node_ExerciseChoiceId
                                                     }) <- [SS.nodeNode n]
        , Just templateId <- [node_ExerciseTemplateId]
        ]
    missingChoices =
        [ ( (LF.moduleNameString $ LF.moduleName m) <> ":" <>
            (T.concat $ LF.unTypeConName $ LF.tplTypeCon t)
          , LF.unChoiceName n)
        | (m, t, n) <- choices
        ] \\
        [(TL.toStrict $ SS.identifierName t, TL.toStrict c) | (t, c) <- coveredChoices]
    nrOfTemplates = length templates
    nrOfChoices = length choices
    coveredNrOfChoices = length coveredChoices
    coveredNrOfTemplates = length coveredTemplates

printScenarioResults :: [(VirtualResource, SS.ScenarioResult)] -> UseColor -> IO ()
printScenarioResults results color = do
    liftIO $ forM_ results $ \(VRScenario vrFile vrName, result) -> do
        let doc = prettyResult result
        let name = DA.Pretty.string (fromNormalizedFilePath vrFile) <> ":" <> DA.Pretty.pretty vrName
        let stringStyleToRender = if getUseColor color then DA.Pretty.renderColored else DA.Pretty.renderPlain
        putStrLn $ stringStyleToRender (name <> ": " <> doc)


prettyErr :: LF.Version -> SSC.Error -> DA.Pretty.Doc Pretty.SyntaxClass
prettyErr lfVersion err = case err of
    SSC.BackendError berr ->
        DA.Pretty.string (show berr)
    SSC.ScenarioError serr ->
        SS.prettyBriefScenarioError
          (LF.initWorld [] lfVersion)
          serr
    SSC.ExceptionError e -> DA.Pretty.string $ show e


prettyResult :: SS.ScenarioResult -> DA.Pretty.Doc Pretty.SyntaxClass
prettyResult result =
    let nTx = length (SS.scenarioResultScenarioSteps result)
        isActive node =
            case SS.nodeNode node of
                Just SS.NodeNodeCreate{} -> isNothing (SS.nodeConsumedBy node)
                _ -> False
        nActive = length $ filter isActive (V.toList (SS.scenarioResultNodes result))
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
