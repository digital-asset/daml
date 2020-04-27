-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Main entry-point of the DAML compiler
module DA.Cli.Damlc.Test (
    execTest
    , UseColor(..)
    ) where

import Control.Monad.Except
import qualified DA.Pretty
import qualified DA.Pretty as Pretty
import DA.Cli.Damlc.Base
import qualified Data.HashSet as HashSet
import Data.Maybe
import Data.List.Extra
import Data.Tuple.Extra
import Control.Monad.Extra
import Development.IDE.Core.Service.Daml
import DA.Daml.Options.Types
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.PrettyScenario as SS
import qualified DA.Daml.LF.ScenarioServiceClient as SSC
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Development.Shake as Shake
import Development.IDE.Core.API
import Development.IDE.Core.IdeState.Daml
import Development.IDE.Core.Rules.Daml
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import qualified ScenarioService as SS
import System.Directory (createDirectoryIfMissing)
import System.Exit (exitFailure)
import System.FilePath
import qualified Text.XML.Light as XML


newtype UseColor = UseColor {getUseColor :: Bool}

-- | Test a DAML file.
execTest :: [NormalizedFilePath] -> UseColor -> Maybe FilePath -> Options -> IO ()
execTest inFiles color mbJUnitOutput opts = do
    loggerH <- getLogger opts "test"
    withDamlIdeState opts loggerH diagnosticsLogger $ \h -> do
        testRun h inFiles (optDamlLfVersion opts) color mbJUnitOutput
        diags <- getDiagnostics h
        when (any (\(_, _, diag) -> Just DsError == _severity diag) diags) exitFailure


testRun :: IdeState -> [NormalizedFilePath] -> LF.Version -> UseColor -> Maybe FilePath -> IO ()
testRun h inFiles lfVersion color mbJUnitOutput  = do
    -- make sure none of the files disappear
    liftIO $ setFilesOfInterest h (HashSet.fromList inFiles)

    -- take the transitive closure of all imports and run on all of them
    -- If some dependencies can't be resolved we'll get a Diagnostic out anyway, so don't worry
    deps <- runActionSync h $ mapM getDependencies inFiles
    let files = nubOrd $ concat $ inFiles : catMaybes deps

    results <- runActionSync h $
        Shake.forP files $ \file -> do
            mbScenarioResults <- runScenarios file
            results <- case mbScenarioResults of
                Nothing -> failedTestOutput h file
                Just scenarioResults -> do
                    -- failures are printed out through diagnostics, so just print the sucesses
                    liftIO $ printScenarioResults [(v, r) | (v, Right r) <- scenarioResults] color
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
