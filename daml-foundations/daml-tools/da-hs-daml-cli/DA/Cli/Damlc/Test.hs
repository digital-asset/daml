-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings   #-}

-- | Main entry-point of the DAML compiler
module DA.Cli.Damlc.Test (
    execTest
    , ColorTestResults(..)
    ) where

import Control.Monad.Except
import qualified Control.Monad.Managed             as Managed
import           DA.Prelude
import qualified DA.Pretty
import DA.Cli.Damlc.Base
import Control.Monad.Extra
import           DA.Service.Daml.Compiler.Impl.Handle as Compiler
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.PrettyScenario as SS
import qualified DA.Daml.LF.ScenarioServiceClient as SSC
import Data.Either
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Text.Prettyprint.Doc.Syntax as Pretty
import qualified Data.Vector as V
import qualified Development.Shake as Shake
import qualified Development.IDE.State.API as CompilerService
import qualified Development.IDE.State.Rules.Daml as CompilerService
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.LSP
import qualified ScenarioService as SS
import System.Directory (createDirectoryIfMissing)
import System.Exit (exitFailure)
import System.FilePath
import qualified Text.XML.Light as XML


newtype ColorTestResults = ColorTestResults{getColorTestResults :: Bool}

-- | Test a DAML file.
execTest :: [FilePath] -> ColorTestResults -> Maybe FilePath -> Compiler.Options -> IO ()
execTest inFiles colorTestResults mbJUnitOutput cliOptions = do
    loggerH <- getLogger cliOptions "test"
    opts <- Compiler.mkOptions cliOptions
    let eventLogger (EventFileDiagnostics diag) = printDiagnostics $ fdDiagnostics diag
        eventLogger _ = return ()
    Managed.with (Compiler.newIdeState opts (Just eventLogger) loggerH) $ \hDamlGhc -> do
        liftIO $ Compiler.setFilesOfInterest hDamlGhc inFiles
        mbDeps <- liftIO $ CompilerService.runAction hDamlGhc $ fmap sequence $ mapM CompilerService.getDependencies inFiles
        whenJust mbDeps $ \depFiles -> do
        let files = Set.toList $ Set.fromList inFiles `Set.union` Set.fromList (concat depFiles)
        let lfVersion = Compiler.optDamlLfVersion cliOptions
        case mbJUnitOutput of
            Nothing -> testStdio lfVersion hDamlGhc files colorTestResults
            Just junitOutput -> testJUnit lfVersion hDamlGhc files junitOutput

testStdio :: LF.Version -> IdeState -> [FilePath] -> ColorTestResults -> IO ()
testStdio lfVersion hDamlGhc files colorTestResults = do
    failed <- fmap or $ CompilerService.runAction hDamlGhc $
        Shake.forP files $ \file -> do
            mbScenarioResults <- CompilerService.runScenarios file
            case mbScenarioResults of
                Nothing -> return True
                Just scenarioResults -> do
                    liftIO $ forM_ scenarioResults $ \(VRScenario vrFile vrName, result) -> do
                        let doc = prettyResult lfVersion result
                        let name = DA.Pretty.string vrFile <> ":" <> DA.Pretty.pretty vrName
                        let stringStyleToRender = if getColorTestResults colorTestResults then DA.Pretty.renderColored else DA.Pretty.renderPlain
                        putStrLn $ stringStyleToRender (name <> ": " <> doc)
                    pure $ any (isLeft . snd) scenarioResults
    when failed exitFailure

testJUnit :: LF.Version -> IdeState -> [FilePath] -> FilePath -> IO ()
testJUnit lfVersion hDamlGhc files junitOutput = do
    failed <- CompilerService.runAction hDamlGhc $ do
        results <- Shake.forP files $ \file -> do
            mbScenarioResults <- CompilerService.runScenarios file
            results <- case mbScenarioResults of
                Nothing -> do
                    -- If we don’t get scenario results, we use the diagnostics
                    -- as the error message for each scenario.
                    mbScenarioNames <- CompilerService.getScenarioNames file
                    diagnostics <- liftIO $ CompilerService.getDiagnostics hDamlGhc
                    let errMsg = T.unlines (map (Pretty.renderPlain . prettyDiagnostic) diagnostics)
                    pure $ map (, Just errMsg) $ fromMaybe [VRScenario file "Unknown"] mbScenarioNames
                Just scenarioResults -> pure $
                    map (\(vr, res) -> (vr, either (Just . T.pack . DA.Pretty.renderPlainOneLine . prettyErr lfVersion) (const Nothing) res))
                        scenarioResults
            pure (file, results)
        liftIO $ do
            createDirectoryIfMissing True $ takeDirectory junitOutput
            writeFile junitOutput $ XML.showTopElement $ toJUnit results
        pure (any (any (isJust . snd) . snd) results)
    when failed exitFailure


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
      DA.Pretty.error_ (DA.Pretty.nest 2 (prettyErr lfVersion err))
  Right result ->
    let nTx = length (SS.scenarioResultScenarioSteps result)
        isActive node =
          case SS.nodeNode node of
            Just SS.NodeNodeCreate{} ->
              isNothing (SS.nodeConsumedBy node)
            _otherwise -> False
        nActive = length $ filter isActive (V.toList (SS.scenarioResultNodes result))
    in DA.Pretty.typeDoc_ "ok, "
    <> DA.Pretty.int nActive <> DA.Pretty.typeDoc_ " active contracts, "
    <> DA.Pretty.int nTx <> DA.Pretty.typeDoc_ " transactions."


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
