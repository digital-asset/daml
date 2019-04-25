-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings   #-}

-- | Main entry-point of the DAML compiler
module DA.Cli.Damlc.Test (execTest) where

import Control.Monad.Except
import qualified Control.Monad.Managed             as Managed
import           DA.Prelude
import qualified DA.Pretty
import DA.Cli.Damlc.Base
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


-- | Test a DAML file.
execTest :: [FilePath] -> Maybe FilePath -> Compiler.Options -> IO ()
execTest inFiles mbJUnitOutput cliOptions = do
    loggerH <- getLogger cliOptions "test"
    opts <- Compiler.mkOptions cliOptions
    -- TODO (MK): For now the scenario service is only started if we have an event logger
    -- so we insert a dummy event logger.
    let eventLogger _ = pure ()
    Managed.with (Compiler.newIdeState opts (Just eventLogger) loggerH) $ \hDamlGhc -> do
        liftIO $ Compiler.setFilesOfInterest hDamlGhc inFiles
        mbDeps <- liftIO $ CompilerService.runAction hDamlGhc $ fmap sequence $ mapM CompilerService.getDependencies inFiles
        depFiles <- maybe (reportDiagnostics hDamlGhc "Failed get dependencies") pure mbDeps
        let files = Set.toList $ Set.fromList inFiles `Set.union`  Set.fromList (concat depFiles)
        let lfVersion = Compiler.optDamlLfVersion cliOptions
        case mbJUnitOutput of
            Nothing -> testStdio lfVersion hDamlGhc files
            Just junitOutput -> testJUnit lfVersion hDamlGhc files junitOutput

testStdio :: LF.Version -> IdeState -> [FilePath] -> IO ()
testStdio lfVersion hDamlGhc files = do
    failed <- fmap or $ CompilerService.runAction hDamlGhc $
        Shake.forP files $ \file -> do
            mbScenarioResults <- CompilerService.runScenarios file
            scenarioResults <- liftIO $ maybe (reportDiagnostics hDamlGhc "Failed to run scenarios") pure mbScenarioResults
            liftIO $ forM_ scenarioResults $ \(VRScenario vrFile vrName, result) -> do
                let doc = prettyResult lfVersion result
                let name = DA.Pretty.string vrFile <> ":" <> DA.Pretty.pretty vrName
                putStrLn $ DA.Pretty.renderPlain (name <> ": " <> doc)
            pure $ any (isLeft . snd) scenarioResults
    when failed exitFailure

testJUnit :: LF.Version -> IdeState -> [FilePath] -> FilePath -> IO ()
testJUnit lfVersion hDamlGhc files junitOutput = do
    failed <- CompilerService.runAction hDamlGhc $ do
        results <- Shake.forP files $ \file -> do
            scenarios <- CompilerService.getScenarios file
            mbScenarioResults <- CompilerService.runScenarios file
            results <- case mbScenarioResults of
                Nothing -> do
                    -- If we don’t get scenario results, we use the diagnostics
                    -- as the error message for each scenario.
                    diagnostics <- liftIO $ CompilerService.getDiagnostics hDamlGhc
                    let errMsg = Pretty.renderPlain $ prettyDiagnosticStore diagnostics
                    pure $ map (, Just errMsg) scenarios
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
      DA.Pretty.nest 2 (prettyErr lfVersion err)
  Right result ->
    let nTx = length (SS.scenarioResultScenarioSteps result)
        isActive node =
          case SS.nodeNode node of
            Just SS.NodeNodeCreate{} ->
              isNothing (SS.nodeConsumedBy node)
            _otherwise -> False
        nActive = length $ filter isActive (V.toList (SS.scenarioResultNodes result))
    in DA.Pretty.typeDoc_ "ok, "
    <> DA.Pretty.int nActive <> " active contracts, "
    <> DA.Pretty.int nTx <> " transactions."


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


reportDiagnostics :: CompilerService.IdeState -> String -> IO a
reportDiagnostics service err = do
    diagnostic <- CompilerService.getDiagnostics service
    reportErr err diagnostic
