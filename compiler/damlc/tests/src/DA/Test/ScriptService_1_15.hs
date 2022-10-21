-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.ScriptService (main) where

import Control.Exception
import Control.Monad
import DA.Bazel.Runfiles
import DA.Cli.Damlc.Packaging
import DA.Cli.Damlc.DependencyDb
import qualified DA.Daml.LF.Ast.Version as LF
import DA.Daml.LF.PrettyScenario (prettyScenarioError, prettyScenarioResult)
import qualified DA.Daml.LF.ScenarioServiceClient as SS
import DA.Daml.Options.Types
import DA.Daml.Package.Config
import DA.Daml.Project.Types
import DA.Pretty
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.IO as Logger
import qualified Data.HashSet as HashSet
import Data.List
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as V
import Development.IDE.Core.Debouncer (noopDebouncer)
import Development.IDE.Core.FileStore (makeVFSHandle, setBufferModified)
import Development.IDE.Core.IdeState.Daml (getDamlIdeState)
import Development.IDE.Core.OfInterest (setFilesOfInterest)
import Development.IDE.Core.RuleTypes.Daml (RunScripts (..), VirtualResource (..))
import Development.IDE.Core.Rules.Daml (worldForFile)
import Development.IDE.Core.Service (getDiagnostics, runActionSync, shutdown)
import Development.IDE.Core.Shake (ShakeLspEnv(..), NotificationHandler(..), use)
import Development.IDE.Types.Diagnostics (showDiagnostics)
import Development.IDE.Types.Location (toNormalizedFilePath')
import SdkVersion
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import Test.Tasty
import Test.Tasty.HUnit
import Text.Regex.TDFA

lfVersion :: LF.Version
lfVersion = LF.version1_15

main :: IO ()
main =
  withTempDir $ \dir -> do
    withCurrentDirectory dir $ do
      setEnv "TASTY_NUM_THREADS" "1" True

      -- Package DB setup, we only need to do this once so we do it at the beginning.
      scriptDar <- locateRunfiles $ mainWorkspace </> "daml-script/daml/daml-script-1.15.dar"
      writeFileUTF8 "daml.yaml" $
        unlines
          [ "sdk-version: " <> sdkVersion,
            "name: script-service",
            "version: 0.0.1",
            "source: .",
            "dependencies:",
            "- daml-prim",
            "- daml-stdlib",
            "- " <> show scriptDar
          ]
      withPackageConfig (ProjectPath ".") $ \PackageConfigFields {..} -> do
        let projDir = toNormalizedFilePath' dir
        installDependencies
            projDir
            options
            pSdkVersion
            pDependencies
            pDataDependencies
        createProjectPackageDb
          projDir
          options
          pModulePrefixes

      logger <- Logger.newStderrLogger Logger.Debug "script-service"

      -- Spinning up the scenario service is expensive so we do it once at the beginning.
      SS.withScenarioService lfVersion logger scenarioConfig $ \scriptService ->
        defaultMain $
          testGroup
            "Script Service 1.15"
            [
              testCase "query by interface" $ do
                rs <-
                  runScripts
                    scriptService
                    [ "module Test where"
                    , "import DA.Assert"
                    , "import Daml.Script"

                    , "interface MyInterface where"
                    , "  viewtype MyView"
                    , "data MyView = MyView { info : Int }"
                    , "template MyTemplate"
                    , "  with"
                    , "    p : Party"
                    , "    v : Int"
                    , "  where"
                    , "    signatory p"
                    , "    interface instance MyInterface for MyTemplate where"
                    , "      view = MyView { info = 100 + v }"
                    , "test : Script ()"
                    , "test = do"
                    , "  p <- allocateParty \"p\""
                    , "  cid <- submit p do createCmd (MyTemplate p 42)"
                    , "  optR <- queryContractId p cid"
                    , "  let (Some r) = optR"
                    , "  r === MyTemplate p 42"
                    , "  let iid : ContractId MyInterface = toInterfaceContractId @MyInterface cid"
                    , "  Some v <- queryViewContractId p iid"
                    , "  v.info === 142"
                    , "  pure ()"
                    ]

                expectScriptSuccess rs (vr "test") $ \r ->
                  matchRegex r "Active contracts:"

            ]
  where
    scenarioConfig = SS.defaultScenarioServiceConfig {SS.cnfJvmOptions = ["-Xmx200M"]}
    vr n = VRScenario (toNormalizedFilePath' "Test.daml") n

matchRegex :: T.Text -> T.Text -> Bool
matchRegex s regex = matchTest (makeRegex regex :: Regex) s

expectScriptSuccess :: HasCallStack =>
  -- | The list of script results.
  [(VirtualResource, Either T.Text T.Text)] ->
  -- | VR of the script
  VirtualResource ->
  -- | Predicate on the result
  (T.Text -> Bool) ->
  -- | Succeeds if there is a successful result for the given
  -- VR and the predicate holds.
  Assertion
expectScriptSuccess xs vr pred = case find ((vr ==) . fst) xs of
  Nothing -> assertFailure $ "No result for " <> show vr
  Just (_, Left err) ->
    assertFailure $
      "Expected success for " <> show vr <> " but got "
        <> show err
  Just (_, Right r) ->
    unless (pred r) $
      assertFailure $ "Predicate for " <> show vr <> " failed on " <> show r

options :: Options
options =
  (defaultOptions (Just lfVersion))
    { optDlintUsage = DlintDisabled
    }

runScripts :: SS.Handle -> [T.Text] -> IO [(VirtualResource, Either T.Text T.Text)]
runScripts service fileContent = bracket getIdeState shutdown $ \ideState -> do
  setBufferModified ideState file $ Just $ T.unlines fileContent
  setFilesOfInterest ideState (HashSet.singleton file)
  mbResult <- runActionSync ideState $ use RunScripts file
  case mbResult of
    Nothing -> do
      diags <- getDiagnostics ideState
      fail (T.unpack $ showDiagnostics diags)
    Just xs -> do
      world <- runActionSync ideState (worldForFile file)
      let render (vr, r) = (vr,) <$> prettyResult world r
      mapM render xs
  where
    prettyResult world (Left err) = case err of
      SS.BackendError err -> assertFailure $ "Unexpected result " <> show err
      SS.ExceptionError err -> assertFailure $ "Unexpected result " <> show err
      SS.ScenarioError err -> pure $ Left $ renderPlain (prettyScenarioError world err)
    prettyResult world (Right r) = pure $ Right $ renderPlain (prettyScenarioResult world (S.fromList (V.toList (SS.scenarioResultActiveContracts r))) r)
    file = toNormalizedFilePath' "Test.daml"
    getIdeState = do
      vfs <- makeVFSHandle
      logger <- Logger.newStderrLogger Logger.Error "script-service"
      getDamlIdeState
        options
        (Just service)
        logger
        noopDebouncer
        (DummyLspEnv $ NotificationHandler $ \_ _ -> pure ())
        vfs
