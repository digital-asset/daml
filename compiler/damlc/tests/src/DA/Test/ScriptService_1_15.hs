-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.ScriptService (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

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
                    , "import DA.List (sortOn)"
                    , "import Daml.Script"

                    -- Two interfaces (1,2)...
                    , "interface MyInterface1 where"
                    , "  viewtype MyView1"
                    , "data MyView1 = MyView1 { info : Int } deriving (Eq,Ord)"

                    , "interface MyInterface2 where"
                    , "  viewtype MyView2"
                    , "data MyView2 = MyView2 { info : Text } deriving (Eq,Ord)"

                    -- ...which are variously implemented by three templates (A,B,C)
                    , "template MyTemplateA"
                    , "  with"
                    , "    p : Party"
                    , "    v : Int"
                    , "  where"
                    , "    signatory p"
                    , "    interface instance MyInterface1 for MyTemplateA where"
                    , "      view = MyView1 { info = 100 + v }"

                    , "template MyTemplateB" -- Note: B implements both interfaces!
                    , "  with"
                    , "    p : Party"
                    , "    v : Int"
                    , "  where"
                    , "    signatory p"
                    , "    interface instance MyInterface1 for MyTemplateB where"
                    , "      view = MyView1 { info = 200 + v }"
                    , "    interface instance MyInterface2 for MyTemplateB where"
                    , "      view = MyView2 { info = \"B:\" <> show v }"

                    , "template MyTemplateC"
                    , "  with"
                    , "    p : Party"
                    , "    text : Text"
                    , "    isError : Bool"
                    , "  where"
                    , "    signatory p"
                    , "    interface instance MyInterface2 for MyTemplateC where"
                    , "      view = (if isError then error else MyView2) (\"C:\" <> text)"

                    , "test : Script ()"
                    , "test = do"
                    , "  p <- allocateParty \"p\"" -- primary party in the test script
                    , "  p2 <- allocateParty \"p2\"" -- other/different party

                    -- Create various contract-instances of A,B,C (for p)
                    , "  a1 <- submit p do createCmd (MyTemplateA p 42)"
                    , "  a2 <- submit p do createCmd (MyTemplateA p 43)"
                    , "  b1 <- submit p do createCmd (MyTemplateB p 44)"
                    , "  c1 <- submit p do createCmd (MyTemplateC p \"I-am-c1\" False)"
                    , "  c2 <- submit p do createCmd (MyTemplateC p \"I-am-c2\" True)"

                    -- Archived contracts wont be visible when querying
                    , "  a3 <- submit p do createCmd (MyTemplateA p 999)"
                    , "  submit p do archiveCmd a3"

                    -- Contracts created by another party (p2) wont be visible when querying (by p)
                    , "  a4 <- submit p2 do createCmd (MyTemplateA p2 911)"

                    -- Refer to p's instances via interface-contract-ids
                    -- (Note: we can refer to b1 via either Interface 1 or 2)
                    , "  let i1a1 = toInterfaceContractId @MyInterface1 a1"
                    , "  let i1a2 = toInterfaceContractId @MyInterface1 a2"
                    , "  let i1a3 = toInterfaceContractId @MyInterface1 a3"
                    , "  let i1a4 = toInterfaceContractId @MyInterface1 a4"
                    , "  let i1b1 = toInterfaceContractId @MyInterface1 b1"
                    , "  let i2b1 = toInterfaceContractId @MyInterface2 b1"
                    , "  let i2c1 = toInterfaceContractId @MyInterface2 c1"
                    , "  let i2c2 = toInterfaceContractId @MyInterface2 c2"

                    -- Test queryInterfaceContractId (Interface1)
                    , "  Some v <- queryInterfaceContractId p i1a1"
                    , "  v.info === 142"
                    , "  Some v <- queryInterfaceContractId p i1a2"
                    , "  v.info === 143"
                    , "  Some v <- queryInterfaceContractId p i1b1"
                    , "  v.info === 244"
                    , "  None <- queryInterfaceContractId p i1a3" -- contract is archived
                    , "  None <- queryInterfaceContractId p i1a4" -- not a stakeholder

                    -- Test queryInterfaceContractId (Interface2)
                    , "  Some v <- queryInterfaceContractId p i2b1"
                    , "  v.info === \"B:44\""
                    , "  Some v <- queryInterfaceContractId p i2c1"
                    , "  v.info === \"C:I-am-c1\""
                    , "  None <- queryInterfaceContractId p i2c2" -- view function failed

                    -- Test queryInterface (Interface1)
                    , "  [(i1,Some v1),(i2,Some v2),(i3,Some v3)] <- sortOn snd <$> queryInterface @MyInterface1 p"
                    , "  i1 === i1a1"
                    , "  i2 === i1a2"
                    , "  i3 === i1b1"
                    , "  v1.info === 142"
                    , "  v2.info === 143"
                    , "  v3.info === 244"

                    -- Test queryInterface (Interface2)
                    , "  [(i1,None),(i2,Some v2),(i3,Some v3)] <- sortOn snd <$> queryInterface @MyInterface2 p"
                    , "  i1 === i2c2" -- view function failed, so no info
                    , "  i2 === i2b1"
                    , "  i3 === i2c1"
                    , "  v2.info === \"B:44\""
                    , "  v3.info === \"C:I-am-c1\""

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
options = defaultOptions (Just lfVersion)


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
