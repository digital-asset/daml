-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Assistant.CreateDamlAppTests (main) where

import Control.Exception.Extra
import Control.Monad
import Data.Aeson
import Data.Aeson.Extra.Merge
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString as BS
import Data.List.Extra
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import System.Info.Extra
import Test.Tasty
import Test.Tasty.HUnit

import DA.Bazel.Runfiles
import DA.Daml.Assistant.IntegrationTestUtils
import DA.Test.Daml2jsUtils
import DA.Test.Process (callCommandSilent)
import DA.Test.Util

main :: IO ()
main = do
    yarn : args <- getArgs
    javaPath <- locateRunfiles "local_jdk/bin"
    oldPath <- getSearchPath
    yarnPath <- takeDirectory <$> locateRunfiles (mainWorkspace </> yarn)
    withArgs args (withEnv
        [ ("PATH", Just $ intercalate [searchPathSeparator] (javaPath : yarnPath : oldPath))
        , ("TASTY_NUM_THREADS", Just "1")
        ] $ defaultMain tests)

tests :: TestTree
tests = withSdkResource $ \_ -> testGroup "Create DAML App tests" [gettingStartedGuideTest | not isWindows]
  where
    gettingStartedGuideTest = testCaseSteps "Getting Started Guide" $ \step ->
      withTempDir $ \tmpDir -> do
        step "Create app from template"
        withCurrentDirectory tmpDir $ do
          callCommandSilent "daml new create-daml-app create-daml-app"
        let cdaDir = tmpDir </> "create-daml-app"

        -- First test the base application (without the user-added feature).
        withCurrentDirectory cdaDir $ do
          step "Build DAML model for base application"
          callCommandSilent "daml build"
          step "Set up TypeScript libraries and Yarn workspaces for codegen"
          setupYarnEnv tmpDir (Workspaces ["create-daml-app/daml.js"]) [DamlTypes, DamlLedger]
          step "Run JavaScript codegen"
          callCommandSilent "daml codegen js -o daml.js .daml/dist/create-daml-app-0.1.0.dar"
        assertFileDoesNotExist (cdaDir </> "ui" </> "build" </> "index.html")
        withCurrentDirectory (cdaDir </> "ui") $ do
          -- NOTE(MH): We set up the yarn env again to avoid having all the
          -- dependencies of the UI already in scope when `daml2js` runs
          -- `yarn install`. Some of the UI dependencies are a bit flaky to
          -- install and might need some retries.
          step "Set up libraries and workspaces again for UI build"
          setupYarnEnv tmpDir (Workspaces ["create-daml-app/ui"]) allTsLibraries
          step "Install dependencies for UI"
          retry 3 (callCommandSilent "yarn install")
          step "Run linter"
          callCommandSilent "yarn lint --max-warnings 0"
          step "Build the application UI"
          callCommandSilent "yarn build"
        assertFileExists (cdaDir </> "ui" </> "build" </> "index.html")

        -- Now test that the messaging feature works by applying the necessary
        -- changes and testing in the same way as above.
        step "Patch the application code with messaging feature"
        messagingPatch <- locateRunfiles (mainWorkspace </> "templates" </> "create-daml-app-test-resources" </> "messaging.patch")
        patchTool <- locateRunfiles "patch_dev_env/bin/patch"
        withCurrentDirectory cdaDir $ do
          callCommandSilent $ unwords [patchTool, "-s", "-p2", "<", messagingPatch]
          forM_ ["MessageEdit", "MessageList"] $ \messageComponent ->
            assertFileExists ("ui" </> "src" </> "components" </> messageComponent <.> "tsx")
          step "Build the new DAML model"
          callCommandSilent "daml build"
          step "Set up TypeScript libraries and Yarn workspaces for codegen again"
          setupYarnEnv tmpDir (Workspaces ["create-daml-app/daml.js"]) [DamlTypes, DamlLedger]
          step "Run JavaScript codegen for new DAML model"
          callCommandSilent "daml codegen js -o daml.js .daml/dist/create-daml-app-0.1.0.dar"
        withCurrentDirectory (cdaDir </> "ui") $ do
          step "Set up libraries and workspaces again for UI build"
          setupYarnEnv tmpDir (Workspaces ["create-daml-app/ui"]) allTsLibraries
          step "Install UI dependencies again, forcing rebuild of generated code"
          callCommandSilent "yarn install --force --frozen-lockfile"
          step "Run linter again"
          callCommandSilent "yarn lint --max-warnings 0"
          step "Build the new UI"
          callCommandSilent "yarn build"

        -- Run end to end testing for the app.
        withCurrentDirectory (cdaDir </> "ui") $ do
          step "Install Jest, Puppeteer and other dependencies"
          extraDepsFile <- locateRunfiles (mainWorkspace </> "templates" </> "create-daml-app-test-resources" </> "testDeps.json")
          addTestDependencies (cdaDir </> "ui" </> "package.json") extraDepsFile
          retry 3 (callCommandSilent "yarn install")
          step "Run Puppeteer end-to-end tests"
          testFile <- locateRunfiles (mainWorkspace </> "templates" </> "create-daml-app-test-resources" </> "index.test.ts")
          copyFile testFile (cdaDir </> "ui" </> "src" </> "index.test.ts")
          callCommandSilent "CI=yes yarn run test --ci --all"

addTestDependencies :: FilePath -> FilePath -> IO ()
addTestDependencies packageJsonFile extraDepsFile = do
    packageJson <- readJsonFile packageJsonFile
    extraDeps <- readJsonFile extraDepsFile
    let newPackageJson = lodashMerge packageJson extraDeps
    BSL.writeFile packageJsonFile (encode newPackageJson)
  where
    readJsonFile :: FilePath -> IO Value
    readJsonFile path = do
        -- Read file strictly to avoid lock being held when we subsequently write to it.
        content <- BSL.fromStrict <$> BS.readFile path
        case decode content of
            Nothing -> error ("Could not decode JSON object from " <> path)
            Just val -> return val
