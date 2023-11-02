-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.DamlcMultiPackage (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Monad.Extra
import DA.Bazel.Runfiles
import Data.List
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import SdkVersion
import System.Directory.Extra
import System.Environment
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit
import Text.Regex.TDFA

main :: IO ()
main = do
  damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
  defaultMain $ tests damlc

data ProjectStructure
  = DamlYaml
      { dyName :: T.Text
      , dyVersion :: T.Text
      , dySdkVersion :: Maybe T.Text
      , dySource :: T.Text
      , dyOutPath :: Maybe T.Text
      , dyDeps :: [T.Text]
      }
  | MultiPackage
      { mpPackages :: [T.Text]
      , mpProjects :: [T.Text]
      }
  | Dir
      { dName :: T.Text 
      , dContents :: [ProjectStructure]
      }
  | DamlSource
      { dsModuleName :: T.Text
      , dsDeps :: [T.Text]
      }

data PackageIdentifier = PackageIdentifier
  { piName :: T.Text
  , piVersion :: T.Text
  }
  deriving (Eq, Ord)
instance Show PackageIdentifier where
  show pi = T.unpack (piName pi) <> "-" <> T.unpack (piVersion pi)

simpleTwoPackageProject :: [ProjectStructure]
simpleTwoPackageProject =
  [ MultiPackage ["./package-a", "./package-b"] []
  , Dir "package-a"
    [ DamlYaml "package-a" "0.0.1" Nothing "daml" Nothing []
    , Dir "daml" [DamlSource "PackageAMain" []]
    ]
  , Dir "package-b"
    [ DamlYaml "package-b" "0.0.1" Nothing "daml" Nothing ["../package-a/.daml/dist/package-a-0.0.1.dar"]
    , Dir "daml" [DamlSource "PackageBMain" ["PackageAMain"]]
    ]
  ]

tests :: FilePath -> TestTree
tests damlc =
  testGroup
    "Multi-Package build"
    [ testGroup
        "Simple two package project"
        [ test "Build A with search" ["--multi-package-search"] "./package-a" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1"]
        , test "Build B with search" ["--multi-package-search"] "./package-b" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build all from A with search" ["--multi-package-search", "--all"] "./package-a" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build all from B with search" ["--multi-package-search", "--all"] "./package-b" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build all from root" ["--all"] "" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        , test "Build all from A with explicit path" ["--all", "--multi-package-path=.."] "./package-a" simpleTwoPackageProject
            $ Right [PackageIdentifier "package-a" "0.0.1", PackageIdentifier "package-b" "0.0.1"]
        ]
    ]

  where
    test
      :: String
      -> [String]
      -> FilePath
      -> [ProjectStructure]
      -- Left is error regex, right is success + expected packages to have build.
      -- Any created dar files that aren't listed here throw an error.
      -> Either T.Text [PackageIdentifier]
      -> TestTree
    test name flags runPath projectStructure expectedResult =
      testCase name $
      withTempDir $ \dir -> do
        allPossibleDars <- buildProject dir projectStructure
        -- Quick check to ensure all the package identifiers are possible
        case expectedResult of
          Left _ -> pure ()
          Right expectedPackageIdentifiers ->
            forM_ expectedPackageIdentifiers $ \pkg ->
              unless (Map.member pkg allPossibleDars) $
                assertFailure $ "Package " <> show pkg <> " can never be built by this setup. Did you mean one of: "
                  <> intercalate ", " (show <$> Map.keys allPossibleDars)

        env <- getEnvironment
        let args = ["build", "--enable-multi-package=yes"] <> flags
            -- Fooling damlc into thinking damlc is daml assistant, since we only call build.
            -- This means we cannot test the different version logic here.
            process = (proc damlc args) {cwd = Just $ dir </> runPath, env = Just $ ("DAML_ASSISTANT", damlc):env}
        (exitCode, _, err) <- readCreateProcessWithExitCode process ""
        case expectedResult of
          Right expectedPackageIdentifiers -> do
            unless (exitCode == ExitSuccess) $ assertFailure $ "Expected success and got " <> show exitCode <> ".\n  StdErr: \n  " <> err

            void $ flip Map.traverseWithKey allPossibleDars $ \pkg darPath -> do
              darExists <- doesFileExist $ dir </> darPath
              let darShouldExist = pkg `elem` expectedPackageIdentifiers
              unless (darExists == darShouldExist) $ do
                assertFailure $ if darExists
                  then "Found dar for " <> show pkg <> " when it should not have been built."
                  else "Couldn't find dar for " <> show pkg <> " when it should have been built."
          Left regex -> do
            assertBool "succeeded unexpectedly" $ exitCode /= ExitSuccess
            unless (matchTest (makeRegex regex :: Regex) err) $
              assertFailure ("Regex '" <> show regex <> "' did not match stderr:\n" <> show err)

    -- Returns paths of all possible expected Dars
    buildProject :: FilePath -> [ProjectStructure] -> IO (Map.Map PackageIdentifier FilePath)
    buildProject initialPath = fmap mconcat . traverse (buildProjectStructure initialPath)
      where
        buildProjectStructure :: FilePath -> ProjectStructure -> IO (Map.Map PackageIdentifier FilePath)
        buildProjectStructure path = \case
          damlYaml@DamlYaml {} -> do
            TIO.writeFile (path </> "daml.yaml") $ T.unlines $
              [ "sdk-version: " <> fromMaybe (T.pack sdkVersion) (dySdkVersion damlYaml)
              , "name: " <> dyName damlYaml
              , "source: " <> dySource damlYaml
              , "version: " <> dyVersion damlYaml
              , "dependencies:"
              , "  - daml-prim"
              , "  - daml-stdlib"
              , "data-dependencies:"
              ]
              ++ fmap ("  - " <>) (dyDeps damlYaml)
              ++ maybe [] (\outputPath -> 
                  [ "build-options:"
                  , "  - --output"
                  , "  - " <> outputPath
                  ]
                ) (dyOutPath damlYaml)
            let relDarPath = fromMaybe (".daml/dist/" <> dyName damlYaml <> "-" <> dyVersion damlYaml <> ".dar") (dyOutPath damlYaml)
            outPath <- canonicalizePath $ path </> T.unpack relDarPath
            pure $ Map.singleton (PackageIdentifier (dyName damlYaml) (dyVersion damlYaml)) $ makeRelative initialPath outPath
          multiPackage@MultiPackage {} -> do
            TIO.writeFile (path </> "multi-package.yaml") $ T.unlines
              $  ["packages:"] ++ fmap ("  - " <>) (mpPackages multiPackage)
              ++ ["projects:"] ++ fmap ("  - " <>) (mpProjects multiPackage)
            pure Map.empty
          dir@Dir {} -> do
            let newDir = path </> (T.unpack $ dName dir)
            createDirectoryIfMissing True newDir
            mconcat <$> traverse (buildProjectStructure newDir) (dContents dir)
          damlSource@DamlSource {} -> do
            let damlFileName = T.unpack $ last (T.split (=='.') $ dsModuleName damlSource) <> ".daml"
            TIO.writeFile (path </> damlFileName) $ T.unlines $
              ["module " <> dsModuleName damlSource <> " where"]
              ++ fmap (\dep -> "import " <> dep <> " ()") (dsDeps damlSource)
            pure Map.empty