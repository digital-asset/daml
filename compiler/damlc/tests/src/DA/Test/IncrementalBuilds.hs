-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.IncrementalBuilds (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Monad.Extra
import DA.Bazel.Runfiles
import Data.Foldable
import qualified Data.Set as Set
import Data.Traversable
import System.Directory.Extra
import System.FilePath
import System.IO.Extra
import DA.Test.Process
import Test.Tasty
import Test.Tasty.HUnit
import SdkVersion

main :: IO ()
main = do
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    damlScript <- locateRunfiles (mainWorkspace </> "daml-script" </> "runner" </> exe "daml-script-binary")
    scriptDar <- locateRunfiles (mainWorkspace </> "daml-script" </> "daml" </> "daml-script.dar")
    defaultMain $ tests damlc damlScript scriptDar

tests :: FilePath -> FilePath -> FilePath -> TestTree
tests damlc damlScript scriptDar = testGroup "Incremental builds"
    [ test "No changes"
        [ ("daml/A.daml", unlines
           [ "module A where"
           ]
          )
        ]
        []
        []
        (ShouldSucceed True)
    , test "Modify single file"
        [ ("daml/A.daml", unlines
           [ "module A where"
           , "import Daml.Script"
           , "test : Script ()"
           , "test = script $ assert True"
           ]
          )
        ]
        [ ("daml/A.daml", unlines
           [ "module A where"
           , "import Daml.Script"
           , "test : Script ()"
           , "test = script $ assert False"
           ]
          )
        ]
        ["daml/A.daml"]
        (ShouldSucceed False)
    , test "Modify dependency without ABI change"
        [ ("daml/A.daml", unlines
           [ "module A where"
           , "import Daml.Script"
           , "import B"
           , "test = script $ b"
           ]
          )
        , ("daml/B.daml", unlines
           [ "module B where"
           , "import Daml.Script"
           , "b : Script ()"
           , "b = script $ assert True"
           ]
          )
        ]
        [ ("daml/B.daml", unlines
           [ "module B where"
           , "import Daml.Script"
           , "b : Script ()"
           , "b = script $ assert False"
           ]
          )
        ]
        ["daml/B.daml"]
        (ShouldSucceed False)
    , test "Modify dependency with ABI change"
        [ ("daml/A.daml", unlines
           [ "module A where"
           , "import Daml.Script"
           , "import B"
           , "test : Script ()"
           , "test = script $ do _ <- b; pure ()"
           ]
          )
        , ("daml/B.daml", unlines
           [ "module B where"
           , "import Daml.Script"
           , "b : Script Bool"
           , "b = pure True"
           ]
          )
        ]
        [ ("daml/B.daml", unlines
           [ "module B where"
           , "import Daml.Script"
           , "b : Script ()"
           , "b = assert False"
           ]
          )
        ]
        ["daml/A.daml", "daml/B.daml"]
        (ShouldSucceed False)
    , test "Transitive dependencies, no modification"
      -- This test checks that we setup dependent modules in the right order. Note that just having imports is not sufficient
      -- to trigger this. The modules actually need to use identifiers from the other modules.
      [ ("daml/A.daml", unlines
         [ "module A where"
         , "import Daml.Script"
         , "import B"
         , "test : Script ()"
         , "test = script $ do"
         , "  p <- allocateParty \"Alice\""
         , "  cid <- submit p $ createCmd X with p = p"
         , "  submit p $ createCmd Y with p = p; cid = cid"
         , "  pure ()"
         ]
        )
      , ("daml/B.daml", unlines
         [ "module B (module C, Y(..)) where"
         , "import C"
         , "template Y"
         , "  with p : Party; cid : ContractId X"
         , "  where signatory p"
         ]
        )
      , ("daml/C.daml", unlines
         [ "module C where"
         , "template X"
         , "  with p : Party"
         , "  where signatory p"
         ]
        )
      ]
      []
      []
      (ShouldSucceed True)
    ]
  where
      -- ShouldSucceed indicates if scenarios should still succeed after modifications.
      -- This is useful to make sure that modifications have propagated correctly into the DAR.
      test :: String -> [(FilePath, String)] -> [(FilePath, String)] -> [FilePath] -> ShouldSucceed -> TestTree
      test name initial modification expectedRebuilds (ShouldSucceed shouldSucceed) = testCase name $ withTempDir $ \dir -> do
          writeFileUTF8 (dir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: test-project"
            , "source: daml"
            , "version: 0.0.1"
            , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
            ]
          for_ initial $ \(file, content) -> do
              createDirectoryIfMissing True (takeDirectory $ dir </> file)
              writeFileUTF8 (dir </> file) content
          let dar = dir </> "out.dar"
          callProcessSilent damlc
            [ "build"
            , "--project-root"
            , dir
            , "-o"
            , dar
            , "--incremental=yes" ]
          callProcessSilent damlc 
            [ "test"
            , "--project-root"
            , dir
            ]
          dalfFiles <- getDalfFiles $ dir </> ".daml/build"
          dalfModTimes <- for dalfFiles $ \f -> do
              modTime <- getModificationTime f
              pure (f, modTime)
          for_ modification $ \(file, content) -> do
              createDirectoryIfMissing True (takeDirectory $ dir </> file)
              writeFileUTF8 (dir </> file) content
          callProcessSilent damlc
            ["build"
            , "--project-root"
            , dir
            , "-o"
            , dar
            , "--incremental=yes" ]
          rebuilds <- forMaybeM dalfModTimes $ \(f, oldModTime) -> do
              newModTime <- getModificationTime f
              pure $ if newModTime == oldModTime
                  then Nothing
                  else Just (makeRelative (dir </> ".daml/build") f -<.> ".daml")
          assertEqual "Expected rebuilds" (Set.fromList $ map normalise expectedRebuilds) (Set.fromList $ map normalise rebuilds)
          callProcessSilent damlc ["validate-dar", dar]
          if shouldSucceed
            then
              callProcessSilent damlScript ["--ide-ledger", "--all", "--dar", dar]
            else
              callProcessSilentError damlScript ["--ide-ledger", "--all", "--dar", dar]
          pure ()

getDalfFiles :: FilePath -> IO [FilePath]
getDalfFiles dir = do
    files <- listFilesRecursive dir
    pure $ filter (\f -> takeExtension f == ".dalf") files

forMaybeM :: Monad m => [a] -> (a -> m (Maybe b)) -> m [b]
forMaybeM = flip mapMaybeM
