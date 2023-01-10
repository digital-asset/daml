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

main :: IO ()
main = do
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    repl <- locateRunfiles (mainWorkspace </> "daml-lf" </> "repl" </> exe "repl")
    defaultMain $ tests damlc repl

tests :: FilePath -> FilePath -> TestTree
tests damlc repl = testGroup "Incremental builds"
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
           , "test = scenario $ assert True"
           ]
          )
        ]
        [ ("daml/A.daml", unlines
           [ "module A where"
           , "test = scenario $ assert False"
           ]
          )
        ]
        ["daml/A.daml"]
        (ShouldSucceed False)
    , test "Modify dependency without ABI change"
        [ ("daml/A.daml", unlines
           [ "module A where"
           , "import B"
           , "test = scenario $ b"
           ]
          )
        , ("daml/B.daml", unlines
           [ "module B where"
           , "b = scenario $ assert True"
           ]
          )
        ]
        [ ("daml/B.daml", unlines
           [ "module B where"
           , "b = scenario $ assert False"
           ]
          )
        ]
        ["daml/B.daml"]
        (ShouldSucceed False)
    , test "Modify dependency with ABI change"
        [ ("daml/A.daml", unlines
           [ "module A where"
           , "import B"
           , "test = scenario $ do _ <- b; pure ()"
           ]
          )
        , ("daml/B.daml", unlines
           [ "module B where"
           , "b : Scenario Bool"
           , "b = pure True"
           ]
          )
        ]
        [ ("daml/B.daml", unlines
           [ "module B where"
           , "b : Scenario ()"
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
         , "import B"
         , "test = scenario $ do"
         , "  p <- getParty \"Alice\""
         , "  cid <- submit p $ create X with p = p"
         , "  submit p $ create Y with p = p; cid = cid"
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
            [ "sdk-version: 0.0.0"
            , "name: test-project"
            , "source: daml"
            , "version: 0.0.1"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          for_ initial $ \(file, content) -> do
              createDirectoryIfMissing True (takeDirectory $ dir </> file)
              writeFileUTF8 (dir </> file) content
          let dar = dir </> "out.dar"
          callProcessSilent damlc
            [ "build"
            , "--enable-scenarios=yes" -- TODO: https://github.com/digital-asset/daml/issues/11316
            , "--project-root"
            , dir
            , "-o"
            , dar
            , "--incremental=yes" ]
          callProcessSilent repl ["testAll", dar]
          dalfFiles <- getDalfFiles $ dir </> ".daml/build"
          dalfModTimes <- for dalfFiles $ \f -> do
              modTime <- getModificationTime f
              pure (f, modTime)
          for_ modification $ \(file, content) -> do
              createDirectoryIfMissing True (takeDirectory $ dir </> file)
              writeFileUTF8 (dir </> file) content
          callProcessSilent damlc
            ["build"
            , "--enable-scenarios=yes" -- TODO: https://github.com/digital-asset/daml/issues/11316
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
          callProcessSilent repl ["validate", dar]
          if shouldSucceed
            then
              callProcessSilent repl ["testAll", dar]
            else
              callProcessSilentError repl ["testAll", dar]
          pure ()

getDalfFiles :: FilePath -> IO [FilePath]
getDalfFiles dir = do
    files <- listFilesRecursive dir
    pure $ filter (\f -> takeExtension f == ".dalf") files

forMaybeM :: Monad m => [a] -> (a -> m (Maybe b)) -> m [b]
forMaybeM = flip mapMaybeM
