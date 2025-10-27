-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Test.PackageImports (
        entry
    ) where

import qualified Data.ByteString      as BS
import qualified Data.ByteString.Lazy as BSL
import           Data.Either
import           Data.Map             (Map)
import qualified Data.Map             as M
import           Data.Set             hiding (map)
import           Text.Printf


import           Test.Tasty.HUnit
import           Test.Tasty

import qualified "zip-archive" Codec.Archive.Zip         as ZipArchive
import qualified               DA.Daml.LF.Proto3.Archive as Archive

import           System.FilePath

import           DA.Bazel.Runfiles
import           DA.Daml.Compiler.ExtractDar
import           DA.Daml.LF.Ast
import           DA.Daml.StablePackages


entry :: IO ()
entry = defaultMain $ testGroup "Round-trip tests"
    [ darTests
    ]

darTests :: TestTree
darTests = testGroup ".dar tests" $ verifyImports <$>
    [ "script-test-v2.dev.dar"
    ]

-- | Computes the transitive closure for a starting key in a graph.
-- The graph is represented as a map from a key to a set of its direct neighbors.
transitiveClosure :: Ord a => Map a (Set a) -> a -> Set a
transitiveClosure graph startNode = go (singleton startNode) empty
  where
    -- go :: Ord a => Set a -> Set a -> Set a
    go toVisit visited =
      case minView toVisit of
        -- Base case: If there are no more nodes to visit, we're done.
        Nothing -> visited

        -- Recursive step:
        Just (current, restToVisit) ->
          -- If we've already visited the current node, just continue with the rest.
          if member current visited
            then go restToVisit visited
            else
              let
                -- Find neighbors of the current node. Use empty set if node has no outgoing edges.
                neighbors = M.findWithDefault empty current graph

                -- Add the new, unvisited neighbors to our set of nodes to visit.
                newToVisit = union restToVisit neighbors

                -- Add the current node to our set of visited nodes.
                newVisited = insert current visited
              in
                -- Continue the traversal.
                go newToVisit newVisited

{- HLINT ignore "locateRunfiles/package_app" -}
verifyImports :: String -> TestTree
verifyImports darname = testCase darname $ do
  scriptDar <- locateRunfiles $ mainWorkspace </> darPath

  dar <- extractDar scriptDar

  let mainDalf = edMain dar
  let bs :: BS.ByteString
      bs = (BSL.toStrict . ZipArchive.fromEntry) mainDalf
  let p :: (PackageId, Package)
      p = (fromRight (error "decoding error") . Archive.decodeArchive Archive.DecodeAsMain) bs
  let p' :: (PackageId, PackageIds)
      p' = fmap (fromRight (error "decoding error") . importedPackages) p

  let deps = edDalfs dar
  let bss :: [BS.ByteString]
      bss = map (BSL.toStrict . ZipArchive.fromEntry) deps
  let pkgIdDepGraph :: [(PackageId, Package)]
      pkgIdDepGraph = map (fromRight (error "decoding error") . Archive.decodeArchive Archive.DecodeAsMain) bss
  let pkgIdDepGraph' :: [(PackageId, PackageIds)]
      pkgIdDepGraph' = map (fmap $ fromRight (error "decoding error") . importedPackages) pkgIdDepGraph

  let mentioned = transitiveClosure (M.fromList pkgIdDepGraph') (fst p')
  let included = (fromList $ map fst pkgIdDepGraph') `difference` fromList allStablePackageIds

  assertBool (printf "Included but not mentioned: %s" $ show $ included `difference` mentioned) $ included `difference` mentioned == empty
  assertBool (printf "Mentioned but not included: %s" $ show $ mentioned `difference` included) $ mentioned `difference` included == empty

  where
    darPath = "daml-script" </> "test" </> darname
