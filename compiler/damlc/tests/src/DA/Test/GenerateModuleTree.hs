-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Generate a module tree consisting of modules FatTree0 to
-- FatTree<N> where FatTree<M> imports FatTree0-FatTree<M-1>. The
-- generated module tree is used as a regression test.
module DA.Test.GenerateModuleTree (main) where

import Data.Foldable
import Data.Text.Extended qualified as T
import System.Environment
import System.FilePath

main :: IO ()
main = do
    [dir, size] <- getArgs
    generateTree dir (read size)

generateTree :: FilePath -> Int -> IO ()
generateTree dir size = for_ (mkModules size) $ \(modName, content) ->
    T.writeFileUtf8 (dir </> modName) content

mkModules :: Int -> [(String, T.Text)] -- file name and contents
mkModules n = [(stdModName k <> ".daml", mkModule k) | k <- [1..n]]

mkModule :: Int -> T.Text
mkModule n = T.unlines
  [ mkHeader n, ""
  , T.unlines $ map mkImport [1..n-1]
  , mkDef n
  ]

mkHeader :: Int -> T.Text
mkHeader n = T.pack $ "module " <> stdModName n <> "\n  where"

mkImport :: Int -> T.Text
mkImport k = T.pack $ "import qualified " <> stdModName k

mkDef :: Int -> T.Text
mkDef n = T.pack $ "test" <> show n <> " = " <> importTerms
  where importTerms = foldr (\k t -> stdModName k <> ".test" <> show k <> " + " <> t) "1" [1..n-1]

stdModName :: Int -> String
stdModName k = "FatTree" <> show k
