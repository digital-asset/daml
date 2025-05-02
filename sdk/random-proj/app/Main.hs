-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Main where

import Control.Monad (filterM)
import System.Directory.Tree (AnchoredDirTree ((:/)), DirTree (Dir, File), writeDirectory)
import System.Environment (getArgs)
import System.Random (randomIO)

type Dag = [(Int, [Int])]

genRandomDag :: Int -> IO Dag
genRandomDag n = mapM genEntry [0 .. n - 1]
  where
    genEntry :: Int -> IO (Int, [Int])
    genEntry i = (i,) <$> filterM (const randomIO) [0 .. i - 1]

pkgName :: Int -> String
pkgName i = "p" ++ show i

moduleName :: Int -> String
moduleName i = "M" ++ show i

fieldName :: Int -> String
fieldName i = "f" ++ show i

mkTopLevelDamlYaml :: String -> DirTree String
mkTopLevelDamlYaml sdkVersion =
    File "daml.yaml" $ ("sdk-version: " ++ sdkVersion)

mkMultiPackageYaml :: Int -> DirTree String
mkMultiPackageYaml n =
    File "multi-package.yaml" $
        unlines ("packages:" : ["  - " ++ pkgName i | i <- [0 .. n - 1]])

mkProject :: String -> Int -> [Int] -> DirTree String
mkProject sdkVersion i deps = Dir (pkgName i) [damlYaml, Dir "daml" [mainDaml]]
  where
    damlYaml =
        File "daml.yaml" $
            unlines
                ( [ "sdk-version: " ++ sdkVersion
                  , "name: " ++ pkgName i
                  , "source: daml"
                  , "version: 0.0.1"
                  , "dependencies:"
                  , "  - daml-prim"
                  , "  - daml-stdlib"
                  , "data-dependencies:"
                  ]
                    ++ ["  - ../" ++ pkgName d ++ "/.daml/dist/" ++ pkgName d ++ "-0.0.1.dar" | d <- deps]
                )
    mainDaml =
        File (moduleName i ++ ".daml") $
            unlines
                ( ("module " ++ moduleName i ++ " where")
                    : ["import qualified M" ++ show d | d <- deps]
                    -- ++ ["data T = T {" ++ intercalate ", " [fieldName d ++ " : " ++ moduleName d ++ ".T" | d <- deps] ++ "}"]
                    ++ ["data T = T {}"]
                    ++ ["data T" ++ show d ++ " = T" ++ show d ++ " " ++ moduleName d ++ ".T" | d <- deps]
                )
dagToFileTree :: String -> Dag -> DirTree String
dagToFileTree sdkVersion g =
    Dir "." $
        mkTopLevelDamlYaml sdkVersion
            : mkMultiPackageYaml (length g)
            : [mkProject sdkVersion i deps | (i, deps) <- g]

main :: IO ()
main = do
    ((read -> n) : sdkVersion : outputDir : _) <- getArgs
    g <- genRandomDag n
    let tree = dagToFileTree sdkVersion g
    print tree
    _ <- writeDirectory (outputDir :/ tree)
    return ()

