-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.GeneratePackagePatternMatch (main) where

import System.Directory
import System.Environment
import System.FilePath

main :: IO ()
main = do
    [dir, numConstrs, numTys] <- getArgs
    removePathForcibly (dir </> "main")
    removePathForcibly (dir </> "dep")
    createDirectoryIfMissing True (dir </> "main")
    createDirectoryIfMissing True (dir </> "dep")
    writeFile (dir </> "dep" </> "Dep.daml") (depMod numTys)
    writeFile (dir </> "dep" </> "ADT.daml") (adtMod numConstrs)
    writeFile (dir </> "dep" </> "daml.yaml") (damlYaml "dep" "." [])
    writeFile (dir </> "main" </> "Main.daml") mainMod
    writeFile (dir </> "main" </> "daml.yaml") (damlYaml "main" "Main.daml" ["../dep/.daml/dist/dep-0.0.1.dar"])
  where
      damlYaml name src deps = unlines $
        [ "sdk-version: 0.0.0"
        , "name: " <> name
        , "version: 0.0.1"
        , "source: " <> src
        , "dependencies:"
        , "  - daml-stdlib"
        , "  - daml-prim"
        ] <> map ("  - " <>) deps
      -- C is basically HasField but I don’t want to rely on our custom HasField and the
      -- record dot preprocessor.
      depMod numTys = unlines $
            [ "module Dep where"
            , "data A"
            , "data B"
            , "data C"
            , "data D"
            , "data E"
            , "class Cl s a b | s a -> b where"
            ] <> concat
            [ [ "data " <> ty <> " = " <> ty <> " {}"
              , "instance Cl A " <> ty <> " ()"
              , "instance Cl B " <> ty <> " ()"
              , "instance Cl C " <> ty <> " ()"
              , "instance Cl D " <> ty <> " ()"
              , "instance Cl E " <> ty <> " ()"
              ]
            | i <- [ 1 :: Int .. read numTys ]
            , let ty = "X" <> show i
            ]
      adtMod numConstrs = unlines $
            [ "module ADT (T(..)) where"
            , "data Opaque = Opaque"
            , "data Arg = Arg Opaque"
            , "data T = C0 Arg"
            ] <>
            [ " | C" <> show i <> " Arg"
            | i <- [ 1 :: Int .. (read numConstrs - 1) ]
            ]
      mainMod = unlines
            [ "module Main where"
            , "import Dep (Cl, A, B, C, D, E)"
            , "import qualified ADT"
            , "f : forall a."
            , "     ( Cl A a ()"
            , "     , Cl B a ()"
            , "     , Cl C a ()"
            , "     , Cl D a ()"
            , "     , Cl E a ()"
            , "     ) => ADT.T -> a"
            , "f x = case x of"
            , "    ADT.C5 _ -> error \"a\""
            , "    ADT.C6 _ -> error \"b\""
            , "    ADT.C7 _ -> error \"c\""
            , "    ADT.C8 _ -> error \"d\""
            , "    e -> error \"e\""
            ]

