-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf #-}
module DA.Test.UnstableTypes (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Data.Bifunctor
import Control.Monad.Extra
import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Proto3.Archive as LFArchive
import qualified Data.ByteString as BS
import Data.List.Extra
import qualified Data.NameMap as NM
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    pkgDb <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "pkg-db" </> "pkg-db_dir")
    dalfs <- filter (\f -> takeExtension f == ".dalf") <$> listFilesRecursive pkgDb
    when (null dalfs) $ do
        fail "Location to pkg db is incorrect, no DALFs found"
    defaultMain $ testGroup "unstable-types"
        [ testCase (makeRelative pkgDb dalf) $ do
              bytes <- BS.readFile dalf
              (_pkgId, pkg) <-
                  either (fail . show) pure $
                  LFArchive.decodeArchive LFArchive.DecodeAsMain bytes
              let serializableTypes =
                      sort $
                      [ (LF.moduleName mod, LF.dataTypeCon ty)
                      | mod <- NM.toList (LF.packageModules pkg)
                      , ty <- NM.toList (LF.moduleDataTypes mod)
                      , LF.getIsSerializable (LF.dataSerializable ty)
                      ]
              if | "daml-prim" == takeBaseName dalf ->
                   serializableTypes @?= sort damlPrimTypes
                 | "daml-stdlib" `isPrefixOf` takeBaseName dalf ->
                   serializableTypes @?= sort damlStdlibTypes
                 | otherwise ->
                   assertFailure ("Unknown package: " <> show dalf)
              pure ()
        | dalf <- dalfs
        ]

-- | This tests that daml-prim only introduces these serializable datatypes
-- (any other serializable datatypes exposed by it are actually from stable packages)
damlPrimTypes :: [(LF.ModuleName, LF.TypeConName)]
damlPrimTypes = map (bimap LF.ModuleName LF.TypeConName)
    [ (["GHC", "Stack", "Types"], ["CallStack"])
    , (["GHC", "Stack", "Types"], ["SrcLoc"])
    ]

-- | This tests that daml-stdlib introduces no serializable datatypes
-- (any serializable datatypes exposed by it are actually from stable packages)
damlStdlibTypes :: [(LF.ModuleName, LF.TypeConName)]
damlStdlibTypes = []
