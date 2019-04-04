-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Cli.Damli.Command.LF
  ( cmdRoundtripLF1
  , cmdAuth
  , valueCheckOpt
  ) where

import           Control.Monad.Except
import           DA.Cli.Damli.Base
import qualified DA.Daml.LF.Ast                   as LF
import qualified DA.Daml.LF.Proto3.Archive        as Archive
import qualified DA.Daml.LF.TypeChecker           as LF
import qualified DA.Daml.LF.Auth                  as LF
import qualified Data.Text as T
import qualified DA.Pretty
import qualified Data.ByteString                  as BS
import           Options.Applicative

-------------------------------------------------------------------------
-- Command specs
-------------------------------------------------------------------------

cmdRoundtripLF1 :: Mod CommandFields Command
cmdRoundtripLF1 =
    command "roundtrip-lf-v1"
      $ info (helper <*> cmd)
      $    progDesc "Load a DAML-LF v1 archive, type-check it and dump it again, verifying that output matches."
        <> fullDesc
  where
    cmd = execRoundtripLF1 <$> lfTypeCheckOpt <*> inputFileOpt <*> outputFileOpt <*> valueCheckOpt

valueCheckOpt :: Parser LF.ValueCheck
valueCheckOpt =
  (\b -> if b then LF.PerformValueCheck else LF.UnsafeSkipValueCheck) <$> switch (long "value-check" <> help "Run DAML-LF value checker")

lfTypeCheckOpt :: Parser Bool
lfTypeCheckOpt =
  not <$> switch (long "unsafe" <> short 'u' <> help "Skip DAML-LF type checker")

-- | Check a DAML-LF module for static authorisation errors.
cmdAuth :: Mod CommandFields Command
cmdAuth
 =  command "auth"
        $ info (helper <*> cmd)
        $ progDesc "Check a DAML-LF module for static authorisation errors."
        <> fullDesc
 where
    cmd = execAuth <$> inputFileOpt

-------------------------------------------------------------------------
-- Implementation
-------------------------------------------------------------------------

loadLFPackage :: FilePath -> IO (LF.Package, BS.ByteString)
loadLFPackage inFile = do
    -- Load, checksum and decode the LF package
    bytes <- BS.readFile inFile

    (_pkgId, package) <- errorOnLeft "Cannot decode header" $ Archive.decodeArchive bytes
    return (package, bytes)
    where
        errorOnLeft desc (Left x) = error (desc <> ":" <> show x)
        errorOnLeft _ (Right x)   = pure x


execRoundtripLF1 :: Bool -> FilePath -> FilePath -> LF.ValueCheck -> Command
execRoundtripLF1 _check inFile outFile valueCheck = do
    (package, bytes) <- loadLFPackage inFile
    -- Type-check
    case LF.checkPackage [] package valueCheck of
      Left err -> do
        error $ T.unpack $ "Type-check failed:\n" <> DA.Pretty.renderPretty err
      Right () ->
        pure ()

    -- Encode the package
    let bytes' = Archive.encodeArchive package

    -- And finally verify that the resulting bytes match.
    when (bytes /= bytes') $ do
      write bytes'
      error $ "Resulting output differs. Dumped output to " <> outFile
  where
    write | outFile == "-" = BS.putStr
          | otherwise = BS.writeFile outFile

execAuth :: FilePath -> Command
execAuth inFile = do
    (package, _) <- loadLFPackage inFile

    -- Auth-check
    let errs = LF.checkAuth [] package
    unless (null errs) $
      error $ unlines $ "Static authorisation check failed:" : map (('\n':) . DA.Pretty.renderPretty) errs
