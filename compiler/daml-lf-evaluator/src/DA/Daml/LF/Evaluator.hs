-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Daml.LF.Evaluator
  ( decodeDalfs,
    simplify,
    runIntProgArg, Counts(..),
  ) where

import qualified Data.ByteString.Lazy as BSL (ByteString,toStrict)
import qualified Data.Map.Strict as Map

import DA.Daml.LF.Evaluator.Eval (runIntProgArg)
import DA.Daml.LF.Evaluator.Simp (simplify)
import DA.Daml.LF.Evaluator.Value (Counts(..))
import DA.Daml.LF.Optimize (World(..))
import DA.Daml.LF.Proto3.Archive (decodeArchive, DecodingMode(DecodeAsMain))
import DA.Daml.LF.Reader (Dalfs(..))
import qualified DA.Daml.LF.Ast as LF

decodeDalfs :: Dalfs -> IO World
decodeDalfs Dalfs{mainDalf,dalfs} = do
  (mainId,mainPackage) <- decodeDalf mainDalf
  otherIdentifiedPackages <- mapM decodeDalf dalfs
  let packageMap = Map.fromList $ otherIdentifiedPackages <> [(mainId,mainPackage)]
  return $ World { mainIdM = Just mainId, packageMap, mainPackageM = Just mainPackage }
  where
    decodeDalf :: BSL.ByteString -> IO (LF.PackageId,LF.Package)
    decodeDalf dalfBS = do
      Right pair <- return $ decodeArchive DecodeAsMain $ BSL.toStrict dalfBS
      return pair

