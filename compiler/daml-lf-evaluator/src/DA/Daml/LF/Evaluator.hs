-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Daml.LF.Evaluator
  ( decodeDalfs
  , simplify, Prog(..), DefKey(..)
  , ppExp
  , runIntProgArg, Counts(..), Throw(..)
  ) where

import qualified Data.ByteString.Lazy as BSL (ByteString,toStrict)
import qualified Data.NameMap as NM

import DA.Daml.LF.Evaluator.Eval (runIntProgArg)
import DA.Daml.LF.Evaluator.Exp (Prog(..),DefKey(..))
import DA.Daml.LF.Evaluator.Pretty (ppExp)
import DA.Daml.LF.Evaluator.Simp (simplify)
import DA.Daml.LF.Evaluator.Value (Counts(..),Throw(..))
import DA.Daml.LF.Proto3.Archive (decodeArchive, DecodingMode(DecodeAsMain,DecodeAsDependency))
import DA.Daml.LF.Reader (Dalfs(..))
import qualified DA.Daml.LF.Ast as LF

decodeDalfs :: Dalfs -> IO ([LF.ExternalPackage],[LF.Module])
decodeDalfs Dalfs{mainDalf,dalfs} = do
  (_,mainPackage) <- decodeDalf DecodeAsMain mainDalf
  otherPackages <- mapM (decodeDalf DecodeAsDependency) dalfs
  let pkgs = [ LF.ExternalPackage pid pkg | (pid,pkg) <- otherPackages ]
  let LF.Package{packageModules} = mainPackage
  let mods = NM.toList packageModules
  return (pkgs,mods)
  where
    decodeDalf :: DecodingMode -> BSL.ByteString -> IO (LF.PackageId,LF.Package)
    decodeDalf mode dalfBS = do
      Right pair <- return $ decodeArchive mode $ BSL.toStrict dalfBS
      return pair
