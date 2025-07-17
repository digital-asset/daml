-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Util (
  module DA.Daml.LF.Proto3.Util
  ) where

import qualified Data.ByteString as BS
import           Data.Int
import           Data.List
import qualified Data.Text       as T
import qualified Data.Vector     as V
import qualified Numeric

import qualified Com.Digitalasset.Daml.Lf.Archive.DamlLf2 as P
import qualified Proto3.Suite                             as P (Enumerated (..))

-- | Encode the hash bytes of the payload in the canonical
-- lower-case ascii7 hex presentation.
encodeHash :: BS.ByteString -> T.Text
encodeHash = T.pack . reverse . foldl' toHex [] . BS.unpack
  where
    toHex xs c =
      case Numeric.showHex c "" of
        [n1, n2] -> n2 : n1 : xs
        [n2]     -> n2 : '0' : xs
        _        -> error "impossible: showHex returned [] on Word8"


------------------------------------------------------------------------
-- Shorthands
------------------------------------------------------------------------

-- Kinds
liftK :: P.KindSum -> P.Kind
liftK = P.Kind . Just

pkstar :: P.Kind
pkstar = (liftK . P.KindSumStar) P.Unit

pknat :: P.Kind
pknat = (liftK . P.KindSumNat) P.Unit

pkarr :: P.Kind -> P.Kind -> P.Kind
pkarr k1 k2 = liftK $ P.KindSumArrow $ P.Kind_Arrow (V.singleton k1) (Just k2)

pkinterned :: Int32 -> P.Kind
pkinterned = liftK . P.KindSumInterned

-- Types
pliftT :: P.TypeSum -> P.Type
pliftT = P.Type . Just

pbuiltin :: P.BuiltinType -> P.Type
pbuiltin bit = pliftT $ P.TypeSumBuiltin $ P.Type_Builtin (P.Enumerated $ Right bit) V.empty

ptunit :: P.Type
ptunit = pbuiltin P.BuiltinTypeUNIT

ptint :: P.Type
ptint = pbuiltin P.BuiltinTypeINT64

ptbool :: P.Type
ptbool = pbuiltin P.BuiltinTypeBOOL

ptarr :: P.Type -> P.Type -> P.Type
ptarr t1 t2 = pliftT $ P.TypeSumBuiltin $ P.Type_Builtin (P.Enumerated $ Right P.BuiltinTypeARROW) (V.fromList [t1, t2])

ptinterned :: Int32 -> P.Type
ptinterned = pliftT . P.TypeSumInterned

ptforall :: Int32 -> P.Kind -> P.Type -> P.Type
ptforall a k t = pliftT $ P.TypeSumForall $ P.Type_Forall (V.singleton $ P.TypeVarWithKind a (Just k)) (Just t)

ptstructSingleton :: Int32 -> P.Type -> P.Type
ptstructSingleton i t = pliftT $ P.TypeSumStruct $ P.Type_Struct $ V.singleton $ P.FieldWithType i (Just t)

ptconid :: Int32 -> P.TypeConId
ptconid = P.TypeConId (Just $ P.ModuleId (Just id) 0)
  where
    id :: P.SelfOrImportedPackageId
    id = P.SelfOrImportedPackageId $ Just sum

    sum :: P.SelfOrImportedPackageIdSum
    sum = P.SelfOrImportedPackageIdSumSelfPackageId P.Unit

ptcon :: Int32 -> V.Vector P.Type -> P.Type
ptcon i args = pliftT $ P.TypeSumCon $ P.Type_Con (Just $ ptconid i) args

ptsynid :: Int32 -> P.TypeSynId
ptsynid = P.TypeSynId (Just $ P.ModuleId (Just id) 0)
  where
    id :: P.SelfOrImportedPackageId
    id = P.SelfOrImportedPackageId $ Just sum

    sum :: P.SelfOrImportedPackageIdSum
    sum = P.SelfOrImportedPackageIdSumSelfPackageId P.Unit

ptsyn :: Int32 -> V.Vector P.Type -> P.Type
ptsyn i args = pliftT $ P.TypeSumSyn $ P.Type_Syn (Just (ptsynid i)) args
