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
pkinterned = liftK . P.KindSumInternedKind

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

-- Exprs
liftE :: P.ExprSum -> P.Expr
liftE = P.Expr Nothing . Just

peInterned :: Int32 -> P.Expr
peInterned = liftE . P.ExprSumInternedExpr

peBuiltinCon :: P.BuiltinCon -> P.Expr
peBuiltinCon bit = liftE $ P.ExprSumBuiltinCon $ P.Enumerated $ Right bit

peUnit :: P.Expr
peUnit = peBuiltinCon P.BuiltinConCON_UNIT

peVar :: Int32 -> P.Expr
peVar = liftE . P.ExprSumVarInternedStr

peSelfOrImportedPackageIdSelf :: Maybe P.SelfOrImportedPackageId
peSelfOrImportedPackageIdSelf = Just $ P.SelfOrImportedPackageId $ Just $ P.SelfOrImportedPackageIdSumSelfPackageId P.Unit

peVal :: Int32 -> Int32 -> P.Expr
peVal mod val = liftE $ P.ExprSumVal $ P.ValueId (Just $ P.ModuleId peSelfOrImportedPackageIdSelf mod) val

peTrue :: P.Expr
peTrue = peBuiltinCon P.BuiltinConCON_TRUE

peApp :: P.Expr -> P.Expr -> P.Expr
peApp e1 e2 = liftE $ P.ExprSumApp $ P.Expr_App (Just e1) (V.singleton e2)

peTyApp :: P.Expr -> P.Type -> P.Expr
peTyApp e t = liftE $ P.ExprSumTyApp $ P.Expr_TyApp (Just e) (V.singleton t)

peTyAbs :: Int32 -> P.Kind -> P.Expr -> P.Expr
peTyAbs i k e = liftE $ P.ExprSumTyAbs $ P.Expr_TyAbs (V.singleton var) (Just e)
  where
    var :: P.TypeVarWithKind
    var = P.TypeVarWithKind i (Just k)
