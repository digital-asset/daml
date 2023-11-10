-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
module DA.Daml.LF.Proto3.Util (
    EitherLike,
    toEither,
    fromEither,
    encodeHash,
    ) where

import Data.ByteString qualified as BS
import           Data.Int
import           Data.List
import Data.Text qualified as T
import Data.Text.Lazy qualified as TL
import           GHC.Generics
import Numeric qualified

import Com.Daml.DamlLfDev.DamlLf1 qualified as P

class EitherLike a b e where
    toEither :: e -> Either a b
    fromEither :: Either a b -> e

    default toEither
        :: (Generic e, Rep e ~ D1 m1 (C1 m2 (S1 m3 (Rec0 a)) :+: C1 m4 (S1 m5 (Rec0 b))))
        => e -> Either a b
    toEither e =
        case unM1 $ from e of
            L1 x -> Left $ unK1 $ unM1 $ unM1 x
            R1 y -> Right $ unK1 $ unM1 $ unM1 y
    default fromEither
        :: (Generic e, Rep e ~ D1 m1 (C1 m2 (S1 m3 (Rec0 a)) :+: C1 m4 (S1 m5 (Rec0 b))))
        => Either a b -> e
    fromEither = to . M1 . either (L1 . M1 . M1 . K1) (R1 . M1 . M1 . K1)

instance EitherLike a b (Either a b) where
    toEither = id
    fromEither = id

instance EitherLike TL.Text Int32 P.CaseAlt_ConsVarHead
instance EitherLike TL.Text Int32 P.CaseAlt_ConsVarTail
instance EitherLike TL.Text Int32 P.CaseAlt_EnumConstructor
instance EitherLike TL.Text Int32 P.CaseAlt_OptionalSomeVarBody
instance EitherLike TL.Text Int32 P.CaseAlt_VariantBinder
instance EitherLike TL.Text Int32 P.CaseAlt_VariantVariant
instance EitherLike TL.Text Int32 P.DefTemplateParam
instance EitherLike TL.Text Int32 P.Expr_EnumConEnumCon
instance EitherLike TL.Text Int32 P.Expr_RecProjField
instance EitherLike TL.Text Int32 P.Expr_RecUpdField
instance EitherLike TL.Text Int32 P.Expr_StructProjField
instance EitherLike TL.Text Int32 P.Expr_StructUpdField
instance EitherLike TL.Text Int32 P.Expr_VariantConVariantCon
instance EitherLike TL.Text Int32 P.FieldWithExprField
instance EitherLike TL.Text Int32 P.FieldWithTypeField
instance EitherLike TL.Text Int32 P.KeyExpr_ProjectionField
instance EitherLike TL.Text Int32 P.KeyExpr_RecordFieldField
instance EitherLike TL.Text Int32 P.TemplateChoiceName
instance EitherLike TL.Text Int32 P.TemplateChoiceSelfBinder
instance EitherLike TL.Text Int32 P.Type_VarVar
instance EitherLike TL.Text Int32 P.TypeVarWithKindVar
instance EitherLike TL.Text Int32 P.Update_ExerciseChoice
instance EitherLike TL.Text Int32 P.Update_SoftExerciseChoice
instance EitherLike TL.Text Int32 P.VarWithTypeVar

instance EitherLike P.DottedName Int32 P.ModuleRefModuleName
instance EitherLike P.DottedName Int32 P.TypeSynNameName
instance EitherLike P.DottedName Int32 P.TypeConNameName
instance EitherLike P.DottedName Int32 P.DefTemplateTycon
instance EitherLike P.DottedName Int32 P.DefTypeSynName
instance EitherLike P.DottedName Int32 P.DefDataTypeName
instance EitherLike P.DottedName Int32 P.ModuleName


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
