-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds          #-}
{-# LANGUAGE DefaultSignatures  #-}
{-# LANGUAGE EmptyCase          #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies       #-}
{-# LANGUAGE TypeOperators      #-}
{-# LANGUAGE UndecidableInstances #-}
module DA.Daml.LF.Ast.Conversion where

import DA.Prelude

import           Data.Kind    (Constraint)
import qualified Data.Text    as T
import           GHC.Generics

import DA.Daml.LF.Ast.Base

-- * The whole machinery for "converting between almost identical types", cf.
-- https://gist.github.com/bitonic/f79ec9c304cbaa77476174648dfbbcbd

-- ** Type classes
type family AstType a :: *

class Applicative m => ToAst m a where
  toAst :: a -> m (AstType a)
  default toAst ::
    (Generic a, Generic (AstType a), GToAst m (Rep a) (Rep (AstType a))) =>
    a -> m (AstType a)
  toAst = fmap to . gtoAst . from

class GToAst m f g where
  gtoAst :: f p -> m (g p)

class Applicative m => FromAst m a where
  fromAst :: AstType a -> m a
  default fromAst ::
    (Generic a, Generic (AstType a), GFromAst m (Rep a) (Rep (AstType a))) =>
    AstType a -> m a
  fromAst = fmap to . gfromAst . from

class GFromAst m f g where
  gfromAst :: g p -> m (f p)

-- ** Instances of GToAst and GFromAst for types from GHC.Generics
instance GToAst m V1 V1 where
  gtoAst = \case {}

instance GFromAst m V1 V1 where
  gfromAst = \case {}

instance Applicative m => GToAst m U1 U1 where
  gtoAst U1 = pure U1

instance Applicative m => GFromAst m U1 U1 where
  gfromAst U1 = pure U1

instance (Applicative m, GToAst m f1 f2, GToAst m g1 g2) =>
  GToAst m (f1 :+: g1) (f2 :+: g2) where
  gtoAst = \case
    L1 x -> L1 <$> gtoAst x
    R1 y -> R1 <$> gtoAst y

instance (Applicative m, GFromAst m f1 f2, GFromAst m g1 g2) =>
  GFromAst m (f1 :+: g1) (f2 :+: g2) where
  gfromAst = \case
    L1 x -> L1 <$> gfromAst x
    R1 y -> R1 <$> gfromAst y

instance (Applicative m, GToAst m f1 f2, GToAst m g1 g2) =>
  GToAst m (f1 :*: g1) (f2 :*: g2) where
  gtoAst ((:*:) x y) = (:*:) <$> gtoAst x <*> gtoAst y

instance (Applicative m, GFromAst m f1 f2, GFromAst m g1 g2) =>
  GFromAst m (f1 :*: g1) (f2 :*: g2) where
  gfromAst ((:*:) x y) = (:*:) <$> gfromAst x <*> gfromAst y

instance (i1 ~ i2, ToAst m a1, AstType a1 ~ a2) => GToAst m (K1 i1 a1) (K1 i2 a2) where
  gtoAst (K1 x) = K1 <$> toAst x

instance (i1 ~ i2, FromAst m a1, AstType a1 ~ a2) => GFromAst m (K1 i1 a1) (K1 i2 a2) where
  gfromAst (K1 x) = K1 <$> fromAst x

type family EquivAstMeta (m1 :: Meta) (m2 :: Meta) :: Constraint where
  EquivAstMeta ('MetaData n1  m1  p1  nt1) ('MetaData n2  m2  p2  nt2) = (n1 ~ n2, nt1 ~ nt2)
  EquivAstMeta ('MetaCons n1  f1  s1     ) ('MetaCons n2  f2  s2     ) = (n1 ~ n2, f1 ~ f2)
  EquivAstMeta ('MetaSel  mn1 su1 ss1 ds1) ('MetaSel  mn2 su2 ss2 ds2) = (() :: Constraint)

instance (Applicative m, i1 ~ i2, EquivAstMeta m1 m2, GToAst m f1 f2) =>
  GToAst m (M1 i1 m1 f1) (M1 i2 m2 f2) where
  gtoAst (M1 x) = M1 <$> gtoAst x

instance (Applicative m, i1 ~ i2, EquivAstMeta m1 m2, GFromAst m f1 f2) =>
  GFromAst m (M1 i1 m1 f1) (M1 i2 m2 f2) where
  gfromAst (M1 x) = M1 <$> gfromAst x


-- ** Instances of AstType, ToAst and FromAst for some generic types
type instance AstType Bool = Bool
instance Applicative m => ToAst   m Bool where toAst   = pure
instance Applicative m => FromAst m Bool where fromAst = pure

type instance AstType T.Text = T.Text
instance Applicative m => ToAst   m T.Text where toAst   = pure
instance Applicative m => FromAst m T.Text where fromAst = pure

type instance AstType (Tagged t a) = Tagged t (AstType a)
instance ToAst   m a => ToAst   m (Tagged t a) where toAst   = traverse toAst
instance FromAst m a => FromAst m (Tagged t a) where fromAst = traverse fromAst

type instance AstType [a] = [AstType a]
instance ToAst   m a => ToAst   m [a] where toAst   = traverse toAst
instance FromAst m a => FromAst m [a] where fromAst = traverse fromAst

type instance AstType (a, b) = (AstType a, AstType b)
instance (ToAst   m a, ToAst   m b) => ToAst   m (a, b) where
  toAst   (x, y) = (,) <$> toAst x <*> toAst y
instance (FromAst m a, FromAst m b) => FromAst m (a, b) where
  fromAst (x, y) = (,) <$> fromAst x <*> fromAst y

type instance AstType (Qualified a) = Qualified (AstType a)
instance ToAst   m a => ToAst   m (Qualified a) where toAst   = traverse toAst
instance FromAst m a => FromAst m (Qualified a) where fromAst = traverse fromAst

-- ** "Reflexive" instances for AstType, ToAst and FromAst for some AST types
type instance AstType BuiltinType = BuiltinType
instance Applicative m => ToAst   m BuiltinType where toAst   = pure
instance Applicative m => FromAst m BuiltinType where fromAst = pure

type instance AstType BuiltinExpr = BuiltinExpr
instance Applicative m => ToAst   m BuiltinExpr where toAst   = pure
instance Applicative m => FromAst m BuiltinExpr where fromAst = pure

type instance AstType CasePattern = CasePattern
instance Applicative m => ToAst   m CasePattern where toAst   = pure
instance Applicative m => FromAst m CasePattern where fromAst = pure

