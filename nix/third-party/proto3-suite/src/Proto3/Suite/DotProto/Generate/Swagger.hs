{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | This module provides helper functions to generate Swagger schemas that
-- describe JSONPB encodings for protobuf types.
module Proto3.Suite.DotProto.Generate.Swagger
  ( ToSchema(..)
  , genericDeclareNamedSchemaJSONPB
  , ppSchema
  )
where

import           Control.Lens                    (over, set, (&), (.~), (?~))
import           Control.Lens.Cons               (_head)
import           Data.Aeson                      (Value (String))
import           Data.Aeson.Encode.Pretty        (encodePretty)
import qualified Data.ByteString                 as BS
import qualified Data.ByteString.Lazy.Char8      as LC8
import           Data.Char                       (toLower)
import           Data.Functor.Identity           (Identity)
import           Data.Swagger
import qualified Data.Swagger.Declare            as Swagger
import qualified Data.Swagger.Internal.Schema    as Swagger.Internal
import qualified Data.Swagger.Internal.TypeShape as Swagger.Internal
import qualified Data.Text                       as T
import           Data.Proxy
import qualified Data.Vector                     as V
import           GHC.Generics
import           GHC.Int
import           GHC.Word
import           Proto3.Suite                    (Enumerated (..), Finite (..),
                                                  Fixed (..), Named (..), enumerate)

genericDeclareNamedSchemaJSONPB :: forall a proxy.
                                   ( Generic a
                                   , Named a
                                   , Swagger.Internal.GenericHasSimpleShape a
                                       "genericDeclareNamedSchemaUnrestricted"
                                       (Swagger.Internal.GenericShape (Rep a))
                                   , Swagger.Internal.GToSchema (Rep a)
                                   )
                                => proxy a
                                -> Swagger.DeclareT (Definitions Schema) Identity NamedSchema
genericDeclareNamedSchemaJSONPB proxy =
  over schema (set required []) <$> genericDeclareNamedSchema opts proxy
  where
    opts = defaultSchemaOptions{
             fieldLabelModifier = over _head toLower
                                . drop (T.length (nameOf (Proxy @a)))
           }

-- | Pretty-prints a schema. Useful when playing around with schemas in the
-- REPL.
ppSchema :: ToSchema a => proxy a -> IO ()
ppSchema = LC8.putStrLn . encodePretty . toSchema

-- | This orphan instance prevents Generic-based deriving mechanism from
-- throwing error on 'ToSchema' for 'ByteString' and instead defaults to
-- 'byteSchema'. It is a damn dirty hack, but very handy, as per:
-- https://github.com/GetShopTV/swagger2/issues/51
instance Swagger.Internal.GToSchema (K1 i BS.ByteString) where
  gdeclareNamedSchema _ _ _ = pure
                            $ NamedSchema Nothing
                            $ byteSchema

-- | This orphan instance prevents Generic-based deriving mechanism from
-- throwing error on 'ToSchema' for 'V.Vector ByteString' and instead defaults
-- to (an array of) 'byteSchema'. It is a damn dirty hack, but very handy, as
-- per: https://github.com/GetShopTV/swagger2/issues/51
instance Swagger.Internal.GToSchema (K1 i (V.Vector BS.ByteString)) where
  gdeclareNamedSchema _ _ _ = pure
                            $ NamedSchema Nothing
                            $ mempty
                                & type_ .~ SwaggerArray
                                & items ?~ SwaggerItemsObject (Inline byteSchema)

-- | JSONPB schemas for protobuf enumerations
instance (Finite e, Named e) => ToSchema (Enumerated e) where
  declareNamedSchema _ = do
    let enumName        = nameOf (Proxy @e)
    let dropPrefix      = T.drop (T.length enumName)
    let enumMemberNames = dropPrefix . fst <$> enumerate (Proxy @e)
    return $ NamedSchema (Just enumName)
           $ mempty
             & type_ .~ SwaggerString
             & enum_ ?~ fmap String enumMemberNames

instance ToSchema (Fixed Int32) where
  declareNamedSchema _ = declareNamedSchema (Proxy @Int32)

instance ToSchema (Fixed Int64) where
  declareNamedSchema _ = declareNamedSchema (Proxy @Int64)

instance ToSchema (Fixed Word32) where
  declareNamedSchema _ = declareNamedSchema (Proxy @Word32)

instance ToSchema (Fixed Word64) where
  declareNamedSchema _ = declareNamedSchema (Proxy @Word64)
