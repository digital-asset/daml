-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}

module DA.Daml.GHC.Damldoc.Types(
    module DA.Daml.GHC.Damldoc.Types
    ) where

import Data.Aeson
import           Data.Text              (Text)
import           Data.Hashable
import GHC.Generics
import Data.String

-- | Doc text type, presumably Markdown format.
newtype DocText = DocText { unDocText :: Text }
    deriving newtype (Eq, Ord, Show, ToJSON, FromJSON, IsString)

-- | Field name, starting with lowercase
newtype Fieldname = Fieldname { unFieldname :: Text }
    deriving newtype (Eq, Ord, Show, ToJSON, FromJSON, IsString)

-- | Type name starting with uppercase
newtype Typename = Typename { unTypename :: Text }
    deriving newtype (Eq, Ord, Show, ToJSON, FromJSON, IsString)

-- | Module name, starting with uppercase, may have dots.
newtype Modulename = Modulename { unModulename :: Text }
    deriving newtype (Eq, Ord, Show, ToJSON, FromJSON, IsString)

-- | Type expression, possibly a (nested) type application
data Type = TypeApp Typename [Type] -- ^ Type application
          | TypeFun [Type] -- ^ Function type
          | TypeList Type   -- ^ List syntax
          | TypeTuple [Type] -- ^ Tuple syntax
  deriving (Eq, Ord, Show, Generic)

instance Hashable Type where
  hashWithSalt salt = hashWithSalt salt . show

------------------------------------------------------------
-- | Documentation data for a module
data ModuleDoc = ModuleDoc
  { md_name      :: Modulename
  , md_descr     :: Maybe DocText
  , md_templates :: [TemplateDoc]
  , md_adts      :: [ADTDoc]
  , md_functions :: [FunctionDoc]
  , md_classes   :: [ClassDoc]
  -- TODO will later be refactored to contain "documentation sections" with an
  -- optional header, containing groups of templates and ADTs. This can be done
  -- storing just linkIDs for them, the renderer would then search the lists.
  }
  deriving (Eq, Show, Generic)


-- | Documentation data for a template
data TemplateDoc = TemplateDoc
  { td_name    :: Typename
  , td_descr   :: Maybe DocText
  , td_payload :: [FieldDoc]
  , td_choices :: [ChoiceDoc]
  }
  deriving (Eq, Show, Generic)

data ClassDoc = ClassDoc
  { cl_name :: Typename
  , cl_descr :: Maybe DocText
  , cl_super :: Maybe Type
  , cl_args :: [Text]
  , cl_functions :: [FunctionDoc]
  }
  deriving (Eq, Show, Generic)

-- | Documentation data for an ADT or type synonym
data ADTDoc = ADTDoc
  { ad_name   :: Typename
  , ad_descr  :: Maybe DocText
  , ad_args   :: [Text] -- retain names of type var.s
  , ad_constrs :: [ADTConstr]  -- allowed to be empty
  }
  | TypeSynDoc
  { ad_name   :: Typename
  , ad_descr  :: Maybe DocText
  , ad_args   :: [Text] -- retain names of type var.s
  , ad_rhs    :: Type
  }
  deriving (Eq, Show, Generic)


-- | Constructors (Record or Prefix)
data ADTConstr =
    PrefixC { ac_name :: Typename
            , ac_descr :: Maybe DocText
            , ac_args :: [Type]   -- use retained var.names
            }
  | RecordC { ac_name :: Typename
            , ac_descr :: Maybe DocText
            , ac_fields :: [FieldDoc]
            }
  deriving (Eq, Show, Generic)


-- | Choices are like ADTs: name, link, optional description, but the
-- associated type always has exactly one record constructor with the same name
-- as the choice.
data ChoiceDoc = ChoiceDoc
  { cd_name   :: Typename
  , cd_descr  :: Maybe DocText
  , cd_fields :: [FieldDoc]
  }
  deriving (Eq, Show, Generic)


-- | Documentation data for a field in a record
data FieldDoc = FieldDoc
  { fd_name  :: Fieldname
  , fd_type  :: Type
    -- TODO align with GHC data structure. The type representation must use FQ
    -- names in components to enable links, and it Can use bound type var.s.
  , fd_descr :: Maybe DocText
  }
  deriving (Eq, Show, Generic)


-- | Documentation data for functions (top level only, type optional)
data FunctionDoc = FunctionDoc
  { fct_name  :: Fieldname
  , fct_context :: Maybe Type
  , fct_type  :: Maybe Type
  , fct_descr :: Maybe DocText
  }
  deriving (Eq, Show, Generic)

-----------------------------------------------------
-- generate JSON instances

instance ToJSON Type where
    toJSON = genericToJSON aesonOptions

instance FromJSON Type where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON FunctionDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON FunctionDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON ClassDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON ClassDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON FieldDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON FieldDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON ADTDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON ADTDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON ADTConstr where
    toJSON = genericToJSON aesonOptions

instance FromJSON ADTConstr where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON ChoiceDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON ChoiceDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON TemplateDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON TemplateDoc where
    parseJSON = genericParseJSON aesonOptions

instance ToJSON ModuleDoc where
    toJSON = genericToJSON aesonOptions

instance FromJSON ModuleDoc where
    parseJSON = genericParseJSON aesonOptions

aesonOptions :: Options
aesonOptions = defaultOptions
    { sumEncoding = ObjectWithSingleField
    , omitNothingFields = True
    }
