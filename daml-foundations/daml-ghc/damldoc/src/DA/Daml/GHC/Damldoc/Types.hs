-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell #-}

module DA.Daml.GHC.Damldoc.Types(
    module DA.Daml.GHC.Damldoc.Types
    ) where

import qualified Data.Aeson.TH.Extended as Aeson.TH
import           Data.Text              (Text)
import           Data.Hashable

-- | Markdown type, simple wrapper for now
type Markdown = Text

-- | Field name, starting with lowercase
type Fieldname = Text

-- | Type name starting with uppercase
type Typename = Text

-- | Module name, starting with uppercase, may have dots.
type Modulename = Text

-- | Type expression, possibly a (nested) type application
data Type = TypeApp Typename [Type] -- ^ Type application
          | TypeFun [Type] -- ^ Function type
          | TypeList Type   -- ^ List syntax
          | TypeTuple [Type] -- ^ Tuple syntax
  deriving (Eq, Show)

instance Hashable Type where
  hashWithSalt salt = hashWithSalt salt . show

------------------------------------------------------------
-- | Documentation data for a module
data ModuleDoc = ModuleDoc
  { md_name      :: Modulename
  , md_descr     :: Maybe Markdown
  , md_templates :: [TemplateDoc]
  , md_adts      :: [ADTDoc]
  , md_functions :: [FunctionDoc]
  , md_classes   :: [ClassDoc]
  -- TODO will later be refactored to contain "documentation sections" with an
  -- optional header, containing groups of templates and ADTs. This can be done
  -- storing just linkIDs for them, the renderer would then search the lists.
  }
  deriving (Eq, Show)


-- | Documentation data for a template
data TemplateDoc = TemplateDoc
  { td_name    :: Typename
  , td_descr   :: Maybe Markdown
  , td_payload :: [FieldDoc]
  , td_choices :: [ChoiceDoc]
  }
  deriving (Eq, Show)

data ClassDoc = ClassDoc
  { cl_name :: Typename
  , cl_descr :: Maybe Markdown
  , cl_super :: Maybe Type
  , cl_args :: [Text]
  , cl_functions :: [FunctionDoc]
  }
  deriving (Eq, Show)

-- | Documentation data for an ADT or type synonym
data ADTDoc = ADTDoc
  { ad_name   :: Typename
  , ad_descr  :: Maybe Markdown
  , ad_args   :: [Text] -- retain names of type var.s
  , ad_constrs :: [ADTConstr]  -- allowed to be empty
  }
  | TypeSynDoc
  { ad_name   :: Typename
  , ad_descr  :: Maybe Markdown
  , ad_args   :: [Text] -- retain names of type var.s
  , ad_rhs    :: Type
  }
  deriving (Eq, Show)


-- | Constructors (Record or Prefix)
data ADTConstr =
    PrefixC { ac_name :: Typename
            , ac_descr :: Maybe Markdown
            , ac_args :: [Type]   -- use retained var.names
            }
  | RecordC { ac_name :: Typename
            , ac_descr :: Maybe Markdown
            , ac_fields :: [FieldDoc]
            }
  deriving (Eq, Show)


-- | Choices are like ADTs: name, link, optional description, but the
-- associated type always has exactly one record constructor with the same name
-- as the choice.
data ChoiceDoc = ChoiceDoc
  { cd_name   :: Typename
  , cd_descr  :: Maybe Markdown
  , cd_fields :: [FieldDoc]
  }
  deriving (Eq, Show)


-- | Documentation data for a field in a record
data FieldDoc = FieldDoc
  { fd_name  :: Fieldname
  , fd_type  :: Type
    -- TODO align with GHC data structure. The type representation must use FQ
    -- names in components to enable links, and it Can use bound type var.s.
  , fd_descr :: Maybe Markdown
  }
  deriving (Eq, Show)


-- | Documentation data for functions (top level only, type optional)
data FunctionDoc = FunctionDoc
  { fct_name  :: Fieldname
  , fct_context :: Maybe Type
  , fct_type  :: Maybe Type
  , fct_descr :: Maybe Markdown
  }
  deriving (Eq, Show)

-----------------------------------------------------
-- generate JSON instances

$(Aeson.TH.deriveDAToJSON   "" ''Type)
$(Aeson.TH.deriveDAFromJSON "" ''Type)

$(Aeson.TH.deriveDAToJSON   "" ''FunctionDoc)
$(Aeson.TH.deriveDAFromJSON "" ''FunctionDoc)

$(Aeson.TH.deriveDAToJSON   "" ''ClassDoc)
$(Aeson.TH.deriveDAFromJSON "" ''ClassDoc)

$(Aeson.TH.deriveDAToJSON   "" ''FieldDoc)
$(Aeson.TH.deriveDAFromJSON "" ''FieldDoc)

$(Aeson.TH.deriveDAToJSON   "" ''ADTDoc)
$(Aeson.TH.deriveDAFromJSON "" ''ADTDoc)

$(Aeson.TH.deriveDAToJSON   "" ''ADTConstr)
$(Aeson.TH.deriveDAFromJSON "" ''ADTConstr)

$(Aeson.TH.deriveDAToJSON   "" ''ChoiceDoc)
$(Aeson.TH.deriveDAFromJSON "" ''ChoiceDoc)

$(Aeson.TH.deriveDAToJSON   "" ''TemplateDoc)
$(Aeson.TH.deriveDAFromJSON "" ''TemplateDoc)

$(Aeson.TH.deriveDAToJSON   "" ''ModuleDoc)
$(Aeson.TH.deriveDAFromJSON "" ''ModuleDoc)
