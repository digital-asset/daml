// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components.helpers;

import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.Identifier;
import com.daml.ledger.javaapi.data.Template;
import com.daml.ledger.javaapi.data.Value;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TemplateUtils {

  /**
   * Creates a transform function which is aware of the template types. Useful to be passed to
   * Bot.wire.
   *
   * @param allowedTemplates the list of allowed template types
   * @return a transformation function returning a new contract typed as one of the allowed
   *     templates
   */
  public static Function<CreatedContract, Template> contractTransformer(
      Class<? extends Template>... allowedTemplates) {
    Map<Identifier, Method> factories = new HashMap<>();
    for (Class<? extends Template> template : allowedTemplates) {
      try {
        Identifier identifier = (Identifier) template.getField("TEMPLATE_ID").get(null);
        Method method = template.getMethod("fromValue", new Class[] {Value.class});
        factories.put(identifier, method);
      } catch (IllegalAccessException | NoSuchFieldException | NoSuchMethodException e) {
        throw new RuntimeException("Argument " + template + " should be derived from Template", e);
      }
    }
    return createdContract -> {
      if (!factories.containsKey(createdContract.getTemplateId())) {
        throw new IllegalStateException(
            "Unknown contract of type " + createdContract.getTemplateId());
      } else {
        DamlRecord args = createdContract.getCreateArguments();
        Method method = factories.get(createdContract.getTemplateId());
        try {
          return (Template) method.invoke(null, args);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(
              "Argument " + method.getDeclaringClass() + " should be derived from Template", e);
        }
      }
    };
  }
}
