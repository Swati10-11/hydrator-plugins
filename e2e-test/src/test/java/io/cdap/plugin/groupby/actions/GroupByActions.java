/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.groupby.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.JsonUtils;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.groupby.locators.GroupByLocators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * GroupBy plugin related Actions
 */

public class GroupByActions {
  private static final Logger logger = (Logger) LoggerFactory.getLogger(GroupByActions.class);

  static {
    SeleniumHelper.getPropertiesLocators(GroupByLocators.class);
  }

  public static void enterFields(String jsonFields) {
    Map<String, String> fields =
      JsonUtils.convertKeyValueJsonArrayToMap(PluginPropertyUtils.pluginProp(jsonFields));
    int index = 0;
    for (Map.Entry<String, String> entry : fields.entrySet()) {
      ElementHelper.sendKeys(GroupByLocators.fieldsKey(index), entry.getKey());
      ElementHelper.clickOnElement(GroupByLocators.fieldsAddRowButton(index));
      index++;
    }
  }

  public static void enterAggregates(String jsonAggreegatesFields) {
    Map<String, String> fieldsMapping =
      JsonUtils.convertKeyValueJsonArrayToMap(PluginPropertyUtils.pluginProp(jsonAggreegatesFields));
    int index = 0;
    for (Map.Entry<String, String> entry : fieldsMapping.entrySet()) {
      ElementHelper.sendKeys(GroupByLocators.field(index), entry.getKey().split("#")[0]);
      ElementHelper.selectDropdownOption(GroupByLocators.fieldFunction(index), CdfPluginPropertiesLocators.locateDropdownListItem(entry.getKey().split("#")[1]));
      ElementHelper.sendKeys(GroupByLocators.fieldFunctionAlias(index), entry.getValue());
      ElementHelper.clickOnElement(GroupByLocators.fieldAddRowButton(index));
      index++;
    }
  }
}
