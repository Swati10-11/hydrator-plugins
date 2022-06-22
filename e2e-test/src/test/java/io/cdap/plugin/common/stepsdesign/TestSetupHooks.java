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
package io.cdap.plugin.common.stepsdesign;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.UUID;

/**
 * GCP test hooks.
 */
public class TestSetupHooks {

  @Before(order = 1, value = "@GCS_CSV_GROUPBY_TEST")
  public static void createBucketWithNormalizeFile() throws IOException, URISyntaxException {
    createGCSBucketWithFile(PluginPropertyUtils.pluginProp("GroupByGcsCsvFile"));
  }

  @After(order = 1, value = "@GCS_CSV_GROUPBY_TEST")
  public static void deleteSourceBucketWithFile() {
    deleteGCSBucket(PluginPropertyUtils.pluginProp("gcsSourceBucketName"));
    PluginPropertyUtils.removePluginProp("gcsSourceBucketName");
    PluginPropertyUtils.removePluginProp("gcsSourcePath");
  }

  private static String createGCSBucketWithFile(String filePath) throws IOException, URISyntaxException {
    String bucketName = StorageClient.createBucket("cdf-e2e-test-" + UUID.randomUUID()).getName();
    StorageClient.uploadObject(bucketName, filePath, filePath);
    PluginPropertyUtils.addPluginProp("gcsSourceBucketName", bucketName);
    PluginPropertyUtils.addPluginProp("gcsSourcePath", "gs://" + bucketName + "/" + filePath);
    BeforeActions.scenario.write("Created GCS Bucket " + bucketName + " containing " + filePath + " file");
    return bucketName;
  }

  private static void deleteGCSBucket(String bucketName) {
    try {
      for (Blob blob : StorageClient.listObjects(bucketName).iterateAll()) {
        StorageClient.deleteObject(bucketName, blob.getName());
      }
      StorageClient.deleteBucket(bucketName);
      BeforeActions.scenario.write("Deleted GCS Bucket " + bucketName);
    } catch (StorageException | IOException e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        BeforeActions.scenario.write("GCS Bucket " + bucketName + " does not exist.");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }
}
