/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.plugin.format.delimited.input;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.SchemaValidator;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Delimited text format that tracks which file each record was read from.
 */
public class PathTrackingDelimitedInputFormat extends PathTrackingInputFormat {
  static final String DELIMITER = "delimiter";
  static final String ENABLE_QUOTES_VALUE = "enable_quotes_value";
  static final String SKIP_HEADER = "skip_header";

  private static final String QUOTE = "\"";
  private static final char QUOTE_CHAR = '\"';

  /**
   * Split the delimited string based on the delimiter. The delimiter should not contain any quotes.
   * The method will behave like this: 1. if there is no quote, it will behave same as {@link
   * String#split(String)} 2. if there are quotes in the string, the method will find pairs of
   * quotes, content within each pair of quotes will not get splitted even if there is delimiter in
   * that. For example, if string is a."b.c"."d.e.f" and delimiter is '.', it will get split into
   * [a, b.c, d.e.f]. if string is "val1.val2", then it will not get splitted since the '.' is
   * within pair of quotes. If the delimited string contains odd number of quotes, which mean the
   * quotes are not closed, an exception will be thrown. The quote within the value will always be
   * trimed.
   *
   * @param delimitedString the string to split
   * @param delimiter the separtor
   * @return a list of splits of the original string
   */
  @VisibleForTesting
  static Iterable<String> splitQuotesString(String delimitedString, String delimiter)
      throws IOException {

    boolean isWithinQuotes = false;
    List<String> result = new ArrayList<>();
    StringBuilder split = new StringBuilder();

    for (int i = 0; i < delimitedString.length(); i++) {
      char cur = delimitedString.charAt(i);
      if (cur == QUOTE_CHAR) {
        isWithinQuotes = !isWithinQuotes;
        continue;
      }

      // if the length is not enough for the delimiter, just add it to split
      if (i + delimiter.length() > delimitedString.length()) {
        split.append(cur);
        continue;
      }

      // not a delimiter
      if (!delimitedString.startsWith(delimiter, i)) {
        split.append(cur);
        continue;
      }

      // find delimiter not within quotes
      if (!isWithinQuotes) {
        result.add(split.toString());
        split = new StringBuilder();
        i = i + delimiter.length() - 1;
        continue;
      }

      // delimiter within quotes
      split.append(cur);
    }
    if (isWithinQuotes) {
      throw new IOException("Quotes are not enclosed.");
    }
    result.add(split.toString());
    return result;
  }

  @Override
  protected RecordReader<NullWritable, StructuredRecord.Builder> createRecordReader(FileSplit split,
                                                                                    TaskAttemptContext context,
                                                                                    @Nullable String pathField,
                                                                                    @Nullable Schema schema) {

    RecordReader<LongWritable, Text> delegate = getDefaultRecordReaderDelegate(split, context);
    String delimiter = context.getConfiguration().get(DELIMITER);
    boolean skipHeader = context.getConfiguration().getBoolean(SKIP_HEADER, false);
    boolean enableQuotesValue = context.getConfiguration().getBoolean(ENABLE_QUOTES_VALUE, false);

    return new RecordReader<NullWritable, StructuredRecord.Builder>() {

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        delegate.initialize(split, context);
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (delegate.nextKeyValue()) {
          // skip to next if the current record is header
          if (skipHeader && delegate.getCurrentKey().get() == 0L) {
            return delegate.nextKeyValue();
          }
          return true;
        }
        return false;
      }

      @Override
      public NullWritable getCurrentKey() {
        return NullWritable.get();
      }

      @Override
      public StructuredRecord.Builder getCurrentValue() throws IOException, InterruptedException {
        String delimitedString = delegate.getCurrentValue().toString();

        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        Iterator<Schema.Field> fields = schema.getFields().iterator();
        Iterable<String> splits;
        if (!enableQuotesValue) {
          splits = Splitter.on(delimiter).split(delimitedString);
        } else {
          splits = splitQuotesString(delimitedString, delimiter);
        }

        int numSchemaFields = schema.getFields().size();
        int numDataFields = 0;
        for (String temp : splits) {
          numDataFields++;
        }

        for (String part : splits) {
          if (!fields.hasNext()) {
            String message =
              String.format(
                "Found a row with %d fields when the schema only contains %d field%s.",
                numDataFields, numSchemaFields, numSchemaFields == 1 ? "" : "s");
            // special error handling for the case when the user most likely set the schema to
            // delimited
            // when they meant to use 'text'.
            Schema.Field bodyField = schema.getField("body");
            if (bodyField != null) {
              Schema bodySchema = bodyField.getSchema();
              bodySchema = bodySchema.isNullable() ? bodySchema.getNonNullable() : bodySchema;
              if (bodySchema.getType() == Schema.Type.STRING) {
                throw new IOException(message + " Did you mean to use the 'text' format?");
              }
            }
            if (!enableQuotesValue && delimitedString.contains(QUOTE)) {
              message += " Check if quoted values should be allowed.";
            }
            throw new IOException(
                message + " Check that the schema contains the right number of fields.");
          }

          Schema.Field nextField = fields.next();
          if (part.isEmpty()) {
            builder.set(nextField.getName(), null);
          } else {
            String fieldName = nextField.getName();
            // Ensure if date time field, value is in correct format
            SchemaValidator.validateDateTimeField(nextField.getSchema(), fieldName, part);
            builder.convertAndSet(fieldName, part);
          }
        }
        return builder;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return delegate.getProgress();
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    };
  }
}
