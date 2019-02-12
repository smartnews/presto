/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.kinesis.decoder.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.prestosql.plugin.kinesis.KinesisColumnHandle;
import io.prestosql.plugin.kinesis.KinesisFieldValueProvider;
import io.prestosql.spi.PrestoException;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Locale;
import java.util.Set;

import static io.prestosql.plugin.kinesis.KinesisErrorCode.KINESIS_CONVERSION_NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class ISO8601JsonKinesisFieldDecoder
        extends JsonKinesisFieldDecoder
{
    @VisibleForTesting
    static final String NAME = "iso8601";

    /**
     * TODO: configurable time zones and locales
     */
    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.dateTimeParser().withLocale(Locale.ENGLISH).withZoneUTC();

    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(long.class, Slice.class);
    }

    @Override
    public String getFieldDecoderName()
    {
        return NAME;
    }

    @Override
    public KinesisFieldValueProvider decode(JsonNode value, KinesisColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        requireNonNull(value, "value is null");

        return new ISO8601JsonKinesisFieldValueProvider(value, columnHandle);
    }

    public static class ISO8601JsonKinesisFieldValueProvider
            extends JsonKinesisValueProvider
    {
        public ISO8601JsonKinesisFieldValueProvider(JsonNode value, KinesisColumnHandle columnHandle)
        {
            super(value, columnHandle);
        }

        @Override
        public boolean getBoolean()
        {
            throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, "conversion to boolean not supported");
        }

        @Override
        public double getDouble()
        {
            throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, "conversion to double not supported");
        }

        @Override
        public long getLong()
        {
            if (isNull()) {
                return 0L;
            }

            if (value.canConvertToLong()) {
                return value.asLong();
            }

            String textValue = value.isValueNode() ? value.asText() : value.toString();
            return FORMATTER.parseMillis(textValue);
        }
    }
}
