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
package io.prestosql.plugin.kinesis.decoder.dummy;

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.prestosql.plugin.kinesis.KinesisColumnHandle;
import io.prestosql.plugin.kinesis.KinesisFieldValueProvider;
import io.prestosql.plugin.kinesis.decoder.KinesisFieldDecoder;
import io.prestosql.spi.PrestoException;

import java.util.Set;

import static io.prestosql.plugin.kinesis.KinesisErrorCode.KINESIS_CONVERSION_NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DummyKinesisFieldDecoder
        implements KinesisFieldDecoder<Void>
{
    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(boolean.class, long.class, double.class, Slice.class);
    }

    @Override
    public String getRowDecoderName()
    {
        return DummyKinesisRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return KinesisFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }

    @Override
    public KinesisFieldValueProvider decode(Void value, KinesisColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");

        return new KinesisFieldValueProvider()
        {
            @Override
            public boolean accept(KinesisColumnHandle handle)
            {
                return false;
            }

            @Override
            public boolean isNull()
            {
                throw new PrestoException(KINESIS_CONVERSION_NOT_SUPPORTED, "is null check not supported");
            }
        };
    }
}
