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
package io.trino.plugin.kafka.encoder.text;

import io.trino.plugin.kafka.encoder.AbstractRowEncoder;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.VarcharType;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class TextRowEncoder
        extends AbstractRowEncoder
{
    public static final String NAME = "text";
    private String row;

    public TextRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles)
    {
        super(session, columnHandles);
        for (EncoderColumnHandle columnHandle : this.columnHandles) {
            checkArgument(columnHandle.getFormatHint() == null, "Unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnHandle.getName());
            checkArgument(columnHandle.getDataFormat() == null, "Unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnHandle.getName());

            checkArgument(columnHandle.getType() instanceof VarcharType, "Unsupported column type '%s' for column '%s'", columnHandle.getType(), columnHandle.getName());
        }
    }

    @Override
    protected void appendNullValue()
    {
        row = null;
    }

    @Override
    protected void appendString(String value)
    {
        row = value;
    }

    @Override
    public byte[] toByteArray()
    {
        return row == null ? null : row.getBytes(StandardCharsets.UTF_8);
    }
}
