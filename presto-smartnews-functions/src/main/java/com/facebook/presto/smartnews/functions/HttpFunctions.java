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
package com.facebook.presto.smartnews.functions;

import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;

import javax.annotation.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;

/**
 * Created by lan on 1/19/16.
 */
public final class HttpFunctions
{
    private static final HttpClient HTTP_CLIENT = new JettyHttpClient(
            new HttpClientConfig()
                    .setConnectTimeout(new Duration(10, TimeUnit.SECONDS)));

    private HttpFunctions()
    {
    }

    @ScalarFunction("http_get")
    @Nullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice httpGet(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        String url = slice.toStringUtf8();

        try {
            String body = HTTP_CLIENT.execute(
                    prepareGet().setUri(new URI(url)).build(),
                    createStringResponseHandler()).getBody();

            return Slices.utf8Slice(body);
        }
        catch (URISyntaxException e) {
            return null;
        }
    }
}
