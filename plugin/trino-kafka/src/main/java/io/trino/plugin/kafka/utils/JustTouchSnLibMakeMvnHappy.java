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
package io.trino.plugin.kafka.utils;

import com.smartnews.dp.kafka.schema.internal.SnTokenAuthCredentialProvider;
// import com.smartnews.dp.kafka.security.sn.sasl.SnTokenSaslClientCallbackHandler;

/**
 * the sn auth related classes is not used directly in java code,
 * instead if purely via config, and SPI, so maven plugins will fail
 * with following error:
 * [INFO] --- dependency:3.3.0:analyze-only (default) @ trino-kafka ---
 * [WARNING] Unused declared dependencies found:
 * [WARNING]    com.smartnews.dp.kafka:kafka-schema:jar:0.2.2:compile
 *
 * So this class is only to touch a class from referenced dependecy
 * to work around that issue, no actual functionality
 */
public class JustTouchSnLibMakeMvnHappy
{
    // SnTokenSaslClientCallbackHandler sasl;
    SnTokenAuthCredentialProvider auth;
}
