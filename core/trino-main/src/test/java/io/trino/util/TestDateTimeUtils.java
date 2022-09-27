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
package io.trino.util;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.scalar.DateTimeFunctions;
import io.trino.operator.scalar.timestamp.DateFormat;
import io.trino.operator.scalar.timestamp.FormatDateTime;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import io.trino.type.DateOperators;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.Test;

import java.time.Instant;

import static org.testng.Assert.assertEquals;

public class TestDateTimeUtils
{
    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();

    @Test
    public void testParseDate()
    {
        Instant now = Instant.now();
        Slice dt = Slices.utf8Slice(DATE_FORMATTER.print(now.toEpochMilli()));
        int days = (int) DateOperators.castFromSlice(dt);
        String dtReturn = DateTimeUtils.printDate(days);
        assertEquals(dt.toStringUtf8(), dtReturn);

        // DateTimeFunctions.dateParse
        long daysReturn = DateTimeFunctions.dateParse(TestingConnectorSession.SESSION, dt, DateFormat.YmD);
        assertEquals(days, daysReturn / (DateTimeUtils.MILLIS_PER_DAY * 1000));

        // try a future date that not cached internally
        Instant farFuture = Instant.ofEpochMilli(now.toEpochMilli() + 10 * 365 * 24 * 3600 * 1000L);
        dt = Slices.utf8Slice(DATE_FORMATTER.print(farFuture.toEpochMilli()));
        days = (int) DateOperators.castFromSlice(dt);
        dtReturn = DateTimeUtils.printDate(days);
        assertEquals(dt.toStringUtf8(), dtReturn);

        // DateTimeFunctions.dateParse
        daysReturn = DateTimeFunctions.dateParse(TestingConnectorSession.SESSION, dt, DateFormat.YmD);
        assertEquals(days, daysReturn / (DateTimeUtils.MILLIS_PER_DAY * 1000));
    }

    @Test
    public void testDateFormat()
    {
        Instant now = Instant.now();
        String dt = DATE_FORMATTER.print(now.toEpochMilli());

        ConnectorSession session = new TestingConnectorSession.Builder().build();
        long ms = now.toEpochMilli();
        ms -= ms % DateTimeUtils.MILLIS_PER_DAY;

        Slice dtRet = DateFormat.format(session, ms * 1000, DateFormat.YmD);
        assertEquals(Slices.utf8Slice(dt), dtRet);

        dtRet = FormatDateTime.format(session, ms * 1000, FormatDateTime.yyyyMMdd);
        assertEquals(Slices.utf8Slice(dt), dtRet);

        // a far future date
        Instant farFuture = Instant.ofEpochMilli(now.toEpochMilli() + 10 * 365 * 24 * 3600 * 1000L);
        dt = DATE_FORMATTER.print(farFuture.toEpochMilli());
        ms = farFuture.toEpochMilli();
        ms -= ms % DateTimeUtils.MILLIS_PER_DAY;

        dtRet = DateFormat.format(session, ms * 1000, DateFormat.YmD);
        assertEquals(Slices.utf8Slice(dt), dtRet);

        dtRet = FormatDateTime.format(session, ms * 1000, FormatDateTime.yyyyMMdd);
        assertEquals(Slices.utf8Slice(dt), dtRet);
    }

    // @Test
    private void testParseDatePerf()
    {
        final long n = 100000000;
        Instant now = Instant.now();
        Slice dt = Slices.utf8Slice(DATE_FORMATTER.print(now.toEpochMilli()));
        DateOperators.castFromSlice(dt);
        long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            DateOperators.castFromSlice(dt);
        }
        long time1 = System.currentTimeMillis() - start;
        // date print
        long days = DateOperators.castFromSlice(dt);
        DateOperators.castToSlice(900, days);
        start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            DateOperators.castToSlice(900, days);
        }
        long time2 = System.currentTimeMillis() - start;
        // try a future date that not cached internally
        Instant farFuture = Instant.ofEpochMilli(now.toEpochMilli() + 10 * 365 * 24 * 3600 * 1000L);
        dt = Slices.utf8Slice(DATE_FORMATTER.print(farFuture.toEpochMilli()));
        DateOperators.castFromSlice(dt);
        start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            DateOperators.castFromSlice(dt);
        }
        long time3 = System.currentTimeMillis() - start;

        // date print
        days = DateOperators.castFromSlice(dt);
        DateOperators.castToSlice(900, days);
        start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            DateOperators.castToSlice(900, days);
        }
        long time4 = System.currentTimeMillis() - start;

        System.out.printf("Parse QPS[new]:%d, Parse QPS[old]:%d, Print QPS[new]:%d, Print QPS[old]:%d\n",
                n * 1000 / time1, n * 1000 / time3, n * 1000 / time2, n * 1000 / time4);
    }

    // @Test
    private void testDateParsePerf()
    {
        final long n = 100000000;
        Instant now = Instant.now();
        Slice dt = Slices.utf8Slice(DATE_FORMATTER.print(now.toEpochMilli()));
        DateTimeFunctions.dateParse(TestingConnectorSession.SESSION, dt, DateFormat.YmD);
        long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            DateTimeFunctions.dateParse(TestingConnectorSession.SESSION, dt, DateFormat.YmD);
        }
        long time1 = System.currentTimeMillis() - start;

        // try a future date that not cached internally
        Instant farFuture = Instant.ofEpochMilli(now.toEpochMilli() + 10 * 365 * 24 * 3600 * 1000L);
        dt = Slices.utf8Slice(DATE_FORMATTER.print(farFuture.toEpochMilli()));
        DateTimeFunctions.dateParse(TestingConnectorSession.SESSION, dt, DateFormat.YmD);
        start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            DateTimeFunctions.dateParse(TestingConnectorSession.SESSION, dt, DateFormat.YmD);
        }
        long time2 = System.currentTimeMillis() - start;

        System.out.printf("Parse QPS[new]:%d, Parse QPS[old]:%d\n",
                n * 1000 / time1, n * 1000 / time2);
    }

    // @Test
    private void testFormatDatePerf()
    {
        final long n = 200000000;
        Instant now = Instant.now();
        DateFormat.format(TestingConnectorSession.SESSION, now.toEpochMilli() * 1000, DateFormat.YmD);
        long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            DateFormat.format(TestingConnectorSession.SESSION, now.toEpochMilli() * 1000, DateFormat.YmD);
        }
        long time1 = System.currentTimeMillis() - start;

        // future
        Instant farFuture = Instant.ofEpochMilli(now.toEpochMilli() + 10 * 365 * 24 * 3600 * 1000L);
        DateFormat.format(TestingConnectorSession.SESSION, farFuture.toEpochMilli() * 1000, DateFormat.YmD);
        start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            DateFormat.format(TestingConnectorSession.SESSION, farFuture.toEpochMilli() * 1000, DateFormat.YmD);
        }
        long time2 = System.currentTimeMillis() - start;

        System.out.printf("QPS[new]:%d, QPS[old]:%d\n",
                n * 1000 / time1, n * 1000 / time2);
    }

    // @Test
    private void testFormatDateTimePerf()
    {
        final long n = 100000000;
        Instant now = Instant.now();
        FormatDateTime.format(TestingConnectorSession.SESSION, now.toEpochMilli() * 1000, FormatDateTime.yyyyMMdd);
        long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            FormatDateTime.format(TestingConnectorSession.SESSION, now.toEpochMilli() * 1000, FormatDateTime.yyyyMMdd);
        }
        long time1 = System.currentTimeMillis() - start;

        // future
        Instant farFuture = Instant.ofEpochMilli(now.toEpochMilli() + 10 * 365 * 24 * 3600 * 1000L);
        FormatDateTime.format(TestingConnectorSession.SESSION, farFuture.toEpochMilli() * 1000, FormatDateTime.yyyyMMdd);
        start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            FormatDateTime.format(TestingConnectorSession.SESSION, farFuture.toEpochMilli() * 1000, FormatDateTime.yyyyMMdd);
        }
        long time2 = System.currentTimeMillis() - start;

        System.out.printf("QPS[new]:%d, QPS[old]:%d\n",
                n * 1000 / time1, n * 1000 / time2);
    }
}
