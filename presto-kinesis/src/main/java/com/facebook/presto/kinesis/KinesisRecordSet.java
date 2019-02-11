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
package com.facebook.presto.kinesis;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.facebook.presto.kinesis.decoder.KinesisFieldDecoder;
import com.facebook.presto.kinesis.decoder.KinesisRowDecoder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class KinesisRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(KinesisRecordSet.class);

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final KinesisSplit split;
    private final KinesisClientManager clientManager;
    private final KinesisConnectorConfig kinesisConnectorConfig;

    private final KinesisRowDecoder messageDecoder;
    private final Map<KinesisColumnHandle, KinesisFieldDecoder<?>> messageFieldDecoders;

    private final List<KinesisColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    private final int batchSize;
    private final int fetchAttempts;
    private final long sleepTime;

    //for checkpointing
    private final boolean checkpointEnabled;
    private final String checkpointDynamodbTable;
    private String lastReadSeqNo;
    private KinesisShardCheckpointer kinesisShardCheckpointer;

    private final Set<KinesisFieldValueProvider> globalInternalFieldValueProviders;

    private final String iterationType;

    private final String checkpointLogicalName;
    private final int checkpointIterationNumber;

    KinesisRecordSet(KinesisSplit split,
                     KinesisClientManager clientManager,
                     List<KinesisColumnHandle> columnHandles,
                     KinesisRowDecoder messageDecoder,
                     Map<KinesisColumnHandle, KinesisFieldDecoder<?>> messageFieldDecoders,
                     KinesisConnectorConfig kinesisConnectorConfig,
                     String iterationType,
                     boolean checkpointEnabled,
                     int checkpointIterationNumber,
                     String checkpointLogicalName)
    {
        this.split = requireNonNull(split, "split is null");
        this.kinesisConnectorConfig = requireNonNull(kinesisConnectorConfig, "KinesisConnectorConfig is null");

        this.globalInternalFieldValueProviders = ImmutableSet.of(
                KinesisInternalFieldDescription.SHARD_ID_FIELD.forByteValue(split.getShardId().getBytes()),
                KinesisInternalFieldDescription.SEGMENT_START_FIELD.forByteValue(split.getStart().getBytes()));

        this.clientManager = requireNonNull(clientManager, "clientManager is null");

        this.messageDecoder = requireNonNull(messageDecoder, "rowDecoder is null");
        this.messageFieldDecoders = requireNonNull(messageFieldDecoders, "messageFieldDecoders is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (KinesisColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();

        this.batchSize = kinesisConnectorConfig.getBatchSize();
        this.fetchAttempts = kinesisConnectorConfig.getFetchAttempts();
        this.sleepTime = kinesisConnectorConfig.getSleepTime().toMillis();

        this.checkpointEnabled = checkpointEnabled;
        this.checkpointDynamodbTable = kinesisConnectorConfig.getCheckpointDynamodbTable();
        this.lastReadSeqNo = null;
        this.kinesisShardCheckpointer = null;

        this.iterationType = iterationType;

        this.checkpointIterationNumber = checkpointIterationNumber;
        this.checkpointLogicalName = checkpointLogicalName;
        checkpoint();
    }

    public void checkpoint()
    {
        if (checkpointEnabled) {
            if (kinesisShardCheckpointer == null) {
                AmazonDynamoDBClient dynamoDBClient = clientManager.getDynamoDBClient();
                long dynamoReadCapacity = kinesisConnectorConfig.getDynamoReadCapacity();
                long dynamoWriteCapacity = kinesisConnectorConfig.getDynamoWriteCapacity();
                long checkpointIntervalMs = kinesisConnectorConfig.getCheckpointIntervalMS().toMillis();
                String logicalProcessName = checkpointLogicalName;
                int curIterationNumber = checkpointIterationNumber;

                kinesisShardCheckpointer = new KinesisShardCheckpointer(dynamoDBClient,
                        checkpointDynamodbTable,
                        split,
                        logicalProcessName,
                        curIterationNumber,
                        checkpointIntervalMs,
                        dynamoReadCapacity,
                        dynamoWriteCapacity);

                lastReadSeqNo = kinesisShardCheckpointer.getLastReadSeqNo();
            }
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KinesisRecordCursor();
    }

    public class KinesisRecordCursor
            implements RecordCursor
    {
        private long totalBytes;
        private long totalMessages;

        private String shardIterator;
        private KinesisFieldValueProvider[] fieldValueProviders;
        private List<Record> kinesisRecords;
        private Iterator<Record> listIterator;
        private GetRecordsRequest getRecordsRequest;
        private GetRecordsResult getRecordsResult;

        @Override
        public long getTotalBytes()
        {
            return totalBytes;
        }

        @Override
        public long getCompletedBytes()
        {
            return totalBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return columnHandles.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (shardIterator == null) {
                getIterator();
                if (getKinesisRecords() == false) {
                    log.debug("No more records in shard to read.");
                    return false;
                }
            }

            while (true) {
                log.debug("Reading data from shardIterator %s", shardIterator);
                if (listIterator.hasNext()) {
                    return nextRow();
                }

                shardIterator = getRecordsResult.getNextShardIterator();

                if (shardIterator == null) {
                    log.debug("Shard closed");
                    return false;
                }
                else {
                    if (getKinesisRecords() == false) {
                        log.debug("No more records in shard to read.");
                        return false;
                    }
                }
            }
        }

        private boolean getKinesisRecords()
                throws ResourceNotFoundException
        {
            getRecordsRequest = new GetRecordsRequest();
            getRecordsRequest.setShardIterator(shardIterator);
            getRecordsRequest.setLimit(batchSize);

            int count = 0;
            int maxTries = 9;
            while (true) {
                try {
                    getRecordsResult = clientManager.getClient().getRecords(getRecordsRequest);
                    log.info(split.getShardId() + " gotRecords: cnt:" + getRecordsResult.getRecords().size() +
                            " behind:" + getRecordsResult.getMillisBehindLatest() +
                            " totalBytes:" + totalBytes);
                    break;
                }
                catch (ProvisionedThroughputExceededException e) {
                    // todo log here
                    log.warn(e, "Get rate exceeded for kinesis stream.");
                    if (++count == maxTries) {
                        throw e;
                    }
                    try {
                        Thread.sleep((long) (sleepTime * Math.pow(1.2, count - 1)));
                    }
                    catch (InterruptedException ie) {
                    }
                }
            }

            int iterationsLeft = fetchAttempts;
            do {
                kinesisRecords = getRecordsResult.getRecords();
                if (!kinesisRecords.isEmpty()) {
                    break;
                }
                try {
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e) {
                    // it's fine to fall out early
                }
                iterationsLeft--;
            } while (iterationsLeft > 0);
            if (kinesisRecords.isEmpty()) {
                return false;
            }
            listIterator = kinesisRecords.iterator();
            return true;
        }

        private boolean nextRow()
        {
            Record currentRecord = listIterator.next();
            String partitionKey = currentRecord.getPartitionKey();
            log.debug("Reading record with partition key %s", partitionKey);

            byte[] messageData = EMPTY_BYTE_ARRAY;
            ByteBuffer message = currentRecord.getData();
            if (message != null) {
                messageData = new byte[message.remaining()];
                message.get(messageData);
            }
            totalBytes += messageData.length;
            totalMessages++;

            log.debug("Fetching %d bytes from current record. %d messages read so far", messageData.length, totalMessages);

            Set<KinesisFieldValueProvider> fieldValueProviders = new HashSet<>();

            fieldValueProviders.addAll(globalInternalFieldValueProviders);
            fieldValueProviders.add(KinesisInternalFieldDescription.SEGMENT_COUNT_FIELD.forLongValue(totalMessages));
            fieldValueProviders.add(KinesisInternalFieldDescription.SHARD_SEQUENCE_ID_FIELD.forByteValue(currentRecord.getSequenceNumber().getBytes()));
            fieldValueProviders.add(KinesisInternalFieldDescription.MESSAGE_FIELD.forByteValue(messageData));
            fieldValueProviders.add(KinesisInternalFieldDescription.MESSAGE_LENGTH_FIELD.forLongValue(messageData.length));
            fieldValueProviders.add(KinesisInternalFieldDescription.MESSAGE_VALID_FIELD.forBooleanValue(messageDecoder.decodeRow(messageData, fieldValueProviders, columnHandles, messageFieldDecoders)));
            fieldValueProviders.add(KinesisInternalFieldDescription.PARTITION_KEY_FIELD.forByteValue(partitionKey.getBytes()));
            fieldValueProviders.add(KinesisInternalFieldDescription.CHECKPOINT_ENABLED.forBooleanValue(checkpointEnabled));
            fieldValueProviders.add(KinesisInternalFieldDescription.CHECKPOINT_LOGIC_NAME.forByteValue(checkpointLogicalName.getBytes()));
            fieldValueProviders.add(KinesisInternalFieldDescription.CHECKPOINT_ITERATION_NUMBER.forLongValue(checkpointIterationNumber));

            this.fieldValueProviders = new KinesisFieldValueProvider[columnHandles.size()];

            for (int i = 0; i < columnHandles.size(); i++) {
                for (KinesisFieldValueProvider fieldValueProvider : fieldValueProviders) {
                    if (fieldValueProvider.accept(columnHandles.get(i))) {
                        this.fieldValueProviders[i] = fieldValueProvider;
                        break;
                    }
                }
            }

            lastReadSeqNo = currentRecord.getSequenceNumber();
            if (checkpointEnabled) {
                kinesisShardCheckpointer.checkpointIfTimeUp(lastReadSeqNo);
            }

            return true;
        }

        @Override
        public boolean getBoolean(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, boolean.class);
            return isNull(field) ? false : fieldValueProviders[field].getBoolean();
        }

        @Override
        public long getLong(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, long.class);
            return isNull(field) ? 0L : fieldValueProviders[field].getLong();
        }

        @Override
        public double getDouble(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, double.class);
            return isNull(field) ? 0.0d : fieldValueProviders[field].getDouble();
        }

        @Override
        public Slice getSlice(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, Slice.class);
            return isNull(field) ? Slices.EMPTY_SLICE : fieldValueProviders[field].getSlice();
        }

        @Override
        public Object getObject(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            return fieldValueProviders[field] == null || fieldValueProviders[field].isNull();
        }

        @Override
        public void close()
        {
            log.info("Read complete");
            if (checkpointEnabled && lastReadSeqNo != null) {
                kinesisShardCheckpointer.checkpoint(lastReadSeqNo);
            }
        }

        private void checkFieldType(int field, Class<?> expected)
        {
            Class<?> actual = getType(field).getJavaType();
            checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
        }

        private void getIterator()
                throws ResourceNotFoundException
        {
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
            getShardIteratorRequest.setStreamName(split.getStreamName());
            getShardIteratorRequest.setShardId(split.getShardId());
            if (lastReadSeqNo == null) {
                getShardIteratorRequest.setShardIteratorType(iterationType);
            }
            else {
                getShardIteratorRequest.setShardIteratorType("AFTER_SEQUENCE_NUMBER");
                getShardIteratorRequest.setStartingSequenceNumber(lastReadSeqNo);
            }

            GetShardIteratorResult getShardIteratorResult = clientManager.getClient().getShardIterator(getShardIteratorRequest);
            shardIterator = getShardIteratorResult.getShardIterator();
        }
    }
}
