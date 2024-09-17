/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.mock_integration_test

import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.data.ObjectValue
import io.airbyte.cdk.message.Batch
import io.airbyte.cdk.message.DestinationRecord
import io.airbyte.cdk.message.SimpleBatch
import io.airbyte.cdk.test.util.OutputRecord
import io.airbyte.cdk.write.DestinationWriter
import io.airbyte.cdk.write.StreamLoader
import java.time.Instant
import java.util.UUID
import javax.inject.Singleton

@Singleton
class MockDestinationWrite : DestinationWriter {
    override fun getStreamLoader(stream: DestinationStream): StreamLoader {
        return MockStreamLoader(stream)
    }
}

class MockStreamLoader(override val stream: DestinationStream) : StreamLoader {
    override suspend fun processRecords(
        records: Iterator<DestinationRecord>,
        totalSizeBytes: Long
    ): Batch {
        records.forEach {
            MockDestinationBackend.insert(
                getFilename(it.stream),
                OutputRecord(
                    UUID.randomUUID(),
                    Instant.ofEpochMilli(it.emittedAtMs),
                    Instant.ofEpochMilli(System.currentTimeMillis()),
                    stream.generationId,
                    it.data as ObjectValue,
                    OutputRecord.Meta(changes = it.meta?.changes, syncId = stream.syncId),
                )
            )
        }
        return SimpleBatch(state = Batch.State.COMPLETE)
    }

    companion object {
        fun getFilename(stream: DestinationStream.Descriptor) =
            getFilename(stream.namespace, stream.name)
        fun getFilename(namespace: String?, name: String) = "(${namespace},${name})"
    }
}
