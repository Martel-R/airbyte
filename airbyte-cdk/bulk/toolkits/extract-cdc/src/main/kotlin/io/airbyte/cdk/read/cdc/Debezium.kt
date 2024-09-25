/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.read.cdc

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import io.airbyte.cdk.command.OpaqueStateValue
import io.airbyte.cdk.read.Stream
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.debezium.relational.history.HistoryRecord
import org.apache.kafka.connect.source.SourceRecord

/** [DebeziumRecord] wraps a Debezium change data event. */
@JvmInline
value class DebeziumRecord(val wrapped: JsonNode) {

    /** True if this is a Debezium heartbeat event. These aren't passed to [DebeziumConsumer]. */
    val isHeartbeat: Boolean
        get() = source.isNull

    /** The datum prior to this event; null for insertions. */
    val before: JsonNode
        get() = element("before")

    /** The datum following this event; null for deletions. */
    val after: JsonNode
        get() = element("after")

    /** Metadata containing transaction IDs, LSNs, etc; null for heartbeats. */
    val source: JsonNode
        get() = element("source")

    /** Convenience function for accessing child object nodes of the debezium event root node. */
    fun element(fieldName: String): JsonNode {
        if (!wrapped.has(fieldName)) {
            return NullNode.getInstance()
        }
        return wrapped[fieldName]
    }
}

/** [DebeziumOffset] wraps the contents of a Debezium offset file. */
@JvmInline value class DebeziumOffset(val wrapped: Map<JsonNode, JsonNode>)

/** [DebeziumSchemaHistory] wraps the contents of a Debezium schema history file. */
@JvmInline value class DebeziumSchemaHistory(val wrapped: List<HistoryRecord>)

/** Debezium Engine input. */
data class DebeziumInput(
    val properties: Map<String, String>,
    val state: DebeziumState,
    val isSynthetic: Boolean,
)

/** Debezium Engine output, other than records of course. */
data class DebeziumState(
    val offset: DebeziumOffset,
    val schemaHistory: DebeziumSchemaHistory?,
)

/** Stateless connector-specific Debezium operations. */
interface DebeziumOperations<T : Comparable<T>> {

    /** Transforms a [DebeziumRecord] into an [AirbyteRecordMessage]. */
    fun toAirbyteRecordMessage(record: DebeziumRecord): AirbyteRecordMessage

    /** Extracts the WAL position from a [DebeziumOffset]. */
    fun position(offset: DebeziumOffset): T

    /** Tries to extract the WAL position from a [DebeziumRecord]. */
    fun position(record: DebeziumRecord): T?

    /** Tries to extract the WAL position from a [SourceRecord]. */
    fun position(sourceRecord: SourceRecord): T?

    /** Synthesizes a [DebeziumInput] when no incumbent [OpaqueStateValue] is available. */
    fun synthesize(): DebeziumInput

    /** Builds a [DebeziumInput] using an incumbent [OpaqueStateValue]. */
    fun deserialize(opaqueStateValue: OpaqueStateValue, streams: List<Stream>): DebeziumInput

    /** Maps a [DebeziumState] to an [OpaqueStateValue]. */
    fun serialize(debeziumState: DebeziumState): OpaqueStateValue
}
