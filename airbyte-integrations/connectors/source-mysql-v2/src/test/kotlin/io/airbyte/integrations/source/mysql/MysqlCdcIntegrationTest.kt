/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mysql

import io.airbyte.cdk.StreamIdentifier
import io.airbyte.cdk.command.CliRunner
import io.airbyte.cdk.discover.CommonMetaField
import io.airbyte.cdk.discover.DiscoveredStream
import io.airbyte.cdk.discover.Field
import io.airbyte.cdk.discover.JdbcAirbyteStreamFactory
import io.airbyte.cdk.jdbc.IntFieldType
import io.airbyte.cdk.jdbc.JdbcConnectionFactory
import io.airbyte.cdk.jdbc.StringFieldType
import io.airbyte.cdk.output.BufferingOutputConsumer
import io.airbyte.cdk.util.Jsons
import io.airbyte.integrations.source.mysql.MysqlContainerFactory.execAsRoot
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.protocol.models.v0.SyncMode
import io.github.oshai.kotlinlogging.KotlinLogging
import java.sql.Connection
import java.sql.Statement
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.MySQLContainer

class MysqlCdcIntegrationTest {

    @Test
    fun test() {
        val run1: BufferingOutputConsumer =
            CliRunner.source("read", config(), configuredCatalog).run()
        // TODO: add assertions on run1 messages.

        connectionFactory.get().use { connection: Connection ->
            connection.isReadOnly = false
            connection.createStatement().use { stmt: Statement ->
                stmt.execute("INSERT INTO test.tbl (k, v) VALUES (3, 'baz')")
            }
        }

        val run2InputState: List<AirbyteStateMessage> = listOf(run1.states().last())
        val run2: BufferingOutputConsumer =
            CliRunner.source("read", config(), configuredCatalog, run2InputState).run()
        // TODO: add assertions on run2 messages.

        println()
        println()
        for (msg in run1.messages()) {
            println(Jsons.valueToTree(msg))
        }
        println()
        for (msg in run2.messages()) {
            println(Jsons.valueToTree(msg))
        }
    }

    companion object {
        val log = KotlinLogging.logger {}
        lateinit var dbContainer: MySQLContainer<*>

        fun config(): MysqlSourceConfigurationJsonObject =
            MysqlContainerFactory.config(dbContainer).apply { setCursorMethodValue(CdcCursor) }

        val connectionFactory: JdbcConnectionFactory by lazy {
            JdbcConnectionFactory(MysqlSourceConfigurationFactory().make(config()))
        }

        val configuredCatalog: ConfiguredAirbyteCatalog = run {
            val desc = StreamDescriptor().withName("tbl").withNamespace("test")
            val discoveredStream =
                DiscoveredStream(
                    id = StreamIdentifier.Companion.from(desc),
                    columns = listOf(Field("k", IntFieldType), Field("v", StringFieldType)),
                    primaryKeyColumnIDs = listOf(listOf("k")),
                )
            val stream: AirbyteStream = JdbcAirbyteStreamFactory().createGlobal(discoveredStream)
            val configuredStream: ConfiguredAirbyteStream =
                CatalogHelpers.toDefaultConfiguredStream(stream)
                    .withSyncMode(SyncMode.INCREMENTAL)
                    .withPrimaryKey(discoveredStream.primaryKeyColumnIDs)
                    .withCursorField(listOf(CommonMetaField.CDC_LSN.id))
            ConfiguredAirbyteCatalog().withStreams(listOf(configuredStream))
        }

        @JvmStatic
        @BeforeAll
        @Timeout(value = 300)
        fun startAndProvisionTestContainer() {
            dbContainer =
                MysqlContainerFactory.exclusive(
                    imageName = "mysql:8.0",
                    MysqlContainerFactory.WithNetwork,
                )
            val grant =
                "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT " +
                    "ON *.* TO '${dbContainer.username}'@'%';"
            dbContainer.execAsRoot(grant)
            dbContainer.execAsRoot("FLUSH PRIVILEGES;")
            connectionFactory.get().use { connection: Connection ->
                connection.isReadOnly = false
                connection.createStatement().use { stmt: Statement ->
                    stmt.execute("CREATE TABLE test.tbl(k INT PRIMARY KEY, v VARCHAR(80))")
                }
                connection.createStatement().use { stmt: Statement ->
                    stmt.execute("INSERT INTO test.tbl (k, v) VALUES (1, 'foo'), (2, 'bar')")
                }
            }
        }
    }
}
