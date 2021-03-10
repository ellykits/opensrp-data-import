package org.smartregister.dataimport.openmrs

import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.tracing.TracingPolicy
import io.vertx.kotlin.coroutines.await
import io.vertx.mysqlclient.MySQLConnectOptions
import io.vertx.mysqlclient.MySQLPool
import io.vertx.sqlclient.PoolOptions
import io.vertx.sqlclient.SqlConnection
import kotlinx.coroutines.launch
import org.smartregister.dataimport.shared.BaseVerticle
import java.sql.SQLException

abstract class BaseOpenMRSVerticle : BaseVerticle() {

  private lateinit var pool: MySQLPool

  override suspend fun start() {
    super.start()
    val connectionOptions = MySQLConnectOptions()
      .setHost(config.getString("openmrs.mysql.host"))
      .setPort(config.getInteger("openmrs.mysql.port"))
      .setUser(config.getString("openmrs.mysql.user"))
      .setPassword(config.getString("openmrs.mysql.password"))
      .setDatabase(config.getString("openmrs.mysql.database"))
      .setReconnectAttempts(3)
      .setReconnectInterval(2000)
      .setTracingPolicy(TracingPolicy.PROPAGATE)

    val poolOptions = PoolOptions().setMaxSize(10).setMaxWaitQueueSize(5)

    pool = MySQLPool.pool(vertx, connectionOptions, poolOptions)

    limit = config.getInteger("data.limit", 50)
  }

  protected fun countRecords(eventBusAddress: String, query: String) {
    pool.withConnection { connection: SqlConnection ->
      connection
        .query(query)
        .execute()
        .onSuccess { rowSet ->
          for (row in rowSet) {
            val count = row.getInteger(0)
            logger.info("Found $count OpenMRS records")
            vertx.eventBus().send(eventBusAddress, count)
          }
        }
        .onFailure {
          vertx.exceptionHandler().handle(it)
        }
    }
  }

  protected fun replyWithOpenMRSData(message: Message<Int>, query: String, columnName: String) {
    pool.getConnection { connection ->
      if (connection.succeeded()) {
        val sqlConnection = connection.result()
        launch {
          try {
            val rowSet = sqlConnection.query(query).execute().await()
            if (rowSet.size() > 0) {
              val records = JsonArray(rowSet.map { JsonObject(it.getJson(columnName) as String) })
              logger.info("Send ${records.size()} $columnName records from OpenMRS")
              message.reply(records)
            } else {
              logger.info("No OpenMRS records for $columnName found")
              message.reply(JsonArray())
            }
          } catch (sqlException: SQLException) {
            vertx.exceptionHandler().handle(sqlException)
          } finally {
            sqlConnection.close()
          }
        }
      }
    }
  }
}
