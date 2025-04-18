package org.vss.impl.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.vss.AbstractKVStoreIntegrationTest;

@Testcontainers
public class PostgresBackendImplIntegrationTest extends AbstractKVStoreIntegrationTest {

  private final String POSTGRES_TEST_CONTAINER_DOCKER_IMAGE = "postgres:15";

  @Container
  private final PostgreSQLContainer postgreSQLContainer =
      new PostgreSQLContainer(POSTGRES_TEST_CONTAINER_DOCKER_IMAGE)
          .withDatabaseName("postgres")
          .withUsername("postgres")
          .withPassword("postgres");

  private Connection connection;

  @BeforeEach
  void initEach() throws Exception {

    // This is required to get postgres driver in classpath before we attempt to fetch a connection
    Class.forName("org.postgresql.Driver");
    this.connection = DriverManager.getConnection(postgreSQLContainer.getJdbcUrl(),
        postgreSQLContainer.getUsername(), postgreSQLContainer.getPassword());
    DSLContext dslContext = DSL.using(connection, SQLDialect.POSTGRES);

    this.kvStore = new PostgresBackendImpl(dslContext);

    createTable(dslContext);
  }

  @AfterEach
  void destroy() throws Exception {
    this.connection.close();
  }

  private void createTable(DSLContext dslContext) {
    dslContext.execute("CREATE TABLE vss_db (" +
        "user_token character varying(120) NOT NULL CHECK (user_token <> '')," +
        "store_id character varying(120) NOT NULL CHECK (store_id <> '')," +
        "key character varying(600) NOT NULL," +
        "value bytea NULL," +
        "version bigint NOT NULL," +
        "created_at TIMESTAMP WITH TIME ZONE," +
        "last_updated_at TIMESTAMP WITH TIME ZONE," +
        "PRIMARY KEY (user_token, store_id, key)" +
        ");");
  }
}
