# Citrus 5.0.0 Integration Test Guide for EIP Examples

Collected set of best practices and patterns used in the examples. 
Use this guide to create tests for new chapters across all three runtimes.

---

## Table of Contents

- [General Principles](#general-principles)
- [Quarkus Runtime](#quarkus-runtime)
- [Spring Boot Runtime](#spring-boot-runtime)
- [YAML DSL Runtime](#yaml-dsl-runtime)
- [Infrastructure (Docker Compose)](#infrastructure-docker-compose)
- [Testing Patterns](#testing-patterns)
- [CI Workflow](#ci-workflow)
- [Checklist for New Chapters](#checklist-for-new-chapters)

---

## General Principles

### Test what the route actually does, not its internals

Always test through the real entry point. If a route exposes a REST endpoint, use the Citrus HTTP client to call that endpoint — do NOT bypass it by sending to `direct:inbound-adapter`. Testing the real endpoint validates the full chain (REST → processing → messaging).

### Disable demo data generators during tests

Every `DemoDataGenerator` route must be toggleable via a config property. Tests produce their own data — background generators interfere with assertions.

- **Quarkus**: `@ConfigProperty(name = "eip.demo.data.generator.enabled", defaultValue = "true")` + `.autoStartup(enabled)`
- **Spring Boot**: `@Value("${eip.demo.data.generator.enabled:true}")` + `.autoStartup(enabled)`

Set `eip.demo.data.generator.enabled=false` in the test `application.properties`. Use a separate property per generator if the chapter has multiple generators (e.g., `eip.pulsar.demo.data.generator.enabled`, `eip.redis.demo.data.generator.enabled`).

### Shutdown timeouts

Set these to 0 in test `application.properties` to avoid slow teardown:

```properties
camel.main.shutdownTimeout=0
camel.component.kafka.shutdownTimeout=0
```

### Order template with Citrus variables

Place a shared `templates/order.json` under test resources. Use Citrus variable interpolation:

```json
{
  "order_id": ${id},
  "customer_id": "CUST-00${id}",
  "item_sku": "SKU-SHIP-${id}",
  "quantity": 1,
  "amount": ${amount},
  "status": "${status}",
  "shipping_priority": "${priority}"
}
```

Only include fields the route actually uses. Simpler templates are better — ch06 only uses `${id}`, `${amount}`, `${status}`, `${priority}`.

**YAML DSL uses different variable names**: `${order.id}`, `${order.status}` — because YAML DSL tests define variables with `name: order.id` (dotted names), while Java tests use `variable("id", ...)`.

### Design routes for testability

**Route all branches to named routes** — when a `choice()` route has an `otherwise()` branch that only logs, replace the inline `.log(...)` with a `direct:` route that has its own `routeId`. This makes the fallback branch verifiable via `assertProcessedExchanges()`:

```java
// BEFORE — otherwise() just logs, not verifiable via exchange counts
.otherwise()
    .log("Unknown event_type '${body[event_type]}' — skipping")

// AFTER — otherwise() routes to a named handler, verifiable
.otherwise()
    .to("direct:handle-order_unknown")

from("direct:handle-order_unknown")
    .routeId("handle-order-unknown")
    .log("Unknown event_type '${body[event_type]}' — skipping");
```

The test can then verify the fallback branch was exercised:

```java
t.then(assertProcessedExchanges("handle-order-unknown", 1, camelContext));
```

This principle applies to any inline processing that should be testable: extract it into a named `direct:` route so the exchange shows up in route-level MBean statistics.

Routes that send to Kafka should produce well-formed JSON, not Java `Map.toString()`. A route that does `unmarshal().json()` converts the body from JSON to a Java Map — if it then sends that Map to Kafka without re-marshalling, the output is `{order_id=1234, ...}` (not valid JSON), making body verification in tests impossible.

**Fix**: add `marshal().json()` before every `to("kafka:...")` that follows an unmarshal:

```java
// BEFORE — body on Kafka is Map.toString(), untestable
from("kafka:eip.orders.placed")
    .unmarshal().json()
    .filter(simple("${body[amount]} >= 100"))
        .log("High-value order ${body[order_id]}")
        .to("kafka:eip.orders.high-value");

// AFTER — body on Kafka is proper JSON, testable
from("kafka:eip.orders.placed")
    .unmarshal().json()
    .filter(simple("${body[amount]} >= 100"))
        .log("High-value order ${body[order_id]}")
        .marshal().json()
        .to("kafka:eip.orders.high-value");
```

The same applies to `choice()` routes — add `marshal().json()` in each branch before the Kafka output. The `log()` step must come BEFORE `marshal()` so `${body[field]}` expressions still work against the Map.

Routes that already call `marshal().json()` (like the Splitter route) are already testable. Routes that pass the body through unchanged (no `unmarshal()`) are also fine — the original JSON string arrives on the output topic as-is.

### Use unique consumer groups

Every Kafka receive action must use a unique `consumerGroup` to avoid conflicts between tests and between the test and the route itself:

```java
.endpoint("kafka:eip.orders.processed?consumerGroup=citrus-processed-group")
```

### EipTestSupport interface

Shared across all Java-based tests (Quarkus and Spring Boot). Provides `waitForCamelRouteStarted()` using the ControlBus. Copy this interface into each test package — it is identical across chapters and runtimes:

```java
public interface EipTestSupport extends TestActionSupport {

    default TestActionBuilder<?> waitForCamelRouteStarted(String routeId, CamelContext camelContext) {
        return repeatOnError()
                .until((i, context) -> i > 20)
                .autoSleep(Duration.ofSeconds(1))
                .actions(
                    camel().camelContext(camelContext)
                            .controlBus()
                            .route(routeId)
                            .status()
                            .result(ServiceStatus.Started)
                            .description("Waiting for Camel route '%s' to be started ...".formatted(routeId)),
                    sleep().seconds(5)
                );
    }
}
```

---

## Quarkus Runtime

### File layout

```
examples/NN-name/quarkus/
  pom.xml
  src/main/java/com/example/eip/<pkg>/
    *Route.java                          — route definitions
    DemoDataGenerator.java               — toggleable demo data
  src/main/resources/
    application.properties               — production config
  src/test/java/com/example/eip/<pkg>/
    EipTestSupport.java                  — shared interface
    EipTests.java                        — all tests in one class
    config/
      EipInfraSetup.java                 — before/after suite
  src/test/resources/
    application.properties               — test overrides
    citrus-application.properties        — Citrus config loader
    _infra/
      compose.yaml                       — Docker Compose
      postgres/
        init-schemas.sql                 — if PostgreSQL needed
    templates/
      order.json                         — message template
```

### POM dependencies

Properties:

```xml
<quarkus.platform.version>3.37.0</quarkus.platform.version>
<citrus.version>5.0.0</citrus.version>
<surefire-plugin.version>3.5.6</surefire-plugin.version>
```

Test dependencies (always the same 6 core + extras as needed):

```xml
<!-- Always required -->
<dependency><groupId>io.quarkus</groupId><artifactId>quarkus-junit</artifactId><scope>test</scope></dependency>
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-camel</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-quarkus</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-junit-jupiter</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-kafka</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-testcontainers</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-validation-json</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>

<!-- Add when testing REST endpoints -->
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-http</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>

<!-- Add when sending to Pulsar endpoints in tests -->
<dependency><groupId>org.apache.camel</groupId><artifactId>camel-endpointdsl</artifactId><scope>test</scope></dependency>

<!-- Add when testing SQL/database routes -->
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-sql</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>

<!-- Add when using assertProcessedExchanges (ManagedRouteMBean) -->
<!-- Quarkus: -->
<dependency><groupId>org.apache.camel.quarkus</groupId><artifactId>camel-quarkus-management</artifactId></dependency>
<!-- Spring Boot: use camel-spring-boot-starter-management instead -->
<!-- Standalone Camel Main: use camel-management instead -->
```

**IMPORTANT**: Use `quarkus-junit` (NOT `quarkus-junit5`).

Build plugins — surefire AND failsafe both needed with `@{argLine}` and `LogManager`:

```xml
<plugin>
    <artifactId>maven-surefire-plugin</artifactId>
    <version>${surefire-plugin.version}</version>
    <configuration>
        <argLine>@{argLine}</argLine>
        <systemPropertyVariables>
            <java.util.logging.manager>org.jboss.logmanager.LogManager</java.util.logging.manager>
            <maven.home>${maven.home}</maven.home>
        </systemPropertyVariables>
    </configuration>
</plugin>
<plugin>
    <artifactId>maven-failsafe-plugin</artifactId>
    <version>${surefire-plugin.version}</version>
    <executions>
        <execution>
            <goals>
                <goal>integration-test</goal>
                <goal>verify</goal>
            </goals>
        </execution>
    </executions>
    <configuration>
        <argLine>@{argLine}</argLine>
        <systemPropertyVariables>
            <native.image.path>${project.build.directory}/${project.build.finalName}-runner</native.image.path>
            <java.util.logging.manager>org.jboss.logmanager.LogManager</java.util.logging.manager>
            <maven.home>${maven.home}</maven.home>
        </systemPropertyVariables>
    </configuration>
</plugin>
```

### Test class structure

```java
@QuarkusTest
@CitrusSupport
class EipTests implements EipTestSupport {

    @CitrusResource
    TestCaseRunner t;            // <-- at OUTER class level

    @Inject
    @BindToRegistry
    CamelContext camelContext;    // <-- both annotations needed

    @Nested
    class SomePatternTest {
        // tests use the outer `t` and `camelContext` directly

        @Test
        public void shouldDoSomething() {
            t.given(waitForCamelRouteStarted("route-id", camelContext));
            t.when(send()...);
            t.then(receive()...);
        }
    }
}
```

Key points:
- `@CitrusResource TestCaseRunner t` goes on the **outer class** (not inside `@Nested`)
- `CamelContext` needs both `@Inject` and `@BindToRegistry`
- Quarkus REST port is **8081** by default

### EipInfraSetup (Quarkus)

Uses `@CitrusConfiguration` and `@BindToRegistry` (Citrus annotations, NOT Spring):

```java
@CitrusConfiguration
public class EipInfraSetup implements TestActionSupport {

    @BindToRegistry
    public BeforeSuite startInfra() {
        return beforeSuite().actions(
                    testcontainers().compose()
                            .up("_infra/compose.yaml")
                            .containerName("eip-infra")
                            .autoRemove(false)
                ).build();
    }

    @BindToRegistry
    public AfterSuite stopInfra() {
        return afterSuite().actions(
                    camel().camelContext().stop(),
                    testcontainers().compose()
                            .down()
                            .containerName("eip-infra")
                ).build();
    }
}
```

### citrus-application.properties (required for Quarkus)

```properties
citrus.java.config=com.example.eip.<pkg>.config.EipInfraSetup
```

This file is **required** — without it, the `EipInfraSetup` is not discovered.

---

## Spring Boot Runtime

### File layout

Same as Quarkus, but no `citrus-application.properties` needed.

### POM dependencies

Properties:

```xml
<java.version>25</java.version>
<camel.version>4.20.0</camel.version>
<citrus.version>5.0.0</citrus.version>
```

Test dependencies:

```xml
<!-- Always required -->
<dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-test</artifactId><scope>test</scope></dependency>
<dependency><groupId>org.apache.camel</groupId><artifactId>camel-test-spring-junit5</artifactId><version>${camel.version}</version><scope>test</scope></dependency>
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-camel</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-spring</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-junit-jupiter</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-kafka</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-testcontainers</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-validation-json</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>

<!-- Add when testing REST endpoints -->
<dependency><groupId>org.citrusframework</groupId><artifactId>citrus-http</artifactId><version>${citrus.version}</version><scope>test</scope></dependency>

<!-- Add when sending to Pulsar endpoints in tests -->
<dependency><groupId>org.apache.camel</groupId><artifactId>camel-endpointdsl</artifactId><version>${camel.version}</version><scope>test</scope></dependency>
```

**IMPORTANT**: Use `citrus-spring` (NOT `citrus-spring-boot`).

Build plugin — failsafe only (no custom surefire needed):

```xml
<plugin>
    <artifactId>maven-failsafe-plugin</artifactId>
    <executions>
        <execution>
            <goals>
                <goal>integration-test</goal>
                <goal>verify</goal>
            </goals>
        </execution>
    </executions>
</plugin>
```

### Test class structure

```java
@SpringBootTest(classes = XxxApplication.class)
@CamelSpringBootTest
@CitrusSpringSupport
@ContextConfiguration(classes = { EipInfraSetup.class, CitrusSpringConfig.class })
class EipTests implements EipTestSupport {

    @Autowired
    CamelContext camelContext;    // <-- @Autowired only (no @BindToRegistry)

    @Nested
    class SomePatternTest {

        @CitrusResource
        TestCaseRunner t;        // <-- inside EACH @Nested class

        @Test
        public void shouldDoSomething() {
            t.given(waitForCamelRouteStarted("route-id", camelContext));
            t.when(send()...);
            t.then(receive()...);
        }
    }
}
```

Key differences from Quarkus:
- `@CitrusResource TestCaseRunner t` goes inside **each `@Nested` class** (not on outer class)
- `CamelContext` uses `@Autowired` only (no `@BindToRegistry`)
- Four annotations on the test class (see above)
- For REST testing, add `webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT`

### EipInfraSetup (Spring Boot)

Uses Spring `@Configuration` and `@Bean` (NOT Citrus annotations):

```java
@Configuration
public class EipInfraSetup implements TestActionSupport {

    @Bean
    public BeforeSuite startInfra() {
        return beforeSuite().actions(
                    testcontainers().compose()
                            .up("_infra/compose.yaml")
                            .containerName("eip-infra")
                            .autoRemove(false)
                ).build();
    }

    @Bean
    public AfterSuite stopInfra() {
        return afterSuite().actions(
                    camel().camelContext().stop(),
                    testcontainers().compose()
                            .down()
                            .containerName("eip-infra")
                ).build();
    }
}
```

### citrus-application.properties

**Not required** for Spring Boot — Spring's `@ContextConfiguration` handles discovery. Some chapters include it, but it is optional.

### Spring Boot REST specifics

When the chapter uses Camel REST with servlet mapping:

1. Main `application.properties`:
   ```properties
   camel.servlet.mapping.enabled=true
   camel.servlet.mapping.context-path=/api/*
   ```

2. Route uses path relative to context path (e.g., `rest("/orders")` not `rest("/api/orders")`)

3. Test `application.properties` repeats the servlet mapping + sets `server.port=8082`

4. Test class adds `webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT`

5. HTTP client uses port 8082:
   ```java
   http().client("http://localhost:8082").send().post("/api/orders")...
   ```

### Spring Boot broker configuration

Always set broker connection properties in the test `application.properties`:

```properties
kafka.brokers=localhost:9092
camel.component.pulsar.service-url=pulsar://localhost:6650
```

---

## YAML DSL Runtime

### File layout

```
examples/NN-name/yaml-dsl/
  application.properties                 — route configuration
  some-route.yaml                        — route definitions
  test/
    some-route.citrus.it.yaml            — one test per route file
    citrus-application.properties        — JBang dump config
    _infra/
      compose.yaml                       — Docker Compose
      postgres/
        init-schemas.sql                 — if PostgreSQL needed
    templates/
      order.json                         — message template
```

### Test file naming

Test files MUST be named `<integration-name>.citrus.it.yaml` — the `.citrus.it.yaml` suffix is the convention that Citrus JBang discovers.

### citrus-application.properties

Always include:

```properties
citrus.camel.jbang.dump.integration.output=true
```

Optionally pin the JBang version (comment out to use latest):

```properties
#citrus.camel.jbang.version=4.21.0
```

### Test structure

Every test follows this pattern:

```yaml
name: route-name-test
description: One-line description of what this test verifies
variables:
  - name: kafka.broker
    value: localhost:9092
  - name: order.id
    value: "citrus:randomNumber(4)"
  - name: order.status
    value: "placed"
actions:
  # 1. Start infrastructure
  - testcontainers:
      compose:
        up:
          file: "_infra/compose.yaml"

  # 2. (Optional) Wait for dependent services
  - waitFor:
      timeout: "25000"
      http:
        url: http://localhost:8090

  # 3. Start the Camel integration
  - camel:
      jbang:
        run:
          integration:
            name: "route-name"
            file: "../route-name.yaml"
            systemProperties:
              file: "../application.properties"

  # 4. (Optional) Verify routes are started
  - camel:
      jbang:
        verify:
          integration: "route-name"
          logMessage: "Routes startup"

  # 5. Send test data
  - send:
      endpoint: >-
        kafka:eip.orders.placed?server=${kafka.broker}
      message:
        body:
          resource:
            file: "templates/order.json"

  # 6. Verify behavior
  - camel:
      jbang:
        verify:
          integration: "route-name"
          logMessage: "expected log output"
```

### REST endpoint testing in YAML DSL

When the route exposes a REST endpoint, use `--port` arg and HTTP client:

```yaml
- camel:
    jbang:
      run:
        args:
          - "--port"
          - "8082"
        integration:
          name: "channel-adapter"
          file: "../channel-adapter.yaml"
          systemProperties:
            file: "../application.properties"

# Verify route started
- camel:
    jbang:
      verify:
        integration: "channel-adapter"
        logMessage: "Routes startup (total:2 rest-dsl:1)"

# Send HTTP request
- http:
    client: "http://localhost:8082"
    sendRequest:
      fork: true
      POST:
        path: "/api/orders"
        contentType: "application/json"
        body:
          resource:
            file: "templates/order.json"

# Verify Kafka output
- receive:
    endpoint: >-
      kafka:eip.orders.incoming?server=${kafka.broker}&consumerGroup=citrus-incoming-group
    message:
      body:
        resource:
          file: "templates/order.json"

# Verify HTTP response
- http:
    client: "http://localhost:8082"
    receiveResponse:
      response:
        status: "200"
        body:
          data: '{"status": "accepted"}'
        contentType: "application/json"
```

### Pulsar endpoints in YAML DSL

Use the `camel:` prefix for Pulsar send endpoints:

```yaml
- send:
    endpoint: >-
      camel:pulsar:persistent://public/default/partner.orders.placed?serviceUrl=pulsar://localhost:6650&producerName=citrus-bridge-test
    fork: true
    message:
      body:
        resource:
          file: "templates/order.json"
```

### Updating variables mid-test

Use `createVariables` to change values between test scenarios within a single test:

```yaml
- createVariables:
    variables:
      - name: order.id
        value: "citrus:randomNumber(4)"
      - name: order.status
        value: "shipped"
```

### YAML DSL datasource configuration

For SQL routes running under JBang, use `spring.datasource.*` properties (not `camel.component.sql.dataSource.*`):

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/eip
spring.datasource.username=eip
spring.datasource.password=eip
spring.datasource.driverClassName=org.postgresql.Driver
```

### Extra Maven dependencies in YAML DSL tests

Use a `# deps:` comment at the top of the `.citrus.it.yaml` file to declare additional Maven dependencies that aren't auto-resolved:

```yaml
# deps: org.postgresql:postgresql:42.7.5
name: sql-polling-consumer-test
description: Test verifying the SQL Polling Consumer pattern
```

This is needed when the test itself (not the route) requires a library — e.g., the PostgreSQL JDBC driver for `org.postgresql.ds.PGSimpleDataSource` used in a `configuration.beans` DataSource declaration.

### YAML DSL SQL testing with bean definitions

When a YAML DSL test needs a DataSource for `citrus-sql` actions, declare it as a YAML bean definition in the `configuration.beans` section:

```yaml
# deps: org.postgresql:postgresql:42.7.5
name: sql-polling-consumer-test
variables:
  - name: order.id
    value: "citrus:randomNumber(4)"
  - name: order.amount
    value: 40
configuration:
  beans:
    - name: dataSource
      type: org.postgresql.ds.PGSimpleDataSource
      properties:
        url: "jdbc:postgresql://localhost:5432/eip"
        user: "eip"
        password: "eip"
actions:
  # ... infrastructure setup, camel run ...

  # Insert test data
  - sql:
      dataSource: "dataSource"
      statements:
        - statement: "INSERT INTO orders.orders (customer_id, item_sku, quantity, amount) VALUES ('CUST-00${order.id}', 'SKU-${order.id}', 1, ${order.amount})"

  # Receive from Kafka and capture auto-generated DB id
  - receive:
      endpoint: >-
        kafka:eip.orders.placed?server=${kafka.broker}&consumerGroup=citrus-placed-group
      timeout: 60000
      message:
        body:
          data: |
            {
              "id": "@variable(order.dbId)@",
              "customer_id": "CUST-00${order.id}",
              "status": "PLACED",
              "amount": ${order.amount}.0,
              "item_sku": "SKU-${order.id}",
              "quantity": 1,
              "created_at": "@ignore@"
            }

  # Verify DB state change using the captured id
  - sql:
      dataSource: "dataSource"
      statements:
        - statement: "SELECT status FROM orders.orders WHERE id = '${order.dbId}'"
      validate:
        - column: "status"
          value: "PROCESSING"
```

Key differences from Java Pattern 12:
- DataSource is declared via `configuration.beans` (not `@Inject`/`@Autowired`)
- `sql.dataSource` references the bean by name (string `"dataSource"`)
- Variable capture uses `@variable(order.dbId)@` with dotted names (not `order_id`)
- SQL validation uses `validate` with `column`/`value` pairs (not `.validate("column", "value")`)

### SQL URI with complex query parameters

When a SQL component URI contains query parameters with special characters (e.g., `?onConsume=UPDATE ... WHERE id = :#id`), the YAML DSL parser in Camel 4.22.0 fails with `Error constructing YAML node id: org.apache.camel.model.FromDefinition`. Move complex parameters out of the URI into the `parameters` section:

```yaml
# WRONG — embedded query parameters cause YAML DSL parse error
- route:
    id: polling-consumer-sql
    from:
      uri: "sql:SELECT * FROM orders WHERE status = 'PLACED'?onConsume=UPDATE orders SET status = 'PROCESSING' WHERE id = :#id"

# CORRECT — move onConsume to parameters
- route:
    id: polling-consumer-sql
    from:
      uri: "sql:SELECT * FROM orders WHERE status = 'PLACED'"
      parameters:
        delay: 30000
        onConsume: "UPDATE orders SET status = 'PROCESSING' WHERE id = :#id"
```

### YAML DSL route structure

Steps must be nested under `from`, not at the same level:

```yaml
# CORRECT
- route:
    id: my-route
    from:
      uri: "kafka:topic"
      parameters:
        brokers: "{{kafka.brokers}}"
      steps:                       # <-- under from
        - log: "message"

# WRONG
- route:
    id: my-route
    from:
      uri: "kafka:topic"
      parameters:
        brokers: "{{kafka.brokers}}"
    steps:                         # <-- same level as from (WRONG)
      - log: "message"
```

---

## Infrastructure (Docker Compose)

### Compose file per chapter

Each chapter gets its own `compose.yaml` with only the services the chapter needs. Copy from the templates below.

### Kafka (always included)

```yaml
kafka:
  image: docker.io/apache/kafka:latest
  ports:
    - "9092:9092"
    - "9094:9094"
  environment:
    - KAFKA_NODE_ID=1
    - KAFKA_PROCESS_ROLES=broker,controller
    - KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
    - KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,DOCKER://0.0.0.0:9094
    - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,DOCKER://kafka:9094
    - KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,DOCKER:PLAINTEXT
    - KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
    - KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT
    - KAFKA_AUTO_CREATE_TOPICS_ENABLE=true
    - KAFKA_NUM_PARTITIONS=3
    - KAFKA_DEFAULT_REPLICATION_FACTOR=1
    - KAFKA_MIN_INSYNC_REPLICAS=1
    - KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
    - KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
    - KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
    - KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0
    - CLUSTER_ID=RUlQQ2FtZWxQYXR0ZXJucw
  volumes:
    - kafka-data:/var/lib/kafka/data:Z
  devices:
    - "/dev/null:/dev/null:rwm"
  mem_limit: 1g
  healthcheck:
    test: ["CMD-SHELL", "nc -z localhost 9092"]
    interval: 5s
    timeout: 5s
    retries: 12
    start_period: 30s
  networks:
    - eip-net
```

### Kafka UI (include when compose has multiple services)

Acts as an infrastructure readiness proxy — when kafka-ui is up, everything else is likely ready too.

```yaml
kafka-ui:
  image: docker.io/provectuslabs/kafka-ui:latest
  ports:
    - "8090:8080"
  environment:
    - KAFKA_CLUSTERS_0_NAME=eip-local
    - KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:9094
    - DYNAMIC_CONFIG_ENABLED=true
  depends_on:
    kafka:
      condition: service_healthy
  networks:
    - eip-net
```

When kafka-ui is included, add a `waitFor` in `EipInfraSetup.startInfra()`:

```java
waitFor().http().url("http://localhost:8090").seconds(25)
```

Or in YAML DSL tests:

```yaml
- waitFor:
    timeout: "25000"
    http:
      url: http://localhost:8090
```

### Pulsar (when chapter uses Pulsar)

```yaml
pulsar:
  image: docker.io/apachepulsar/pulsar:latest
  command: bin/pulsar standalone --advertised-address localhost
  ports:
    - "6650:6650"
    - "8080:8080"
  environment:
    - PULSAR_MEM=-Xms512m -Xmx1024m -XX:MaxDirectMemorySize=512m
    - PULSAR_PREFIX_advertisedAddress=localhost
    - PULSAR_STANDALONE_USE_ZOOKEEPER=0
  volumes:
    - pulsar-data:/pulsar/data:Z
  devices:
    - "/dev/null:/dev/null:rwm"
  mem_limit: 2g
  healthcheck:
    test: ["CMD-SHELL", "bin/pulsar-admin brokers healthcheck"]
    interval: 10s
    timeout: 5s
    retries: 15
    start_period: 60s
  networks:
    - eip-net
```

### PostgreSQL (when chapter uses SQL)

```yaml
postgres:
  image: docker.io/library/postgres:16-alpine
  ports:
    - "5432:5432"
  environment:
    - POSTGRES_DB=eipdb
    - POSTGRES_USER=eipuser
    - POSTGRES_PASSWORD=eippass
    - POSTGRES_INITDB_ARGS=--encoding=UTF8 --locale=C
  volumes:
    - postgres-data:/var/lib/postgresql/data:Z
    - ./postgres/init-schemas.sql:/docker-entrypoint-initdb.d/01-init-schemas.sql:Z
  devices:
    - "/dev/null:/dev/null:rwm"
  mem_limit: 1g
  command: ["postgres", "-c", "log_statement=all", "-c", "shared_buffers=128MB"]
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U eipuser -d eipdb"]
    interval: 5s
    timeout: 3s
    retries: 12
    start_period: 10s
  networks:
    - eip-net
```

**Note**: YAML DSL variant uses different credentials (`eip`/`eip`/`eip`) for JBang compatibility with `spring.datasource.*` properties. Keep credentials consistent between `compose.yaml` and `application.properties`.

### Redis (when chapter uses Redis)

```yaml
redis:
  image: docker.io/library/redis:7-alpine
  ports:
    - "6379:6379"
  volumes:
    - redis-data:/data:Z
  mem_limit: 512m
  healthcheck:
    test: ["CMD", "redis-cli", "ping"]
    interval: 5s
    timeout: 3s
    retries: 12
    start_period: 5s
  networks:
    - eip-net
```

---

## Testing Patterns

### Pattern 1: Send to Kafka, receive from Kafka (most common)

Send a message to an inbound topic, verify it appears on an outbound topic:

```java
t.when(
    send()
        .endpoint("kafka:eip.orders.placed")
        .fork(true)
        .message()
        .body(Resources.create("templates/order.json"))
        .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
);

t.then(
    receive()
        .endpoint("kafka:eip.orders.processed?consumerGroup=citrus-processed-group")
        .message()
        .body(Resources.create("templates/order.json"))
        .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
);
```

Use `.fork(true)` on the send when the route processes synchronously and the test needs to proceed to the receive step.

### Pattern 2: Eventual consistency with repeatOnError

When the route involves async processing (DLQ after retries, multi-hop routing), wrap the receive in `repeatOnError`:

```java
t.then(
    repeatOnError()
        .until((i, context) -> i > 10)
        .autoSleep(Duration.ofSeconds(1))
        .actions(
            receive()
                .endpoint("kafka:eip.orders.dlq?consumerGroup=citrus-dlq-group")
                .message()
                .body(Resources.create("templates/order.json"))
                .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
        )
);
```

### Pattern 3: REST endpoint testing (HTTP → Kafka → HTTP response)

Fork the HTTP send, receive the Kafka message, then receive the HTTP response:

```java
t.when(
    http()
        .client("http://localhost:8081")
        .send()
        .post("/api/orders")
        .fork(true)
        .message()
        .body(Resources.create("templates/order.json"))
        .contentType("application/json")
);

t.then(
    receive()
        .endpoint("kafka:eip.orders.incoming?consumerGroup=citrus-incoming-group")
        .message()
        .body(Resources.create("templates/order.json"))
);

t.then(
    http()
        .client("http://localhost:8081")
        .receive()
        .response(HttpStatus.OK)
        .message()
        .body("{\"status\": \"accepted\"}")
        .contentType("application/json")
);
```

### Pattern 4: Pulsar send via CamelSupport endpoint DSL

Requires `camel-endpointdsl` dependency:

```java
t.when(
    camel()
        .send()
        .endpoint(CamelSupport.camel().endpoints()
                .pulsar("persistent://public/default/partner.orders.placed")
                .serviceUrl("pulsar://localhost:6650")
                .producerName("citrus-test")::getRawUri)
        .fork(true)
        .message()
        .body(Resources.create("templates/order.json"))
);
```

### Pattern 5: Sending to Camel-internal endpoints (direct:, seda:)

When a test needs to send to a Camel-internal endpoint like `direct:` or `seda:`, use `CamelEndpointBuilder` with an explicit `camelContext` reference. A plain URI string (e.g., `.endpoint("direct:send-command")`) causes Citrus to create a standalone Camel context that cannot resolve the route registered in the application's context.

```java
import org.citrusframework.camel.endpoint.CamelEndpointBuilder;

t.when(
    camel()
        .send()
        .endpoint(new CamelEndpointBuilder()
                .endpointUri("direct:send-command")
                .camelContext(camelContext)
                .build())
        .fork(true)
        .message()
        .body(Resources.create("templates/command.json"))
        .header("kafka.KEY", "PAY-${id}")
);
```

This is the correct way to test producer routes that accept messages on a `direct:` endpoint and forward them to Kafka (or any other destination). The test can then verify the output with a standard Kafka `receive()`.

### Pattern 6: KafkaMessageFilter for targeted receive

When multiple messages may be on a topic and you need a specific one:

```java
t.then(
    repeatOnError()
        .until((i, context) -> i > 5)
        .autoSleep(Duration.ofSeconds(1))
        .actions(
            receive()
                .selector(KafkaMessageFilter.kafkaMessageFilter()
                        .eventLookbackWindow(Duration.ofSeconds(10))
                        .kafkaMessageSelector(kafkaHeaderEquals("order-id", "${id}"))
                        .build())
                .endpoint("kafka:eip.orders.processed?consumerGroup=citrus-processed-group")
                .message()
                .body(Resources.create("templates/order.json"))
        )
);
```

### Pattern 7: Redis Pub/Sub testing

Uses Camel processor to bridge to Redis (Quarkus uses `RedisDataSource`, Spring Boot uses `StringRedisTemplate`):

```java
// Quarkus
@Inject RedisDataSource redis;

t.when(
    camel().send().endpoint("log:info")
        .message().body("...")
        .process(processor().camel().process()
            .camelContext(camelContext)
            .processor((Processor) exchange -> {
                PubSubCommands<String> pubsub = redis.pubsub(String.class);
                pubsub.publish("eip.orders.notifications",
                    exchange.getMessage().getBody(String.class).strip());
            }))
);

// Spring Boot
@Autowired StringRedisTemplate redisTemplate;

// Same pattern but use:
redisTemplate.convertAndSend("eip.orders.notifications", ...);
```

### Pattern 8: Verify exchange processing via ManagedRouteMBean

When a route has no output topic to receive from (log-only, `direct:` handler, etc.), verify that the route actually **processed** the exchange — not just that it's still running. Use Camel's `ManagedCamelContext` to query the route's MBean for completed/failed exchange counts.

**Requires** Camel management to be on the classpath so that `ManagedCamelContext` and `ManagedRouteMBean` are available. Add the runtime-appropriate dependency:
- **Quarkus**: `camel-quarkus-management`
- **Spring Boot**: `camel-spring-boot-starter-management` (or `camel-management` for standalone Camel Main)

**EipTestSupport helper** (add to the shared interface):

```java
import java.util.function.Predicate;
import org.apache.camel.api.management.ManagedCamelContext;
import org.apache.camel.api.management.mbean.ManagedRouteMBean;
import org.citrusframework.exceptions.CitrusRuntimeException;
import org.citrusframework.exceptions.ValidationException;

default TestActionBuilder<?> assertProcessedExchanges(String routeId, long expected, CamelContext camelContext) {
    return assertProcessedExchanges(routeId, it -> it == expected, camelContext);
}

default TestActionBuilder<?> assertProcessedExchanges(String routeId, Predicate<Long> check, CamelContext camelContext) {
    return repeatOnError()
            .until((i, context) -> i > 20)
            .autoSleep(Duration.ofSeconds(1))
            .actions(
                context -> {
                    ManagedCamelContext managedContext = camelContext.getCamelContextExtension()
                            .getContextPlugin(ManagedCamelContext.class);
                    ManagedRouteMBean routeMBean = managedContext.getManagedRoute(routeId);

                    if (routeMBean != null) {
                        long failed = routeMBean.getExchangesFailed();
                        if (failed > 0) {
                            throw new ValidationException("Route '%s' has %d failed exchanges"
                                    .formatted(routeId, failed));
                        }
                        long completed = routeMBean.getExchangesCompleted();
                        if (!check.test(completed)) {
                            throw new ValidationException("Route '%s' has %d completed exchanges"
                                    .formatted(routeId, completed));
                        }
                    } else {
                        throw new CitrusRuntimeException("No managed route for routeId '%s'"
                                .formatted(routeId));
                    }
                }
            );
}
```

**Usage — exact count**:

```java
t.when(
    send()
        .endpoint("kafka:eip.consumer.events")
        .message()
        .body(Resources.create("templates/order.json"))
        .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
);

t.then(
    assertProcessedExchanges("event-driven-consumer", 1, camelContext)
);
```

**Usage — predicate** (e.g., timer-based routes where count is non-deterministic):

```java
t.then(
    assertProcessedExchanges("polling-consumer", it -> it > 1, camelContext)
);
```

**Usage — verify dispatched branch**:

```java
// Send to dispatcher route, verify the specific handler route processed it
t.when(
    send()
        .endpoint("kafka:eip.consumer.dispatch")
        .message()
        .body(Resources.create("templates/order.json"))
);

t.then(
    assertProcessedExchanges("handle-order-placed", 1, camelContext)
);
```

This is strictly better than the old sleep+controlBus approach: it verifies the exchange was **completed without errors**, not just that the route is still in `Started` state.

### Pattern 9: Verify message was NOT sent (expectTimeout)

When testing filters or conditional routing, verify that messages that should be dropped do NOT appear on the output topic. Use `expectTimeout` — it waits for a configurable duration and passes only if no message arrives:

```java
import static org.citrusframework.actions.ReceiveTimeoutAction.Builder.expectTimeout;

// Send a low-value order that should be filtered out
t.when(
    send()
        .endpoint("kafka:eip.orders.placed")
        .message()
        .body(Resources.create("templates/order.json"))
        .header("kafka.KEY", "${id}")
);

// Verify nothing arrives on the high-value topic
t.then(
    expectTimeout()
        .endpoint("kafka:eip.orders.high-value?consumerGroup=citrus-filter-reject-group")
        .timeout(5000)
);
```

YAML DSL equivalent:

```yaml
- expectTimeout:
    endpoint: >-
      kafka:eip.orders.high-value?server=${kafka.broker}&consumerGroup=citrus-filter-reject-group
    wait: 5000
```

Use a unique consumer group for the `expectTimeout` action so it doesn't interfere with other receive actions on the same topic.

### Pattern 10: Verify all split messages (splitter coverage)

When testing a Splitter route, verify that ALL expected messages are produced — not just one. Use a generic template with `@ignore@` for fields whose values vary, and receive once per expected split item:

```java
// Template: templates/item.json
// { "item_sku": "@ignore@", "quantity": "@ignore@" }

t.then(
    repeatOnError()
        .until((i, context) -> i > 25)
        .autoSleep(Duration.ofMillis(500))
        .actions(
            receive()
                .endpoint("kafka:eip.orders.individual?consumerGroup=citrus-individual-group")
                .message()
                .body(Resources.create("templates/item.json")),
            receive()
                .endpoint("kafka:eip.orders.individual?consumerGroup=citrus-individual-group")
                .message()
                .body(Resources.create("templates/item.json"))
        )
);
```

Both receives share the same consumer group so the second receive picks up where the first left off. Wrapping both in a single `repeatOnError` block ensures they retry together if the splitter hasn't finished processing.

### Pattern 11: Deferred initialization for external-service beans

When a production bean eagerly connects to an external service in `@PostConstruct` (e.g., seeding a Redis catalog, preloading a database cache), the Spring Boot test context will fail because Citrus hasn't started the infrastructure yet. The fix:

1. Add a toggle property with a default of `true` (no change to production behavior)
2. Split `@PostConstruct` into a guard method + a public initialization method
3. Set the toggle to `false` in test `application.properties`
4. `@Autowired` the bean into the test and call the public method after infrastructure is ready

```java
// In the test — call BEFORE waitForCamelRouteStarted if the route depends on the seeded data
@Autowired
RedisProductCatalog redisProductCatalog;

@Test
public void shouldEnrichOrderWithProductData() {
    redisProductCatalog.seedCatalog();
    t.given(waitForCamelRouteStarted("content-enricher", camelContext));
    t.when(send()...);
    t.then(receive()...);
}
```

This only affects **Spring Boot** tests. Quarkus manages the lifecycle differently — `@PostConstruct` runs after Citrus `beforeSuite` has already started compose, so the service is available. If you encounter the same issue in Quarkus in the future, apply the same pattern.

### Pattern 12: SQL database interaction with citrus-sql

When a route polls from a database (SQL Polling Consumer), use `citrus-sql` to insert test data and verify database state changes. This requires the `citrus-sql` dependency and injecting the `DataSource`.

**Dependency**:

```xml
<dependency>
    <groupId>org.citrusframework</groupId>
    <artifactId>citrus-sql</artifactId>
    <version>${citrus.version}</version>
    <scope>test</scope>
</dependency>
```

**Inject the DataSource** (Quarkus — both annotations needed):

```java
@Inject
@BindToRegistry
DataSource dataSource;
```

**Insert test data, verify Kafka output, then verify DB state change**:

```java
t.when(
    sql(dataSource)
        .statement("INSERT INTO orders.orders (customer_id, item_sku, quantity, amount) "
                + "VALUES ('CUST-00${id}', 'SKU-${id}', '1', '${amount}')")
);

t.then(
    repeatOnError()
        .until((i, context) -> i > 15)
        .autoSleep(Duration.ofSeconds(1))
        .actions(
            receive()
                .endpoint("kafka:eip.orders.placed?consumerGroup=citrus-placed-group")
                .message()
                .body("""
                {
                  "id": "@variable(order_id)@",
                  "customer_id": "CUST-00${id}",
                  "status": "PLACED",
                  "amount": ${amount}.0,
                  "item_sku": "SKU-${id}",
                  "quantity": 1,
                  "created_at": "@ignore@"
                }
                """)
        )
);

// Verify the route's onConsume SQL updated the row status
t.then(
    sql(dataSource)
        .query()
        .statement("SELECT status FROM orders.orders WHERE id = '${order_id}'")
        .validate("status", "PROCESSING")
);
```

Key points:
- Use `@variable(order_id)@` in the receive body to capture the auto-generated DB primary key into a Citrus variable for later SQL query validation.
- The `@ignore@` matcher skips fields with non-deterministic values (timestamps).
- This pattern tests the full SQL Polling Consumer flow: DB insert → route polls and publishes to Kafka → route's `onConsume` updates the row.

---

## Testing Pitfalls

### Kafka `auto.offset.reset=latest` on intermediate topics

When a route has multi-hop flows (topic A → route1 → topic B → route2 → topic C), sending a test message to topic A may silently fail because the consumer on auto-created intermediate topic B hasn't established its offset yet. By the time it does, the message is already past.

**Workaround**: Send directly to the intermediate topic with pre-set headers that the second route expects. This bypasses the timing issue entirely. Example from ch08 message-expiration test:

```java
// Instead of sending to kafka:eip.metadata.orders (two-hop flow),
// send directly to kafka:eip.metadata.orders.expiring with pre-set headers
t.when(
    send()
        .endpoint("kafka:eip.metadata.orders.expiring")
        .message()
        .body(Resources.create("templates/order.json"))
        .header("kafka.KEY", "${id}")
        .header("messageCreatedAt", System.currentTimeMillis())
        .header("messageExpiresAt", System.currentTimeMillis() + 60_000)
);
```

### Empty `message()` assertions give false confidence

An empty `receive().message()` only proves *some* message arrived on the topic. It could be a leftover from a demo generator, another test, or a prior run. Always verify at least one field that ties the received message to your test input:

- **Headers the route sets**: e.g., `.header("messageExpired", "false")` or `.header("BulkOrderId", "BULK-${id}")`
- **Body via a template**: e.g., `.body(Resources.create("templates/order.json"))`

When the body format is unreliable (e.g., after `unmarshal().json()` without re-marshalling, where the Kafka body becomes `Map.toString()` instead of JSON), verify headers instead — or better yet, fix the route by adding `marshal().json()` before the Kafka output (see [Design routes for testability](#design-routes-for-testability)).

### Cover all branches of route logic

A single happy-path test is not enough when the route contains branching logic (choice, filter, content-based router). Write one test per distinct outcome to verify each branch is reachable and produces the correct result.

**Example**: A Format Indicator route with a `choice()` on `contentType` has three branches (JSON → processed, XML → processed, unknown → dead letter). Test all three by sending messages with different `contentType` headers and verifying each lands on the expected output topic:

```java
// Branch 1: JSON format → processed topic
send().endpoint("kafka:eip.metadata.orders.tagged")
    .message().body(jsonBody).header("contentType", "application/json");
receive().endpoint("kafka:eip.metadata.orders.processed?consumerGroup=citrus-json-group")
    .message().body(jsonBody);

// Branch 2: XML format → processed topic
send().endpoint("kafka:eip.metadata.orders.tagged")
    .message().body(xmlBody).header("contentType", "application/xml");
receive().endpoint("kafka:eip.metadata.orders.processed?consumerGroup=citrus-xml-group")
    .message().body(xmlBody);

// Branch 3: Unknown format → dead letter topic
send().endpoint("kafka:eip.metadata.orders.tagged")
    .message().body(plainBody).header("contentType", "text/plain");
receive().endpoint("kafka:eip.metadata.orders.dead?consumerGroup=citrus-dead-group")
    .message().body(plainBody);
```

The same principle applies to filters (test messages that pass AND messages that are filtered out), enrichers (verify the enriched fields are present), and aggregators (verify the aggregation header and reassembled output).

### Spring Boot `@PostConstruct` and infrastructure lifecycle

When a production bean has `@PostConstruct` that connects to an external service (Redis, database, message broker), the Spring Boot test context loads **before** Citrus `beforeSuite` starts the compose infrastructure. The `@PostConstruct` fires during context creation, the service isn't running yet, and the test fails with a connection error (e.g., `RedisConnectionFailureException`).

**Fix**: make the eager initialization conditional and call it explicitly in the test after infrastructure is up. See [Pattern 11](#pattern-11-deferred-initialization-for-external-service-beans).

Production bean:

```java
@Component
public class RedisProductCatalog {

    @Value("${redis.catalog.seed:true}")
    boolean seedEnabled;

    @PostConstruct
    void init() {
        if (seedEnabled) seedCatalog();   // skipped in tests
    }

    public void seedCatalog() {
        // actual initialization logic (Redis writes, DB inserts, etc.)
    }
}
```

Test `application.properties`:

```properties
redis.catalog.seed=false
```

Test class:

```java
@Autowired
RedisProductCatalog redisProductCatalog;

@Test
public void shouldEnrichOrder() {
    redisProductCatalog.seedCatalog();   // call after infra is up
    t.given(waitForCamelRouteStarted("content-enricher", camelContext));
    // ...
}
```

This pattern applies whenever a `@Component`/`@Service` connects to infrastructure in `@PostConstruct`. The default property value stays `true` so production behavior is unchanged.

### Cross-route interference on shared topics

When multiple routes consume from the same topic with different consumer groups (e.g., ContentBasedRouter and MessageFilter both read `eip.orders.placed`), test data for one route can trigger unintended behavior in the other.

**Workaround**: Design test data to be inert for unrelated routes. For example, keep amounts below 100 in ContentBasedRouter tests to avoid triggering the MessageFilter (which filters `amount >= 100`).

---

## CI Workflow

### Matrix strategy

The CI workflow (`.github/workflows/tests.yml`) uses a matrix strategy with one entry per chapter. After adding tests for a new chapter, add its directory name to the appropriate matrices.

### Adding a new chapter

Add the chapter to the relevant matrix arrays:

```yaml
# Quarkus job
matrix:
  example:
    - 04-channel-types
    - 05-reliability
    - 06-channel-infra
    - NN-new-chapter          # <-- add here

# Spring Boot job (same pattern)
# YAML DSL job (only if chapter has yaml-dsl variant)
```

### YAML DSL CI specifics

The YAML DSL job installs JBang via SDKMAN and runs tests with the Citrus JBang CLI. It also configures a Testcontainers registry mirror for CI:

```yaml
env:
  CITRUS_CAMEL_JBANG_DUMP_INTEGRATION_OUTPUT: "true"
  CITRUS_TESTCONTAINERS_REGISTRY_MIRROR_ENABLED: "true"
  CITRUS_TESTCONTAINERS_REGISTRY_MIRROR: "mirror.gcr.io"
```

---

## Checklist for New Chapters

### Per runtime (Quarkus / Spring Boot)

- [ ] Make all demo data generators toggleable via config property
- [ ] Check for `@PostConstruct` beans that connect to external services — add toggle property if needed (Spring Boot only, see Pattern 11)
- [ ] Add Camel management dependency if using `assertProcessedExchanges` (`camel-quarkus-management` / `camel-spring-boot-starter-management`)
- [ ] Copy `EipTestSupport.java` into the test package (includes `assertProcessedExchanges`)
- [ ] Create `config/EipInfraSetup.java` (use correct annotations per runtime)
- [ ] Create `_infra/compose.yaml` with required services only
- [ ] Create `_infra/postgres/init-schemas.sql` if PostgreSQL is needed
- [ ] Create `templates/order.json` with required fields
- [ ] Create test `application.properties` (disable generators, shutdown timeouts, broker connections)
- [ ] Create `citrus-application.properties` (Quarkus only)
- [ ] Write `EipTests.java` with one `@Nested` class per route/pattern
- [ ] Refactor inline `otherwise()` / fallback branches into named `direct:` routes for testability
- [ ] Add POM dependencies (check if REST/Pulsar/SQL extras are needed)
- [ ] Add surefire/failsafe plugins to POM
- [ ] Run `mvn verify` and confirm tests pass
- [ ] Commit

### Per YAML DSL

- [ ] Create `test/` directory alongside route YAMLs
- [ ] Create `test/_infra/compose.yaml` with required services
- [ ] Create `test/_infra/postgres/init-schemas.sql` if PostgreSQL is needed
- [ ] Create `test/templates/order.json` (use `${order.id}` variable style)
- [ ] Create `test/citrus-application.properties`
- [ ] Create one `.citrus.it.yaml` per route file
- [ ] Verify route YAML structure (`steps` under `from`)
- [ ] Check `application.properties` for JBang compatibility
- [ ] Run `camel test <test-file>` and confirm tests pass
- [ ] Commit

### CI

- [ ] Add chapter to `.github/workflows/tests.yml` matrix for each applicable runtime
- [ ] Commit
