# Java 21 Upgrade Design — ethiohri-mamba module

**Date:** 2026-05-02
**Branch:** palladium_uprade
**Scope:** This module only (`ethiohri-mamba`). `mamba-core-api` will be upgraded separately by the author.

## Goal

Run the `ethiohri-mamba` OpenMRS module on Java 21. No new language features are adopted — this is a pure build/runtime compatibility lift.

## What Changes

All changes are confined to `pom.xml` (root).

### 1. Maven parent

```xml
<!-- before -->
<version>1.1.1</version>

<!-- after -->
<version>1.1.3</version>
```

### 2. Compiler plugin

```xml
<!-- before -->
<plugin>
    <artifactId>maven-compiler-plugin</artifactId>
    <version>3.8.1</version>
    <configuration>
        <source>8</source>
        <target>8</target>
        <encoding>UTF-8</encoding>
    </configuration>
</plugin>

<!-- after -->
<plugin>
    <artifactId>maven-compiler-plugin</artifactId>
    <version>3.13.0</version>
    <configuration>
        <release>21</release>
        <encoding>UTF-8</encoding>
    </configuration>
</plugin>
```

Using `<release>21</release>` instead of `<source>`+`<target>` prevents accidental use of APIs removed before Java 21.

### 3. Surefire plugin — add JVM opens

Spring + Hibernate test infrastructure uses reflection that the Java module system blocks. Add `argLine` to the existing surefire configuration:

```xml
<plugin>
    <artifactId>maven-surefire-plugin</artifactId>
    <version>3.2.5</version>
    <configuration>
        <includes>
            <include>**/*Test.java</include>
        </includes>
        <argLine>
            --add-opens java.base/java.lang=ALL-UNNAMED
            --add-opens java.base/java.util=ALL-UNNAMED
            --add-opens java.base/java.io=ALL-UNNAMED
        </argLine>
    </configuration>
</plugin>
```

### 4. Dependency version properties

| Property | Current | Target |
|---|---|---|
| `openmrsPlatformVersion` | 2.1.1 | 2.8.6 |
| `reportingVersion` | 1.19.0 | 1.26.0 |
| `webservicesRestVersion` | 2.21.0 | 2.42.0 |
| `cohortVersion` | 3.0.0-SNAPSHOT | 3.5.0 |
| `serialization.xstreamVersion` | 0.2.11 | 0.2.16 |
| `mambaETLCoreVersion` | 2.0.1-SNAPSHOT | *(bump after mamba-core Java 21 release)* |

## What Does NOT Change

- **All Java source code** — no removed APIs, no `sun.*` internals, no `finalize()`. Zero code changes required.
- **`javax.annotation-api 1.3.2`** — OpenMRS 2.8.6 is on Spring 5.x (`javax.*` namespace), not Spring 6 (`jakarta.*`). No migration needed.
- **`commons-dbcp2 2.11.0`** — already Java 21 compatible.
- **`jackson-databind 2.15.3`** — already Java 21 compatible.
- **Build scripts / shell scripts** — no changes needed.
- **`omod/pom.xml` and `api/pom.xml`** — no changes needed; they inherit from the root.

## Verification Steps

1. `mvn clean compile` — must succeed with no warnings about illegal reflective access
2. `mvn test` — all tests pass
3. `mvn package` — `.omod` artifact builds successfully
4. Deploy to an OpenMRS 2.8.6 instance running Java 21 and confirm module starts

## Out of Scope

- Adopting Java 21 language features (records, sealed classes, pattern matching, virtual threads)
- Upgrading `mamba-core-api` — handled separately
- Upgrading any CI/CD pipeline configuration
