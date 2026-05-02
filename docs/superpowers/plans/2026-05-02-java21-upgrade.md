# Java 21 Upgrade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the `ethiohri-mamba` OpenMRS module so it builds and runs on Java 21, and is compatible with OpenMRS platform 2.8.6.

**Architecture:** All changes are in the root `pom.xml` only — no Java source changes are needed. The upgrade covers three areas: the Maven compiler target, the surefire JVM arguments, and the dependency version properties.

**Tech Stack:** Maven 3.9+, Java 21, OpenMRS 2.8.6

---

## Files Modified

- Modify: `pom.xml` (root) — compiler plugin, surefire plugin, parent version, dependency version properties

---

### Task 1: Bump Maven parent version

**Files:**
- Modify: `pom.xml:6-10`

- [ ] **Step 1: Update the parent POM version**

In `pom.xml`, find the `<parent>` block at the top and change the version from `1.1.1` to `1.1.3`:

```xml
<parent>
    <groupId>org.openmrs.maven.parents</groupId>
    <artifactId>maven-parent-openmrs-module</artifactId>
    <version>1.1.3</version>
</parent>
```

- [ ] **Step 2: Verify the change parses correctly**

```bash
mvn help:effective-pom -q
```

Expected: exits 0 with no XML parse errors.

- [ ] **Step 3: Commit**

```bash
git add pom.xml
git commit -m "build: bump maven-parent-openmrs-module to 1.1.3"
```

---

### Task 2: Update Maven compiler plugin to Java 21

**Files:**
- Modify: `pom.xml:364-379`

- [ ] **Step 1: Replace the compiler plugin configuration**

In `pom.xml`, find the `maven-compiler-plugin` block inside `<pluginManagement>` and replace it with:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <version>3.13.0</version>
    <configuration>
        <release>21</release>
        <encoding>UTF-8</encoding>
    </configuration>
    <executions>
        <execution>
            <id>default-testCompile</id>
            <phase>test</phase>
        </execution>
    </executions>
</plugin>
```

Note: `<release>21</release>` replaces the old `<source>8</source>/<target>8</target>` pair. The `<executions>` block must be preserved.

- [ ] **Step 2: Verify the module compiles on Java 21**

```bash
mvn clean compile
```

Expected: `BUILD SUCCESS`. No `illegal reflective access` warnings.

- [ ] **Step 3: Commit**

```bash
git add pom.xml
git commit -m "build: upgrade maven-compiler-plugin to 3.13.0, target Java 21"
```

---

### Task 3: Add JVM --add-opens to Surefire

**Files:**
- Modify: `pom.xml:414-420`

- [ ] **Step 1: Add argLine to the surefire plugin configuration**

In `pom.xml`, find the `maven-surefire-plugin` block inside `<pluginManagement>` and add an `<argLine>` element:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
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

- [ ] **Step 2: Verify tests run without module-access errors**

```bash
mvn test
```

Expected: `BUILD SUCCESS` (or `BUILD SUCCESS` with `Tests run: 0` if no tests exist yet). No `InaccessibleObjectException` in the output.

- [ ] **Step 3: Commit**

```bash
git add pom.xml
git commit -m "build: add --add-opens JVM args for Java 21 module system compatibility"
```

---

### Task 4: Upgrade OpenMRS platform and dependent module versions

**Files:**
- Modify: `pom.xml:489-500` (the `<properties>` block)

- [ ] **Step 1: Update the version properties**

In `pom.xml`, find the `<properties>` block and apply these changes:

```xml
<properties>
    <openmrsPlatformVersion>2.8.6</openmrsPlatformVersion>
    <reportingVersion>1.26.0</reportingVersion>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <calculationVersion>1.1</calculationVersion>
    <serialization.xstreamVersion>0.2.16</serialization.xstreamVersion>
    <cohortVersion>3.5.0</cohortVersion>
    <javaxAnnotationVersion>1.3.2</javaxAnnotationVersion>
    <mambaETLCoreVersion>2.0.1-SNAPSHOT</mambaETLCoreVersion>
    <webservicesRestVersion>2.42.0</webservicesRestVersion>
    <apache.commons.dbcp>2.11.0</apache.commons.dbcp>
    <jacksonCoreVersion>2.15.3</jacksonCoreVersion>
</properties>
```

Note: `mambaETLCoreVersion` stays at its current snapshot value and will be updated separately once the mamba-core Java 21 upgrade is released.

- [ ] **Step 2: Resolve dependencies and check for conflicts**

```bash
mvn dependency:resolve
```

Expected: `BUILD SUCCESS`. If any dependency is not found in the repository, check the OpenMRS Nexus at `https://mavenrepo.openmrs.org/nexus/content/repositories/public` for the correct available version.

- [ ] **Step 3: Full build**

```bash
mvn clean package -DskipTests
```

Expected: `BUILD SUCCESS` and the `.omod` artifact is generated under `omod/target/`.

- [ ] **Step 4: Run tests**

```bash
mvn test
```

Expected: `BUILD SUCCESS` with no failures. If tests fail due to API changes between OpenMRS 2.1.1 and 2.8.6, investigate the specific failure — the most common cause is a changed method signature on an OpenMRS service interface.

- [ ] **Step 5: Commit**

```bash
git add pom.xml
git commit -m "build: upgrade OpenMRS platform to 2.8.6 and dependent module versions for Java 21"
```

---

### Task 5: Final verification

- [ ] **Step 1: Full clean build including install**

```bash
mvn clean install
```

Expected: `BUILD SUCCESS`. The `.omod` file is produced. No `illegal access` or `InaccessibleObjectException` output.

- [ ] **Step 2: Confirm Java version used**

```bash
mvn -version
```

Expected output includes `Java version: 21`.

- [ ] **Step 3: Confirm artifact**

```bash
ls omod/target/*.omod
```

Expected: one `.omod` file, e.g. `ethiohri-mamba-2.1.0-SNAPSHOT.omod`.
