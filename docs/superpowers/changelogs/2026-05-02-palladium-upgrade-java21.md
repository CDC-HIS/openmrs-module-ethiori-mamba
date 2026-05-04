# Palladium Upgrade — Java 21 & OpenMRS 2.8.6 Migration

**Branch:** `palladium_upgrade` (renamed from `palladium_uprade`)  
**Date:** 2026-05-02  
**Author:** samuelabebayehu  
**Scope:** `ethiohri-mamba` OpenMRS module  

---

## Objective

Upgrade the `ethiohri-mamba` module from **Java 8 / OpenMRS 2.1.1** to **Java 21 / OpenMRS 2.8.6**, ensuring build and runtime compatibility without modifying Java source code.

---

## Task List

### Phase 1: Design & Planning

- [x] **Task 1.1:** Write Java 21 upgrade design specification  
  - File: `docs/superpowers/specs/2026-05-02-java21-upgrade-design.md`  
  - Commit: `cdb7f19`

- [x] **Task 1.2:** Write Java 21 upgrade implementation plan  
  - File: `docs/superpowers/plans/2026-05-02-java21-upgrade.md`  
  - Commit: `831a4aa`

---

### Phase 2: Build System Updates (`pom.xml`)

- [x] **Task 2.1:** Upgrade Maven compiler plugin to target Java 21  
  - `maven-compiler-plugin`: `3.8.1` → `3.15.0`  
  - `<source>8</source>` / `<target>8</target>` → `<release>21</release>`  
  - Added `<javaCompilerSource>21</javaCompilerTarget>` properties  
  - Commit: `efa0986`

- [x] **Task 2.2:** Add `--add-opens` JVM arguments for Java 21 module system compatibility  
  - Added `argLine` to `maven-surefire-plugin` configuration  
  - Opens: `java.lang`, `java.lang.reflect`, `java.util`, `java.util.concurrent`, `java.io`, `sun.rmi.transport`, `com.sun.org.apache.xpath.internal`  
  - Commit: `aefe864`

- [x] **Task 2.3:** Upgrade OpenMRS platform and dependent module versions  
  - `openmrsPlatformVersion`: `2.1.1` → `2.8.6`  
  - `reportingVersion`: `1.19.0` → `1.26.0`  
  - `webservicesRestVersion`: `2.21.0` → `2.42.0`  
  - `cohortVersion`: `3.0.0-SNAPSHOT` → `3.5.0`  
  - `serialization.xstreamVersion`: `0.2.11` → `0.2.16`  
  - Commit: `5fe842a`

---

### Phase 3: CI/CD Workflow Upgrades

- [x] **Task 3.1:** Upgrade GitHub Actions to JDK 21  
  - All workflows (`main.yml`, `build-and-deploy-mamba.yml`, `release.yml`)  
  - `java-version`: `'8'` → `'21'`  
  - `distribution`: `corretto` / `adopt` → `corretto` / `temurin`  
  - Commit: `3d8c8e1`

- [x] **Task 3.2:** Implement two-pass build for SQL packaging  
  - **Problem:** ETL compiler generates SQL files into `api/src/main/resources/mamba/` (gitignored). A single-pass build creates the API jar *before* SQL files exist on disk, so they are missing from the final OMOD.
  - **Solution:**
    1. Pass 1: `mvn install -DskipTests` — runs ETL compiler, deposits SQL files
    2. Pass 2: `mvn clean install -DskipTests` — rebuilds API jar with SQL included
  - Commit: `3d8c8e1`

- [x] **Task 3.3:** Simplify and clean up workflow steps  
  - Removed verbose debug `echo`/`ls` commands  
  - Added focused verification steps that check `jdbc_create_stored_procedures.sql` is present in both API jar and OMOD  
  - Commit: `3d8c8e1`

---

### Phase 4: Post-Upgrade CI Improvements

- [x] **Task 4.1:** Add `workflow_dispatch` to `main.yml` for manual runs  
  - Commit: `f9ca556`

- [x] **Task 4.2:** Add mamba-core checkout and install steps  
  - Enables building against mamba-core dependency in CI  
  - Commit: `39948d9`

- [x] **Task 4.3:** Update mamba-core checkout for public access  
  - Commented out token requirement for public repo access  
  - Added `MODULE_VERSION` environment variable to release workflow  
  - Commit: `ca7b14e`

- [x] **Task 4.4:** Update branch triggers and add Dependabot configuration  
  - Re-enabled `master` branch triggers in `main.yml`  
  - Added `.github/dependabot.yml` for dependency update automation  
  - Commit: `7cb6003`

- [x] **Task 4.5:** Improve OMOD file verification and error messaging  
  - Cleaner verification steps with better error handling  
  - Commit: `2b8b179`

- [x] **Task 4.6:** Add caching for mamba-core artifacts and retrieve commit SHA  
  - Added Maven caching for mamba-core builds  
  - Commit: `3c569b0`

---

## Summary of Changes

| Area | Files Changed | Key Changes |
|---|---|---|
| **Documentation** | `docs/superpowers/specs/2026-05-02-java21-upgrade-design.md` | Design spec for Java 21 compatibility |
| **Documentation** | `docs/superpowers/plans/2026-05-02-java21-upgrade.md` | Step-by-step implementation checklist |
| **Build** | `pom.xml` | Compiler plugin, JVM args, dependency versions |
| **CI/CD** | `.github/workflows/main.yml` | JDK 21, two-pass build, simplified steps |
| **CI/CD** | `.github/workflows/build-and-deploy-mamba.yml` | JDK 21, two-pass build, deployment cleanup |
| **CI/CD** | `.github/workflows/release.yml` | JDK 21, two-pass build, release automation |
| **CI/CD** | `.github/dependabot.yml` | Automated dependency updates |

---

## What Did NOT Change

- **Zero Java source code changes** — no removed APIs, no `sun.*` internals, no `finalize()`
- **No Jakarta migration** — OpenMRS 2.8.6 uses Spring 5.x (`javax.*`), so `javax.annotation-api 1.3.2` remains valid
- **No changes to `omod/pom.xml` or `api/pom.xml`** — they inherit all changes from the root `pom.xml`
- **Build scripts / shell scripts** — no changes needed outside of CI workflows

---

## Verification Steps

1. `mvn clean compile` — must succeed with no warnings about illegal reflective access
2. `mvn test` — all tests pass (with `--add-opens` JVM args)
3. `mvn clean install` — `.omod` artifact builds successfully
4. Deploy to an OpenMRS 2.8.6 instance running Java 21 and confirm module starts

---

## Branch History

```
cdb7f19 docs: add Java 21 upgrade design spec
831a4aa docs: add Java 21 upgrade implementation plan
efa0986 build: upgrade maven-compiler-plugin to 3.15.0, target Java 21
aefe864 build: add --add-opens JVM args for Java 21 module system compatibility
5fe842a build: upgrade OpenMRS platform to 2.8.6 and dependent module versions for Java 21
3d8c8e1 ci: upgrade to JDK 21 and fix SQL packaging with two-pass build
f9ca556 ci: add workflow_dispatch to main.yml for manual runs
39948d9 build: add steps to checkout and install mamba-core module
ca7b14e build: update mamba-core checkout step to comment out token for public access / add MODULE_VERSION to release workflow
7cb6003 build: update branch triggers for GitHub Actions and add Dependabot configuration
2b8b179 ci: improve OMOD file verification and error messaging in workflow
3c569b0 build: add caching for mamba-core artifacts and retrieve commit SHA
```

---

## Notes

- The branch was originally named `palladium_uprade` (typo). It has been renamed to `palladium_upgrade` locally.
- `master` remains on Java 8 / OpenMRS 2.1.1 for backward compatibility with existing deployments.
- This branch is ready for Java 21 deployments and can be merged when the organization is ready to migrate.
