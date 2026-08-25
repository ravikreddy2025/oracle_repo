# Local Spark on Windows (test-only shim)

These three Java classes exist so the Delta-backed tests (`pytest -m spark`) can
run on Windows **without `winutils.exe` / `hadoop.dll`**.

They are a **test harness only**. Nothing in `src/ingestion_framework/`
references them, and they are never deployed — Databricks runs on Linux where
none of this applies.

## The problem

Hadoop's local filesystem implementation assumes POSIX. On Windows it satisfies
four operations by shelling out to `winutils.exe` or calling into `hadoop.dll`,
neither of which ships with `pyspark`:

| Operation | Needs | Where it bites |
|---|---|---|
| `setPermission` (chmod) | `winutils.exe` | Creating the warehouse directory |
| file create with a mode | `winutils.exe` | Writing any data file |
| `getFileLinkStatus` (readlink) | `winutils.exe` | Delta's atomic log rename |
| `listStatus` → `canRead` | `hadoop.dll` (native `access()`) | Listing a table directory |

The usual fix is to download `winutils.exe` and `hadoop.dll` from a third-party
GitHub mirror. There is no official Apache Windows binary, so that means running
an unsigned executable from an untrusted source — not something to do on a
developer machine.

## The fix

Subclass the local filesystem and answer those four questions with plain
`java.io` calls, which give the same answers on a local test warehouse:

- **`setPermission`** → no-op. NTFS ACLs are not POSIX modes.
- **`createOutputStreamWithMode`** → pass a `null` permission, which is Hadoop's
  own "don't chmod" path.
- **`getFileLinkStatus`** → return `getFileStatus`. A test warehouse has no
  symlinks.
- **`listStatus`** → list with `File.listFiles()` instead of
  `FileUtil.canRead()`.

`NoChmodLocalFs` binds the same raw filesystem to the `FileContext` API, which
resolves a *different* config key (`fs.AbstractFileSystem.file.impl`) and is what
Delta's transaction log uses for atomic renames.

## What this does and does not prove

It changes **how the local filesystem answers permission questions**, nothing
else. Delta's transaction log, MERGE semantics, dedupe behaviour, and commit
protocol are entirely untouched — those run exactly as they would on a cluster.

It says nothing about how permissions behave on a real deployment, and it is not
a substitute for running the suite on Linux or Databricks before a production
release.

## Building

`tests/conftest.py` compiles these automatically with `javac` when a JDK is
present, caching the classes under the pytest tmp path. If no JDK is available,
the Spark tests skip with a reason.

Manual build:

```bash
javac -cp "$(python -c "import pyspark,os,glob;print(glob.glob(os.path.join(os.path.dirname(pyspark.__file__),'jars','hadoop-client-api-*.jar'))[0])")" -d build/localspark tools/localspark/*.java
```
