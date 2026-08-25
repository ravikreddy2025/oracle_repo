package local.fs;

import org.apache.hadoop.fs.LocalFileSystem;

/** LocalFileSystem wired to the chmod-free raw filesystem. */
public class NoChmodLocalFileSystem extends LocalFileSystem {
  public NoChmodLocalFileSystem() {
    super(new NoChmodRawLocalFileSystem());
  }
}
