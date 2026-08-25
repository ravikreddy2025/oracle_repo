package local.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * RawLocalFileSystem that never shells out to winutils.exe for POSIX chmod.
 *
 * Two paths need handling: setPermission() itself, and file creation, which
 * calls setPermission internally whenever it is given a non-null permission.
 * Hadoop skips the chmod entirely when the permission is null.
 */
public class NoChmodRawLocalFileSystem extends RawLocalFileSystem {

  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    // No-op: NTFS ACLs are not POSIX modes.
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    // Hadoop's listStatus filters entries with FileUtil.canRead(), which on
    // Windows calls the hadoop.dll native access() check. java.io.File answers
    // the same question without a native library.
    File localFile = pathToFile(f);
    if (!localFile.exists()) {
      throw new FileNotFoundException("File " + f + " does not exist");
    }
    if (localFile.isFile()) {
      return new FileStatus[] {getFileStatus(f)};
    }
    File[] entries = localFile.listFiles();
    if (entries == null) {
      throw new IOException("Could not list directory " + f);
    }
    List<FileStatus> statuses = new ArrayList<>(entries.length);
    for (File entry : entries) {
      try {
        statuses.add(getFileStatus(new Path(f, entry.getName())));
      } catch (FileNotFoundException raced) {
        // Deleted between listing and stat; skip it, as Hadoop does.
      }
    }
    return statuses.toArray(new FileStatus[0]);
  }

  @Override
  public FileStatus getFileLinkStatus(Path f) throws IOException {
    // Hadoop resolves symlinks by shelling out to winutils. A local test
    // warehouse has none, so the plain status is the same answer.
    return getFileStatus(f);
  }

  @Override
  protected OutputStream createOutputStreamWithMode(Path f, boolean append, FsPermission permission)
      throws IOException {
    return super.createOutputStreamWithMode(f, append, null);
  }
}
