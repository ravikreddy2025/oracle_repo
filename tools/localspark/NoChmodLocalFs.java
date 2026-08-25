package local.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

/**
 * AbstractFileSystem ("FileContext") binding for file://.
 *
 * fs.file.impl only covers the FileSystem API; Delta's transaction log goes
 * through FileContext, which resolves fs.AbstractFileSystem.file.impl and would
 * otherwise wrap a stock RawLocalFileSystem that chmods.
 */
public class NoChmodLocalFs extends DelegateToFileSystem {
  public NoChmodLocalFs(URI theUri, Configuration conf) throws IOException, URISyntaxException {
    super(theUri, new NoChmodRawLocalFileSystem(), conf, "file", false);
  }
}
