package cascading.flow.hadoop.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StepStateCache {

  private static Logger LOG = LoggerFactory.getLogger(StepStateCache.class);

  public static Path getRandomTemporaryPath(FileSystem fs, Path pathPrefix) throws IOException {
    Path randomTemporaryDir = getRandomPath(pathPrefix);
    fs.mkdirs(randomTemporaryDir);
    fs.deleteOnExit(randomTemporaryDir);
    return getRandomPath(randomTemporaryDir);
  }

  public static Path getRandomPath(Path pathPrefix) {
    return new Path(pathPrefix, UUID.randomUUID().toString());
  }

  public static synchronized <T extends Comparable> void cacheState(JobConf conf, String confs) {
    try {
      FileSystem fs = FileSystem.get(conf);
      Path path = getRandomTemporaryPath(fs, new Path("/tmp/input_conf_cache/"));
      LOG.info("Storing confs at " + path);
      FSDataOutputStream stream = fs.create(path);
      ObjectOutputStream oos = new ObjectOutputStream(stream);
      oos.writeObject(confs);
      oos.close();

      conf.set("cascading.step.cache.location", path.toString());

      LOG.info("Storing confs at path " + path.toString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static synchronized <T extends Comparable> String retrieveRemoteState(JobConf conf) {
    String pathString = conf.get("cascading.step.cache.location");

    try {
      FileSystem fs = FileSystem.get(conf);
      Path path = new Path(pathString);
      LOG.info("Retrieving confs at " + path);
      FSDataInputStream stream = fs.open(path);
      ObjectInputStream ois = new ObjectInputStream(stream);
      return (String)ois.readObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
