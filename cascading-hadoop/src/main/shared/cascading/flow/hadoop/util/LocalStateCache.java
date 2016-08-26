package cascading.flow.hadoop.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalStateCache {

  private static LocalStateCache cache;
  private static Logger LOG = LoggerFactory.getLogger(LocalStateCache.class);

  private final Set<String> addedConfKeys = new HashSet<>();
  private final HashMap<String, String> remoteConfMap;

  public LocalStateCache() {
    remoteConfMap = new HashMap<String, String>();
  }

  public synchronized static LocalStateCache getCache() {
    if (cache == null) {
      cache = new LocalStateCache();
    }
    return cache;
  }

  public static void safeDeleteOnExit(FileSystem fs, Path path) throws IOException {

    //  if it's a viewFS, get the child FS and attach the deleteOnExit to the right child
    //  (https://issues.apache.org/jira/browse/HDFS-10323)
//    if(fs instanceof ViewFileSystem) {
//      ViewFileSystem viewfs = (ViewFileSystem)fs;
//      Path withoutPrefix = stripPrefix(path);
//
//      for (FileSystem fileSystem : viewfs.getChildFileSystems()) {
//        if (fileSystem.exists(withoutPrefix)) {
//          fileSystem.deleteOnExit(withoutPrefix);
//        }
//      }
//    }
//
//    else
    {
      fs.deleteOnExit(path);
    }

  }

  protected static Path stripPrefix(Path path){
    String rawPath = path.toUri().getPath();
    return new Path(rawPath);
  }

  @Deprecated
  public static FileSystem getFS() {
    try {
      return FileSystem.get(new Configuration());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  public static Path getRandomTemporaryPath(Path pathPrefix) throws IOException {
    Path randomTemporaryDir = getRandomPath(pathPrefix);
    FileSystem fs = getFS();
    fs.mkdirs(randomTemporaryDir);
    safeDeleteOnExit(fs, randomTemporaryDir);
    return getRandomPath(randomTemporaryDir);
  }

  public static Path getRandomPath(Path pathPrefix) {
    return new Path(pathPrefix, UUID.randomUUID().toString());
  }

  /**
   * Creates a random path under a given path prefix
   *
   * @param pathPrefix
   * @return the random path
   */
  public static Path getRandomPath(String pathPrefix) {
    return getRandomPath(new Path(pathPrefix));
  }


  public synchronized <T extends Comparable> void cache(JobConf conf, String confs) {
    try {
      Path path = getRandomTemporaryPath(new Path("/tmp/input_conf_cache/"));
      LOG.info("Storing confs at " + path);
      FileSystem fs = FileSystem.get(conf);
      FSDataOutputStream stream = fs.create(path);
      ObjectOutputStream oos = new ObjectOutputStream(stream);
      oos.writeObject(confs);
      oos.close();

      conf.set("cascading.step.cache.location", path.toString());

      addedConfKeys.add(path.toString());
      LOG.info("Storing confs at path " + path.toString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  public synchronized <T extends Comparable> String retrieveRemote(JobConf conf) {
    String list;
    String pathString = conf.get("cascading.step.cache.location");
    if (remoteConfMap.containsKey(pathString)) {
      list = remoteConfMap.get(pathString);
    } else {
      try {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathString);
        LOG.info("Retrieving confs at " + path);
        FSDataInputStream stream = fs.open(path);
        ObjectInputStream ois = new ObjectInputStream(stream);
        list = (String)ois.readObject();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      remoteConfMap.put(pathString, list);
    }
    return list;
  }

}
