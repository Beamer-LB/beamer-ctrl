package ro.pub.cs.nets.beamer.experimental;

import ro.pub.cs.nets.beamer.util.*;
import java.util.TreeSet;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class ReadOnlySanitizedZooCache extends SanitizedZooCache
{
	public ReadOnlySanitizedZooCache(ZooKeeper zk)
	{
		super(zk);
	}
	
	@Override
	public void delete(String path) throws KeeperException, InterruptedException
	{
		path = ZKPathSanitizer.sanitize(path);
		
		String parentPath = path.substring(0, path.lastIndexOf("/"));
		String child = path.substring(path.lastIndexOf("/") + 1);
		
		dataCache.remove(path);
		
		TreeSet<String> children = childCache.get(parentPath);
		if (children != null)
			children.remove(child);
	}
	
	@Override
	public void create(String path, byte data[], CreateMode mode) throws KeeperException, InterruptedException
	{
		path = ZKPathSanitizer.sanitize(path);
		cacheNode(path, data, true);
	}
	
	@Override
	public void setData(String path, byte data[]) throws KeeperException, InterruptedException
	{
		path = ZKPathSanitizer.sanitize(path);
		cacheNode(path, data, false);
	}
}
