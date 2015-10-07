package ro.pub.cs.nets.beamer.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class SanitizedZooCache
{
	protected ZooKeeper zk;
	protected HashMap<String, byte[]> dataCache = new HashMap<>();
	protected HashMap<String, TreeSet<String>> childCache = new HashMap<>();

	public SanitizedZooCache(ZooKeeper zk)
	{
		this.zk = zk;
	}
	
	public List<String> getChildren(String path) throws KeeperException, InterruptedException
	{
		path = ZKPathSanitizer.sanitize(path);
		
		TreeSet<String> ret = childCache.get(path);
		
		if (ret == null)
		{
			List<String> children = zk.getChildren(path, false);
			ret = new TreeSet<>(children);
			childCache.put(path, ret);
		}
		
		return new LinkedList<>(ret);
	}
	
	public void delete(String path) throws KeeperException, InterruptedException
	{
		path = ZKPathSanitizer.sanitize(path);
		
		String parentPath = path.substring(0, path.lastIndexOf("/"));
		String child = path.substring(path.lastIndexOf("/") + 1);
		
		zk.delete(path, -1);
		
		dataCache.remove(path);
		
		TreeSet<String> children = childCache.get(parentPath);
		if (children != null)
			children.remove(child);
	}
	
	protected void cacheNode(String path, byte data[], boolean surelyChildless)
	{
		String parent = path.substring(0, path.lastIndexOf("/"));
		String child = path.substring(path.lastIndexOf("/") + 1);
		
		TreeSet<String> siblings = childCache.get(parent);
		if (siblings != null)
			siblings.add(child);
		
		dataCache.put(path, data);
		if (surelyChildless)
			childCache.put(path, new TreeSet<>());
	}
	
	public void create(String path, byte data[], CreateMode mode) throws KeeperException, InterruptedException
	{
		path = ZKPathSanitizer.sanitize(path);
		path = zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
		cacheNode(path, data, true);
	}
	
	public void setData(String path, byte data[]) throws KeeperException, InterruptedException
	{
		path = ZKPathSanitizer.sanitize(path);
		zk.setData(path, data, -1);
		cacheNode(path, data, false);
	}
	
	public byte[] getData(String path) throws KeeperException, InterruptedException
	{
		path = ZKPathSanitizer.sanitize(path);
		
		byte ret[] = dataCache.get(path);
		
		if (ret == null)
		{
			ret = zk.getData(path, false, null);
			dataCache.put(path, ret);
		}
		
		return ret;
	}

	boolean isCached(String path)
	{
		path = ZKPathSanitizer.sanitize(path);
		return dataCache.containsKey(path);
	}
}
