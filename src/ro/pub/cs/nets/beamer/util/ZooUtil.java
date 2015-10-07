package ro.pub.cs.nets.beamer.util;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

public class ZooUtil
{
	public static void createOrSet(ZooKeeper zk, String path, byte data[], List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException
	{
		try
		{
			zk.create(path, data, acl, createMode);
		}
		catch (KeeperException.NodeExistsException ex)
		{
			zk.setData(path, data, -1);
		}
	}
	
	public static void createOrSet(ZooKeeper zk, final String path, byte data[]) throws KeeperException, InterruptedException
	{
		createOrSet(zk, path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
	
	public static void createOrSet(ZooKeeper zk, final String path, byte data[], CreateMode createMode) throws KeeperException, InterruptedException
	{
		createOrSet(zk, path, data, Ids.OPEN_ACL_UNSAFE, createMode);
	}
	
	public static void createOrSet(SanitizedZooCache zc, String path, byte data[], CreateMode createMode) throws KeeperException, InterruptedException
	{
		if (zc.isCached(path))
		{
			zc.setData(path, data);
			return;
		}
		
		try
		{
			zc.create(path, data, createMode);
		}
		catch (NodeExistsException ex)
		{
			zc.setData(path, data);
		}
	}
	
	public static void createOrSet(SanitizedZooCache zc, final String path, byte data[]) throws KeeperException, InterruptedException
	{
		createOrSet(zc, path, data, CreateMode.PERSISTENT);
	}
	
	public static void create(ZooKeeper zk, String path) throws KeeperException, InterruptedException
	{
		zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
	
	public static void create(SanitizedZooCache zc, String path) throws KeeperException, InterruptedException
	{
		zc.create(path, new byte[0], CreateMode.PERSISTENT);
	}
	
	public static void deleteIfExists(ZooKeeper zk, String path) throws KeeperException, InterruptedException
	{
		try
		{
			zk.delete(path, -1);
		}
		catch (IllegalArgumentException | NoNodeException ex) {}
	}
	
	public static void deleteIfExists(SanitizedZooCache zc, String path) throws KeeperException, InterruptedException
	{
		try
		{
			zc.delete(path);
		}
		catch (IllegalArgumentException | NoNodeException ex) {}
	}
	
	public static void deleteRecursiveIfExists(ZooKeeper zk, String path) throws KeeperException, InterruptedException
	{
		try
		{
			ZKUtil.deleteRecursive(zk, path);
		}
		catch (IllegalArgumentException | NoNodeException ex) {}
	}
	
	public static void setIfExists(ZooKeeper zk, String path, byte data[], int version) throws KeeperException, InterruptedException
	{
		try
		{
			zk.setData(path, data, version);
		}
		catch (IllegalArgumentException | NoNodeException ex) {}
	}
	
	public static void syncFully(ZooKeeper zk, String path) throws InterruptedException
	{
		Semaphore sem = new Semaphore(0);
		
		zk.sync(path, new VoidCallback()
		{
			@Override
			public void processResult(int rc, String path, Object ctx)
			{
				sem.release();
			}
		}, null);
		
		sem.acquire();
	}
	
	public static void createOrSetHuge(SanitizedZooCache zc, final String path, byte data[]) throws KeeperException, InterruptedException
	{
		byte[] compData = ZKFormat.compress(data);
		List<byte []> chunkedCompData = new LinkedList<>();
		int chunks = 0;
		
		//System.out.println(path + " uncompressed " + data.length + " compressed " + compressedData.length);
		
		for (int i = 0; i < compData.length;)
		{
			int left = compData.length - i;
			int maxChunkLen = (chunks == 0) ? ZKFormat.MAX_NODE_SIZE - 4 : ZKFormat.MAX_NODE_SIZE;
			int chunkLen = Math.min(left, maxChunkLen);
			
			byte buf[] = Arrays.copyOfRange(compData, i, i + chunkLen);
			chunkedCompData.add(buf);
			
			i += buf.length;
			chunks++;
		}
		
		/* prefix first chunk with chunk count */
		ByteBuffer bb = ByteBuffer.allocate(4 + chunkedCompData.get(0).length);
		bb.put(ZKFormat.serialize(chunkedCompData.size()));
		bb.put(chunkedCompData.get(0));
		chunkedCompData.set(0, bb.array());
		
		for (int i = 0; i < chunks; i++)
		{
			createOrSet(zc, path + "_" + i, chunkedCompData.get(0));
			chunkedCompData.remove(0);
		}
	}
	
	public static byte[] getHuge(SanitizedZooCache zc, final String path) throws KeeperException, InterruptedException
	{
		List<byte []> chunkedData = new LinkedList<>();
		
		/* first chunk */
		byte headerAndChunk[] = zc.getData(path + "_0");
		byte header[] = Arrays.copyOfRange(headerAndChunk, 0, 4);
		byte firstChunk[] = Arrays.copyOfRange(headerAndChunk, 4, headerAndChunk.length);
		int chunks = ZKFormat.deserializeInt(header);
		
		chunkedData.add(firstChunk);
		int totalSize = firstChunk.length;
		
		for (int i = 1; i < chunks; i++)
		{
			byte chunk[] = zc.getData(path + "_" + i);
			chunkedData.add(chunk);
			totalSize += chunk.length;
		}
		
		ByteBuffer bb = ByteBuffer.allocate(totalSize);
		for (byte chunk[]: chunkedData)
			bb.put(chunk);
		
		return ZKFormat.decompress(bb.array());
	}
}
