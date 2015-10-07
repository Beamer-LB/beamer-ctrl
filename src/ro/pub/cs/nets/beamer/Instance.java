package ro.pub.cs.nets.beamer;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.Inet4Address;
import ro.pub.cs.nets.beamer.manager.Manager;
import ro.pub.cs.nets.beamer.util.SanitizedZooCache;
import ro.pub.cs.nets.beamer.util.ZKFormat;
import ro.pub.cs.nets.beamer.util.ZooUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class Instance
{
	private static class IdleWatcher implements Watcher
	{
		@Override
		public void process(WatchedEvent event) {}
	}
	
	protected static final int SESSION_TIMEOUT = 40 * 1000;
	
	protected SanitizedZooCache zc;
	
	protected Manager manager;
	
	public Instance(String connectString) throws IOException, KeeperException, InterruptedException
	{
		ZooKeeper zk = new ZooKeeper(connectString, SESSION_TIMEOUT, new IdleWatcher());
		ZooUtil.syncFully(zk, "/");
		zc = new SanitizedZooCache(zk);
		
		int ringSize = ZKFormat.deserializeInt(zc.getData("/beamer/config/ring_size"));
		
		manager = new Manager(zc, ringSize);
		manager.update();
	}
	
	public static void bootstrap(String connectString, Inet4Address vip, int ringSize) throws IOException, KeeperException, InterruptedException
	{
		ZooKeeper zk = new ZooKeeper(connectString, SESSION_TIMEOUT, new IdleWatcher());
		ZooUtil.syncFully(zk, "/");
		ZooUtil.deleteRecursiveIfExists(zk, "/beamer");
		SanitizedZooCache zc = new SanitizedZooCache(zk);
		
		zc.create("/beamer", new byte[0], CreateMode.PERSISTENT);
		
		zc.create("/beamer/config", new byte[0], CreateMode.PERSISTENT);
		zc.create("/beamer/config/vip", vip.getAddress(), CreateMode.PERSISTENT);
		zc.create("/beamer/config/ring_size", ZKFormat.serialize(ringSize), CreateMode.PERSISTENT);
		
		Manager dipManager = new Manager(zc, ringSize);
		dipManager.bootstrap();
	}
	
	public static void rebuild(String connectString, Inet4Address vip, int ringSize, String hashDumpName, String idDumpName) throws IOException, KeeperException, InterruptedException, Exception
	{
		FileInputStream hashDump = new FileInputStream(hashDumpName);
		FileInputStream idDump = new FileInputStream(idDumpName);
		
		ZooKeeper zk = new ZooKeeper(connectString, SESSION_TIMEOUT, new IdleWatcher());
		ZooUtil.syncFully(zk, "/");
		ZooUtil.deleteRecursiveIfExists(zk, "/beamer");
		SanitizedZooCache zc = new SanitizedZooCache(zk);
		
		zc.create("/beamer", new byte[0], CreateMode.PERSISTENT);
		
		zc.create("/beamer/config", new byte[0], CreateMode.PERSISTENT);
		zc.create("/beamer/config/vip", vip.getAddress(), CreateMode.PERSISTENT);
		zc.create("/beamer/config/ring_size", ZKFormat.serialize(ringSize), CreateMode.PERSISTENT);
		
		Manager dipManager = new Manager(zc, ringSize);
		dipManager.rebuild(ringSize, hashDump, idDump);
	}

	public Manager getManager()
	{
		return manager;
	}
}
