package ro.pub.cs.nets.beamer.manager.dipmap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.util.Arrays;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import ro.pub.cs.nets.beamer.manager.ZKAbstraction;
import ro.pub.cs.nets.beamer.util.SanitizedZooCache;
import ro.pub.cs.nets.beamer.util.ZKFormat;
import ro.pub.cs.nets.beamer.util.ZooUtil;
import org.apache.zookeeper.KeeperException;

public class DIPMap implements ZKAbstraction
{
	protected static final String ROOT_BASE      = "/beamer/";
	protected static final String LATEST_BLOB    = "latest_blob";
	protected static final String LATEST_GEN     = "latest_gen";
	protected static final String GEN_BASE       = "gen";
	protected static final String BLOB_PART_BASE = "blob";
	
	protected final SanitizedZooCache zc;
	protected String root;
	protected Ring ring;
	protected int latestBlob;
	protected int latestGen;
	protected boolean history;
	
	RingLog assignments = new RingLog();
	
	public DIPMap(SanitizedZooCache zc, String name, int ringSize, boolean history)
	{
		this.zc = zc;
		ring = new Ring(ringSize);
		root = ROOT_BASE + name + "/";
		this.history = history;
	}
	
	@Override
	public void bootstrap() throws KeeperException, InterruptedException
	{
		latestBlob = -1;
		latestGen = -1;
		
		ZooUtil.create(zc, root);
		ZooUtil.create(zc, root + "latest_blob");
		ZooUtil.create(zc, root + "latest_gen");
		
		publishGen();
		publishBlob();
	}
	
	public void rebuild(Ring ring, int gen) throws KeeperException, InterruptedException
	{
		latestBlob = gen - 1;
		latestGen = gen - 1;
		 
		ZooUtil.create(zc, root);
		ZooUtil.create(zc, root + "latest_blob");
		ZooUtil.create(zc, root + "latest_gen");
		
		this.ring = ring;
		
		publishEmptyGen();
		publishBlob();
	}
	
	@Override
	public void update() throws KeeperException, InterruptedException
	{
		latestBlob = ZKFormat.deserializeInt(zc.getData(root + LATEST_BLOB));
		latestGen = ZKFormat.deserializeInt(zc.getData(root + LATEST_GEN));
		
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		
		try
		{
			stream.write(ZooUtil.getHuge(zc, root + GEN_BASE + "_" + latestBlob + "/" + BLOB_PART_BASE));
		}
		catch (IOException ex)
		{
			Logger.getLogger(DIPMap.class.getName()).log(Level.SEVERE, null, ex);
			System.exit(1);
		}
		ZKFormat.deserializeRing(stream.toByteArray(), ring, history);
		for (int i = latestBlob + 1; i <= latestGen; i++)
		{
			ZKFormat.deserializeRingLog(ZooUtil.getHuge(zc, root + GEN_BASE + "_" + i + "/log"), ring, history);
		}
	}
	
	public void assign(int bucket, Inet4Address dip)
	{
		if (!Arrays.equals(ring.contents[bucket].getDip().getAddress(), dip.getAddress()))
			assignments.assign(bucket, dip);
	}
	
	public void publishGen() throws KeeperException, InterruptedException
	{
		if (assignments.isEmpty() && latestGen >= 0)
			return;
		
		assignments.setTS(getCurrentTS());
		ring.apply(assignments);
		
		byte [] serializedAssignments = ZKFormat.serialize(assignments, history);
		
		latestGen++;
		ZooUtil.createOrSet(zc, root + GEN_BASE + "_" + latestGen, new byte[0]);
		ZooUtil.createOrSetHuge(zc, root + GEN_BASE + "_" + latestGen + "/log", serializedAssignments);
		
		zc.setData(root + LATEST_GEN, ZKFormat.serialize(latestGen));
		
		assignments = new RingLog();
	}
	
	public void publishEmptyGen() throws KeeperException, InterruptedException
	{
		latestGen++;
		ZooUtil.createOrSet(zc, root + GEN_BASE + "_" + latestGen, ZKFormat.compress(new byte[0]));
		ZooUtil.createOrSetHuge(zc, root + GEN_BASE + "_" + latestGen + "/log", new byte[0]);
		
		zc.setData(root + LATEST_GEN, ZKFormat.serialize(latestGen));
	}
	
	public void publishBlob() throws KeeperException, InterruptedException
	{
		if (latestBlob == latestGen)
			return;
		
		latestBlob = latestGen;
		
		ZooUtil.createOrSetHuge(zc, root + GEN_BASE + "_" + latestBlob + "/" + BLOB_PART_BASE, ZKFormat.serialize(ring, history));
		
		zc.setData(root + LATEST_BLOB, ZKFormat.serialize(latestBlob));
	}
	
	@Override
	public void flush() throws KeeperException, InterruptedException
	{
		publishGen();
		//publishBlob();
	}

	public Ring getRing()
	{
		return ring;
	}
	
	public static int getCurrentTS()
	{
		return (int)(new Date().getTime() / 1000);
	}

	public RingLog getAssignments()
	{
		return assignments;
	}
}
