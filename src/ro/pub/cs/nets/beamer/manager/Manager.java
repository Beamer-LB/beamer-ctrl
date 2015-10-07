package ro.pub.cs.nets.beamer.manager;

import java.io.FileInputStream;
import java.net.Inet4Address;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import ro.pub.cs.nets.beamer.manager.dipmap.DIPMap;
import ro.pub.cs.nets.beamer.manager.dipmap.Ring;
import ro.pub.cs.nets.beamer.util.InetUtil;
import ro.pub.cs.nets.beamer.util.DIPInfo;
import ro.pub.cs.nets.beamer.util.BeamerException;
import ro.pub.cs.nets.beamer.util.ZKFormat;
import ro.pub.cs.nets.beamer.util.SanitizedZooCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public class Manager implements ZKAbstraction
{
	protected class DIPInfoManager implements ZKAbstraction
	{
		public static final String DIP_ROOT = "/beamer/mgr_dips/";
		
		protected HashMap<Inet4Address, DIPInfo> infos = new HashMap<>();
		protected boolean dirty = false;
		
		public DIPInfo getDIPInfo(Inet4Address addr)
		{
			return infos.get(addr);
		}
		
		public void setDIPInfo(Inet4Address addr, DIPInfo info) throws KeeperException, InterruptedException
		{
			infos.put(addr, info);
			dirty = true;
		}
		
		public void deleteDIPInfo(Inet4Address addr) throws KeeperException, InterruptedException
		{
			infos.remove(addr);
			dirty = true;
		}
		
		public Set<Inet4Address> getDIPs()
		{
			return infos.keySet();
		}
		
		@Override
		public void bootstrap() throws KeeperException, InterruptedException
		{
			zc.create(DIP_ROOT, new byte[0], CreateMode.PERSISTENT);
			setDIPInfo(InetUtil.quadToAddr("0.0.0.0"), new DIPInfo(0, 0, true));
			flush();
		}

		@Override
		public void update() throws KeeperException, InterruptedException
		{
			infos = ZKFormat.deserializeDIPInfos(ZKFormat.decompress(zc.getData(DIP_ROOT)));
		}

		@Override
		public void flush() throws KeeperException, InterruptedException
		{
			if (!dirty)
				return;
			
			zc.setData(DIP_ROOT, ZKFormat.compress(ZKFormat.serialize(infos)));
			dirty = false;
		}
	}
	
	protected final SanitizedZooCache zc;
	protected final int ringSize;
	
	protected HashMap<Inet4Address, DIP> dips = new HashMap<>();
	protected List<DIP> unviables = new LinkedList<>();
	
	protected DIPInfoManager dipInfoManager;
	protected DIPMap muxRingManager;
	protected DIPMap muxIDManager;
	
	protected Rebalancer rebalancer = new Rebalancer(this);
	protected Defragmenter defragmenter = new Defragmenter(this);

	public Manager(SanitizedZooCache zc, int ringSize)
	{
		this.zc = zc;
		this.ringSize = ringSize;
		
		dipInfoManager = new DIPInfoManager();
		muxRingManager = new DIPMap(zc, "mux_ring", ringSize, true);
		muxIDManager =   new DIPMap(zc, "id",       1 << 16,  false);
	}
	
	@Override
	public void bootstrap() throws KeeperException, InterruptedException
	{
		dipInfoManager.bootstrap();
		muxRingManager.bootstrap();
		muxIDManager.bootstrap();
	}
	
	static protected int getInt(FileInputStream stream) throws Exception
	{
		byte[] bytes = new byte[4];
		
		if (stream.read(bytes) < 4)
			throw new Exception("Unexpected end of stream");
		return ZKFormat.deserializeInt(bytes);
	}
	
	static protected Inet4Address getIP(FileInputStream stream) throws Exception
	{
		byte[] bytes = new byte[4];
		
		if (stream.read(bytes) < 4)
			throw new Exception("Unexpected end of stream");
		return InetUtil.quadToAddr(bytes);
	}

	public int getRingSize()
	{
		return ringSize;
	}
	
	protected class HashEntry implements Comparable<HashEntry>
	{
		protected int bucket;
		protected Inet4Address dip;
		protected Inet4Address pdip;
		protected int timestamp;

		public HashEntry(int bucket, Inet4Address dip, Inet4Address pdip, int timestamp)
		{
			this.bucket = bucket;
			this.dip = dip;
			this.pdip = pdip;
			this.timestamp = timestamp;
		}
		
		@Override
		public int compareTo(HashEntry other)
		{
			return this.timestamp - other.timestamp;
		}
		
	}
	
	public void rebuild(int ringSize, FileInputStream hashDump, FileInputStream idDump) throws KeeperException, InterruptedException, Exception
	{
		final int DEFAULT_WEIGHT = 1;
		final int GEN_OFFSET = 1000;
		
		HashMap<Inet4Address, List<Integer>> assignments = new HashMap<>();
		assignments.put(InetUtil.QUAD_ZERO, new LinkedList<>());
		
		dipInfoManager.bootstrap();
		
		int idGen = getInt(idDump);
		int idCount = getInt(idDump);
		if (idCount != 0x10000)
		{
			Logger.getLogger(Manager.class.getName()).log(Level.SEVERE, null,
				new Exception("Bad id count: " + idCount));
			System.exit(1);
		}
		Ring idRing = new Ring(idCount);
		for (int i = 0; i < idCount; i++)
		{
			Inet4Address ip = getIP(idDump);
			
			if (ip.equals(InetUtil.QUAD_ZERO))
				continue;
			
			assignments.put(ip, new LinkedList<>());
			dipInfoManager.setDIPInfo(ip, new DIPInfo(i, DEFAULT_WEIGHT, true));
			
			idRing.assign(i, ip, 0);
		}
		muxIDManager.rebuild(idRing, idGen + 1000);
		
		int hashGen = getInt(hashDump);
		int hashCount = getInt(hashDump);
		if (hashCount != ringSize)
		{
			Logger.getLogger(Manager.class.getName()).log(Level.SEVERE, null,
				new Exception("Bad bucket count: " + idCount));
			System.exit(1);
		}
		Ring hashRing = new Ring(hashCount);
		for (int i = 0; i < hashCount; i++)
		{
			Inet4Address dip = getIP(hashDump);
			Inet4Address pdip = getIP(hashDump);
			int timestamp = getInt(hashDump);
			
			//System.out.println("entry :" + i + " " + dip + " " + pdip + " " + timestamp);
			
			if (!assignments.keySet().contains(dip))
			{
				// TODO: maybe create dip with weight 1 and ID 0 ?
				//System.out.println("orphaning because no parent:" + i + " " + dip);
				dip = InetUtil.QUAD_ZERO;
			}
			
			hashRing.assign(i, dip, pdip, timestamp);
		}
		muxRingManager.rebuild(hashRing, hashGen + GEN_OFFSET);
	}

	@Override
	public void update() throws KeeperException, InterruptedException
	{
		muxRingManager.update();
		muxIDManager.update();
		dipInfoManager.update();
		
		for (Inet4Address addr: dipInfoManager.getDIPs())
		{
			DIPInfo dipInfo = dipInfoManager.getDIPInfo(addr);
			
			DIP dip = new DIP(addr, dipInfo.getID(), dipInfo.getWeight(), dipInfo.getViable());
			
			muxIDManager.assign(dipInfo.getID(), addr);
			
			dips.put(addr, dip);
			if (!dip.isViable())
				unviables.add(dip);
		}
		
		TreeSet<Ring.Entry> entries = new TreeSet<>(new Comparator<Ring.Entry>() {
			@Override
			public int compare(Ring.Entry b1, Ring.Entry b2)
			{
				if (b1.getTS() != b2.getTS())
					return b1.getTS() - b2.getTS();
				return 1;
			}
			
		});
		for (int i = 0; i < muxRingManager.getRing().size(); i++)
		{
			Ring.Entry entry = muxRingManager.getRing().get(i);
			entries.add(entry);
		}
		for (Ring.Entry entry: entries)
			dips.get(entry.getDip()).getBuckets().add(entry.getIndex());
		
		/* kill killable unviables */
		LinkedList<DIP> unkillableUnviables = new LinkedList<>();
		for (DIP unviable: unviables)
		{
			if (unviable.getBuckets().isEmpty())
			{
				dips.remove(unviable.getAddr());
				dipInfoManager.deleteDIPInfo(unviable.getAddr());
			}
			else
			{
				unkillableUnviables.add(unviable);
			}
		}
		unviables = unkillableUnviables;
		//dipInfoManager.flush();
	}
	
	protected void killUnviableDIPs() throws KeeperException, InterruptedException
	{
		for (DIP unviable: unviables)
		{
			dips.remove(unviable.getAddr());
			dipInfoManager.deleteDIPInfo(unviable.getAddr());
		}
		
		unviables = new LinkedList<>();
	}
	
	@Override
	public void flush() throws KeeperException, InterruptedException
	{
		dipInfoManager.flush();
		muxIDManager.flush();
		rebalancer.rebalance();
		killUnviableDIPs();
		muxRingManager.flush();
	}
	
	public int defragment(int maxChurn, int minAge) throws KeeperException, InterruptedException, BeamerException
	{
		dipInfoManager.flush();
		muxIDManager.flush();
		muxRingManager.flush();

		defragmenter.defragment(maxChurn, minAge);
		int reassignments = muxRingManager.getAssignments().size();
		
		muxRingManager.flush();
		
		return reassignments;
	}
	
	public void addDIP(Inet4Address addr, int id, int weight) throws KeeperException, InterruptedException
	{
		DIP dip = new DIP(addr, id, weight, true);
		
		dips.put(addr, dip);
		
		dipInfoManager.setDIPInfo(addr, new DIPInfo(id, weight, true));
		muxIDManager.assign(id, addr);
		
		defragmenter.setDirty(true);
	}
	
	public void setDIPWeight(Inet4Address addr, int weight) throws KeeperException, InterruptedException
	{
		DIP dip = dips.get(addr);
		
		dip.setWeight(weight);
		dipInfoManager.setDIPInfo(addr, new DIPInfo(dip.getID(), weight, dip.isViable()));
		
		defragmenter.setDirty(true);
	}
	
	public void setDIPUnviable(Inet4Address addr) throws KeeperException, InterruptedException
	{
		DIP dip = dips.get(addr);
		
		dip.setWeight(0);
		dip.setViable(false);
		unviables.add(dip);
		dipInfoManager.setDIPInfo(addr, new DIPInfo(dip.getID(), 0, false));
		
		defragmenter.setDirty(true);
	}

	public HashMap<Inet4Address, DIP> getDIPs()
	{
		return dips;
	}
	
	public DIPMap getMuxRingManager()
	{
		return muxRingManager;
	}

	public DIPMap getMuxIDManager()
	{
		return muxIDManager;
	}
}
