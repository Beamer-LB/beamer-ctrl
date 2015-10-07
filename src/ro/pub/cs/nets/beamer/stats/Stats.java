package ro.pub.cs.nets.beamer.stats;

import java.net.Inet4Address;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import ro.pub.cs.nets.beamer.manager.DIP;
import ro.pub.cs.nets.beamer.manager.Manager;
import ro.pub.cs.nets.beamer.manager.dipmap.DIPMap;

public class Stats
{
	protected Manager manager;

	public Stats(Manager manager)
	{
		this.manager = manager;
	}

	public int contiguousBuckets()
	{
		DIPMap muxRingManager = manager.getMuxRingManager();
		HashMap <Inet4Address, DIP> dips = manager.getDIPs();
		
		int ranges = 0;
		for (DIP dip: dips.values())
		{
			TreeSet<Integer> buckets = new TreeSet<>(dip.getBuckets());
			if (buckets.isEmpty())
				continue;
			
			Iterator<Integer> it = buckets.iterator();
			int prevBucket = it.next();
			ranges++;
			while (it.hasNext())
			{
				int bucket = it.next();
				if (bucket != prevBucket + 1)
					ranges++;
				prevBucket = bucket;
			}
		}
		return ranges;
	}
}
