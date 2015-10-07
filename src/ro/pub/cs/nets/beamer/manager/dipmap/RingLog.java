package ro.pub.cs.nets.beamer.manager.dipmap;

import java.net.Inet4Address;
import java.util.HashMap;
import java.util.LinkedList;

public class RingLog
{
	//TODO: make resilient to duplicate additions
	protected HashMap<Inet4Address, LinkedList<Integer>> dipToBucketsMap = new HashMap<>();
	protected int count = 0;
	protected int ts = 0;
	
	public void assign(int bucket, Inet4Address dip)
	{
		LinkedList<Integer> buckets = dipToBucketsMap.get(dip);
		if (buckets == null)
		{
			buckets = new LinkedList<>();
			dipToBucketsMap.put(dip, buckets);
		}
		
		buckets.add(bucket);
		
		count++;
	}
	
	public int size()
	{
		return count;
	}
	
	public boolean isEmpty()
	{
		return count == 0;
	}

	public HashMap<Inet4Address, LinkedList<Integer>> getDipToBucketsMap()
	{
		return dipToBucketsMap;
	}

	public void setTS(int ts)
	{
		this.ts = ts;
	}

	public int getTS()
	{
		return ts;
	}
}
