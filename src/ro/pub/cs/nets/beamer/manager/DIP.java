package ro.pub.cs.nets.beamer.manager;

import java.net.Inet4Address;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

public class DIP //implements Comparable<DIP>
{
	public static class LoadComparator implements Comparator<DIP>
	{
		@Override
		public int compare(DIP a, DIP b)
		{
			float aLoad = a.getLoad();
			float bLoad = b.getLoad();

			if (aLoad < bLoad)
				return -1;
			if (aLoad > bLoad)
				return 1;
			return 0;
		}
	}
	
	public static class OrdinalComparator implements Comparator<DIP>
	{
		@Override
		public int compare(DIP a, DIP b)
		{
			int aOrd = ByteBuffer.wrap(a.addr.getAddress()).order(ByteOrder.BIG_ENDIAN).getInt();
			int bOrd = ByteBuffer.wrap(b.addr.getAddress()).order(ByteOrder.BIG_ENDIAN).getInt();
			
			if (aOrd < bOrd)
				return -1;
			if (aOrd > bOrd)
				return 1;
			return 0;
		}
	}
	
	protected final Inet4Address addr;
	protected final int id;
	protected int weight;
	protected boolean viable;

	protected List<Integer> buckets = new LinkedList<>();

	public DIP(Inet4Address addr, int id, int weight, boolean viable)
	{
		this.addr = addr;
		this.id = id;
		this.weight = weight;
		this.viable = viable;
	}

	public Inet4Address getAddr()
	{
		return addr;
	}
	
	public List<Integer> getBuckets()
	{
		return buckets;
	}
	
	public float getLoad()
	{
		return (float)buckets.size() / weight;
	}
	
	public float simulateAssignment()
	{
		return ((float)buckets.size() + 1) / weight;
	}
	
	public float simulateDeassignment()
	{
		return ((float)buckets.size() - 1) / weight;
	}
	
	public boolean isActive()
	{
		return weight > 0 || buckets.size() > 0;
	}

	public int getWeight()
	{
		return weight;
	}

	public void setWeight(int weight)
	{
		this.weight = weight;
	}

	public int getID()
	{
		return id;
	}
	
	public boolean isViable()
	{
		return viable;
	}

	public void setViable(boolean viable)
	{
		this.viable = viable;
	}

	@Override
	public String toString()
	{
		return "" + " " + addr + " id=" + id + " weight=" + weight + " load=" + getLoad() +  ":" + Arrays.toString(buckets.toArray());
	}
	
	public String toStringShort()
	{
		return "" + " " + addr + " id=" + id + " weight=" + weight + " load=" + getLoad() +  ": buckets=" + buckets.size();
	}
	
//	@Override
//	public int compareTo(DIP other)
//	{
//		return new OrdinalComparator().compare(this, other);
//	}
}
