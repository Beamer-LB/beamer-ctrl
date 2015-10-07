package ro.pub.cs.nets.beamer.manager.dipmap;

import java.net.Inet4Address;
import java.util.HashMap;
import java.util.LinkedList;
import ro.pub.cs.nets.beamer.util.InetUtil;

public class Ring
{
	public static class Entry
	{
		protected int index;
		
		protected Inet4Address dip;
		protected Inet4Address prevDip;
		protected int ts;

		public Entry(int index)
		{
			this.index = index;
			
			dip = InetUtil.QUAD_ZERO;
			prevDip = InetUtil.QUAD_ZERO;
			ts = 0;
		}

		public int getIndex()
		{
			return index;
		}
		
		public Inet4Address getDip()
		{
			return dip;
		}

		public Inet4Address getPrevDip()
		{
			return prevDip;
		}

		public int getTS()
		{
			return ts;
		}

		@Override
		public String toString()
		{
			return "" + getDip() + "\t" + getPrevDip() + "\t" + ts;
		}
	}
	
	protected final Entry contents[];
		
	public Ring(int size)
	{
		contents = new Entry[size];
		for (int i = 0; i < size; i++)
			contents[i] = new Entry(i);
	}
	
	public int size()
	{
		return contents.length;
	}
	
	public Entry get(int bucket)
	{
		return contents[bucket];
	}
	
	public void assign(int bucket, Inet4Address dip, int ts)
	{
		contents[bucket].prevDip = contents[bucket].dip;
		contents[bucket].dip = dip;
		contents[bucket].ts = ts;
	}
	
	public void assign(int bucket, Inet4Address dip, Inet4Address prevDip, int ts)
	{
		contents[bucket].prevDip = prevDip;
		contents[bucket].dip = dip;
		contents[bucket].ts = ts;
	}
	
	public void apply(RingLog log)
	{
		HashMap<Inet4Address, LinkedList<Integer>> map = log.getDipToBucketsMap();
		
		for (Inet4Address dip: map.keySet())
		{
			for (int bucket: map.get(dip))
			{
				contents[bucket].prevDip = contents[bucket].dip;
				contents[bucket].dip = dip;
				contents[bucket].ts = log.getTS();
			}
		}
	}
}
