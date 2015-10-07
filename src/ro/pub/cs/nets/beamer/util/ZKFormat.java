package ro.pub.cs.nets.beamer.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import ro.pub.cs.nets.beamer.manager.dipmap.Ring;
import ro.pub.cs.nets.beamer.manager.dipmap.RingLog;

public class ZKFormat
{
	public static final int MAX_NODE_SIZE = (int) ((1 << 20) * 0.95); // 0.95MB
	public static final int MAX_DATA_SIZE = 100 * 1024 * 1024;        // 100MB
	
	public static byte[] serialize(int x)
	{
		return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(x).array();
	}
	
	public static byte[] serialize(DIPInfo x)
	{
		return ByteBuffer.allocate(7).order(ByteOrder.LITTLE_ENDIAN)
			.putShort((short)x.getID())
			.putInt(x.getWeight())
			.put(x.getViable() ? (byte)1 : (byte)0)
			.array();
	}
	
	public static byte [] serialize(RingLog log, boolean history)
	{
		HashMap<Inet4Address, LinkedList<Integer>> map = log.getDipToBucketsMap();
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		
		try
		{
			if (history)
				stream.write(ZKFormat.serialize(log.getTS()));

			for (Inet4Address dip: map.keySet())
			{
				LinkedList<Integer> buckets = map.get(dip);
				
				stream.write(dip.getAddress());
				stream.write(ZKFormat.serialize(buckets.size()));
				for (int bucket: buckets)
					stream.write(ZKFormat.serialize(bucket));
			}
		}
		catch (IOException ex)
		{
			Logger.getLogger(ZKFormat.class.getName()).log(Level.SEVERE, null, ex);
			System.exit(1);
		}
		
		return stream.toByteArray();
	}
	
	public static byte [] serialize(Ring ring, boolean history)
	{
		final int ENTRY_SIZE = history ? 12 : 4; 
		
		ByteBuffer buf = ByteBuffer.allocate(ring.size() * ENTRY_SIZE).order(ByteOrder.LITTLE_ENDIAN);

		for (int i = 0; i < ring.size(); i++)
		{
			Ring.Entry entry = ring.get(i);

			buf.put(entry.getDip().getAddress());
			if (history)
			{
				buf.put(entry.getPrevDip().getAddress());
				buf.putInt(entry.getTS());
			}
		}
			
		return buf.array();
	}
	
	public static byte[] serialize(List<Integer> bucketNos)
	{
		ByteBuffer bb = ByteBuffer.allocate(bucketNos.size() * 4).order(ByteOrder.LITTLE_ENDIAN);
		
		for (int bucketNo: bucketNos)
			bb.putInt(bucketNo);
		return bb.array();
	}
	
	public static byte[] serialize(HashMap<Inet4Address, DIPInfo> dipInfos)
	{
		ByteBuffer bb = ByteBuffer.allocate(dipInfos.size() * (4 + 7)).order(ByteOrder.LITTLE_ENDIAN);
		
		for (Inet4Address addr: dipInfos.keySet())
		{
			bb.put(addr.getAddress());
			bb.put(serialize(dipInfos.get(addr)));
		}
		return bb.array();
	}
	
	public static short deserializeShort(byte bytes[])
	{
		return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getShort();
	}
	
	public static int deserializeInt(byte bytes[])
	{
		return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
	}
	
	public static DIPInfo deserializeDIPInfo(byte bytes[])
	{
		ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		
		return new DIPInfo(buf.getInt(), buf.getInt(), buf.get() == 1);
	}
	
	public static void deserializeRing(byte bytes[], Ring dst, boolean history)
	{
		ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		byte dipBytes[] = new byte[4];
		byte prevDipBytes[] = new byte[4];
		
		for (int i = 0; i < dst.size(); i++)
		{
			if (history)
			{
				buf.get(dipBytes);
				buf.get(prevDipBytes);
				
				Inet4Address dip = InetUtil.quadToAddr(dipBytes);
				Inet4Address prevDip = InetUtil.quadToAddr(prevDipBytes);
				int ts = buf.getInt();
				
				dst.assign(i, dip, prevDip, ts);
			}
			else
			{
				buf.get(dipBytes);
				dst.assign(i, InetUtil.quadToAddr(dipBytes), 0);
			}
		}
	}
	
	public static void deserializeRingLog(byte bytes[], Ring dst, boolean history)
	{
		ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		byte dipBytes[] = new byte[4];
		int ts = 0;
		
		if (history)
			ts = buf.getInt();
		
		while (buf.hasRemaining())
		{
			buf.get(dipBytes);
			Inet4Address dip = InetUtil.quadToAddr(dipBytes);

			int count = buf.getInt();

			for (int i = 0; i < count; i++)
			{
				int bucket = buf.getInt();

				dst.assign(bucket, dip, ts);
			}
		}
	}
	
	public static HashMap<Inet4Address, DIPInfo> deserializeDIPInfos(byte bytes[])
	{
		HashMap<Inet4Address, DIPInfo> dipInfos = new HashMap<>();
		ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		byte dipBytes[] = new byte[4];
		
		while (buf.hasRemaining())
		{
			buf.get(dipBytes);
			
			Inet4Address dip = InetUtil.quadToAddr(dipBytes);
			DIPInfo info = new DIPInfo(buf.getShort(), buf.getInt(), buf.get() == 1);
			
			dipInfos.put(dip, info);
		}
		return dipInfos;
	}
	
	public static byte[] compress(byte input[])
	{
		byte[] output = new byte[MAX_DATA_SIZE];
		Deflater compresser = new Deflater(Deflater.BEST_COMPRESSION);
		
		compresser.setInput(input);
		compresser.finish();
		int length = compresser.deflate(output, 0, output.length, Deflater.SYNC_FLUSH);
		compresser.end();
		
		byte[] smallerOutput = new byte[length];
		System.arraycopy(output, 0, smallerOutput, 0, length);
		return smallerOutput;
	}
	
	public static byte[] decompress(byte input[])
	{
		byte[] output = new byte[MAX_DATA_SIZE];
		Inflater decompresser = new Inflater();
		
		decompresser.setInput(input);
		int length = 0;
		try
		{
			length = decompresser.inflate(output);
		}
		catch (DataFormatException ex)
		{
			Logger.getLogger(ZKFormat.class.getName()).log(Level.SEVERE, null, ex);
			System.exit(1);
		}
		decompresser.end();
		
		byte[] smallerOutput = new byte[length];
		System.arraycopy(output, 0, smallerOutput, 0, length);
		return smallerOutput;
	}
}
