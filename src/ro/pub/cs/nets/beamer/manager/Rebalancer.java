package ro.pub.cs.nets.beamer.manager;

import java.net.Inet4Address;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeSet;
import org.apache.zookeeper.KeeperException;
import ro.pub.cs.nets.beamer.manager.dipmap.DIPMap;
import ro.pub.cs.nets.beamer.util.InetUtil;
import ro.pub.cs.nets.beamer.util.StackedComparator;

public class Rebalancer
{
	protected Manager owner;

	public Rebalancer(Manager owner)
	{
		this.owner = owner;
	}
	
	protected enum Assessment
	{
		OK,
		SRC_DONE,
		DST_DONE,
		BOTH_DONE,
	}

	/* src.weight != 0 and dst is done */
	protected Assessment assessSource(DIP source, float avgLoad)
	{
		if (source.getLoad() <= avgLoad)
			return Assessment.BOTH_DONE;
		if (source.simulateDeassignment() < avgLoad)
			return Assessment.BOTH_DONE;
		return Assessment.DST_DONE;
	}
	
	/* src is done */
	protected Assessment assessDestination(DIP destination, float avgLoad)
	{
		if (destination.getLoad() >= avgLoad)
			return Assessment.BOTH_DONE;
		if (destination.simulateAssignment() > avgLoad)
			return Assessment.BOTH_DONE;
		return Assessment.SRC_DONE;
	}
	
	protected Assessment assessReassignment(DIP source, DIP destination, float avgLoad)
	{
		if (destination.getWeight() == 0)
			throw new RuntimeException("BUG IN CODE: Destination should never have a weight of 0");
		
		if (source.getWeight() == 0)
		{
			if (source.getBuckets().isEmpty())
			{
				if (destination.getLoad() >= avgLoad)
					return Assessment.BOTH_DONE;
				return Assessment.SRC_DONE;
			}
			else
			{
				if (destination.getLoad() >= avgLoad)
					return Assessment.DST_DONE;
				return Assessment.OK;
			}
		}
		
		if (source.getLoad() <= avgLoad)
			return assessDestination(destination, avgLoad);
		
		if (destination.getLoad() >= avgLoad)
			return assessSource(source, avgLoad);

		float gapBefore = Math.abs(source.getLoad() - destination.getLoad());
		float gapAfter = Math.abs(source.simulateDeassignment() - destination.simulateAssignment());

		if (gapAfter < gapBefore)
			return Assessment.OK;
		
		return Assessment.BOTH_DONE;
	}

	public void rebalance() throws KeeperException, InterruptedException
	{
		DIPMap muxRingManager = owner.getMuxRingManager();
		HashMap<Inet4Address, DIP> dips = owner.getDIPs();

		Comparator<DIP> cmp = new StackedComparator<>(new DIP.LoadComparator(), new DIP.OrdinalComparator());

		int totalBuckets = 0;
		int totalWeight = 0;
		for (DIP dip: dips.values())
		{
			totalWeight += dip.getWeight();
			totalBuckets += dip.getBuckets().size();
		}
		
		if (totalBuckets != muxRingManager.getRing().size())
			throw new RuntimeException("Total buckets does not match ring size");

		if (totalWeight == 0)
		{
			zeroTakesItAll();
			return;
		}

		TreeSet<DIP> activeDips = new TreeSet<>(cmp);

		for (DIP dip: dips.values())
		{
			if (dip.getWeight() > 0 || dip.getBuckets().size() > 0)
				activeDips.add(dip);
		}

		while (activeDips.size() > 1)
		{
			DIP destination = activeDips.first();
			DIP source = activeDips.last();

			activeDips.remove(source);
			activeDips.remove(destination);
			
			Assessment assessment = assessReassignment(source, destination, (float)totalBuckets / totalWeight);
			
			if (assessment == Assessment.BOTH_DONE)
				break;

			while((assessment = assessReassignment(source, destination, (float)totalBuckets / totalWeight)) == Assessment.OK)
			{
				int bucket = source.getBuckets().get(0);

				source.getBuckets().remove(0);
				destination.getBuckets().add(bucket);

				muxRingManager.assign(bucket, destination.getAddr());
			}

			if (assessment == Assessment.DST_DONE)
			{
				activeDips.add(source);
			}
			else
			{
				totalBuckets -= source.getBuckets().size();
				totalWeight -= source.getWeight();
			}
			if (assessment == Assessment.SRC_DONE)
			{
				activeDips.add(destination);
			}
			else
			{
				totalBuckets -= destination.getBuckets().size();
				totalWeight -= destination.getWeight();
			}
		}
	}
	
	private void zeroTakesItAll()
	{
		DIPMap muxRingManager = owner.getMuxRingManager();
		HashMap<Inet4Address, DIP> dips = owner.getDIPs();
		DIP zero = dips.get(InetUtil.QUAD_ZERO);
		
		for (DIP dip: dips.values())
		{
			if (dip == zero)
				continue;
			
			while (!dip.getBuckets().isEmpty())
			{
				int bucket = dip.getBuckets().get(0);

				dip.getBuckets().remove(0);
				zero.getBuckets().add(bucket);

				muxRingManager.assign(bucket, zero.getAddr());
			}
		}
	}
}
