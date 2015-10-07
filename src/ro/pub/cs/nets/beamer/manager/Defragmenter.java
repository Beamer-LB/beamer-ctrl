package ro.pub.cs.nets.beamer.manager;

import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.zookeeper.KeeperException;
import ro.pub.cs.nets.beamer.manager.dipmap.DIPMap;
import ro.pub.cs.nets.beamer.util.BeamerException;
import ro.pub.cs.nets.beamer.util.InetUtil;

public class Defragmenter
{
	protected class DisplacedDIPSet
	{
		/* quota -> missing -> dips */
		protected TreeMap<Integer, TreeMap<Integer, TreeSet<DIP>>> qmdMap = new TreeMap<>();
		
		/* dip -> missing */
		protected LinkedHashMap<DIP, Integer> dmMap = new LinkedHashMap<>();
		
		public void put(DIP dip, int missing)
		{
			int quota = dip.getBuckets().size();
			int prevMissing = dmMap.getOrDefault(dip, 0);
			
			if (prevMissing != 0)
			{
				if (missing == 0)
					dmMap.remove(dip);
				qmdMap.get(quota).get(prevMissing).remove(dip);
			}
			
			if (missing != 0)
			{
				dmMap.put(dip, missing);
				
				TreeMap<Integer, TreeSet<DIP>> mdMap;
				if (!qmdMap.containsKey(quota))
				{
					mdMap = new TreeMap<>();
					qmdMap.put(quota, mdMap);
				}
				else
				{
					mdMap = qmdMap.get(quota);
				}
				
				TreeSet<DIP> d;
				if (!mdMap.containsKey(missing))
				{
					d = new TreeSet<>(new DIP.OrdinalComparator());
					mdMap.put(missing, d);
				}
				else
				{
					d = mdMap.get(missing);
				}
				d.add(dip);
			}
		}
		
		public void remove(DIP dip)
		{
			put(dip, 0);
		}
		
		public int get(DIP dip)
		{
			return dmMap.getOrDefault(dip, 0);
		}
		
		public LinkedHashSet<DIP> extraCandidates(Set<DIP> established)
		{
			LinkedHashSet<DIP> ret = new LinkedHashSet<>();
			
			q_loop: for (int quota: qmdMap.keySet())
			{
				TreeMap<Integer, TreeSet<DIP>> mdMap = qmdMap.get(quota);
				m_loop: for (int missing: mdMap.keySet())
				{
					TreeSet<DIP> d = mdMap.get(missing);
					d_loop: for (DIP dip: d)
					{
						if (established.contains(dip))
							continue;
						
						/* found best DIP for quota */
						ret.add(dip);
						continue q_loop;
					}
				}
			}
			
			return ret;
		}

		private boolean containsKey(DIP dip)
		{
			return dmMap.containsKey(dip);
		}
		
		public int size()
		{
			return dmMap.size();
		}
	}
	
	protected Manager owner;
	boolean dirty = true;
	
//	boolean fragmented;
	int maxQuota;
	ArrayList<DIP> shadowRing;
	LinkedHashMap<DIP, Integer> reassignmentCount;

	public Defragmenter(Manager owner)
	{
		this.owner = owner;
	}

	private void computeProposal()
	{
		int ringSize = owner.getRingSize();
		HashMap<Inet4Address, DIP> dips = owner.getDIPs();
		DIPMap muxRingManager = owner.getMuxRingManager();
		
//		fragmented = false;
		
		int base = 0;
		maxQuota = 0;
		
		reassignmentCount = new LinkedHashMap<>();
		for (DIP dip: dips.values())
			reassignmentCount.put(dip, 0);

		shadowRing = new ArrayList<>(ringSize);
		for (int i = 0; i < ringSize; i++)
			shadowRing.add(dips.get(muxRingManager.getRing().get(i).getDip()));

		DisplacedDIPSet missing = new DisplacedDIPSet();
		for (DIP dip: dips.values())
		{
			int bucketCount = dip.getBuckets().size();
			if (bucketCount > maxQuota)
				maxQuota = bucketCount;
		}

		while (base < ringSize)
		{
			System.out.println("base " + base);
			/* get candidates */
			LinkedHashSet<DIP> candidates = new LinkedHashSet<>();
			for (int i = base; i < ringSize && i - base < maxQuota; i++)
			{
				if (shadowRing.get(i) == null)
					continue;
				candidates.add(shadowRing.get(i));
			}
			System.out.println("candidates " + candidates.size() + " missing " + missing.size());
			candidates.addAll(missing.extraCandidates(candidates));
			System.out.println("final candidates " + candidates.size());

			/* find min cost */
			int minCost = Integer.MAX_VALUE;
			DIP winner = null;
			for (DIP candidate: candidates)
			{
				int ownedSlots = 0;
				int freeSlots = 0;
				int displacedSlots = 0;
				int quota = candidate.getBuckets().size();
				for (int i = base; i < base + quota; i++)
				{
					DIP displacee = shadowRing.get(i);
					if (displacee == candidate)
						ownedSlots++;
					else if (displacee == null)
						freeSlots++;
					else
						displacedSlots++;
				}
				int cost = displacedSlots + //eviction cost
					quota - ownedSlots - missing.get(candidate); //costs of moving in
				if (cost < minCost)
				{
					minCost = cost;
					winner = candidate;
				}
			}
			
			System.out.println("winner " + winner.toStringShort());

			/* orphan everything */
			int quota = winner.getBuckets().size();
			for (int bucket: winner.getBuckets())
			{
				if (bucket < base + quota)
					continue;

				shadowRing.set(bucket, null);
			}

			/* reassign everything */
			for (int i = base; i < base + quota; i++)
			{
				DIP source = shadowRing.get(i);
				if (source == winner)
					continue;

				if (source != null)
				{
					if (missing.containsKey(source))
						missing.put(source, missing.get(source) + 1);
					else
						missing.put(source, 1);
				}
				shadowRing.set(i, winner);
				reassignmentCount.put(winner, reassignmentCount.get(winner) + 1);
//				fragmented = true;
			}

			missing.remove(winner);

			base += quota;
		}
		
		dirty = false;
	}
	
	public void defragment(int maxChurn, int minAge) throws KeeperException, InterruptedException, BeamerException
	{
		int ringSize = owner.getRingSize();
		HashMap<Inet4Address, DIP> dips = owner.getDIPs();
		DIPMap muxRingManager = owner.getMuxRingManager();

		if (maxChurn <= 0)
			maxChurn = Integer.MAX_VALUE;

		int maxTS = minAge > 0 ? DIPMap.getCurrentTS() - minAge : Integer.MAX_VALUE;
		
		DIP zero = dips.get(InetUtil.QUAD_ZERO);
		if (zero.getBuckets().size() > 0)
			throw new BeamerException("Can't defrag when 0.0.0.0 has buckets");
		
		if (dirty)
			computeProposal();

//		if (!fragmented)
//			throw new BeamerException("Ring is not fragmented");

		if (maxChurn >= maxQuota && maxTS == Integer.MAX_VALUE)
		{
			/* dumb reassignment */
			for (int i = 0; i < ringSize; i++)
			{
				DIP src = dips.get(muxRingManager.getRing().get(i).getDip());
				DIP dst = shadowRing.get(i);
				if (src == dst)
					continue;

				src.getBuckets().remove((Integer)i);
				dst.getBuckets().add(i);
				//System.out.println("gave " + i + " from " + src.getAddr() + " to " + dst.getAddr());
				muxRingManager.assign(i, dst.getAddr());
			}
		}
		else
		{
			/* smart reassignment */
			LinkedHashMap<DIP, Integer> reassignmentsLeft = new LinkedHashMap<>();
			for (DIP dip: dips.values())
			{
				int credits = Math.min(reassignmentCount.get(dip), maxChurn);
				if (credits == 0)
					continue;

				reassignmentsLeft.put(dip, credits);
			}

			while (!reassignmentsLeft.isEmpty())
			{
				LinkedHashSet<DIP> visitedDIPs = new LinkedHashSet<>();
				defragVisit(reassignmentsLeft.keySet().iterator().next(), visitedDIPs, reassignmentsLeft, shadowRing, maxTS);
			}
		}
	}

	private DIP defragVisit(DIP dip, LinkedHashSet<DIP> visitedDIPs, HashMap<DIP, Integer> reassignmentsLeft, ArrayList<DIP> shadowRing, int maxTS)
	{
		DIPMap muxRingManager = owner.getMuxRingManager();

		int credits = reassignmentsLeft.getOrDefault(dip, 0);

		if (visitedDIPs.contains(dip))
			return dip;

		List<Integer> buckets = new LinkedList<>(dip.getBuckets());
		for (int bucket: buckets)
		{
			if (credits == 0)
				break;

			if (muxRingManager.getRing().get(bucket).getTS() > maxTS)
				break;

			DIP next = shadowRing.get(bucket);
			if (next == dip)
				continue;

			visitedDIPs.add(dip);
			DIP looper = defragVisit(next, visitedDIPs, reassignmentsLeft, shadowRing, maxTS);
			visitedDIPs.remove(dip);

			if (looper == null)
				continue;

			dip.getBuckets().remove((Integer)bucket);
			next.getBuckets().add(bucket);
			muxRingManager.assign(bucket, next.getAddr());

			//System.out.println("gave " + bucket + " from " + dip.getAddr() + " to " + next.getAddr());

			credits--;

			if (looper == dip)
				continue;

			if (credits > 0)
				reassignmentsLeft.put(dip, credits);
			else
				reassignmentsLeft.remove(dip);
			return looper;
		}

		/* dead end */
		reassignmentsLeft.remove(dip);
		return null;
	}
	
	public void setDirty(boolean dirty)
	{
		this.dirty = dirty;
	}

	public boolean isDirty()
	{
		return dirty;
	}
}
