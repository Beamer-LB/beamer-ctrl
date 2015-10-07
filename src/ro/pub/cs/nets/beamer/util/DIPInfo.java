package ro.pub.cs.nets.beamer.util;

public class DIPInfo
{
	protected int id;
	protected int weight;
	protected boolean viable;

	public DIPInfo(int id, int weight, boolean viable)
	{
		this.id = id;
		this.weight = weight;
		this.viable = viable;
	}

	public int getID()
	{
		return id;
	}

	public int getWeight()
	{
		return weight;
	}
	
	public boolean getViable()
	{
		return viable;
	}
	
	@Override
	public String toString()
	{
		return id + " " + weight + " " + viable;
	}
}
