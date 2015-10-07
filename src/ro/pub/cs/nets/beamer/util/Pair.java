package ro.pub.cs.nets.beamer.util;

import java.util.Objects;

//TODO: delete is unused
public class Pair<T1, T2>
{
	protected T1 first;
	protected T2 second;

	public Pair(T1 first, T2 second)
	{
		this.first = first;
		this.second = second;
	}

	public T1 getFirst()
	{
		return first;
	}

	public T2 getSecond()
	{
		return second;
	}

	@Override
	public boolean equals(Object o)
	{
		if (!o.getClass().equals(this.getClass()))
			return false;
		
		Pair<T1, T2> other = (Pair<T1, T2>)o;
		
		return this.getFirst().equals(other.getFirst()) && this.getSecond().equals(other.getSecond());
	}

	@Override
	public int hashCode()
	{
		int hash = 7;
		hash = 61 * hash + Objects.hashCode(this.first);
		hash = 61 * hash + Objects.hashCode(this.second);
		return hash;
	}
}
