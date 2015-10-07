package ro.pub.cs.nets.beamer.util;

import java.util.Comparator;

public class StackedComparator<T> implements Comparator<T>
{
	protected Comparator<T> c1;
	protected Comparator<T> c2;

	public StackedComparator(Comparator<T> c1, Comparator<T> c2)
	{
		this.c1 = c1;
		this.c2 = c2;
	}
	
	@Override
	public int compare(T a, T b)
	{
		int ret = c1.compare(a, b);
		if (ret != 0)
			return ret;
		return c2.compare(a, b);
	}
}
