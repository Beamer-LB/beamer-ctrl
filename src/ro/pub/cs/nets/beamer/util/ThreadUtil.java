package ro.pub.cs.nets.beamer.util;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ThreadUtil
{
	public static void forceJoin(Thread thread)
	{
		boolean done = false;
		while (!done)
		{
			try
			{
				thread.join();
				done = true;
			}
			catch (InterruptedException ex)
			{
				Logger.getLogger(ThreadUtil.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}
}
