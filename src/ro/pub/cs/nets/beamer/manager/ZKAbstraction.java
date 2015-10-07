package ro.pub.cs.nets.beamer.manager;

import org.apache.zookeeper.KeeperException;

public interface ZKAbstraction
{
	public void bootstrap() throws KeeperException, InterruptedException;
		
	public void update() throws KeeperException, InterruptedException;
	
	public void flush() throws KeeperException, InterruptedException;
}
