package ro.pub.cs.nets.beamer.cli;

import java.io.IOException;
import java.io.PrintStream;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.SimpleCompletor;
import ro.pub.cs.nets.beamer.manager.DIP;
import ro.pub.cs.nets.beamer.manager.Manager;
import org.apache.zookeeper.KeeperException;
import ro.pub.cs.nets.beamer.Instance;
import ro.pub.cs.nets.beamer.stats.Stats;
import ro.pub.cs.nets.beamer.util.BeamerException;
import ro.pub.cs.nets.beamer.util.InetUtil;

public class CLI implements Runnable
{
	protected Instance instance;
	protected ConsoleReader reader;
	PrintStream outStream;
	PrintStream errorStream;

	public CLI(Instance instance, ConsoleReader reader, PrintStream outStream, PrintStream errorStream) throws IOException
	{
		this.instance = instance;
		this.reader = reader;
		this.outStream = outStream;
		this.errorStream = errorStream;
		
		List completors = new LinkedList();
		completors.add(new SimpleCompletor(new String[] { "add", "remove", "weight", "commit", "dump", "blob", "list", "quit", "exit" }));
		reader.addCompletor(new ArgumentCompletor(completors));
	}

	@Override
	public void run()
	{
		Manager manager = instance.getManager();
		
		try
		{
			String line;
			
			while ((line = reader.readLine("mgr> ")) != null)
			{
				
				String command[] = line.split(" ");
				
				switch (command[0].toLowerCase())
				{
				case "":
					break;
				
				case "add":
					if (command.length != 4)
					{
						errorStream.println("Usage: add <dip> <id> <weight>");
						break;
					}
					synchronized (manager)
					{
						manager.addDIP((Inet4Address)Inet4Address.getByName(command[1]), Integer.parseInt(command[2]), Integer.parseInt(command[3]));
					}
					break;
					
				case "remove":
					if (command.length != 2)
					{
						errorStream.println("Usage: remove <dip>");
						break;
					}
					manager.setDIPUnviable((Inet4Address)Inet4Address.getByName(command[1]));
					break;
					
				case "weight":
					if (command.length != 3)
					{
						errorStream.println("Usage: weight <dip> <weight>");
						break;
					}
					synchronized (manager)
					{
						manager.setDIPWeight((Inet4Address)Inet4Address.getByName(command[1]), Integer.parseInt(command[2]));
					}
					break;
					
				case "commit":
					if (command.length != 1)
					{
						errorStream.println("Usage: commit");
						break;
					}
					synchronized (manager)
					{
						manager.flush();
					}
					break;
					
				case "dump":
				{
					if (command.length != 1)
					{
						errorStream.println("Usage: dump");
						break;
					}
					ArrayList<DIP> dips;
					synchronized (manager)
					{
						dips = new ArrayList<>(manager.getDIPs().values());
					}
					Collections.sort(dips, new DIP.OrdinalComparator());
					
					for (DIP dip: dips)
						outStream.println(dip);
					
					break;
				}
				
				case "list":
				{
					if (command.length != 1)
					{
						errorStream.println("Usage: list");
						break;
					}
					ArrayList<DIP> dips;
					synchronized (manager)
					{
						dips = new ArrayList<>(manager.getDIPs().values());
					}
					Collections.sort(dips, new DIP.OrdinalComparator());
					
					for (DIP dip: dips)
						outStream.println(dip.toStringShort());
					
					break;
				}
				
				case "blob":
				{
					if (command.length != 1)
					{
						errorStream.println("Usage: blob");
						break;
					}
					synchronized (manager)
					{
						manager.getMuxRingManager().publishBlob();
						manager.getMuxIDManager().publishBlob();
					}
					
					break;
				}
				
				case "quit":
					//TODO: usage
					return;
					
				case "exit":
					//TODO: usage
					return;
					
				case "time":
					//TODO: usage
					outStream.println(System.currentTimeMillis());
					break;
					
				case "randomize":
					if (command.length != 2)
					{
						errorStream.println("Usage: randomize <seed>");
						break;
					}
					synchronized (manager)
					{
						DIP zero = manager.getDIPs().get(InetUtil.QUAD_ZERO);
						Collections.shuffle(zero.getBuckets(), new Random(Long.parseLong(command[1])));
					}
					break;
					
				case "defrag":
					if (command.length != 3)
					{
						errorStream.println("Usage: defrag <max churn> <min age>");
						break;
					}
					synchronized (manager)
					{
						try
						{
							int reassigs = manager.defragment(Integer.parseInt(command[1]), Integer.parseInt(command[2]));
							outStream.println("Buckets reassigned: " + reassigs);
						}
						catch (BeamerException ex)
						{
							errorStream.println(ex);
						}
					}
					break;
					
				case "stats":
					//TODO: usage
					int ranges;
					synchronized (manager)
					{
						Stats stats = new Stats(manager);
						ranges = stats.contiguousBuckets();
					}
					outStream.println("Contiguous ranges: " + ranges);
					break;
					
				case "atomic":
					//TODO: usage
					synchronized (manager)
					{
						run();
					}
					break;
				default:
					errorStream.println("Unknown command: " + command[0]);
					break;
				}
			}
		}
		catch (IOException | KeeperException | InterruptedException ex)
		{
			Logger.getLogger(CLI.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
