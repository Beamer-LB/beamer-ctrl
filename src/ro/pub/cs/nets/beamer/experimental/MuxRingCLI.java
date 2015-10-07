package ro.pub.cs.nets.beamer.experimental;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.SimpleCompletor;
import ro.pub.cs.nets.beamer.Instance;
import org.apache.zookeeper.KeeperException;

public class MuxRingCLI implements Runnable
{
	protected Instance owner;
	protected ConsoleReader reader;

	public MuxRingCLI(Instance owner) throws IOException
	{
		this.owner = owner;
		
		reader = new ConsoleReader();
		reader.setBellEnabled(false);
		reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));
		List completors = new LinkedList();
		completors.add(new SimpleCompletor(new String[] { "assign", "commit", "quit", "make_blob", "print_ring", "exit" }));
		reader.addCompletor(new ArgumentCompletor(completors));
	}

	@Override
	public void run()
	{
		try
		{
			String line;
			
			while ((line = reader.readLine("muxring> ")) != null)
			{
				
				String command[] = line.split(" ");
				
				switch (command[0].toLowerCase())
				{
				case "":
					break;
					
				case "assign":
					if (command.length != 3)
					{
						System.err.println("Usage: assign <bucket> <dip>");
						break;
					}
					owner.getManager().getMuxRingManager().assign(Integer.parseInt(command[1]), (Inet4Address)(Inet4Address.getByName(command[2])));
					break;
					
				case "commit":
					if (command.length != 1)
					{
						System.err.println("Usage: commit");
						break;
					}
					owner.getManager().getMuxRingManager().publishGen();
					break;
					
				case "make_blob":
					if (command.length != 1)
					{
						System.err.println("Usage: make_blob");
						break;
					}
					owner.getManager().getMuxRingManager().publishBlob();
					break;
					
				case "print_ring":
					if (command.length != 1)
					{
						System.err.println("Usage: print_ring");
						break;
					}
					for (int i = 0; i < owner.getManager().getMuxRingManager().getRing().size(); i++)
						System.out.println(owner.getManager().getMuxRingManager().getRing().get(i));
					break;
				
				case "quit":
				case "exit":
					return;
					
				default:
					System.err.println("Unknown command: " + command[0]);
					break;
				}
			}
		}
		catch (IOException | KeeperException | InterruptedException ex)
		{
			Logger.getLogger(MuxRingCLI.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
