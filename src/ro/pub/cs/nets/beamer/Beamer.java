package ro.pub.cs.nets.beamer;

import java.net.Inet4Address;
import jline.ConsoleReader;
import ro.pub.cs.nets.beamer.cli.CLI;
import ro.pub.cs.nets.beamer.experimental.MuxRingCLI;
import ro.pub.cs.nets.beamer.cli.CLIServer;

public class Beamer
{	
	private static void usage()
	{
		System.err.println("arguments:" + "\n" +
			"\t" + "bootstrap <zookeeper connect string> <VIP> <ring size>" + "\n" +
			"\t\t" + "or" + "\n" +
			"\t" + "rebuild <zookeeper connect string> <VIP> <ring size> <hash table dump> <id table dump>" + "\n" +
			"\t\t" + "or" + "\n" +
			"\t" + "cli <zookeeper connect string>" + "\n" +
			"\t\t" + "or" + "\n" +
			"\t" + "cli_server <zookeeper connect string> <server port>" + "\n" +
			"\t\t" + "or" + "\n" +
			"\t" + "muxring_cli <zookeeper connect string>" + "\n"
		);
		System.exit(1);
	}
	
	/**
	 * @param args the command line arguments
	 * @throws java.io.IOException
	 */
	public static void main(String[] args) throws Exception
	{
		if (args.length <= 1)
			usage();
		
		switch (args[0])
		{
		case "bootstrap":
			if (args.length != 4)
				usage();
			
			Instance.bootstrap(args[1], (Inet4Address)Inet4Address.getByName(args[2]), Integer.parseInt(args[3]));
			break;
			
		case "rebuild":
			if (args.length != 6)
				usage();
			
			Instance.rebuild(args[1], (Inet4Address)Inet4Address.getByName(args[2]), Integer.parseInt(args[3]), args[5], args[6]);
			break;
			
		case "cli":
			if (args.length != 2)
				usage();
			
			ConsoleReader reader = new ConsoleReader();
			reader.setBellEnabled(false);
			//reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));
			CLI cli = new CLI(new Instance(args[1]), reader, System.out, System.err);
			cli.run();
			break;
			
		case "cli_server":
			if (args.length != 3)
				usage();
			
			CLIServer cliSrv = new CLIServer(new Instance(args[1]), Integer.parseInt(args[2]));
			cliSrv.run();
			break;
			
		/* EXPERIMENTAL */
		case "muxring_cli":
			if (args.length != 2)
				usage();
			
			MuxRingCLI muxRingCLI = new MuxRingCLI(new Instance(args[1]));
			muxRingCLI.run();
			break;

		default:
			usage();
			
			break;
		}
	}
	
}
