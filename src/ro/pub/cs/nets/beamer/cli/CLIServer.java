package ro.pub.cs.nets.beamer.cli;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;
import jline.ConsoleReader;
import ro.pub.cs.nets.beamer.Instance;

public class CLIServer implements Runnable
{
	protected Instance manager;
	protected int port;

	public CLIServer(Instance manager, int port)
	{
		this.manager = manager;
		this.port = port;
	}
	
	@Override
	public void run()
	{
		ServerSocket serverSocket;
		try
		{
			serverSocket = new ServerSocket(port);
		}
		catch (IOException ex)
		{
			Logger.getLogger(CLIServer.class.getName()).log(Level.SEVERE, null, ex);
			return;
		}
		while (true)
		{
			try
			{
				Socket connectionSocket = serverSocket.accept();
				
				ConsoleReader reader = new ConsoleReader(connectionSocket.getInputStream(),
					new OutputStreamWriter(connectionSocket.getOutputStream()));
				
				PrintStream printStream = new PrintStream(connectionSocket.getOutputStream());
				
				CLI cli = new CLI(manager, reader, printStream, printStream);
				(new Thread(new Runnable()
				{
					@Override
					public void run()
					{
						cli.run();
						try
						{
							connectionSocket.close();
						}
						catch (IOException ex)
						{
							Logger.getLogger(CLIServer.class.getName()).log(Level.SEVERE, null, ex);
						}
					}
				})).start();
			}
			catch (IOException ex)
			{
				Logger.getLogger(CLIServer.class.getName()).log(Level.SEVERE, null, ex);
			}
		}

	}
	
}
