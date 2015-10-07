package ro.pub.cs.nets.beamer.util;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InetUtil
{
	public static final Inet4Address QUAD_ZERO = quadToAddr("0.0.0.0");
	
	public static Inet4Address quadToAddr(String quad)
	{
		try
		{
			return (Inet4Address)Inet4Address.getByName(quad);
		}
		catch (UnknownHostException ex)
		{
			Logger.getLogger(InetUtil.class.getName()).log(Level.SEVERE, null, ex);
			System.exit(1);
		}
		
		return null;
	}
	
	public static Inet4Address quadToAddr(byte quad[])
	{
		try
		{
			return (Inet4Address)Inet4Address.getByAddress(quad);
		}
		catch (UnknownHostException ex)
		{
			Logger.getLogger(InetUtil.class.getName()).log(Level.SEVERE, null, ex);
			System.exit(1);
		}
		
		return null;
	}
}
