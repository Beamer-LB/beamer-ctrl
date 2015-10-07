package ro.pub.cs.nets.beamer.util;

public class ZKPathSanitizer
{
	public static String sanitize(String path)
	{
		if (path.equals("/"))
			return path;
		
		while (path.contains("//"))
			path = path.replaceAll("//", "/");
		
		if (path.endsWith("/"))
			path = path.substring(0, path.length() - 1);
		
		return path;
	}
}
