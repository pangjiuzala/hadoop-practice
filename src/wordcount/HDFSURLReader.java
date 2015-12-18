package wordcount;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class HDFSURLReader {
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	public static void main(String[] args) {
      InputStream inputStream =null;
      try {
		inputStream=new URL(args[0]).openStream();
		IOUtils.copyBytes(inputStream,System.out,1024,false);
		} catch (Exception e) {
			IOUtils.closeStream(inputStream);
	}
	}
}
