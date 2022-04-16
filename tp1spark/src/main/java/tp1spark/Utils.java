package tp1spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Utils {
	public static void stopSparkLogs(Level logLevel) {
		Logger.getLogger("org.apache.spark").setLevel(logLevel);
		Logger.getLogger("org.spark-project").setLevel(logLevel);
	}
}
