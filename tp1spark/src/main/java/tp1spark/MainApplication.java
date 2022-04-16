package tp1spark;

import java.io.IOException;

public class MainApplication {
	public static void main(String[] args) throws IOException {
		//Utils.stopSparkLogs(Level.ALL);
		//Exercice1.runExo();
		Exercice1TotalVentes.runExo1(args[0], args[1]);
		//Exercice1TotalVentes.runExo2(args[0], args[1], null);


	}
}
