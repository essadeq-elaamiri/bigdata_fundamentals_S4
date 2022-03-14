package mini.pro;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ManipulatorTP1 {
	public static void manipulateTP1() {
		System.out.println("=========== TP ===========");
		Configuration config  = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "root");
		//set server connection address & protocol
		config.set("fs.defaultFS", "hdfs://localhost:9000");
		
		FileSystem fileSys = null;
		Path pathFile = null;
		
		
		
		try {
			fileSys = FileSystem.get(config);
			System.out.println("=============== Diretories Creation ===============");

			//create directories 
			createDirectoryIfNotExist(fileSys, new Path("/BDCC"));
			createDirectoryIfNotExist(fileSys, new Path("/BDCC/JAVA"));
			createDirectoryIfNotExist(fileSys, new Path("/BDCC/CPP"));
			createDirectoryIfNotExist(fileSys, new Path("/BDCC/JAVA/TPs"));
			createDirectoryIfNotExist(fileSys, new Path("/BDCC/JAVA/Cours"));
			createDirectoryIfNotExist(fileSys, new Path("/BDCC/CPP/TPs"));
			createDirectoryIfNotExist(fileSys, new Path("/BDCC/CPP/Cours"));
			System.out.println("Directories have been created!");

			
			/*
			 * Créez dans le répertoire Cours de CPP les fichiers CoursCPP1, CoursCPP2 et
				CoursCPP3. Puis ajoutez du contenu dans les fichiers crées.
			 */
			System.out.println("=============== Files Creation ===============");

			
			for(int i=1; i<=3; i++) {
				pathFile = new Path("/BDCC/CPP/Cours/CoursCPP"+i);
				createFileIfNotExist(fileSys, pathFile);
				System.out.println(pathFile.toString()+ " has been created!");
				//append content to file
				writeToFile(fileSys, pathFile, "This is "+pathFile.toString()+"\n");
				 
			}
			
			/*
			4 Affichez le contenu des fichiers CoursCPP1, CoursCPP2 et CoursCPP3.
			 */
			System.out.println("=============== Files Content ===============");
			for(int i=1; i<=3; i++) {
				pathFile = new Path("/BDCC/CPP/Cours/CoursCPP"+i);
				
				System.out.println(pathFile.toString()+" Content:" +readFromFile(fileSys, pathFile));
				 
			}
			
			/*
			5. Copiez les fichiers CoursCPP1, CoursCPP2 et CoursCPP3 dans le répertoire Cours
			de l’JAVA.
			 */
			for(int i=1; i<=3; i++) {
				FileUtil.copy(fileSys, new Path("/BDCC/CPP/Cours/CoursCPP"+i), fileSys, new Path("/BDCC/JAVA/Cours"), false, config);
			}
			
			/*
			6. Supprimer le fichier CoursCPP3 dans le répertoire Cours de l’JAVA, et renommer
			CoursCPP1 et CoursCPP2 par CoursJAVA1 et CoursJAVA2 respectivement.
			 */
			System.out.println("=============== File deleting ===============");
			fileSys.delete(new Path("/BDCC/JAVA/Cours/CoursCPP3"), false);
			System.out.println("file has been deleted !");

			
			System.out.println("=============== Files renaming ===============");
			
			fileSys.rename(new Path("/BDCC/JAVA/Cours/CoursCPP1"), new Path("/BDCC/JAVA/Cours/CoursJAVA1"));
			fileSys.rename(new Path("/BDCC/JAVA/Cours/CoursCPP2"), new Path("/BDCC/JAVA/Cours/CoursJAVA2"));
			System.out.println("Files have been renamed !");
			
			
			/*
			7. Créer un répertoire dans le système de fichier local les fichiers :
			TP1CPP,TP2CPP,TP1JAVA,TP2JAVA,TP3JAVA.
			 */
			System.out.println("=============== Local directory creation ===============");

			File tempDir = new File("tempDir");
			tempDir.mkdir();
			System.out.println("Directory has been created ");

			
			System.out.println("=============== Local files creation ===============");
			File file = null;
			
			for (int i = 1; i <= 2; i++) {
				file = new File(tempDir.getName()+"/TP"+i+"JAVA");
				file.createNewFile();
				System.out.println("File has been created ");

				file = new File(tempDir.getName()+"/TP"+i+"CPP");
				file.createNewFile();
				System.out.println("File has been created ");
			}
			file = new File(tempDir.getName()+"/TP"+3+"JAVA");
			file.createNewFile();
			System.out.println("File has been created ");

			/*
			8. Copier les fichiers TP1CPP,TP2CPP à partir du système de fichier local vers le
			répertoire TPs de répertoire CPP.
			 */
			System.out.println("=============== Local files coping to HDFS ===============");

			for (int i = 1; i <= 2; i++) {
				fileSys.copyFromLocalFile(new Path(tempDir.getName()+"/TP"+i+"CPP"), new Path("/BDCC/CPP/TPs"));
				System.out.println("File has been copied ");
			}
			
			/*
			 9. Copier les fichiers TP1CJAVA,TP2JAVA à partir du système de fichier local vers le
			répertoire TPs de répertoire JAVA.
			 */
			System.out.println("=============== Local files coping to HDFS ===============");

			for (int i = 1; i <= 3; i++) {
				fileSys.copyFromLocalFile(new Path(tempDir.getName()+"/TP"+i+"JAVA"), new Path("/BDCC/JAVA/TPs"));
				System.out.println("File has been copied ");
			}

			/*
			 10. Afficher tout le contenu du répertoire BDDC d’une manière récursive.
			 */
			System.out.println("=============== Showing /BDCC content ===============");
			Path mainPath = new Path("/BDCC");
			System.out.println(getPathChildren(fileSys, mainPath));
			/*
			 11. Supprimer le fichier TP1CPP du répertoire TPs.
			 */
			System.out.println("=============== File deleting ===============");
			fileSys.delete(new Path("/BDCC/CPP/TPs/TP1CPP"), false);
			System.out.println("File has been deleted! ");

			
			/*
			
			12. Supprimer le répertoire Java avec son contenu.
			 */
			
			System.out.println("=============== Diretory deleting ===============");
			fileSys.delete(new Path("/BDCC/JAVA"), true);
			System.out.println("Directory has been deleted! ");
			
			System.out.println("=============== /BDCC Diretory deleting ===============");
			fileSys.delete(new Path("/BDCC"), true);
			System.out.println("Directory has been deleted! ");
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			try {
				
				fileSys.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
	private static void createDirectoryIfNotExist(FileSystem fileSystem, Path path) {
		try {
			if(!fileSystem.exists(path)) {
				fileSystem.mkdirs(path);
				System.out.println(path.toString()+" has been created successfully.");
			}
			System.out.println(path.toString()+ " already exists.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static void createFileIfNotExist(FileSystem fileSystem, Path path) {
		try {
			if(!fileSystem.exists(path)) {
				fileSystem.createNewFile(path);
				System.out.println(path.toString()+" has been created successfully.");
			}
			System.out.println(path.toString()+ " already exists.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static void writeToFile(FileSystem fileSys, Path pathFile, String content) {
		FSDataOutputStream fsDataOutputStream = null;
		BufferedWriter bufferedWriter = null;
		try {
			fsDataOutputStream = fileSys.create(pathFile);
			bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
			bufferedWriter.write("Hello this is "+pathFile.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			try {
				bufferedWriter.close();
				fsDataOutputStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private static String readFromFile(FileSystem fileSys, Path pathFile) {
		String content = "";
		//TODO: read from file and append to content
		FSDataInputStream fsDataInputStream = null;
		BufferedReader bufferedReader = null;
		
		try {
			fsDataInputStream = fileSys.open(pathFile);
			bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream,  StandardCharsets.UTF_8));
			String contentLine = bufferedReader.readLine();
			while(contentLine != null) {
				content += contentLine + "\n";
				contentLine = bufferedReader.readLine();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			try {
				bufferedReader.close();
				fsDataInputStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		return content;
	}
	
	private static String getPathChildren(FileSystem fileSys, Path path) {
		String content = "";
		FileStatus[] fileStatusList = null;
		try {
			if(!fileSys.isDirectory(path)){
				content += path.toString()+"\n";
				System.out.println(path.toString());
			}
			else {
				fileStatusList = fileSys.listStatus(path);
				for(FileStatus fileStatus: fileStatusList) {
					getPathChildren(fileSys, fileStatus.getPath());
				}
				
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return content;
	}
	
}
