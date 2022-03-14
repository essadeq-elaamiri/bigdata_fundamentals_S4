#TP1 Hadoop
#!/bin/sh

echo -e "\033[4;33mQ1: creation des repertoires \033[0m\n"

hdfs dfs -mkdir /BDCC
hdfs dfs -mkdir /BDCC/JAVA
hdfs dfs -mkdir /BDCC/CPP
hdfs dfs -mkdir /BDCC/JAVA/Cours
hdfs dfs -mkdir /BDCC/CPP/Cours
hdfs dfs -mkdir /BDCC/JAVA/TPs
hdfs dfs -mkdir /BDCC/CPP/TPs

hdfs dfs -ls -R /

echo -e  "\033[4;33mQ1: creation des fichier \033[0m\n"
echo -e "Creation des fichiers"
hdfs dfs -touchz /BDCC/CPP/Cours/CoursCPP1 
hdfs dfs -touchz /BDCC/CPP/Cours/CoursCPP2
hdfs dfs -touchz /BDCC/CPP/Cours/CoursCPP3 
hdfs dfs -touchz /BDCC/CPP/Cours/CoursCPP4
 
hdfs dfs -ls -R 

echo -e "\033[4;33mQ1: creation de fichier local et le remplir \033[0m\n"

echo  "Hello  content to file, here is some text" > /home/content.txt
ls  /home/content.txt
cat /home/content.txt

echo -e  "\033[4;33mQ1: ajouter le contenu au fichier  \033[0m\n"

#append cotent from local to hdfs
hdfs dfs -appendToFile /home/content.txt /BDCC/CPP/Cours/CoursCPP1
hdfs dfs -appendToFile /home/content.txt /BDCC/CPP/Cours/CoursCPP2
hdfs dfs -appendToFile /home/content.txt /BDCC/CPP/Cours/CoursCPP3 

hdfs dfs -ls -R /

echo -e "\033[4;33mQ1: afficher le contenu du fichier \033[0m\n"

hdfs dfs -cat /BDCC/CPP/Cours/CoursCPP1
hdfs dfs -cat /BDCC/CPP/Cours/CoursCPP2
hdfs dfs -cat /BDCC/CPP/Cours/CoursCPP3

echo -e "\033[4;33mQ: Copiez les fichiers CoursCPP1,\033[0m\n"


hdfs dfs -cp  /BDCC/CPP/Cours/CoursCPP1 /BDCC/JAVA/Cours 
hdfs dfs -cp  /BDCC/CPP/Cours/CoursCPP2 /BDCC/JAVA/Cours 
hdfs dfs -cp  /BDCC/CPP/Cours/CoursCPP3 /BDCC/JAVA/Cours 

hdfs dfs -ls -R /

echo -e "\033[4;33mQ: Supprimer le fichier CoursCPP3 dans \033[0m\n"


hdfs dfs -rm /BDCC/JAVA/Cours/CoursCPP3

hdfs dfs -ls -R /

echo -e  "\033[4;33mQ6: renommer les fichier \033[0m\n"


hdfs dfs -mv /BDCC/JAVA/Cours/CoursCPP1 /BDCC/JAVA/Cours/CoursJAVA1
hdfs dfs -mv /BDCC/JAVA/Cours/CoursCPP2 /BDCC/JAVA/Cours/CoursJAVA2

hdfs dfs -ls -R /

echo -e  "\033[4;33mQ7: creation du repertoir local \033[0m\n"


mkdir reploc 
ls .
cd reploc 

echo -e  "\033[4;33mQ7: creation des fichier \033[0m\n"


touch TP1CPP TP2CPP TP1JAVA TP2JAVA TP3JAVA 

ls .

cd ..

echo -e  "\033[4;33mQ8: copier de local vers hdfs \033[0m\n"


hdfs dfs -put ./reploc/TP1CPP /BDCC/CPP/TPs
hdfs dfs -put ./reploc/TP2CPP /BDCC/CPP/TPs

hdfs dfs -ls -R /

echo -e  "\033[4;33mQ9: Copier les fichiers TP1CJAVA,TP2JAVA \033[0m\n"


hdfs dfs -put ./reploc/TP1JAVA /BDCC/JAVA/TPs
hdfs dfs -put ./reploc/TP2JAVA /BDCC/JAVA/TPs

hdfs dfs -ls -R /

echo -e  "\033[4;33mQ10:Afficher tout le contenu du répertoire BDDC d’une manière recursive. \033[0m\n"


hdfs dfs -ls -R /BDCC

echo -e  "\033[4;33mQ11: supprimer TP1CPP \033[0m\n"

hdfs dfs -rm  /BDCC/CPP/TPs/TP1CPP

hdfs dfs -ls -R /BDCC

echo -e  "\033[4;33mQ12: supprimer /BDCC/JAVA et son contenu \033[0m\n"


hdfs dfs -rm -R  /BDCC/JAVA

hdfs dfs -ls -R /BDCC

hdfs dfs -rm -R  /BDCC

hdfs dfs -ls -R /

rm -R reploc



