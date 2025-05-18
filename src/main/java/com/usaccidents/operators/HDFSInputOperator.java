package com.usaccidents.operators;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class HDFSInputOperator {

    private String hdfsUri;
    private String filePath;
    private Configuration configuration;
    private FileSystem fileSystem;

    /**
     * Constructeur qui initialise la connexion HDFS
     * @param hdfsUri URI du système HDFS, ex: "hdfs://localhost:9000"
     * @param filePath Chemin du fichier dans HDFS, ex: "/data/accidents.csv"
     * @throws IOException si la connexion échoue
     */
    public HDFSInputOperator(String hdfsUri, String filePath) throws IOException {
        this.hdfsUri = hdfsUri;
        this.filePath = filePath;
        this.configuration = new Configuration();
        this.fileSystem = FileSystem.get(URI.create(hdfsUri), configuration);
    }

    /**
     * Lit toutes les lignes du fichier situé sur HDFS.
     * @return une liste de chaînes, chaque chaîne est une ligne du fichier
     * @throws IOException en cas de problème de lecture
     */
    public List<String> readLines() throws IOException {
        List<String> lines = new ArrayList<>();
        Path path = new Path(filePath);

        if (!fileSystem.exists(path)) {
            throw new IOException("Fichier non trouvé sur HDFS : " + filePath);
        }

        try (FSDataInputStream inputStream = fileSystem.open(path);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }

        return lines;
    }

    /**
     * Ferme la connexion HDFS
     * @throws IOException en cas d'erreur à la fermeture
     */
    public void close() throws IOException {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }

    // Méthode main de test rapide
    public static void main(String[] args) {
        try {
            String hdfsUri = "hdfs://localhost:9000"; // adapte selon ta config
            String filePath = "/user/hadoop/us-accidents/us_accidents_march23_clean.csv";

            HDFSInputOperator hdfsInput = new HDFSInputOperator(hdfsUri, filePath);
            List<String> lines = hdfsInput.readLines();

            // Affiche les 5 premières lignes (sans en-tête)
            for (int i = 1; i < Math.min(6, lines.size()); i++) {
                System.out.println(lines.get(i));
            }

            hdfsInput.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}