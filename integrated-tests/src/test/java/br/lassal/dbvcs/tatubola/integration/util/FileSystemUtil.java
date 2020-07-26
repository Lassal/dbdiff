package br.lassal.dbvcs.tatubola.integration.util;

import java.io.File;

public class FileSystemUtil {

    /**
     * Delete all directory content recursively
     * @param file
     */
    public static void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDir(f);
            }
        }
        file.delete();
    }
}
