package br.lassal.dbvcs.tatubola.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class FSManager {

    private static Logger logger = LoggerFactory.getLogger(FSManager.class);

    public void copyFullFolderStructure(Path sourceDir, Path destinationDir) throws IOException {

        // Traverse the file tree and copy each file/directory.
        Files.walk(sourceDir)
                .forEach(sourcePath -> {
                    try {
                        Path targetPath = destinationDir.resolve(sourceDir.relativize(sourcePath));

                        if (logger.isTraceEnabled()) {
                            //     logger.trace(String.format("Copying %s to %s", sourcePath, targetPath));
                        }
                        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException ex) {
                        // logger.warn(String.format("I/O error: %s%n", ex));
                    }
                });
    }
}
