package br.lassal.dbvcs.tatubola.integration;

import br.lassal.dbvcs.tatubola.versioncontrol.GitController;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Date;

public class GitControllerTest {

    private static final String INTEGRATION_TESTS_REPO_DEFAULT_BRANCH = "common";


    private File getDefaultTestFile(File parentPath) throws Exception {
        File testFile = new File(parentPath, "unique.txt");

        if(!testFile.exists()){
            throw  new Exception("Test file not available locally. Have you checkout the repository from remote?");
        }

        return testFile;
    }

    /***
     * Test the following cenario
     *       - Load non existing local repository from remote
     *       - Checkout existing branch
     *       - Add new file
     *       - Change existing file
     *       - Create new branch
     *       - Add file
     *       - Send changes to the server
     */
    @Test
    public void setupNewEnviront_plus_changesMultipleBranchesTest() throws Exception {
        File testWorkspace = new File("gitController/testA");
        URL githubRepo = new URL(IntegrationTestInfo.REMOTE_REPO);
        String username = IntegrationTestInfo.getVCSRepositoryUsername();
        String password = IntegrationTestInfo.getVCSRepositoryPassword();
        String baseBranch = IntegrationTestInfo.REPO_BASE_BRANCH;


        GitController vcs = new GitController(githubRepo, username, password);
        vcs.setWorkspacePath(testWorkspace);
        vcs.setBaseBranch(baseBranch);

        // 1. Load non existing local repository from remote
        vcs.setupRepositoryInitialState();

        // 2. Checkout existing branch
        vcs.checkout(GitControllerTest.INTEGRATION_TESTS_REPO_DEFAULT_BRANCH);
        // 3. Add new file
        this.createNewTextFile(vcs.getWorkspacePath());
        // 4. Change existing file
        this.changeExistingFile(this.getDefaultTestFile(vcs.getWorkspacePath()));
        vcs.commitAllChanges("Changed existing branch at => " + new Date());

        //5. Create new branch
        vcs.checkout(String.format("%1$tY%1$tm%1$td_%1$tH-%1$tM-%1$tS_Branch", new Date()));
        // 6. Add file
        this.createNewTextFile(vcs.getWorkspacePath());
        vcs.commitAllChanges("Created new branch");

        // 7. Send changes to the server
        vcs.syncChangesToServer();

    }

    private void createNewTextFile(File rootPath) throws IOException {
        String fileName = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS_file.txt", new Date());

        File targetFile = new File(rootPath , fileName);
        try(FileWriter fw = new FileWriter(targetFile, true);){
            fw.write(String.format(">>> New line --- | %tc =|%n", new Date()));
        }

    }

    private void changeExistingFile(File targetFile) throws IOException {
        try(FileWriter fw = new FileWriter(targetFile, true);){
            fw.write(String.format(">>> New line --- | %tc =|%n", new Date()));
        }

    }


}
