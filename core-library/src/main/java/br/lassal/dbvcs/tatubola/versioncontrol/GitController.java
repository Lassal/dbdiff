package br.lassal.dbvcs.tatubola.versioncontrol;

import org.eclipse.jgit.api.CreateBranchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.PushResult;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class GitController implements VersionControlSystem, Closeable {

    private static Logger logger = LoggerFactory.getLogger(GitController.class);
    private static final File DEFAULT_WORKSPACE = new File("workspace");

    private URL gitRemoteURL;
    private File workspace;
    private Git git;
    private UsernamePasswordCredentialsProvider credentials;
    private boolean commitEmptyChanges;
    private String currentBranch;
    private Set<RefSpec> branchesAltered;

    public GitController(URL gitRemoteURL, String username, String password, boolean commitEmptyChanges){
        this.gitRemoteURL = gitRemoteURL;
        this.credentials = new UsernamePasswordCredentialsProvider(username, password);
        this.commitEmptyChanges = commitEmptyChanges;
        this.branchesAltered = new HashSet<>();

        this.workspace = GitController.DEFAULT_WORKSPACE;
    }

    public GitController(URL gitRemoteURL, String username, String password){
        this(gitRemoteURL, username, password, false);
    }

    public void setWorkspacePath(File workspacePath){
        this.workspace = workspacePath;
    }

    private boolean isGitFolder(Path localFolder){
        Path gitPath = Paths.get(localFolder.toString(), ".git");

        return Files.exists(gitPath);
    }

    @Override
    public void setupRepositoryInitialState() throws IOException, GitAPIException {
        if(this.isGitFolder(this.workspace.toPath())) {
            this.git = Git.open(this.workspace);

        }
        else{

            this.git = Git.cloneRepository()
                    .setURI(this.gitRemoteURL.toString())
                    .setCredentialsProvider(this.credentials)
                    .setDirectory(this.workspace)
                    .call();
        }

    }

    @Override
    public void checkout(String branchName) throws IOException, GitAPIException{
        this.currentBranch = branchName;
        boolean existsRemote = false;
        boolean existsLocal = false;

        List<Ref> repoFoundBranches = this.git.branchList()
                .setListMode(ListBranchCommand.ListMode.ALL)
                .call()
                .stream()
                .filter(branch -> branch.getName().endsWith(this.currentBranch))
                .collect(Collectors.toList());

        //find by remote and local branch references
        for(Ref localRef : repoFoundBranches){

            if(localRef.getName().equals("refs/remotes/origin/" + branchName)){
                existsRemote = true;
            }

            if(localRef.getName().equals("refs/heads/" + branchName)){
                existsLocal = true;
            }
        }


        Ref gitRef = null;

        // all remotes returned from git show the references in this format refs/heads/<branch name>
        // git ls-remote --heads
        if(existsRemote){

            // case branch does NOT exists create it
            boolean createBranch = !existsLocal;

            // setUpstreamMode allow to connect the branch with the existing remote branch
            gitRef = this.git.checkout()
                    .setName(this.currentBranch)
                    .setCreateBranch(createBranch)
                    .setUpstreamMode(CreateBranchCommand.SetupUpstreamMode.SET_UPSTREAM)
                    .setStartPoint("origin/" + this.currentBranch)
                    .call();
        }
        else{
            // this create a new branch that will be sent to the remote
            gitRef = this.git.checkout()
                    .setName(this.currentBranch)
                    .setCreateBranch(true)
                    .setUpstreamMode(CreateBranchCommand.SetupUpstreamMode.TRACK)
                    .call();

        }


    }

    @Override
    public void commitAllChanges(String changeMessage) throws GitAPIException {
        DirCache response = git.add()
                .addFilepattern(".")
                .call();

        try{
            RevCommit commit = git.commit()
                    .setMessage(changeMessage)
                    .setAllowEmpty(this.commitEmptyChanges)
                    .call();

            this.registerCurrentBranchAltered();
        }catch (GitAPIException ex){
            logger.info("There are no changes to be commited. CommitEmptyChanges = " + this.commitEmptyChanges, ex);
        }

    }

    @Override
    public void syncChangesToServer() throws GitAPIException{
        Iterable<PushResult> results = git.push()
                .setRemote("origin")
                .setRefSpecs(new ArrayList<RefSpec>(this.branchesAltered))
                .setCredentialsProvider(this.credentials)
                .call();
    }

    @Override
    public void close() throws IOException {

        if(this.git != null){
            this.git.close();
        }
    }

    private boolean registerCurrentBranchAltered(){
        RefSpec branchRef = new RefSpec(this.currentBranch + ":" + this.currentBranch);
        return this.branchesAltered.add(branchRef);
    }
}
