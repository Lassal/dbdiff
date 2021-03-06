package br.lassal.dbvcs.tatubola.versioncontrol;

import org.eclipse.jgit.api.CreateBranchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
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

//TODO: add documentation
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
    private String baseBranch;

    public GitController(URL gitRemoteURL, String username, String password, boolean commitEmptyChanges) {
        this.gitRemoteURL = gitRemoteURL;
        this.credentials = new UsernamePasswordCredentialsProvider(username, password);
        this.commitEmptyChanges = commitEmptyChanges;
        this.branchesAltered = new HashSet<>();

        this.workspace = GitController.DEFAULT_WORKSPACE;
    }

    public GitController(URL gitRemoteURL, String username, String password) {
        this(gitRemoteURL, username, password, false);
    }

    public void setWorkspacePath(File workspacePath) {
        this.workspace = workspacePath;
    }

    public File getWorkspacePath() {
        return this.workspace;
    }

    /***
     * Set the base branch to be used as initial state for the new branches
     * @param branchName
     */
    public void setBaseBranch(String branchName) {
        this.baseBranch = branchName;
    }

    private boolean isGitFolder(Path localFolder) {
        Path gitPath = Paths.get(localFolder.toString(), ".git");

        return Files.exists(gitPath);
    }

    @Override
    public void setupRepositoryInitialState() throws VersionControlSystemException {

        try {
            if (this.isGitFolder(this.workspace.toPath())) {
                this.git = Git.open(this.workspace);

            } else {

                this.git = Git.cloneRepository()
                        .setURI(this.gitRemoteURL.toString())
                        .setCredentialsProvider(this.credentials)
                        .setDirectory(this.workspace)
                        .call();
            }

        } catch (Exception ex) {
            throw VersionControlSystemException.createGitException(ex, "Error trying to setup local repo initial state");
        }

    }

    @Override
    public void checkout(String branchName) throws VersionControlSystemException {
        this.currentBranch = branchName;
        boolean existsRemote = false;
        boolean existsLocal = false;

        try {
            List<Ref> repoFoundBranches = this.git.branchList()
                    .setListMode(ListBranchCommand.ListMode.ALL)
                    .call()
                    .stream()
                    .filter(branch -> branch.getName().endsWith(this.currentBranch))
                    .collect(Collectors.toList());

            //find by remote and local branch references
            for (Ref localRef : repoFoundBranches) {

                if (localRef.getName().equals("refs/remotes/origin/" + branchName)) {
                    existsRemote = true;
                }

                if (localRef.getName().equals("refs/heads/" + branchName)) {
                    existsLocal = true;
                }
            }


            // all remotes returned from git show the references in this format refs/heads/<branch name>
            // git ls-remote --heads
            if (existsRemote) {

                // case branch does NOT exists create it
                boolean createBranch = !existsLocal;

                // setUpstreamMode allow to connect the branch with the existing remote branch
                this.git.checkout()
                        .setName(this.currentBranch)
                        .setCreateBranch(createBranch)
                        .setUpstreamMode(CreateBranchCommand.SetupUpstreamMode.SET_UPSTREAM)
                        .setStartPoint("origin/" + this.currentBranch)
                        .call();
            } else {

                RevCommit baseCommitBranch = this.getBaseCommitNewBranch();

                // this create a new branch that will be sent to the remote
                this.git.checkout()
                        .setName(this.currentBranch)
                        .setCreateBranch(true)
                        .setStartPoint(baseCommitBranch)
                        .setUpstreamMode(CreateBranchCommand.SetupUpstreamMode.TRACK)
                        .call();

            }
        } catch (Exception ex) {
            throw VersionControlSystemException.createGitException(ex, "Error trying to checkout current branch.");
        }


    }

    @Override
    public void commitAllChanges(String changeMessage) throws VersionControlSystemException {
        try {
            this.git.add()
                    .addFilepattern(".")
                    .call();

        } catch (Exception ex) {
            VersionControlSystemException.createGitException(ex, "Error trying to register all changes in local repository.");
        }

        try {
            this.git.commit()
                    .setMessage(changeMessage)
                    .setAllowEmpty(this.commitEmptyChanges)
                    .call();

            this.registerCurrentBranchAltered();
        } catch (GitAPIException ex) {

            logger.info("There are no changes to be commited, Branch = " + this.currentBranch + ", CommitEmptyChanges = " + this.commitEmptyChanges, ex);
        }

    }

    @Override
    public void syncChangesToServer() throws VersionControlSystemException {
        try {
            this.git.push()
                    .setRemote("origin")
                    .setRefSpecs(new ArrayList<RefSpec>(this.branchesAltered))
                    .setCredentialsProvider(this.credentials)
                    .call();

        } catch (Exception ex) {
            throw VersionControlSystemException.createGitException(ex, "Error trying to sync local VCS changes to server");
        }
    }

    @Override
    public void close() throws IOException {

        if (this.git != null) {
            this.git.close();
        }
    }

    private boolean registerCurrentBranchAltered() {
        RefSpec branchRef = new RefSpec(this.currentBranch + ":" + this.currentBranch);
        return this.branchesAltered.add(branchRef);
    }

    private Repository getJGitRepository() {
        return this.git.getRepository();
    }

    /**
     * Return the HEAD commit of the base branch informed or the current branch in case
     * it is not informed
     *
     * @return Revision commit to the head of the informed branch
     * @throws IOException
     * @throws GitAPIException
     */
    private RevCommit getBaseCommitNewBranch() throws IOException, GitAPIException {

        if (this.baseBranch == null) {
            ObjectId head = this.getJGitRepository().resolve("HEAD");
            Iterable<RevCommit> revs = this.git.log().setMaxCount(3).add(head).call();

            return revs.iterator().next();
        } else {
            ObjectId baseBranchObjId = this.getJGitRepository().resolve("refs/heads/" + this.baseBranch);
            Iterable<RevCommit> revs = this.git.log().setMaxCount(3).add(baseBranchObjId).call();

            return revs.iterator().next();
        }
    }
}
