package br.lassal.dbvcs.tatubola.maven;

import br.lassal.dbvcs.tatubola.ParallelDBVersionCommand;
import br.lassal.dbvcs.tatubola.builder.ParallelDBVersionCommandBuilder;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@Mojo( name = "dbVersionSnapshot", defaultPhase = LifecyclePhase.PROCESS_SOURCES )
public class DBVersionSnapshotGoal extends AbstractMojo {

    @Parameter(property = "executionthreads", defaultValue = "4")
    private int parallelismLevel;

    @Parameter(alias = "versioncontrolsystem", required = true)
    private VCSInfo vcsInfo;

    @Parameter(property = "schemas")
    private List<String> schemas;

    @Parameter(property = "environments", required = true)
    private List<DBEnvironment> environments;

    @Parameter( defaultValue = "${project.basedir}", property = "baseDir", required = true )
    private File baseDir;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {

        try {
            ParallelDBVersionCommand cmd = this.buildCommand();
            cmd.takeDatabaseSchemaSnapshotVersion();
        }catch(Exception ex){
            this.getLog().error("Error trying to take database version snapshot", ex);
            throw new MojoExecutionException("Error trying to take database version snapshot", ex);
        }
    }

    private ParallelDBVersionCommand buildCommand() throws MalformedURLException {
        Path workspacePath = Paths.get(this.baseDir.getAbsolutePath(), "vcsrepo");
        Path tmpPath = Paths.get(this.baseDir.getAbsolutePath(), "tmpwork");

        this.getLog().info(this.vcsInfo.getRemoteRepositoryUrl());
        this.getLog().info(this.vcsInfo.getCredentials().getUsername());

        ParallelDBVersionCommandBuilder builder =
                new ParallelDBVersionCommandBuilder(this.parallelismLevel)
                .setVCSRemoteInfo(this.vcsInfo.getRemoteRepositoryUrl(), this.vcsInfo.getSourceBranch()
                        , this.vcsInfo.getCredentials().getUsername(), this.vcsInfo.getCredentials().getPassword())
                .setWorkspaceInfo(workspacePath.toString(), tmpPath.toString())
                .setDBSchemasToBeSerialized(this.schemas);

        for(DBEnvironment env: this.environments){
            builder.addDBEnvironment(env.getName(), env.getJdbcUrl(), env.getCredentials().getUsername()
            ,env.getCredentials().getPassword());
        }

        return builder.build();
    }

}
