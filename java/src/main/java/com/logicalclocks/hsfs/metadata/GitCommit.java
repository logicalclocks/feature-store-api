package com.logicalclocks.hsfs.metadata;

public class GitCommit {

  public String commitId;

  public GitCommit() {
  }

  public GitCommit(String commitId) {
    this.commitId = commitId;
  }

  public String getCommitId() {
    return commitId;
  }

  public void setCommitId(String commitId) {
    this.commitId = commitId;
  }
}
