import configparser
import os
from ..utils.repo_utils import repo_dir, repo_file, repo_default_config

def repo_find(path=".", required=True):
    """
    Find the root of the current repo. e.g root can be in ~/Documents/MyProject
    but we might be working in ~/Documents/MyProject/src/tui/frames/mainview/
    We start at the current directory and recurse up to / 
    To identify a path as a repository, it will check for the presence of a .git directory.
    """
    path = os.path.realpath(path)

    if os.path.isdir(os.path.join(path, ".git")):
        return GitRepository(path)

    # If we haven't returned, recurse in parent, if w
    parent = os.path.realpath(os.path.join(path, ".."))

    if parent == path:
        # Bottom case
        # os.path.join("/", "..") == "/":
        # If parent==path, then path is root.
        if required:
            raise Exception("No git directory.")
        else:
            return None

    # Recursive case
    return repo_find(parent, required)

def repo_create(path):
    """Create a new repository at path."""

    repo = GitRepository(path, True)

    # First, we make sure the path either doesn't exist or is an
    # empty dir.

    if os.path.exists(repo.worktree):
        if not os.path.isdir(repo.worktree):
            raise Exception ("%s is not a directory!" % path)
        if os.path.exists(repo.gitdir) and os.listdir(repo.gitdir):
            raise Exception("%s is not empty!" % path)
    else:
        os.makedirs(repo.worktree)

    assert repo_dir(repo, "branches", mkdir=True)
    assert repo_dir(repo, "objects", mkdir=True)
    assert repo_dir(repo, "refs", "tags", mkdir=True)
    assert repo_dir(repo, "refs", "heads", mkdir=True)

    # .git/description
    with open(repo_file(repo, "description"), "w") as f:
        f.write("Unnamed repository; edit this file 'description' to name the repository.\n")

    # .git/HEAD
    with open(repo_file(repo, "HEAD"), "w") as f:
        f.write("ref: refs/heads/master\n")

    with open(repo_file(repo, "config"), "w") as f:
        config = repo_default_config()
        config.write(f)

    return repo

class GitRepository (object):
    """
    A git repository.
    To create a new Repository object, we only need to make a few checks:
        - We must verify that the directory exists, and contains a subdirectory 
        called .git.
        - We read its configuration in .git/config (it’s just an INI file) and 
        control that core.repositoryformatversion is 0.
    The constructor takes an optional force which disables all checks.
    """

    worktree = None
    gitdir = None
    conf = None

    def __init__(self, path, force=False):
        # only 2 paths for now: worktree and worktree/.git
        self.worktree = path
        self.gitdir = os.path.join(path, ".git") # worktree/.git

        # If force is set to True, this check is bypassed, allowing the code 
        # to proceed regardless of whether self.gitdir is a valid directory.
        if not (force or os.path.isdir(self.gitdir)):
            raise Exception("Not a Git repository %s" % path)
        
        # Read configuration file in .git/config
        self.conf = configparser.ConfigParser()
        cf = repo_file(self, "config")

        if cf and os.path.exists(cf):
            self.conf.read([cf])
        elif not force:
            raise Exception("Configuration file missing")
        
        if not force:
            vers = int(self.conf.get("core", "repositoryformatversion"))
            if vers != 0:
                raise Exception("Unsupported repositoryformatversion %s" % vers)