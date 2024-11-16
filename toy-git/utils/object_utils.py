from .repo_utils import repo_file
import os
import zlib
import hashlib
from core.gitBlob import GitBlob

def object_hash(fd, fmt, repo=None):
    """ Hash object, writing it to repo if provided."""
    data = fd.read()

    # Choose constructor according to fmt argument
    match fmt:
        case b'commit' : obj=GitCommit(data)
        case b'tree'   : obj=GitTree(data)
        case b'tag'    : obj=GitTag(data)
        case b'blob'   : obj=GitBlob(data)
        case _: raise Exception("Unknown type %s!" % fmt)

    return object_write(obj, repo)

# ToDo: currently we only refer using full hash. Need to add other ways.
def object_find(repo, name, fmt=None, follow=True):
    """
    Git has a lot of ways to refer to objects: full hash, short hash, tags, etc
    This function takes these and returns the object's name. 
    """
    return name

# Hash is computed after the header is added  (so it’s the hash of the object itself, 
# uncompressed, not just its contents)
def object_write(obj, repo=None):
    # Serialize object data
    data = obj.serialize()
    # Add header
    result = obj.fmt + b' ' + str(len(data)).encode() + b'\x00' + data
    # Compute hash
    sha = hashlib.sha1(result).hexdigest()

    if repo:
        # Compute path
        path=repo_file(repo, "objects", sha[0:2], sha[2:], mkdir=True)

        if not os.path.exists(path):
            with open(path, 'wb') as f:
                # Compress and write
                f.write(zlib.compress(result))
    return sha

def object_read(repo, sha):
    """Read object from .git/objects/ using it's SHA-1 hash.
    e.g: path for e673d1b7eaa0aa01b5bc2442d570a765bdaae751 is .git/objects/e6/73d1b7eaa0aa01b5bc2442d570a765bdaae751
    1. binary file -zlib-> decompress
    2. extract 2 header components -- object type and size
    3. From the type, we determine the actual class to use and call its constructor
    4. We convert the size to a Python integer, and check if it matches.
    """

    path = repo_file(repo, "objects", sha[0:2], sha[2:])

    if not os.path.isfile(path):
        return None

    with open (path, "rb") as f:
        raw = zlib.decompress(f.read())

        # Read object type
        x = raw.find(b' ')
        fmt = raw[0:x]

        # Read and validate object size
        y = raw.find(b'\x00', x)
        size = int(raw[x:y].decode("ascii"))
        if size != len(raw)-y-1:
            raise Exception("Malformed object {0}: bad length".format(sha))

        # Pick constructor
        match fmt:
            case b'commit' : c=GitCommit
            case b'tree'   : c=GitTree
            case b'tag'    : c=GitTag
            case b'blob'   : c=GitBlob
            case _:
                raise Exception("Unknown type {0} for object {1}".format(fmt.decode("ascii"), sha))

        # Call constructor and return object
        return c(raw[y+1:])