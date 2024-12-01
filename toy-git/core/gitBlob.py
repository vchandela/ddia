from .gitObject import GitObject

class GitBlob(GitObject):
    """
    serialize and deserialize functions just have to store and return 
    their input unmodified.
    """
    
    fmt=b'blob'

    def serialize(self):
        return self.blobdata

    def deserialize(self, data):
        self.blobdata = data
