package meta

import "path/filepath"

// Collection defines a collection type under the metastore
type Collection string

const (
	//ObjectCollection defines object collection
	ObjectCollection Collection = "object"
	//BlobCollection defines blob collection
	BlobCollection Collection = "blob"
	//UploadCollection defines upload collection
	UploadCollection Collection = "upload"
	//BucketCollection defines bucket collection
	BucketCollection Collection = "bucket"
	//VersionCollection defines version collection
	VersionCollection Collection = "version"
)

// Path defines a path to object in the metastore
type Path struct {
	Collection Collection
	// Prefix is basically the "directory" where this object is stored.
	// a Path with Prefix only is assumed to be a directory.
	Prefix string
	// Name is the name of the object, a Path with Name set is assumed to
	// be a file.
	Name string
}

// NewPath creates a new path from collection, prefix and name
func NewPath(collection Collection, prefix string, name string) Path {
	if len(collection) == 0 {
		panic("invalid collection")
	}

	return Path{Collection: collection, Prefix: prefix, Name: name}
}

// FilePath always create a Path to an object (Name will be set to last part)
// to create a path to a directory use NewPath
func FilePath(collection Collection, parts ...string) Path {
	prefix, name := filepath.Split(filepath.Join(parts...))
	return NewPath(collection, prefix, name)
}

// DirPath always create a Path to a directory
// to create a path to a file use FilePath
func DirPath(collection Collection, parts ...string) Path {
	return NewPath(collection, filepath.Join(parts...), "")
}

// Base return the name
func (p *Path) Base() string {
	return p.Name
}

// Relative joins prefix and name
func (p *Path) Relative() string {
	return filepath.Join(p.Prefix, p.Name)
}

// IsDir only check how the Path is constructed, a Path with empty Name (only Prefix)
// is assumed to be a Directory. It's up to the implementation to make sure this
// rule is in effect.
func (p *Path) IsDir() bool {
	return len(p.Name) == 0
}

// Join creates a new path from this path plus the new parts
func (p *Path) Join(parts ...string) Path {
	return FilePath(p.Collection, filepath.Join(p.Prefix, p.Name, filepath.Join(parts...)))
}

func (p *Path) String() string {
	return filepath.Join(string(p.Collection), p.Prefix, p.Name)
}
