#if !NETFX_CORE && !PORTABLE
using System.IO.IsolatedStorage;

namespace Lex.Db.IsolatedStorage
{
  class DbSchemaStorage : IDbSchemaStorage
  {
    readonly string _path;
    readonly IsolatedStorageFile _storage;

    public DbSchemaStorage(IsolatedStorageFile storage, string path)
    {
      _storage = storage;
      _path = path;
    }

    public void Open()
    {
      if (!_storage.DirectoryExists(_path))
        _storage.CreateDirectory(_path);
    }

    void DeleteFolder(string folder, bool root = false)
    {
      foreach (var i in _storage.GetFileNames(folder + @"\*.*"))
        _storage.DeleteFile(folder + @"\" + i);

      foreach (var i in _storage.GetDirectoryNames(folder + @"\*.*"))
        DeleteFolder(folder + @"\" + i);

      if (!root)
        _storage.DeleteDirectory(folder);
    }

    public void Purge()
    {
      DeleteFolder(_path, true);
    }

    public IDbTableStorage GetTable(string name)
    {
      return new DbTableStorage(_storage, _path, name);
    }

    public string Path
    {
      get { return _path; }
    }
  }
}
#endif