#if !PORTABLE 
using System.IO;
#if NETFX_CORE
using Directory = Lex.Db.OSDirectory;
#endif

namespace Lex.Db.FileSystem
{
  class DbSchemaStorage : IDbSchemaStorage
  {
    readonly string _path;

    public DbSchemaStorage(string path)
    {
      _path = path;
    }

    public void Open()
    {
      if (!Directory.Exists(_path))
        Directory.CreateDirectory(_path);
    }

    public void Purge()
    {
      if (Directory.Exists(_path))
        Directory.Delete(_path, true);

      Directory.CreateDirectory(_path);
    }

    public IDbTableStorage GetTable(string name)
    {
      return new DbTableStorage(_path, name);
    }

    public string Path
    {
      get { return _path; }
    }
  }
}
#endif