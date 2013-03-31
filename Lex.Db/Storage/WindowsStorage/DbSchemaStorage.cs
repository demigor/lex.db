#if NETFX_CORE
using System.Collections.Generic;
using Windows.Storage;

namespace Lex.Db.WindowsStorage
{
  class DbSchemaStorage : IDbSchemaStorage
  {
    readonly List<DbTableStorage> _tables = new List<DbTableStorage>();
    readonly string _path;
    readonly StorageFolder _home;

    public DbSchemaStorage(string path, StorageFolder home)
    {
      _path = path;
      _home = home ?? ApplicationData.Current.LocalFolder;
    }

    public string Path { get { return _path; } }

    StorageFolder _folder;

    void Open(Awaiter awaiter)
    {
      _folder = awaiter.Await(_home.CreateFolderAsync(_path, CreationCollisionOption.OpenIfExists));
    }

    public void Open()
    {
      using (var awaiter = new Awaiter(true))
        Open(awaiter);
    }

    void Purge(Awaiter awaiter)
    {
      awaiter.Await(_folder.DeleteAsync(StorageDeleteOption.PermanentDelete));
    }

    public void Purge()
    {
      using (var awaiter = new Awaiter(true))
      {
        Purge(awaiter);
        Open(awaiter);

        foreach (var i in _tables)
          i.Open(awaiter);
      }
    }

    public IDbTableStorage GetTable(string name)
    {
      var result = new DbTableStorage(_folder, name);
      _tables.Add(result);
      return result;
    }
  }
}
#endif