using System;
using System.IO;
#if SILVERLIGHT
using System.IO.IsolatedStorage;
using System.Windows;
#endif

namespace Lex.Db
{
  class DbStorage : IDbStorage
  {
#if SILVERLIGHT
    readonly IsolatedStorageFile _storage = IsolatedStorageFile.GetUserStoreForApplication();
#endif
    public IDbSchemaStorage OpenSchema(string path, object home)
    {
#if PORTABLE
      return null;
#else
      path = Path.Combine("Lex.Db", path);
      var root = home as string;

#if NETFX_CORE 
      if (root == null) 
      {
        var folder = home as Windows.Storage.StorageFolder;
        if (folder != null)
          root = folder.Path;
        else
          root = Windows.Storage.ApplicationData.Current.LocalFolder.Path;
      }

      return new FileSystem.DbSchemaStorage(Path.Combine(root, path));
#elif WINDOWS_PHONE
      return new IsolatedStorage.DbSchemaStorage(_storage, path);
#elif SILVERLIGHT
      
      if (Application.Current.HasElevatedPermissions) 
      {
        if (root == null)
          root = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);

        return new FileSystem.DbSchemaStorage(Path.Combine(root, path));
      }

      return new IsolatedStorage.DbSchemaStorage(_storage, path);
#else
      if (root == null)
        root = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);

      return new FileSystem.DbSchemaStorage(Path.Combine(root, path));
#endif
#endif
    }

    public bool IncreaseQuotaTo(long quota)
    {
#if SILVERLIGHT
      return _storage.IncreaseQuotaTo(quota);
#else
      return true;
#endif
    }

    public bool HasEnoughQuota(long quota)
    {
#if SILVERLIGHT
      return _storage.Quota >= quota;
#else
      return true;
#endif
    }
  }
}
