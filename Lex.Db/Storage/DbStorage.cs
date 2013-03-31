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
      path = Path.Combine("Lex.Db", path);

#if NETFX_CORE
      return new WindowsStorage.DbSchemaStorage(path, home as Windows.Storage.StorageFolder);
#else

#if SILVERLIGHT
#if !WINDOWS_PHONE
      if (Application.Current.HasElevatedPermissions)
        return new FileSystem.DbSchemaStorage(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), path));
#endif
      return new IsolatedStorage.DbSchemaStorage(_storage, path);
#else

      return new FileSystem.DbSchemaStorage(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), path));
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
