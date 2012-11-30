using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
#if !TPL4
using TaskEx = System.Threading.Tasks.Task;
#endif

namespace Lex.Db
{
  public static class DbTableAsync
  {
    public static Task<List<T>> LoadAllAsync<T>(this DbTable<T> table) where T : class
    {
      return TaskEx.Run(() => table.LoadAll());
    }

    public static Task<int> CountAsync<T>(this DbTable<T> table) where T : class
    {
      return TaskEx.Run(() => table.Count());
    }

    public static Task<List<K>> AllKeysAsync<K>(this DbTable table)
    {
      return TaskEx.Run(() => table.AllKeys<K>());
    }

    public static Task<IEnumerable> AllKeysAsync(this DbTable table)
    {
      return TaskEx.Run(() => table.AllKeys());
    }

    public static Task<List<T>> LoadAllAsync<T, I1>(this DbTable<T> table, string index, I1 key) where T : class
    {
      return TaskEx.Run(() => table.LoadAll<I1>(index, key));
    }

    public static Task<T> LoadByKeyAsync<T, K>(this DbTable<T> table, K key) where T : class
    {
      return TaskEx.Run(() => table.LoadByKey(key));
    }

    public static Task<IEnumerable<T>> LoadByKeysAsync<T, K>(this DbTable<T> table, IEnumerable<K> keys, bool yieldNotFound = false) where T : class
    {
      return TaskEx.Run(() => table.LoadByKeys(keys, yieldNotFound));
    }

    public static Task<T> RefreshAsync<T>(this DbTable<T> table, T item) where T : class
    {
      return TaskEx.Run(() => table.Refresh(item));
    }

    public static Task SaveAsync<T>(this DbTable<T> table, IEnumerable<T> items) where T : class
    {
      return TaskEx.Run(() => table.Save(items));
    }

    public static Task SaveAsync<T>(this DbTable<T> table, T item) where T : class
    {
      return TaskEx.Run(() => table.Save(item));
    }

    public static Task<bool> DeleteByKey<K>(this DbTable table, K key)
    {
      return TaskEx.Run(() => table.DeleteByKey(key));
    }

    public static Task<int> DeleteByKey<K>(this DbTable table, IEnumerable<K> keys)
    {
      return TaskEx.Run(() => table.DeleteByKeys(keys));
    }

    public static Task<bool> Delete<T>(this DbTable<T> table, T item) where T : class
    {
      return TaskEx.Run(() => table.Delete(item));
    }

    public static Task<int> Delete<T>(this DbTable<T> table, IEnumerable<T> items) where T : class
    {
      return TaskEx.Run(() => table.Delete(items));
    }

    public static Task InitializeAsync(this DbInstance db)
    {
      return TaskEx.Run(() => db.Initialize());
    }

    public static Task PurgeAsync(this DbTable db)
    {
      return TaskEx.Run(() => db.Compact());
    }

    public static Task PurgeAsync(this DbInstance db)
    {
      return TaskEx.Run(() => db.Purge());
    }

    public static Task CompactAsync(this DbTable db)
    {
      return TaskEx.Run(() => db.Compact());
    }

    public static Task CompactAsync(this DbInstance db)
    {
      return TaskEx.Run(() => db.Compact());
    }
  }
}

