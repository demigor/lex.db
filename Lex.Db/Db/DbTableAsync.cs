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
  /// <summary>
  /// Asynchronous extensions for DbTable
  /// </summary>
  public static class DbTableAsync
  {
    /// <summary>
    /// Asynchronously loads all entities from table 
    /// </summary>
    /// <typeparam name="T">Type of the entity class</typeparam>
    /// <param name="table">Table of the entity class</param>
    /// <returns>Awaitable Task with array of entities in result</returns>
    public static Task<T[]> LoadAllAsync<T>(this DbTable<T> table) where T : class
    {
      return TaskEx.Run(() => table.LoadAll());
    }

    /// <summary>
    /// Asynchronously counts all entities from table 
    /// </summary>
    /// <typeparam name="T">Type of the entity class</typeparam>
    /// <param name="table">Table of the entity class</param>
    /// <returns>Awaitable Task with count in result</returns>
    public static Task<int> CountAsync<T>(this DbTable<T> table) where T : class
    {
      return TaskEx.Run(() => table.Count());
    }

    /// <summary>
    /// Asynchronously loads all PK values from PK index 
    /// </summary>
    /// <typeparam name="K">Type of the PK</typeparam>
    /// <param name="table">Table of the entity class</param>
    /// <returns>Awaitable Task with list of PK values in result</returns>
    public static Task<List<K>> AllKeysAsync<K>(this DbTable table)
    {
      return TaskEx.Run(() => table.AllKeys<K>());
    }

    /// <summary>
    /// Asynchronously loads all PK values from PK index 
    /// </summary>
    /// <param name="table">Table of the entity class</param>
    /// <returns>Awaitable Task with enumeration of untyped PK values in result</returns>
    public static Task<IEnumerable> AllKeysAsync(this DbTable table)
    {
      return TaskEx.Run(() => table.AllKeys());
    }

    /// <summary>
    /// Asynchronously loads entities by specified index and value 
    /// </summary>
    /// <typeparam name="T">Type of the entity class</typeparam>
    /// <typeparam name="I1">Type of the indexed value</typeparam>
    /// <param name="table">Table of the entity class</param>
    /// <param name="index">Name of the index</param>
    /// <param name="key">Typed index value to search for</param>
    /// <returns>Awaitable Task with list of found entities in result</returns>
    public static Task<List<T>> LoadAllAsync<T, I1>(this DbTable<T> table, string index, I1 key) where T : class
    {
      return TaskEx.Run(() => table.LoadAll<I1>(index, key));
    }

    /// <summary>
    /// Asynchronously loads entity by PK value 
    /// </summary>
    /// <typeparam name="T">Type of the entity class</typeparam>
    /// <typeparam name="K">Type of the PK</typeparam>
    /// <param name="table">Table of the entity class</param>
    /// <param name="key">The PK value of entity to load</param>
    /// <returns>Awaitable Task with loaded entity in result, or null if not found</returns>
    public static Task<T> LoadByKeyAsync<T, K>(this DbTable<T> table, K key) where T : class
    {
      return TaskEx.Run(() => table.LoadByKey(key));
    }

    /// <summary>
    /// Asynchronously loads entities with speicified PK values 
    /// </summary>
    /// <typeparam name="T">Type of the entity class</typeparam>
    /// <typeparam name="K">Type of the PK</typeparam>
    /// <param name="table">Table of the entity class</param>
    /// <param name="keys">The enumeration of PK values to load</param>
    /// <param name="yieldNotFound">Specifies that missing records should be ignored or returnes as nulls</param>
    /// <returns>Awaitable Task with enumeration of loaded entities in result</returns>
    public static Task<IEnumerable<T>> LoadByKeysAsync<T, K>(this DbTable<T> table, IEnumerable<K> keys, bool yieldNotFound = false) where T : class
    {
      return TaskEx.Run(() => table.LoadByKeys(keys, yieldNotFound));
    }

    /// <summary>
    /// Asynchronously reloads specified entity by its PK 
    /// </summary>
    /// <typeparam name="T">Type of the entity class</typeparam>
    /// <param name="table">Table of the entity class</param>
    /// <param name="item">Entity to reload from table</param>
    /// <returns>Awaitable Task with reloaded entity in result</returns>
    public static Task<T> RefreshAsync<T>(this DbTable<T> table, T item) where T : class
    {
      return TaskEx.Run(() => table.Refresh(item));
    }

    /// <summary>
    /// Asynchronously saves specified entities 
    /// </summary>
    /// <typeparam name="T">Type of the entity class</typeparam>
    /// <param name="table">Table of the entity class</param>
    /// <param name="items">Enumeration of entities to save</param>
    /// <returns>Awaitable Task of the save operation</returns>
    public static Task SaveAsync<T>(this DbTable<T> table, IEnumerable<T> items) where T : class
    {
      return TaskEx.Run(() => table.Save(items));
    }

    /// <summary>
    /// Asynchronously saves specified entity 
    /// </summary>
    /// <typeparam name="T">Type of the entity class</typeparam>
    /// <param name="table">Table of the entity class</param>
    /// <param name="item">Entity to save</param>
    /// <returns>Awaitable Task of the save operation</returns>
    public static Task SaveAsync<T>(this DbTable<T> table, T item) where T : class
    {
      return TaskEx.Run(() => table.Save(item));
    }

    /// <summary>
    /// Asynchronously deletes specified entity by PK supplied 
    /// </summary>
    /// <typeparam name="K">Type of the PK</typeparam>
    /// <param name="table">Table of the entity class</param>
    /// <param name="key">The PK value</param>
    /// <returns>Awaitable Task with success flag in result</returns>
    public static Task<bool> DeleteByKeyAsync<K>(this DbTable table, K key)
    {
      return TaskEx.Run(() => table.DeleteByKey(key));
    }

    /// <summary>
    /// Asynchronously deletes specified entities by their PKs 
    /// </summary>
    /// <typeparam name="K">Type of the PK</typeparam>
    /// <param name="table">Table of the entity class</param>
    /// <param name="keys">The enumeration of PK values</param>
    /// <returns>Awaitable Task with count of successfully deleted entities in result</returns>
    public static Task<int> DeleteByKeyAsync<K>(this DbTable table, IEnumerable<K> keys)
    {
      return TaskEx.Run(() => table.DeleteByKeys(keys));
    }

    /// <summary>
    /// Asynchronously deletes specified entity 
    /// </summary>
    /// <typeparam name="T">Type of the entity class</typeparam>
    /// <param name="table">Table of the entity class</param>
    /// <param name="item">Entity to delete</param>
    /// <returns>Awaitable Task with success flag in result</returns>
    public static Task<bool> DeleteAsync<T>(this DbTable<T> table, T item) where T : class
    {
      return TaskEx.Run(() => table.Delete(item));
    }

    /// <summary>
    /// Asynchronously deletes specified entities 
    /// </summary>
    /// <typeparam name="T">Type of the entity class</typeparam>
    /// <param name="table">Table of the entity class</param>
    /// <param name="items">Entities to delete</param>
    /// <returns>Awaitable Task with count of successfully deleted entities in result</returns>
    public static Task<int> DeleteAsync<T>(this DbTable<T> table, IEnumerable<T> items) where T : class
    {
      return TaskEx.Run(() => table.Delete(items));
    }

    /// <summary>
    /// Asynchronously initializes database instance 
    /// </summary>
    /// <param name="db">Database instance to initialize</param>
    /// <returns>Awaitable Task of initialize operation</returns>
    public static Task InitializeAsync(this DbInstance db)
    {
      return TaskEx.Run(() => db.Initialize());
    }

    /// <summary>
    /// Asynchronously purges specified table
    /// </summary>
    /// <param name="table">Table to purge</param>
    /// <returns>Awaitable Task of purge operation</returns>
    public static Task PurgeAsync(this DbTable table)
    {
      return TaskEx.Run(() => table.Purge());
    }

    /// <summary>
    /// Asynchronously purges specified database instance
    /// </summary>
    /// <param name="db">Database instance to purge</param>
    /// <returns>Awaitable Task of purge operation</returns>
    public static Task PurgeAsync(this DbInstance db)
    {
      return TaskEx.Run(() => db.Purge());
    }

    /// <summary>
    /// Asynchronously compacts specified table
    /// </summary>
    /// <param name="table">Table to compact</param>
    /// <returns>Awaitable Task of compact operation</returns>
    public static Task CompactAsync(this DbTable table)
    {
      return TaskEx.Run(() => table.Compact());
    }

    /// <summary>
    /// Asynchronously compacts specified database instance
    /// </summary>
    /// <param name="db">Database instance to compact</param>
    /// <returns>Awaitable Task of compact operation</returns>
    public static Task CompactAsync(this DbInstance db)
    {
      return TaskEx.Run(() => db.Compact());
    }

    /// <summary>
    /// Asynchronously determines table sizes
    /// </summary>
    /// <param name="table">Table to inspect</param>
    /// <returns>Awaitable Task of inspect operation</returns>
    public static Task<DbTableInfo> GetInfoAsync(this DbTable table)
    {
      return TaskEx.Run(() => table.GetInfo());
    }

    /// <summary>
    /// Asynchronously determines database sizes
    /// </summary>
    /// <param name="db">Database instance to inspect</param>
    /// <returns>Awaitable Task of inspect operation</returns>
    public static Task GetInfoAsync(this DbInstance db)
    {
      return TaskEx.Run(() => db.GetInfo());
    }
  }
}

