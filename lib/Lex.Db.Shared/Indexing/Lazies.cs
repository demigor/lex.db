using System;

namespace Lex.Db
{
  using Indexing;

  /// <summary>
  /// Provides support for lazy initialization
  /// </summary>
  /// <typeparam name="T">Specifies the type of entity that is being lazily loaded</typeparam>
  public abstract class Lazy<T> where T : class
  {
    T _result;
    DbTable<T> _table;
    Exception _error;

    internal Lazy(DbTable<T> table, object pk)
    {
      PK = pk;
      _table = table;
    }

    /// <summary>
    /// Primary key used to lazy load the entity
    /// </summary>  
    public readonly object PK;

    /// <summary>
    /// Gets the lazily loaded entity of the current Lazy instance.
    /// </summary>
    public T Value
    {
      get
      {
        return GetValue();
      }
    }

    T GetValue()
    {
      if (_table == null)
      {
        if (_error != null)
          throw _error;

        return _result;
      }

      try
      {
        _result = _table.LoadByKey(PK);
      }
      catch (Exception e)
      {
        _error = e;
        throw;
      }
      finally
      {
        _table = null;
      }
      return _result;
    }
  }

  /// <summary>
  /// Provides support for lazy entity loading as well as access to index value consisting from one component
  /// </summary>
  /// <typeparam name="T">Specifies the type of entity that is being lazily loaded</typeparam>
  /// <typeparam name="I1">Type of the index component</typeparam>
  public sealed class Lazy<T, I1> : Lazy<T> where T : class
  {
    /// <summary>
    /// Index component
    /// </summary>
    public readonly I1 Key;

    internal Lazy(DbTable<T> table, object pk, I1 key)
      : base(table, pk)
    {
      Key = key;
    }
  }

  /// <summary>
  /// Provides support for lazy entity loading as well as access to index value consisting from two components
  /// </summary>
  /// <typeparam name="T">Specifies the type of entity that is being lazily loaded</typeparam>
  /// <typeparam name="I1">Type of the first index component</typeparam>
  /// <typeparam name="I2">Type of the second index component</typeparam>
  public sealed class Lazy<T, I1, I2> : Lazy<T> where T : class
  {
    internal Lazy(DbTable<T> table, object pk, Indexer<I1, I2> source)
      : base(table, pk)
    {
      Key1 = source.Key1;
      Key2 = source.Key2;
    }
  
    /// <summary>
    /// First index component
    /// </summary>
    public readonly I1 Key1;

    /// <summary>
    /// Second index component
    /// </summary>
    public readonly I2 Key2;
  }

  /// <summary>
  /// Provides support for lazy entity loading as well as access to index value consisting from three components
  /// </summary>
  /// <typeparam name="T">Specifies the type of entity that is being lazily loaded</typeparam>
  /// <typeparam name="I1">Type of the first index component</typeparam>
  /// <typeparam name="I2">Type of the second index component</typeparam>
  /// <typeparam name="I3">Type of the third index component</typeparam>
  public sealed class Lazy<T, I1, I2, I3> : Lazy<T> where T : class
  {
    internal Lazy(DbTable<T> table, object pk, Indexer<I1, I2, I3> source)
      : base(table, pk)
    {
      Key1 = source.Key1;
      Key2 = source.Key2;
      Key3 = source.Key3;
    }

    /// <summary>
    /// First index component
    /// </summary>
    public readonly I1 Key1;
    
    /// <summary>
    /// Second index component
    /// </summary>
    public readonly I2 Key2;
    
    /// <summary>
    /// Third index component
    /// </summary>
    public readonly I3 Key3;
  }
}
