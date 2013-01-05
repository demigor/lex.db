using System;

namespace Lex.Db
{
  using Indexing;

  /// <summary>
  /// Provides support for lazy initialization
  /// </summary>
  /// <typeparam name="T">Specifies the type of entity that is being lazily loaded</typeparam>
  public abstract class Lazy<T> where T: class
  {
    T _result;
    object _pk;
    DbTable<T> _table;
    Exception _error;

    /// <summary>
    /// Initializes a new instance of the Lazy class. When lazy initialization occurs, the specified initialization function is used.
    /// </summary>
    /// <param name="table">The delegate that is invoked to produce the lazily initialized value when it is needed</param>
    protected Lazy(DbTable<T> table, object pk)
    {
      _pk = pk;
      _table = table;
    }

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
        _result = _table.LoadByKey(_pk);
      }
      catch (Exception e)
      {
        _error = e;
        throw;
      }
      finally
      {
        _pk = null;
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
  public sealed class Lazy<T, I1> : Lazy<T> where T: class
  {
    readonly I1 _key;

    internal Lazy(DbTable<T> table, object pk, I1 key)
      : base(table, pk)
    {
      _key = key;
    }

    /// <summary>
    /// Index component
    /// </summary>
    public I1 Key { get { return _key; } }
  }

  /// <summary>
  /// Provides support for lazy entity loading as well as access to index value consisting from two components
  /// </summary>
  /// <typeparam name="T">Specifies the type of entity that is being lazily loaded</typeparam>
  /// <typeparam name="I1">Type of the first index component</typeparam>
  /// <typeparam name="I2">Type of the second index component</typeparam>
  public sealed class Lazy<T, I1, I2> : Lazy<T> where T: class
  {
    readonly I1 _key1;
    readonly I2 _key2;

    internal Lazy(DbTable<T> table, object pk, Indexer<I1, I2> source)
      : base(table, pk)
    {
      _key1 = source.Key1;
      _key2 = source.Key2;
    }

    /// <summary>
    /// First index component
    /// </summary>
    public I1 Key1 { get { return _key1; } }

    /// <summary>
    /// Second index component
    /// </summary>
    public I2 Key2 { get { return _key2; } }
  }

  /// <summary>
  /// Provides support for lazy entity loading as well as access to index value consisting from three components
  /// </summary>
  /// <typeparam name="T">Specifies the type of entity that is being lazily loaded</typeparam>
  /// <typeparam name="I1">Type of the first index component</typeparam>
  /// <typeparam name="I2">Type of the second index component</typeparam>
  /// <typeparam name="I3">Type of the third index component</typeparam>
  public sealed class Lazy<T, I1, I2, I3> : Lazy<T> where T: class
  {
    readonly I1 _key1;
    readonly I2 _key2;
    readonly I3 _key3;

    internal Lazy(DbTable<T> table, object pk, Indexer<I1, I2, I3> source)
      : base(table, pk)
    {
      _key1 = source.Key1;
      _key2 = source.Key2;
      _key3 = source.Key3;
    }

    /// <summary>
    /// First index component
    /// </summary>
    public I1 Key1 { get { return _key1; } }

    /// <summary>
    /// Second index component
    /// </summary>
    public I2 Key2 { get { return _key2; } }

    /// <summary>
    /// Third index component
    /// </summary>
    public I3 Key3 { get { return _key3; } }
  }
}
