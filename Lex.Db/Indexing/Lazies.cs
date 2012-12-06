using System;

namespace Lex.Db
{
  using Indexing;

  /// <summary>
  /// Provides support for lazy initialization
  /// </summary>
  /// <typeparam name="T">Specifies the type of entity that is being lazily initialized</typeparam>
  public abstract class Lazy<T>
  {
    T _result;
    Func<T> _loader;
    Exception _error;

    /// <summary>
    /// Initializes a new instance of the Lazy class. When lazy initialization occurs, the specified initialization function is used.
    /// </summary>
    /// <param name="loader">The delegate that is invoked to produce the lazily initialized value when it is needed</param>
    protected Lazy(Func<T> loader)
    {
      _loader = loader;
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
      if (_loader == null)
      {
        if (_error != null)
          throw _error;

        return _result;
      }

      try
      {
        _result = _loader();
      }
      catch (Exception e)
      {
        _error = e;
        throw;
      }
      finally
      {
        _loader = null;
      }
      return _result;
    }
  }

  /// <summary>
  /// Provides support for lazy entity loading as well as access to index value concisting from one component
  /// </summary>
  /// <typeparam name="T">Specifies the type of entity that is being lazily loaded</typeparam>
  /// <typeparam name="I1">Type of the index component</typeparam>
  public sealed class Lazy<T, I1> : Lazy<T>
  {
    readonly I1 _key;

    internal Lazy(I1 key, Func<T> loader)
      : base(loader)
    {
      _key = key;
    }

    /// <summary>
    /// Index component
    /// </summary>
    public I1 Key { get { return _key; } }
  }

  /// <summary>
  /// Provides support for lazy entity loading as well as access to index value concisting from two components
  /// </summary>
  /// <typeparam name="T">Specifies the type of entity that is being lazily loaded</typeparam>
  /// <typeparam name="I1">Type of the first index component</typeparam>
  /// <typeparam name="I2">Type of the second index component</typeparam>
  public sealed class Lazy<T, I1, I2> : Lazy<T>
  {
    readonly Indexer<I1, I2> _source;

    internal Lazy(Indexer<I1, I2> source, Func<T> loader)
      : base(loader)
    {
      _source = source;
    }

    /// <summary>
    /// First index component
    /// </summary>
    public I1 Key1 { get { return _source.Key1; } }

    /// <summary>
    /// Second index component
    /// </summary>
    public I2 Key2 { get { return _source.Key2; } }
  }

  /// <summary>
  /// Provides support for lazy entity loading as well as access to index value concisting from three components
  /// </summary>
  /// <typeparam name="T">Specifies the type of entity that is being lazily loaded</typeparam>
  /// <typeparam name="I1">Type of the first index component</typeparam>
  /// <typeparam name="I2">Type of the second index component</typeparam>
  /// <typeparam name="I3">Type of the third index component</typeparam>
  public sealed class Lazy<T, I1, I2, I3> : Lazy<T>
  {
    readonly Indexer<I1, I2, I3> _source;

    internal Lazy(Indexer<I1, I2, I3> source, Func<T> loader)
      : base(loader)
    {
      _source = source;
    }

    /// <summary>
    /// First index component
    /// </summary>
    public I1 Key1 { get { return _source.Key1; } }

    /// <summary>
    /// Second index component
    /// </summary>
    public I2 Key2 { get { return _source.Key2; } }

    /// <summary>
    /// Third index component
    /// </summary>
    public I3 Key3 { get { return _source.Key3; } }
  }
}
