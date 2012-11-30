using System;

namespace Lex.Db
{
  using Indexing;

  public class Lazy<T>
  {
    T _result;
    Func<T> _loader;
    Exception _error;

    public Lazy(Func<T> loader)
    {
      _loader = loader;
    }

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


  public sealed class Lazy<T, I1> : Lazy<T>
  {
    readonly I1 _key;

    internal Lazy(I1 key, Func<T> loader)
      : base(loader)
    {
      _key = key;
    }

    public I1 Key { get { return _key; } }
  }

  public sealed class Lazy<T, I1, I2> : Lazy<T>
  {
    readonly Indexer<I1, I2> _source;

    internal Lazy(Indexer<I1, I2> source, Func<T> loader)
      : base(loader)
    {
      _source = source;
    }

    public I1 Key1 { get { return _source.Key1; } }
    public I2 Key2 { get { return _source.Key2; } }
  }

  public sealed class Lazy<T, I1, I2, I3> : Lazy<T>
  {
    readonly Indexer<I1, I2, I3> _source;

    internal Lazy(Indexer<I1, I2, I3> source, Func<T> loader)
      : base(loader)
    {
      _source = source;
    }

    public I1 Key1 { get { return _source.Key1; } }
    public I2 Key2 { get { return _source.Key2; } }
    public I3 Key3 { get { return _source.Key3; } }
  }
}
