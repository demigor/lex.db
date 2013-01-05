using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;

namespace Lex.Db
{
  using Indexing;
  using Serialization;

  interface IIndex<T> where T : class
  {
    DbTable<T> Table { get; }
    MemberInfo[] Keys { get; }

    void Read(DataReader reader);
    void Write(DataWriter writer);
    void Purge();

    string Name { get; }
    int Count { get; }
  }

  interface IIndex<T, K> : IIndex<T> where T : class
  {
    int ExecuteCount(IndexQueryArgs<K> args);
    List<T> ExecuteToList(IndexQueryArgs<K> args);
    List<L> ExecuteToList<L>(IndexQueryArgs<K> args, Func<K, IKeyNode, L> selector);
  }

  class IndexQueryArgs<K>
  {
    public int? Skip, Take;
    public bool? MinInclusive, MaxInclusive;
    public K Min, Max;

    public IndexQueryArgs() { }

    public IndexQueryArgs(IndexQueryArgs<K> source)
    {
      Skip = source.Skip;
      Take = source.Take;
      MinInclusive = source.MinInclusive;
      MaxInclusive = source.MaxInclusive;
      Min = source.Min;
      Max = source.Max;
    }
  }

  public interface IIndexQuery<T, I1>
    where T : class
  {
    int Count();
    List<T> ToList();
    List<Lazy<T, I1>> ToLazyList();
    IIndexQuery<T, I1> Take(int count);
    IIndexQuery<T, I1> Skip(int count);
    IIndexQuery<T, I1> GreaterThan(I1 key, bool orEqual = false);
    IIndexQuery<T, I1> LessThan(I1 key, bool orEqual = false);
    IIndexQuery<T, I1> Key(I1 key);
    IIndexQuery<T, I1> Reset();
  }

  public interface IIndexQuery<T, I1, I2>
    where T : class
  {
    int Count();
    List<T> ToList();
    List<Lazy<T, I1, I2>> ToLazyList();
    IIndexQuery<T, I1, I2> Take(int count);
    IIndexQuery<T, I1, I2> Skip(int count);
    IIndexQuery<T, I1, I2> MinBound(I1 key1, I2 key2 = default(I2), bool inclusive = false);
    IIndexQuery<T, I1, I2> MaxBound(I1 key1, I2 key2 = default(I2), bool inclusive = false);
    IIndexQuery<T, I1, I2> Key(I1 key1, I2 key2);
    IIndexQuery<T, I1, I2> Reset();
  }

  public interface IIndexQuery<T, I1, I2, I3>
    where T : class
  {
    int Count();
    List<T> ToList();
    List<Lazy<T, I1, I2, I3>> ToLazyList();
    IIndexQuery<T, I1, I2, I3> Take(int count);
    IIndexQuery<T, I1, I2, I3> Skip(int count);
    IIndexQuery<T, I1, I2, I3> MinBound(I1 key1, I2 key2 = default(I2), I3 key3 = default(I3), bool inclusive = false);
    IIndexQuery<T, I1, I2, I3> MaxBound(I1 key1, I2 key2 = default(I2), I3 key3 = default(I3), bool inclusive = false);
    IIndexQuery<T, I1, I2, I3> Key(I1 key1, I2 key2, I3 key3);
    IIndexQuery<T, I1, I2, I3> Reset();
  }

  abstract class IndexQueryBase<T, K, R>
    where T : class
    where R : class
  {
    protected readonly IIndex<T, K> _index;
    protected readonly IndexQueryArgs<K> _args;

    public IndexQueryBase(IIndex<T, K> index)
    {
      _index = index;
      _args = new IndexQueryArgs<K>();
    }

    protected IndexQueryBase(IndexQueryBase<T, K, R> source)
    {
      _index = source._index;
      _args = new IndexQueryArgs<K>(source._args);
    }

    public int Count()
    {
      return _index.ExecuteCount(_args);
    }

    public List<T> ToList()
    {
      return _index.ExecuteToList(_args);
    }

    public R Take(int count)
    {
      return CloneAndUpdate(i => i._args.Take = count);
    }

    public R Skip(int count)
    {
      return CloneAndUpdate(i => i._args.Skip = count);
    }

    public R GreaterThan(K min, bool orEqual = false)
    {
      return CloneAndUpdate(i => { i._args.Min = min; i._args.MinInclusive = orEqual; });
    }

    public R LessThan(K max, bool orEqual = false)
    {
      return CloneAndUpdate(i => { i._args.Max = max; i._args.MaxInclusive = orEqual; });
    }

    public R Key(K key)
    {
      return CloneAndUpdate(i => { i._args.Max = key; i._args.Min = key; i._args.MaxInclusive = true; i._args.MinInclusive = true; });
    }

    public abstract R Reset();

    protected abstract R Clone();

    protected R CloneAndUpdate(Action<IndexQueryBase<T, K, R>> updater)
    {
      var result = Clone();

      updater(result as IndexQueryBase<T, K, R>);

      return result;
    }
  }

  class IndexQuery<T, I1> : IndexQueryBase<T, I1, IIndexQuery<T, I1>>, IIndexQuery<T, I1> where T : class
  {
    IndexQuery(IndexQuery<T, I1> source) : base(source) { }

    public IndexQuery(IIndex<T, I1> index) : base(index) { }

    public List<Lazy<T, I1>> ToLazyList()
    {
      return _index.ExecuteToList(_args, _index.Table.LazyCtor<I1>);
    }

    public override IIndexQuery<T, I1> Reset()
    {
      return new IndexQuery<T, I1>(_index);
    }

    protected override IIndexQuery<T, I1> Clone()
    {
      return new IndexQuery<T, I1>(this);
    }
  }

  class IndexQuery<T, I1, I2> : IndexQueryBase<T, Indexer<I1, I2>, IIndexQuery<T, I1, I2>>, IIndexQuery<T, I1, I2> where T : class
  {
    IndexQuery(IndexQuery<T, I1, I2> source) : base(source) { }

    public IndexQuery(IIndex<T, Indexer<I1, I2>> index) : base(index) { }

    public List<Lazy<T, I1, I2>> ToLazyList()
    {
      return _index.ExecuteToList(_args, _index.Table.LazyCtor<I1, I2>);
    }

    public IIndexQuery<T, I1, I2> MinBound(I1 key1, I2 key2 = default(I2), bool inclusive = false)
    {
      return GreaterThan(new Indexer<I1, I2>(key1, key2), inclusive);
    }

    public IIndexQuery<T, I1, I2> MaxBound(I1 key1, I2 key2 = default(I2), bool inclusive = false)
    {
      return LessThan(new Indexer<I1, I2>(key1, key2), inclusive);
    }

    public IIndexQuery<T, I1, I2> Key(I1 key1, I2 key2)
    {
      return Key(new Indexer<I1, I2>(key1, key2));
    }

    public override IIndexQuery<T, I1, I2> Reset()
    {
      return new IndexQuery<T, I1, I2>(_index);
    }

    protected override IIndexQuery<T, I1, I2> Clone()
    {
      return new IndexQuery<T, I1, I2>(this);
    }
  }

  class IndexQuery<T, I1, I2, I3> : IndexQueryBase<T, Indexer<I1, I2, I3>, IIndexQuery<T, I1, I2, I3>>, IIndexQuery<T, I1, I2, I3> where T : class
  {
    IndexQuery(IndexQuery<T, I1, I2, I3> source) : base(source) { }

    public IndexQuery(IIndex<T, Indexer<I1, I2, I3>> index) : base(index) { }

    public List<Lazy<T, I1, I2, I3>> ToLazyList()
    {
      return _index.ExecuteToList(_args, _index.Table.LazyCtor<I1, I2, I3>);
    }

    public IIndexQuery<T, I1, I2, I3> MinBound(I1 key1, I2 key2 = default(I2), I3 key3 = default(I3), bool inclusive = false)
    {
      return GreaterThan(new Indexer<I1, I2, I3>(key1, key2, key3), inclusive);
    }

    public IIndexQuery<T, I1, I2, I3> MaxBound(I1 key1, I2 key2 = default(I2), I3 key3 = default(I3), bool inclusive = false)
    {
      return LessThan(new Indexer<I1, I2, I3>(key1, key2, key3), inclusive);
    }

    public IIndexQuery<T, I1, I2, I3> Key(I1 key1, I2 key2, I3 key3)
    {
      return Key(new Indexer<I1, I2, I3>(key1, key2, key3));
    }

    public override IIndexQuery<T, I1, I2, I3> Reset()
    {
      return new IndexQuery<T, I1, I2, I3>(_index);
    }

    protected override IIndexQuery<T, I1, I2, I3> Clone()
    {
      return new IndexQuery<T, I1, I2, I3>(this);
    }
  }
}
