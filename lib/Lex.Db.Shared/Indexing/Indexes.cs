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

    void Read(DataReader reader, DbFormat format);
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
    public Func<K, bool> Filter;

    public IndexQueryArgs() { }

    public IndexQueryArgs(IndexQueryArgs<K> source)
    {
      Filter = source.Filter;
      Skip = source.Skip;
      Take = source.Take;
      MinInclusive = source.MinInclusive;
      MaxInclusive = source.MaxInclusive;
      Min = source.Min;
      Max = source.Max;
    }
  }

  /// <summary>
  /// Query via index interface
  /// </summary>
  public interface IIndexQuery
  {
    /// <summary>
    /// Counts the number of the indexed entities identitified by the query
    /// </summary>
    /// <returns>Number of entities identitified by the query</returns>
    int Count();

    /// <summary>
    /// Returns the list of PK values for entities identitified by the query
    /// </summary>
    /// <typeparam name="K">Type of the primary key</typeparam>
    /// <returns>List of PK values for entities identitified by the query</returns>
    List<K> ToIdList<K>();
  }

  /// <summary>
  /// Typed query via index interface
  /// </summary>
  /// <typeparam name="T">Entity type</typeparam>
  public interface IIndexQuery<T> : IIndexQuery where T : class
  {
    /// <summary>
    /// Loads entities returned by the query
    /// </summary>
    /// <returns>List of entities identitified by the query</returns>
    List<T> ToList();
  }

  /// <summary>
  /// Typed query via index interface
  /// </summary>
  /// <typeparam name="T">Entity type</typeparam>
  /// <typeparam name="I1">Type of the indexed member</typeparam>
  public interface IIndexQuery<T, I1> : IIndexQuery<T>
    where T : class
  {
    /// <summary>
    /// Lazy loads entities returned by the query
    /// </summary>
    /// <returns>List of lazy-loaded entities identitified by the query</returns>
    List<Lazy<T, I1>> ToLazyList();

    /// <summary>
    /// Returns a specified number of contiguous entities from the start of a query.
    /// </summary>
    /// <param name="count">The number of elements to return.</param>
    /// <returns>A new query that returns the specified number of entities.</returns>
    IIndexQuery<T, I1> Take(int count);

    /// <summary>
    /// Bypasses a specified number of entities in a query and then returns the remaining entities.
    /// </summary>
    /// <param name="count">The number of entities to skip before returning the remaining entities.</param>
    /// <returns>A new query that bypasses the specified number of entities.</returns>
    IIndexQuery<T, I1> Skip(int count);

    /// <summary>
    /// Returns entities with specified lower bound
    /// </summary>
    IIndexQuery<T, I1> GreaterThan(I1 key, bool orEqual = false);

    /// <summary>
    /// Returns entities with specified upper bound
    /// </summary>
    IIndexQuery<T, I1> LessThan(I1 key, bool orEqual = false);

    /// <summary>
    /// Returns entities with specified key value 
    /// </summary>
    /// <param name="key">Key value</param>
    /// <returns>A new query that returns entities with specified key value.</returns>
    IIndexQuery<T, I1> Key(I1 key);

    /// <summary>
    /// Returns entities with filtered key values
    /// </summary>
    /// <param name="predicate">Predicate function to filter entities</param>
    /// <returns>A new query that returns entities with filtered key values.</returns>
    IIndexQuery<T, I1> Where(Func<I1, bool> predicate);
  }

  /// <summary>
  /// Typed query via 2-component index interface 
  /// </summary>
  /// <typeparam name="T">Entity type</typeparam>
  /// <typeparam name="I1">Type of the first component indexed member</typeparam>
  /// <typeparam name="I2">Type of the second component indexed member</typeparam>
  public interface IIndexQuery<T, I1, I2> : IIndexQuery<T>
    where T : class
  {
    /// <summary>
    /// Lazy loads entities returned by the query
    /// </summary>
    /// <returns>List of lazy-loaded entities identitified by the query</returns>
    List<Lazy<T, I1, I2>> ToLazyList();

    /// <summary>
    /// Returns a specified number of contiguous entities from the start of a query.
    /// </summary>
    /// <param name="count">The number of elements to return.</param>
    /// <returns>A new query that returns the specified number of entities.</returns>
    IIndexQuery<T, I1, I2> Take(int count);

    /// <summary>
    /// Bypasses a specified number of entities in a query and then returns the remaining entities.
    /// </summary>
    /// <param name="count">The number of entities to skip before returning the remaining entities.</param>
    /// <returns>A new query that bypasses the specified number of entities.</returns>
    IIndexQuery<T, I1, I2> Skip(int count);

    /// <summary>
    /// Returns entities with specified 2-component lower bound
    /// </summary>
    IIndexQuery<T, I1, I2> GreaterThan(I1 key1, I2 key2 = default(I2), bool inclusive = false);

    /// <summary>
    /// Returns entities with specified 2-component upper bound
    /// </summary>
    IIndexQuery<T, I1, I2> LessThan(I1 key1, I2 key2 = default(I2), bool inclusive = false);

    /// <summary>
    /// Returns entities with specified 2-component key value 
    /// </summary>
    /// <param name="key1">First component key value</param>
    /// <param name="key2">Second component key value</param>
    /// <returns>A new query that returns entities with specified key value.</returns>
    IIndexQuery<T, I1, I2> Key(I1 key1, I2 key2);

    /// <summary>
    /// Returns entities with filtered key values
    /// </summary>
    /// <param name="predicate">Predicate function to filter entities</param>
    /// <returns>A new query that returns entities with filtered key values.</returns>
    IIndexQuery<T, I1, I2> Where(Func<I1, I2, bool> predicate);
  }

  /// <summary>
  /// Typed query via 3-component index interface
  /// </summary>
  /// <typeparam name="T">Entity type</typeparam>
  /// <typeparam name="I1">Type of the first component indexed member</typeparam>
  /// <typeparam name="I2">Type of the second component indexed member</typeparam>
  /// <typeparam name="I3">Type of the third component indexed member</typeparam>
  public interface IIndexQuery<T, I1, I2, I3> : IIndexQuery<T>
    where T : class
  {
    /// <summary>
    /// Lazy loads entities returned by the query
    /// </summary>
    /// <returns>List of lazy-loaded entities identitified by the query</returns>
    List<Lazy<T, I1, I2, I3>> ToLazyList();

    /// <summary>
    /// Returns a specified number of contiguous entities from the start of a query.
    /// </summary>
    /// <param name="count">The number of elements to return.</param>
    /// <returns>A new query that returns the specified number of entities.</returns>
    IIndexQuery<T, I1, I2, I3> Take(int count);

    /// <summary>
    /// Bypasses a specified number of entities in a query and then returns the remaining entities.
    /// </summary>
    /// <param name="count">The number of entities to skip before returning the remaining entities.</param>
    /// <returns>A new query that bypasses the specified number of entities.</returns>
    IIndexQuery<T, I1, I2, I3> Skip(int count);

    /// <summary>
    /// Returns entities with specified 3-component lower bound
    /// </summary>
    IIndexQuery<T, I1, I2, I3> GreaterThan(I1 key1, I2 key2 = default(I2), I3 key3 = default(I3), bool inclusive = false);

    /// <summary>
    /// Returns entities with specified 3-component upper bound
    /// </summary>
    IIndexQuery<T, I1, I2, I3> LessThan(I1 key1, I2 key2 = default(I2), I3 key3 = default(I3), bool inclusive = false);

    /// <summary>
    /// Returns entities with specified 3-component key value 
    /// </summary>
    /// <param name="key1">First component key value</param>
    /// <param name="key2">Second component key value</param>
    /// <param name="key3">Third component key value</param>
    /// <returns>A new query that returns entities with specified key value.</returns>
    IIndexQuery<T, I1, I2, I3> Key(I1 key1, I2 key2, I3 key3);

    /// <summary>
    /// Returns entities with filtered key values
    /// </summary>
    /// <param name="predicate">Predicate function to filter entities</param>
    /// <returns>A new query that returns entities with filtered key values.</returns>
    IIndexQuery<T, I1, I2, I3> Where(Func<I1, I2, I3, bool> predicate);
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

    public List<PK> ToIdList<PK>()
    {
      _index.Table.GetPrimaryIndex<PK>();
      return _index.ExecuteToList<PK>(_args, (k, pk) => (PK)pk.Key);
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

    public R Where(Func<K, bool> predicate)
    {
      return CloneAndUpdate(i => i._args.Filter = predicate);
    }

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

    public IIndexQuery<T, I1, I2> GreaterThan(I1 key1, I2 key2 = default(I2), bool inclusive = false)
    {
      return GreaterThan(new Indexer<I1, I2>(key1, key2), inclusive);
    }

    public IIndexQuery<T, I1, I2> LessThan(I1 key1, I2 key2 = default(I2), bool inclusive = false)
    {
      return LessThan(new Indexer<I1, I2>(key1, key2), inclusive);
    }

    public IIndexQuery<T, I1, I2> Key(I1 key1, I2 key2)
    {
      return Key(new Indexer<I1, I2>(key1, key2));
    }

    protected override IIndexQuery<T, I1, I2> Clone()
    {
      return new IndexQuery<T, I1, I2>(this);
    }

    public IIndexQuery<T, I1, I2> Where(Func<I1, I2, bool> predicate)
    {
      return base.Where(k => predicate(k.Key1, k.Key2));
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

    public IIndexQuery<T, I1, I2, I3> GreaterThan(I1 key1, I2 key2 = default(I2), I3 key3 = default(I3), bool inclusive = false)
    {
      return GreaterThan(new Indexer<I1, I2, I3>(key1, key2, key3), inclusive);
    }

    public IIndexQuery<T, I1, I2, I3> LessThan(I1 key1, I2 key2 = default(I2), I3 key3 = default(I3), bool inclusive = false)
    {
      return LessThan(new Indexer<I1, I2, I3>(key1, key2, key3), inclusive);
    }

    public IIndexQuery<T, I1, I2, I3> Key(I1 key1, I2 key2, I3 key3)
    {
      return Key(new Indexer<I1, I2, I3>(key1, key2, key3));
    }

    protected override IIndexQuery<T, I1, I2, I3> Clone()
    {
      return new IndexQuery<T, I1, I2, I3>(this);
    }

    public IIndexQuery<T, I1, I2, I3> Where(Func<I1, I2, I3, bool> predicate)
    {
      return base.Where(k => predicate(k.Key1, k.Key2, k.Key3));
    }
  }
}
