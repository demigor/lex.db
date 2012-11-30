using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Lex.Db.Mapping
{
  using Indexing;

  [DebuggerDisplay("{Begin}, {End}")]
  class Allocation
  {
    public static Allocation New<K>(KeyNode<K> node)
    {
      return new Allocation(node.Offset, node.Offset + node.Length);
    }

    public Allocation(long begin, long end)
    {
#if DEBUG
      if (begin < 0)
        throw new ArgumentException("begin");

      if (end <= begin)
        throw new ArgumentException("end");

      _end = end;
      _begin = begin;
#else
      End = end;
      Begin = begin;
#endif
    }

#if DEBUG
    long _begin, _end;
    public long Begin
    {
      get
      {
        return _begin;
      }
      set
      {
        if (value >= _end)
          throw new ArgumentException();

        _begin = value;
      }
    }
    public long End
    {
      get
      {
        return _end;
      }
      set
      {
        if (value <= _begin)
          throw new ArgumentException();

        _end = value;
      }
    }
#else
    public long Begin;
    public long End;
#endif
  }

  class DataMap<K> : IComparer<Allocation>, IEnumerable<Allocation>
  {
    readonly List<Allocation> _allocs;

    public DataMap()
    {
      _allocs = new List<Allocation>();
    }

    public DataMap(RBTree<K, KeyNode<K>> tree)
    {
      _allocs = new List<Allocation>(tree.Count);
      Gather(tree.Root);
      _allocs = _allocs.OrderBy(i => i.Begin).ToList();
      Compactify();
    }

    void Gather(KeyNode<K> node)
    {
      if (node != null)
      {
        _allocs.Add(Allocation.New(node));

        Gather(node.Left);
        Gather(node.Right);
      }
    }

    void Compactify()
    {
      if (_allocs.Count <= 1)
        return;

//#if DEBUG
//      Check();
//#endif

      var currIndex = _allocs.Count - 1;

      while (currIndex >= 0)
      {
        var currAlloc = _allocs[currIndex];

        var mergeAlloc = default(Allocation);
        var mergeIndex = currIndex;

        var scanIndex = currIndex - 1;
        var prevAlloc = currAlloc;

        while (scanIndex >= 0)
        {
          var scanAlloc = _allocs[scanIndex];

          if (scanAlloc.End > prevAlloc.Begin)
            throw new InvalidOperationException();

          if (scanAlloc.End == prevAlloc.Begin)
          {
            mergeAlloc = scanAlloc;
            mergeIndex = scanIndex;
          }
          else
            break;

          --scanIndex;
          prevAlloc = scanAlloc;
        }

        if (mergeAlloc != null)
        {
          mergeAlloc.End = currAlloc.End;
          _allocs.RemoveRange(mergeIndex + 1, currIndex - mergeIndex);
          currIndex = mergeIndex - 1;
          continue;
        }

        --currIndex;
      }
    }

    public void Free(KeyNode<K> node)
    {
      DoFree(node);
    }

    void DoFree(KeyNode<K> node)
    {
      var cut = Allocation.New(node);
      var idx = _allocs.BinarySearch(cut, this);
      if (idx < 0)
        idx = Math.Max(~idx - 1, 0);

      while (idx < _allocs.Count)
      {
        var alloc = _allocs[idx];

        #region simple cases
        if (alloc.Begin >= cut.End)
          break;

        if (alloc.End < cut.Begin)
        {
          idx++;
          continue;
        }

        #endregion

        if (alloc.Begin < cut.Begin)
        {
          if (alloc.End > cut.End)
          {
            var end = alloc.End;
            alloc.End = cut.Begin;

            _allocs.Insert(idx + 1, new Allocation(cut.End, end));
            return;
          }
          alloc.End = cut.Begin;
          idx++;
          continue;
        }

        if (alloc.End <= cut.End)
        {
          _allocs.RemoveAt(idx);
          continue;
        }

        alloc.Begin = cut.End;
        ++idx;
      }
    }

/*
    void Check()
    {
      long x = 0;

      foreach (var alloc in _allocs)
      {
        if (alloc.Begin < x)
          throw new InvalidOperationException("1");

        if (alloc.Begin >= alloc.End)
          throw new InvalidOperationException("2");

        x = alloc.End;
      }
    }
*/

    public void Realloc(KeyNode<K> node, int size)
    {
      Free(node);

      node.Length = size;

      Alloc(node);
    }

    public void Alloc(KeyNode<K> add)
    {
      DoAlloc(add);
    }

    void DoAlloc(KeyNode<K> add)
    {
      if (_allocs.Count == 0)
      {
        add.Offset = 0;
        _allocs.Add(Allocation.New(add));
        return;
      }

      var prev = default(Allocation);
      var idx = 0;

      foreach (var alloc in _allocs)
      {
        if (prev == null)
        {
          if (alloc.Begin > add.Length)
          {
            alloc.Begin -= add.Length;
            add.Offset = alloc.Begin;
            return;
          }
        }
        else
        {
          if (prev.End + add.Length <= alloc.Begin)
          {
            add.Offset = prev.End;
            prev.End += add.Length;

            if (prev.End == alloc.Begin)
            {
              alloc.Begin = prev.Begin;
              _allocs.RemoveAt(idx);
            }

            return;
          }
        }

        prev = alloc;
        ++idx;
      }

      add.Offset = prev.End;
      prev.End += add.Length;
    }

    int IComparer<Allocation>.Compare(Allocation x, Allocation y)
    {
      if (x.Begin < y.Begin)
        return -1;

      return x.Begin > y.Begin ? 1 : 0;
    }

    public int Count { get { return _allocs.Count; } }

    public long Max
    {
      get
      {
        var last = _allocs.LastOrDefault();
        return last == null ? 0 : last.End;
      }
    }

    IEnumerator<Allocation> IEnumerable<Allocation>.GetEnumerator()
    {
      return _allocs.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return _allocs.GetEnumerator();
    }
  }
}
