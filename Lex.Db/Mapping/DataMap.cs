using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Lex.Db.Mapping
{
  using Indexing;

  [DebuggerDisplay("{Begin} - {End}")]
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

    internal void Set(long begin, int length)
    {
#if DEBUG
      if (begin < 0)
        throw new ArgumentException("begin");

      if (length <= 0)
        throw new ArgumentException("length");

      _begin = begin;
      _end = begin + length;
#else
      Begin = begin;
      End = begin + length;
#endif
    }
  }

  class DataMap<K> : List<Allocation>, IComparer<Allocation>
  {
    public DataMap(RBTree<K, KeyNode<K>> tree)
    {
      Capacity = tree.Count;

      Gather(tree.Root);

      Sort(this);

#if DEBUG
      Check();
#endif

      Compactify();
    }

    void Compactify()
    {
      for (int i = Count - 1; i > 0; i--)
      {
        var curr = this[i];
        var prev = this[i - 1];

        if (prev.End == curr.Begin)
        {
          prev.End = curr.End;
          RemoveAt(i);
        }
#if DEBUG
        else
        {
          if (prev.End > curr.Begin)
            throw new InvalidOperationException();
        }
#endif
      }
    }

#if DEBUG

    void Check()
    {
      long x = 0;

      foreach (var alloc in this)
      {
        if (alloc.Begin < x)
          throw new InvalidOperationException("1");

        if (alloc.Begin >= alloc.End)
          throw new InvalidOperationException("2");

        x = alloc.End;
      }
    }

#endif

    void Gather(KeyNode<K> node)
    {
      if (node != null)
      {
        Add(Allocation.New(node));

        Gather(node.Left);
        Gather(node.Right);
      }
    }

    public void Alloc(KeyNode<K> node)
    {
      node.Offset = DoAlloc(node.Length);
    }

    long DoAlloc(int length)
    {
      var c = Count - 1;

      if (c < 0)
      {
        Add(new Allocation(0, length));
        return 0;
      }

      var next = default(Allocation);

      if (c == 0)
      {
        next = this[0];
        
        var diff = next.Begin - length;
        
        if (diff == 0)
        {
          next.Begin = 0;
          return 0;
        }

        if (diff > 0)
        {
          Insert(0, new Allocation(0, length));
          return 0;
        }

      }
      else
        for (int i = 0; i < c; i++)
        {
          var curr = this[i];
          next = this[i + 1];

          var space = next.Begin - curr.End;
          var diff = space - length;

          // found exact slot - merge to next, remove current
          if (diff == 0)
          {
            next.Begin = curr.Begin;
            RemoveAt(i);
            return curr.End;
          }

          // found bigger slot - expand current
          if (diff > 0)
          {
            next = curr;
            break;
          }
        }

      var result = next.End;
      next.End += length;

      return result;
    }

    Allocation search = new Allocation(0, 1);

    void DoFree(long offset, int length)
    {
      search.Set(offset, length);

      var i = BinarySearch(search, this);

      if (i < 0)
      {
        i = ~i;

        var left = this[i - 1];

        if (left.End == search.End) // exact match on End
          left.End = offset;
        else
        {
          if (left.End < offset)
            throw new InvalidOperationException();

          Insert(i, new Allocation(search.End, left.End));
          left.End = offset;
        }
      }
      else
      // exact match on Begin
      {
        var a = this[i];
        var begin = a.Begin + length;
        if (begin < a.End)
          a.Begin = begin;
        else
        {
          if (begin != a.End)
            throw new InvalidOperationException();

          RemoveAt(i);
        }
      }
    }

    public void Realloc(KeyNode<K> node, int length)
    {
      DoFree(node.Offset, node.Length);

      node.Offset = DoAlloc(length);
      node.Length = length;
    }

    public void Free(KeyNode<K> node)
    {
      DoFree(node.Offset, node.Length);
    }

    public long Max
    {
      get
      {
        var c = Count - 1;

        if (c >= 0)
          return this[c].End;

        return 0;
      }
    }

    public int Compare(Allocation x, Allocation y)
    {
      return x.Begin.CompareTo(y.Begin);
    }
  }
}
