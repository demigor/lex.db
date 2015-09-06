using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Lex.Db.Indexing
{
  ///<summary>
  ///Colour of the node
  ///</summary>
  enum RBTreeColor : byte
  {
    ///<summary>
    ///Red
    ///</summary>
    Red,
    ///<summary>
    ///Black
    ///</summary>
    Black
  }

  [DebuggerDisplay("{Key}")]
  class RBTreeNode<TKey, TNode> where TNode : RBTreeNode<TKey, TNode>
  {
    public TKey Key;
    public TNode Parent, Left, Right;
    public RBTreeColor Color;
  }

  class RBTree<TKey, TNode> : IEnumerable<TNode> where TNode : RBTreeNode<TKey, TNode>, new()
  {
    public readonly IComparer<TKey> Comparer;
    internal static readonly Func<TNode> _ctor = Ctor<TNode>.New;

    public RBTree(IComparer<TKey> comparer)
    {
      Comparer = comparer ?? Comparer<TKey>.Default;
    }

    #region Count Property

    int _count;
    public int Count { get { return _count; } }

    #endregion

    #region Root Property

    TNode _root;
    public TNode Root
    {
      get { return _root; }
      set
      {
        _root = value;
        _count = GetCount(value);
      }
    }

    static int GetCount(TNode node)
    {
      return node == null ? 0 : 1 + GetCount(node.Left) + GetCount(node.Right);
    }

    #endregion

    ///<summary>
    ///Remove all items
    ///</summary>
    public void Clear()
    {
      _root = null;
      _count = 0;
    }

    ///<summary>
    ///Add new key into the tree
    ///
    ///This operation is O(logN) operation
    ///</summary>
    ///<exception cref="System.ArgumentException">In case the key is already in the tree</exception>
    public TNode Add(TKey key)
    {
      var insert = true;
      var result = Traverse(key, ref insert);

      if (!insert)
        throw new ArgumentException();

      _count++;

      return result;
    }

    ///<summary>
    ///Add new key into the tree or get existing node
    ///This operation is O(logN) operation
    ///</summary>
    public TNode AddOrGet(TKey key)
    {
      var insert = true;

      var result = Traverse(key, ref insert);
      if (insert)
        _count++;

      return result;
    }

    ///<summary>
    ///Remove key from the dictionary
    ///This operation is O(logN) operation
    ///</summary>
    public bool Remove(TKey key)
    {
      var result = Find(key);
      if (result == null)
        return false;

      _count--;

      Delete(result);
      return true;
    }

    ///<summary>
    ///Remove node from the dictionary
    ///This operation is O(1) operation
    ///</summary>
    public bool Remove(TNode node)
    {
      Delete(node);
      _count--;
      return true;
    }

    /// <summary>
    /// Delete the node z, and free up the space
    /// </summary>
    protected void Delete(TNode z)
    {
      TNode y;

      if (z.Left == null || z.Right == null)
        y = z;
      else
        y = Next(z);

      var x = y.Left ?? y.Right;

      if (x != null)
        x.Parent = y.Parent;

      if (y.Parent == null)
        _root = x;
      else
      {
        if (y == y.Parent.Left)
          y.Parent.Left = x;
        else
          y.Parent.Right = x;
      }

      if (y != z)
      {
        //we must replace 'z' with 'y' node
        CopyNode(z, y);

        if (z == _root)
          _root = y;

        //we do this all above instead of the following line in original code
        //to provide guarantee of the persistence of the node in the tree
        //z.mKey = y.mKey;
      }

      if (y.Color == RBTreeColor.Black && x != null)
        DeleteFix(x);
    }

    /// <summary>
    /// Restore the reb-black properties after a delete
    /// </summary>
    /// <param name="x"></param>
    protected void DeleteFix(TNode x)
    {
      while (x != _root && x.Color == RBTreeColor.Black)
      {
        TNode w;

        if (x == x.Parent.Left)
        {
          w = x.Parent.Right;
          if (w == null)
          {
            x = x.Parent;
            continue;
          }


          if (w.Color == RBTreeColor.Red)
          {
            w.Color = RBTreeColor.Black;
            x.Parent.Color = RBTreeColor.Red;
            LeftRotate(x.Parent);
            w = x.Parent.Right;
          }

          if (w == null)
          {
            x = x.Parent;
            continue;
          }

          if ((w.Left == null || w.Left.Color == RBTreeColor.Black) &&
              (w.Right == null || w.Right.Color == RBTreeColor.Black))
          {
            w.Color = RBTreeColor.Red;
            x = x.Parent;
          }
          else
          {
            if (w.Right == null || w.Right.Color == RBTreeColor.Black)
            {
              if (w.Left != null)
                w.Left.Color = RBTreeColor.Black;

              w.Color = RBTreeColor.Red;
              RightRotate(w);
              w = x.Parent.Right;
            }

            w.Color = x.Parent.Color;
            x.Parent.Color = RBTreeColor.Black;

            if (w.Right != null)
              w.Right.Color = RBTreeColor.Black;

            LeftRotate(x.Parent);
            x = _root;
          }
        }
        else
        {
          w = x.Parent.Left;
          if (w == null)
          {
            x = x.Parent;
            continue;
          }

          if (w.Color == RBTreeColor.Red)
          {
            w.Color = RBTreeColor.Black;
            x.Parent.Color = RBTreeColor.Red;
            RightRotate(x.Parent);
            w = x.Parent.Left;
          }

          if (w == null)
          {
            x = x.Parent;
            continue;
          }

          if ((w.Right == null || w.Right.Color == RBTreeColor.Black) &&
              (w.Left == null || w.Left.Color == RBTreeColor.Black))
          {
            w.Color = RBTreeColor.Red;
            x = x.Parent;
          }
          else
          {
            if (w.Left == null || w.Left.Color == RBTreeColor.Black)
            {
              if (w.Right != null)
                w.Right.Color = RBTreeColor.Black;

              w.Color = RBTreeColor.Red;
              LeftRotate(w);
              w = x.Parent.Left;
            }

            w.Color = x.Parent.Color;
            x.Parent.Color = RBTreeColor.Black;

            if (w.Left != null)
              w.Left.Color = RBTreeColor.Black;

            RightRotate(x.Parent);
            x = _root;
          }
        }
      }

      x.Color = RBTreeColor.Black;
    }

    static void CopyNode(TNode source, TNode target)
    {
      if (source.Left != null)
        source.Left.Parent = target;

      target.Left = source.Left;

      if (source.Right != null)
        source.Right.Parent = target;

      target.Right = source.Right;

      if (source.Parent != null)
        if (source.Parent.Left == source)
          source.Parent.Left = target;
        else
          source.Parent.Right = target;

      target.Color = source.Color;
      target.Parent = source.Parent;
    }

    ///<summary>
    ///Find key in the dictionary
    ///This operation is O(logN) operation
    ///</summary>
    public TNode Find(TKey key)
    {
      //walk down the tree
      var x = _root;
      while (x != null)
      {
        var cmp = Comparer.Compare(key, x.Key);
        if (cmp < 0)
          x = x.Left;
        else if (cmp > 0)
          x = x.Right;
        else
          return x;
      }
      return null;
    }

    ///<summary>
    ///Go trough tree and find the node by the key.
    ///Might add new node if node doesn't exist.
    ///</summary>
    internal TNode Traverse(TKey key, ref bool insert)
    {
      //walk down the tree
      TNode y = null;
      var x = _root;
      while (x != null)
      {
        y = x;
        var cmp = Comparer.Compare(key, x.Key);

        if (cmp < 0)
          x = x.Left;
        else if (cmp > 0)
          x = x.Right;
        else
        {
          insert = false;
          return x;
        }
      }

      //x is null. return null if node must not be inserted
      if (!insert)
        return null;

      //x is null and insert operation is requested
      //create new node
      var z = _ctor();
      z.Key = key;
      z.Parent = y;

      if (y == null)
        _root = z;
      else
      {
        var cmp = Comparer.Compare(z.Key, y.Key);
        if (cmp == 0)
          cmp = 1;

        if (cmp < 0)
          y.Left = z;
        else
          y.Right = z;
      }
      z.Color = RBTreeColor.Red;

      Balance(z);
      _root.Color = RBTreeColor.Black;
      return z;
    }

    ///<summary>
    ///Balance tree past inserting
    ///</summary>
    protected void Balance(TNode z)
    {
      //Having added a red node, we must now walk back up the tree balancing
      //it, by a series of rotations and changing of colours
      var x = z;

      //While we are not at the top and our parent node is red
      //N.B. Since the root node is garanteed black, then we
      //are also going to stop if we are the child of the root
      while (x != _root && (x.Parent.Color == RBTreeColor.Red))
      {
        //if our parent is on the left side of our grandparent
        if (x.Parent == x.Parent.Parent.Left)
        {
          //get the right side of our grandparent (uncle?)
          var y = x.Parent.Parent.Right;
          if (y != null && y.Color == RBTreeColor.Red)
          {
            //make our parent black
            x.Parent.Color = RBTreeColor.Black;
            //make our uncle black
            y.Color = RBTreeColor.Black;
            //make our grandparent red
            x.Parent.Parent.Color = RBTreeColor.Red;
            //now consider our grandparent
            x = x.Parent.Parent;
          }
          else
          {
            //if we are on the right side of our parent
            if (x == x.Parent.Right)
            {
              //Move up to our parent
              x = x.Parent;
              LeftRotate(x);
            }

            /* make our parent black */
            x.Parent.Color = RBTreeColor.Black;
            /* make our grandparent red */
            x.Parent.Parent.Color = RBTreeColor.Red;
            /* right rotate our grandparent */
            RightRotate(x.Parent.Parent);
          }
        }
        else
        {
          //everything here is the same as above, but
          //exchanging left for right
          var y = x.Parent.Parent.Left;
          if (y != null && y.Color == RBTreeColor.Red)
          {
            x.Parent.Color = RBTreeColor.Black;
            y.Color = RBTreeColor.Black;
            x.Parent.Parent.Color = RBTreeColor.Red;

            x = x.Parent.Parent;
          }
          else
          {
            if (x == x.Parent.Left)
            {
              x = x.Parent;
              RightRotate(x);
            }

            x.Parent.Color = RBTreeColor.Black;
            x.Parent.Parent.Color = RBTreeColor.Red;
            LeftRotate(x.Parent.Parent);
          }
        }
      }
      _root.Color = RBTreeColor.Black;
    }

    /*
         Rotate our tree Left
    
                     X        rb_left_rotate(X)--->            Y
                   /   \                                     /   \
                  A     Y                                   X     C
                      /   \                               /   \
                     B     C                             A     B
    
         N.B. This does not change the ordering.
    
         We assume that neither X or Y is NULL
    */
    protected void LeftRotate(TNode x)
    {
      // set Y
      var y = x.Right;

      // Turn Y's left subtree into X's right subtree (move B)
      x.Right = y.Left;

      // If B is not null, set it's parent to be X
      if (y.Left != null)
        y.Left.Parent = x;

      // Set Y's parent to be what X's parent was
      y.Parent = x.Parent;

      // if X was the root
      if (x.Parent == null)
        _root = y;
      else
      {
        // Set X's parent's left or right pointer to be Y
        if (x == x.Parent.Left)
          x.Parent.Left = y;
        else
          x.Parent.Right = y;
      }

      // Put X on Y's left
      y.Left = x;

      // Set X's parent to be Y
      x.Parent = y;
    }

    /*
         Rotate our tree Right
    
                     X                                         Y
                   /   \                                     /   \
                  A     Y     < ---rb_right_rotate(Y)       X     C
                      /   \                               /   \
                     B     C                             A     B
    
         N.B. This does not change the ordering.
    
         We assume that neither X or Y is NULL
    */
    protected void RightRotate(TNode y)
    {
      // set X
      var x = y.Left;

      // Turn X's right subtree into Y's left subtree (move B)
      y.Left = x.Right;

      // If B is not null, set it's parent to be Y
      if (x.Right != null)
        x.Right.Parent = y;

      // Set X's parent to be what Y's parent was
      x.Parent = y.Parent;

      // if Y was the root
      if (y.Parent == null)
        _root = x;
      else
      {
        // Set Y's parent's left or right pointer to be X
        if (y == y.Parent.Left)
          y.Parent.Left = x;
        else
          y.Parent.Right = x;
      }

      // Put Y on X's right
      x.Right = y;

      // Set Y's parent to be X
      y.Parent = x;
    }

    ///<summary>
    ///Return a pointer to the smallest key greater than x
    ///</summary>
    public static TNode Next(TNode x)
    {
      TNode y;

      if (x.Right != null)
      {
        // If right is not NULL then go right one and
        // then keep going left until we find a node with
        // no left pointer.
        for (y = x.Right; y.Left != null; y = y.Left) { }
      }
      else
      {
        // Go up the tree until we get to a node that is on the
        // left of its parent (or the root) and then return the
        // parent.
        y = x.Parent;
        while (y != null && x == y.Right)
        {
          x = y;
          y = y.Parent;
        }
      }
      return y;
    }
    ///<summary>
    ///Return a pointer to the largest key smaller than x
    ///</summary>
    public TNode Prev(TNode x)
    {
      TNode y;

      if (x.Left != null)
      {
        // If left is not NULL then go left one and
        // then keep going right until we find a node with
        // no right pointer.
        for (y = x.Left; y.Right != null; y = y.Right) { }
      }
      else
      {
        // Go up the tree until we get to a node that is on the
        // right of its parent (or the root) and then return the
        // parent.
        y = x.Parent;
        while (y != null && x == y.Left)
        {
          x = y;
          y = y.Parent;
        }
      }
      return y;
    }

    ///<summary>
    ///Get first node
    ///This operation is O(logN) operation
    ///</summary>
    public TNode First()
    {
      return First(_root);
    }

    ///<summary>
    ///Get first node
    ///This operation is O(logN) operation
    ///</summary>
    public static TNode First(TNode root)
    {
      var x = root;
      var y = default(TNode);

      // Keep going left until we hit a NULL
      while (x != null)
      {
        y = x;
        x = x.Left;
      }

      return y;
    }

    ///<summary>
    ///Get last node
    ///This operation is O(logN) operation
    ///</summary>
    public TNode Last()
    {
      var x = _root;
      var y = default(TNode);

      // Keep going right until we hit a NULL
      while (x != null)
      {
        y = x;
        x = x.Right;
      }
      return y;
    }

    #region Enumeration logic

    public TResult[] Select<TResult>(Func<TNode, TResult> map)
    {
      var node = First();
      var result = new TResult[Count];
      for (int i = 0; i < result.Length; i++)
      {
        result[i] = map(node);
        node = Next(node);
      }
      return result;
    }

    public IEnumerator<TNode> GetEnumerator()
    {
      for (var i = First(); i != null; i = Next(i))
        yield return i;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    /*
      class TreeScanner
      {
        TKey _min, _max;
        bool _minInclusive, _maxInclusive;
        Action<TNode> _callback;
        IComparer<TKey> _comparer;

        public TreeScanner(Action<TNode> callback, IComparer<TKey> comparer)
        {
          _callback = callback;
          _comparer = comparer;
        }

        public void ScanAll(TNode node)
        {
          while (node != null)
          {
            ScanAll(node.Left);
            _callback(node);
            node = node.Right;
          }
        }

        public void ScanMin(TNode node, TKey min, bool inclusive)
        {
          _min = min;
          _minInclusive = inclusive;
          EnumMin(node);
        }

        public void ScanMax(TNode node, TKey max, bool inclusive)
        {
          _max = max;
          _maxInclusive = inclusive;
          EnumMax(node);
        }

        public void ScanMinMax(TNode node, TKey min, bool minInclusive, TKey max, bool maxInclusive)
        {
          _min = min;
          _minInclusive = minInclusive;

          _max = max;
          _maxInclusive = maxInclusive;

          EnumMinMax(node);
        }

        void EnumMax(TNode node)
        {
          while (node != null)
          {
            var cmp = _comparer.Compare(node.Key, _max);

            if (cmp > 0)
              node = node.Left;
            else
            {
              ScanAll(node.Left);

              if (cmp < 0)
              {
                _callback(node);
                EnumMax(node.Right);
              }
              else
              {
                if (_maxInclusive)
                  _callback(node);
              }

              break;
            }
          }
        }

        void EnumMin(TNode node)
        {
          while (node != null)
          {
            var cmp = _comparer.Compare(node.Key, _min);
            if (cmp < 0)
              node = node.Right;
            else
            {
              if (cmp > 0)
              {
                EnumMin(node.Left);
                _callback(node);
              }
              else
              {
                if (_minInclusive)
                  _callback(node);
              }

              ScanAll(node.Right);

              break;
            }
          }
        }

        void EnumMinMax(TNode node)
        {
          while (node != null)
          {
            var lowCmp = _comparer.Compare(node.Key, _min);

            if (lowCmp < 0)
              node = node.Right;
            else
            {
              if (lowCmp == 0)
              {
                if (_minInclusive)
                {
                  var highCmp = _comparer.Compare(node.Key, _max);
                  if (highCmp < 0)
                  {
                    _callback(node);
                    EnumMax(node.Right);
                  }
                  else if (highCmp == 0 && _maxInclusive)
                    _callback(node);
                }
                else
                  EnumMax(node.Right);
              }
              else
              {
                var highCmp = _comparer.Compare(node.Key, _max);

                if (highCmp > 0)
                  EnumMinMax(node.Left);
                else if (highCmp < 0)
                {
                  EnumMin(node.Left);
                  _callback(node);
                  EnumMax(node.Right);
                }
                else
                {
                  EnumMin(node.Left);

                  if (_maxInclusive)
                    _callback(node);
                }
              }
              break;
            }
          }
        }
      }
   */

    #endregion

    public IEnumerable<TNode> Enum(IndexQueryArgs<TKey> args)
    {
      if (args.MinInclusive == null)
      {
        if (args.MaxInclusive == null)
          return WrapFilter(args, this);

        return WrapFilter(args, EnumMax(args.Max, args.MaxInclusive.Value));
      }

      if (args.MaxInclusive == null)
        return WrapFilter(args, EnumMin(args.Min, args.MinInclusive.Value));

      if (Comparer.Compare(args.Min, args.Max) <= 0)
        return WrapFilter(args, EnumMinMax(args.Min, args.MinInclusive.Value, args.Max, args.MaxInclusive.Value));

      return Enumerable.Empty<TNode>();
    }

    static IEnumerable<TNode> WrapFilter(IndexQueryArgs<TKey> args, IEnumerable<TNode> source)
    {
      var filter = args.Filter;
      return filter == null ? source : source.Where(i => filter(i.Key));
    }

    int Find(TKey value, out TNode result)
    {
      var node = _root;
      var last = node;
      var cmp = 0;

      while (node != null)
      {
        last = node;
        cmp = Comparer.Compare(node.Key, value);

        if (cmp > 0)
          node = node.Left;
        else if (cmp < 0)
          node = node.Right;
        else
        {
          result = node;
          return 0;
        }
      }

      result = last;
      return cmp;
    }

    bool FindMax(TKey max, bool inclusive, out TNode node)
    {
      var cmp = Find(max, out node);
      if (node == null)
        return false;

      if (cmp < 0 || cmp == 0 && inclusive)
        node = Next(node);

      return true;
    }

    bool FindMin(TKey min, bool inclusive, out TNode node)
    {
      var cmp = Find(min, out node);
      if (node == null)
        return false;

      if (cmp < 0 || cmp == 0 && !inclusive)
        node = Next(node);

      return true;
    }

    IEnumerable<TNode> EnumMin(TKey min, bool inclusive)
    {
      TNode start;
      if (FindMin(min, inclusive, out start))
        for (var i = start; i != null; i = Next(i))
          yield return i;
    }

    IEnumerable<TNode> EnumMax(TKey max, bool inclusive)
    {
      TNode stop;
      if (FindMax(max, inclusive, out stop))
        for (var i = First(); i != stop; i = Next(i))
          yield return i;
    }

    IEnumerable<TNode> EnumMinMax(TKey min, bool minInclusive, TKey max, bool maxInclusive)
    {
      TNode start, stop;
      if (FindMin(min, minInclusive, out start) && FindMax(max, maxInclusive, out stop))
        for (var i = start; i != stop; i = Next(i))
          yield return i;
    }
  }
}
