#if SILVERLIGHT 
/*
 * This class is new for Silverlight CLR, for WP8 it fixes unstable original implementation. 
 */ 

using System;                // for TraceInformation ...
using System.Diagnostics;
using System.Threading;

namespace Lex.Db
{
  enum LockRecursionPolicy
  {
    NoRecursion = 0,
    SupportsRecursion = 1,
  }

  class SLLockRecursionException : Exception
  {
    public SLLockRecursionException(string message) : base(message) { }
  }

  class SLSynchronizationLockException : Exception
  {
    public SLSynchronizationLockException(string message) : base(message) { }
  }

  class RecursiveCounts
  {
    public int WriterCount;
    public int UpgradeCount;
  }

  //Ideally ReadCount should be part of recursivecount too.
  //However,to avoid an extra lookup in the common case (readers only) 
  //we maintain the readercount in the common per-thread structure.
  class ReaderWriterCount
  {
    public int ThreadId;
    public int ReaderCount;
    public ReaderWriterCount Next;
    public RecursiveCounts Rc;

    public ReaderWriterCount(bool isReentrant)
    {
      ThreadId = -1;
      if (isReentrant)
        Rc = new RecursiveCounts();
    }
  }

  /// <summary>
  /// A reader-writer lock implementation that is intended to be simple, yet very 
  /// efficient.  In particular only 1 interlocked operation is taken for any lock
  /// operation (we use spin locks to achieve this).  The spin lock is never held 
  /// for more than a few instructions (in particular, we never call event APIs 
  /// or in fact any non-trivial API while holding the spin lock).
  /// </summary> 

  class ReaderWriterLockSlim : IDisposable
  {
    //Specifying if locked can be reacquired recursively. 
    readonly bool _isReentrant;

    // Lock specifiation for myLock:  This lock protects exactly the local fields associted
    // instance of ReaderWriterLockSlim.  It does NOT protect the memory associted with the
    // the events that hang off this lock (eg writeEvent, readEvent upgradeEvent).
    int _myLock;

    //The variables controlling spinning behaviior of Mylock(which is a spin-lock) 

    const int LockSpinCycles = 20;
    const int LockSpinCount = 10;
    const int LockSleep0Count = 5;

    // These variables allow use to avoid Setting events (which is expensive) if we don't have to.
    uint _numWriteWaiters;        // maximum number of threads that can be doing a WaitOne on the writeEvent 
    uint _numReadWaiters;         // maximum number of threads that can be doing a WaitOne on the readEvent
    uint _numWriteUpgradeWaiters;      // maximum number of threads that can be doing a WaitOne on the upgradeEvent (at most 1). 
    uint _numUpgradeWaiters;

    //Variable used for quick check when there are no waiters. 
    bool _noWaiters;

    int _upgradeLockOwnerId;
    int _writeLockOwnerId;

    // conditions we wait on. 
    EventWaitHandle _writeEvent;    // threads waiting to aquire a write lock go here. 
    EventWaitHandle _readEvent;     // threads waiting to aquire a read lock go here (will be released in bulk)
    EventWaitHandle _upgradeEvent;  // thread waiting to acquire the upgrade lock 
    EventWaitHandle _waitUpgradeEvent;  // thread waiting to upgrade from the upgrade lock to a write lock go here (at most one)

    ReaderWriterCount[] _rwc;

    bool _upgradeThreadHoldingRead;

    //Per thread Hash; 
    private const int HashTableSize = 0xff;

    private const int MaxSpinCount = 20;

    //The uint, that contains info like if the writer lock is held, num of
    //readers etc. 
    uint _owners;

    //Various R/W masks 
    //The Uint is divided as follows: 
    //
    //Writer-Owned  Waiting-Writers   Waiting Upgraders     Num-REaders
    //    31          30                 29                 28.......0
    // 
    //Dividing the uint, allows to vastly simplify logic for checking if a
    //reader should go in etc. Setting the writer bit, will automatically 
    //make the value of the uint much larger than the max num of readers 
    //allowed, thus causing the check for max_readers to fail.

    private const uint WriterHeld = 0x80000000;
    private const uint WaitingWriters = 0x40000000;
    private const uint WaitingUpgrader = 0x20000000;

    //The max readers is actually one less then it's theoretical max.
    //This is done in order to prevent reader count overflows. If the reader 
    //count reaches max, other readers will wait. 
    private const uint MaxReader = 0x10000000 - 2;

    private const uint ReaderMask = 0x10000000 - 1;

    private bool _disposed;

    private void InitializeThreadCounts()
    {
      _rwc = new ReaderWriterCount[HashTableSize + 1];
      _upgradeLockOwnerId = -1;
      _writeLockOwnerId = -1;
    }

    public ReaderWriterLockSlim()
      : this(LockRecursionPolicy.NoRecursion)
    {
    }

    public ReaderWriterLockSlim(LockRecursionPolicy recursionPolicy)
    {
      if (recursionPolicy == LockRecursionPolicy.SupportsRecursion)
      {
        _isReentrant = true;
      }
      InitializeThreadCounts();
    }

    private static bool IsRWEntryEmpty(ReaderWriterCount rwc)
    {
      if (rwc.ThreadId == -1)
        return true;

      if (rwc.ReaderCount == 0 && rwc.Rc == null)
        return true;

      if (rwc.ReaderCount == 0 && rwc.Rc.WriterCount == 0 && rwc.Rc.UpgradeCount == 0)
        return true;

      return false;
    }

    private static bool IsRwHashEntryChanged(ReaderWriterCount lrwc, int id)
    {
      return lrwc.ThreadId != id;
    }

    /// <summary> 
    /// This routine retrieves/sets the per-thread counts needed to enforce the
    /// various rules related to acquiring the lock. It's a simple hash table, 
    /// where the first entry is pre-allocated for optimizing the common case.
    /// After the first element has been allocated, duplicates are kept of in
    /// linked-list. The entries are never freed, and the max size of the
    /// table would be bounded by the max number of threads that held the lock 
    /// simultaneously.
    /// 
    /// DontAllocate is set to true if the caller just wants to get an existing 
    /// entry for this thread, but doesn't want to add one if an existing one
    /// could not be found. 
    /// </summary>
    private ReaderWriterCount GetThreadRWCount(int id, bool dontAllocate)
    {
      int hash = id & HashTableSize;
      ReaderWriterCount firstfound = null;
#if DEBUG
      Debug.Assert(MyLockHeld);
#endif

      if (null == _rwc[hash])
      {
        if (dontAllocate)
          return null;
        
        _rwc[hash] = new ReaderWriterCount(_isReentrant);
      }

      if (_rwc[hash].ThreadId == id)
      {
        return _rwc[hash];
      }

      if (IsRWEntryEmpty(_rwc[hash]) && !dontAllocate)
      {
        //No more entries in chain, so no more searching required. 
        if (_rwc[hash].Next == null)
        {
          _rwc[hash].ThreadId = id;
          return _rwc[hash];
        }
        
        firstfound = _rwc[hash];
      }

      //SlowPath

      var temp = _rwc[hash].Next;

      while (temp != null)
      {
        if (temp.ThreadId == id)
        {
          return temp;
        }

        if (firstfound == null)
        {
          if (IsRWEntryEmpty(temp))
            firstfound = temp;
        }

        temp = temp.Next;
      }

      if (dontAllocate)
        return null;

      if (firstfound == null)
      {
        temp = new ReaderWriterCount(_isReentrant)
        {
          ThreadId = id,
          Next = _rwc[hash].Next
        };
        _rwc[hash].Next = temp;
        return temp;
      }
      
      firstfound.ThreadId = id;
      return firstfound;
    }

    public void EnterReadLock()
    {
      TryEnterReadLock(-1);
    }

    public bool TryEnterReadLock(TimeSpan timeout)
    {
      var ltm = (long)timeout.TotalMilliseconds;
      if (ltm < -1 || ltm > Int32.MaxValue)
        throw new ArgumentOutOfRangeException("timeout");
      var tm = (int)timeout.TotalMilliseconds;
      return TryEnterReadLock(tm);
    }

    public bool TryEnterReadLock(int millisecondsTimeout)
    {
      return TryEnterReadLockCore(millisecondsTimeout);
    }

    private bool TryEnterReadLockCore(int millisecondsTimeout)
    {

      if (millisecondsTimeout < -1)
        throw new ArgumentOutOfRangeException("millisecondsTimeout");

      if (_disposed)
        throw new ObjectDisposedException(null);

      ReaderWriterCount lrwc;
      var id = Thread.CurrentThread.ManagedThreadId;

      if (!_isReentrant)
      {

        if (id == _writeLockOwnerId)
        {
          //Check for AW->AR
          throw new SLLockRecursionException("Read After Write Not Allowed");
        }

        EnterMyLock();

        lrwc = GetThreadRWCount(id, false);

        //Check if the reader lock is already acquired. We could
        //check the presence of a reader by not allocating rwc (But that
        //would lead to two lookups in the common case. It's better to keep 
        //a count in the struucture).
        if (lrwc.ReaderCount > 0)
        {
          ExitMyLock();
          throw new SLLockRecursionException("Recursive Read Not Allowed");
        }

        if (id == _upgradeLockOwnerId)
        {
          //The upgrade lock is already held.
          //Update the global read counts and exit. 

          lrwc.ReaderCount++;
          _owners++;
          ExitMyLock();
          return true;
        }
      }
      else
      {
        EnterMyLock();
        lrwc = GetThreadRWCount(id, false);
        if (lrwc.ReaderCount > 0)
        {
          lrwc.ReaderCount++;
          ExitMyLock();
          return true;
        }
        if (id == _upgradeLockOwnerId)
        {
          //The upgrade lock is already held.
          //Update the global read counts and exit. 
          lrwc.ReaderCount++;
          _owners++;
          ExitMyLock();
          _upgradeThreadHoldingRead = true;
          return true;
        }
        if (id == _writeLockOwnerId)
        {
          //The write lock is already held. 
          //Update global read counts here,
          lrwc.ReaderCount++;
          _owners++;
          ExitMyLock();
          return true;
        }
      }

      var spincount = 0;

      for (; ; )
      {
        // We can enter a read lock if there are only read-locks have been given out 
        // and a writer is not trying to get in. 

        if (_owners < MaxReader)
        {
          // Good case, there is no contention, we are basically done
          _owners++;       // Indicate we have another reader
          lrwc.ReaderCount++;
          break;
        }

        if (spincount < MaxSpinCount)
        {
          ExitMyLock();
          if (millisecondsTimeout == 0)
            return false;
          spincount++;
          SpinWait(spincount);
          EnterMyLock();
          //The per-thread structure may have been recycled as the lock is released, load again. 
          if (IsRwHashEntryChanged(lrwc, id))
            lrwc = GetThreadRWCount(id, false);
          continue;
        }

        // Drat, we need to wait.  Mark that we have waiters and wait. 
        if (_readEvent == null)      // Create the needed event
        {
          LazyCreateEvent(ref _readEvent, false);
          if (IsRwHashEntryChanged(lrwc, id))
            lrwc = GetThreadRWCount(id, false);
          continue;   // since we left the lock, start over.
        }

        var retVal = WaitOnEvent(_readEvent, ref _numReadWaiters, millisecondsTimeout);
        if (!retVal)
          return false;

        if (IsRwHashEntryChanged(lrwc, id))
          lrwc = GetThreadRWCount(id, false);
      }

      ExitMyLock();
      return true;
    }

    public void EnterWriteLock()
    {
      TryEnterWriteLock(-1);
    }

    public bool TryEnterWriteLock(TimeSpan timeout)
    {
      var ltm = (long)timeout.TotalMilliseconds;
      if (ltm < -1 || ltm > Int32.MaxValue)
        throw new ArgumentOutOfRangeException("timeout");

      var tm = (int)timeout.TotalMilliseconds;
      return TryEnterWriteLock(tm);
    }

    public bool TryEnterWriteLock(int millisecondsTimeout)
    {
      return TryEnterWriteLockCore(millisecondsTimeout);
    }

    private bool TryEnterWriteLockCore(int millisecondsTimeout)
    {
      if (millisecondsTimeout < -1)
        throw new ArgumentOutOfRangeException("millisecondsTimeout");

      if (_disposed)
        throw new ObjectDisposedException(null);

      var id = Thread.CurrentThread.ManagedThreadId;
      ReaderWriterCount lrwc;
      var upgradingToWrite = false;

      if (!_isReentrant)
      {
        if (id == _writeLockOwnerId)
        {
          //Check for AW->AW
          throw new SLLockRecursionException("Recursive Write Not Allowed");
        }
        if (id == _upgradeLockOwnerId)
        {
          //AU->AW case is allowed once. 
          upgradingToWrite = true;
        }

        EnterMyLock();
        lrwc = GetThreadRWCount(id, true);

        //Can't acquire write lock with reader lock held.
        if (lrwc != null && lrwc.ReaderCount > 0)
        {
          ExitMyLock();
          throw new SLLockRecursionException("Write After Read Not Allowed");
        }
      }
      else
      {
        EnterMyLock();
        lrwc = GetThreadRWCount(id, false);

        if (id == _writeLockOwnerId)
        {
          lrwc.Rc.WriterCount++;
          ExitMyLock();
          return true;
        }
        if (id == _upgradeLockOwnerId)
        {
          upgradingToWrite = true;
        }
        else if (lrwc.ReaderCount > 0)
        {
          //Write locks may not be acquired if only read locks have been
          //acquired.
          ExitMyLock();
          throw new SLLockRecursionException("Write After Read Not Allowed");
        }
      }

      var spincount = 0;

      for (; ; )
      {
        if (IsWriterAcquired())
        {
          // Good case, there is no contention, we are basically done 
          SetWriterAcquired();
          break;
        }

        //Check if there is just one upgrader, and no readers.
        //Assumption: Only one thread can have the upgrade lock, so the 
        //following check will fail for all other threads that may sneak in
        //when the upgrading thread is waiting. 

        if (upgradingToWrite)
        {
          uint readercount = GetNumReaders();

          if (readercount == 1)
          {
            //Good case again, there is just one upgrader, and no readers.
            SetWriterAcquired();    // indicate we have a writer. 
            break;
          }

          if (readercount == 2)
          {
            if (lrwc != null)
            {
              if (IsRwHashEntryChanged(lrwc, id))
                lrwc = GetThreadRWCount(id, false);

              if (lrwc.ReaderCount > 0)
              {
                //This check is needed for EU->ER->EW case, as the owner count will be two. 
                Debug.Assert(_isReentrant);
                Debug.Assert(_upgradeThreadHoldingRead);

                //Good case again, there is just one upgrader, and no readers. 
                SetWriterAcquired();   // indicate we have a writer.
                break;
              }
            }
          }
        }

        if (spincount < MaxSpinCount)
        {
          ExitMyLock();
          if (millisecondsTimeout == 0)
            return false;
          spincount++;
          SpinWait(spincount);
          EnterMyLock();
          continue;
        }


        if (upgradingToWrite)
        {
          if (_waitUpgradeEvent == null)   // Create the needed event 
          {
            LazyCreateEvent(ref _waitUpgradeEvent, true);
            continue;   // since we left the lock, start over.
          }

          Debug.Assert(_numWriteUpgradeWaiters == 0, "There can be at most one thread with the upgrade lock held.");

          var retVal = WaitOnEvent(_waitUpgradeEvent, ref _numWriteUpgradeWaiters, millisecondsTimeout);

          //The lock is not held in case of failure.
          if (!retVal)
            return false;
        }
        else
        {
          // Drat, we need to wait.  Mark that we have waiters and wait.
          if (_writeEvent == null)     // create the needed event. 
          {
            LazyCreateEvent(ref _writeEvent, true);
            continue;   // since we left the lock, start over. 
          }

          var retVal = WaitOnEvent(_writeEvent, ref _numWriteWaiters, millisecondsTimeout);
          //The lock is not held in case of failure. 
          if (!retVal)
            return false;
        }
      }

      Debug.Assert((_owners & WriterHeld) > 0);

      if (_isReentrant)
      {
        if (IsRwHashEntryChanged(lrwc, id))
          lrwc = GetThreadRWCount(id, false);
        lrwc.Rc.WriterCount++;
      }

      ExitMyLock();

      _writeLockOwnerId = id;

      return true;
    }

    public void EnterUpgradeableReadLock()
    {
      TryEnterUpgradeableReadLock(-1);
    }

    public bool TryEnterUpgradeableReadLock(TimeSpan timeout)
    {
      var ltm = (long)timeout.TotalMilliseconds;
      if (ltm < -1 || ltm > Int32.MaxValue)
        throw new ArgumentOutOfRangeException("timeout");

      var tm = (int)timeout.TotalMilliseconds;
      return TryEnterUpgradeableReadLock(tm);
    }

    public bool TryEnterUpgradeableReadLock(int millisecondsTimeout)
    {
      return TryEnterUpgradeableReadLockCore(millisecondsTimeout);
    }

    private bool TryEnterUpgradeableReadLockCore(int millisecondsTimeout)
    {
      if (millisecondsTimeout < -1)
        throw new ArgumentOutOfRangeException("millisecondsTimeout");

      if (_disposed)
        throw new ObjectDisposedException(null);

      var id = Thread.CurrentThread.ManagedThreadId;
      ReaderWriterCount lrwc;

      if (!_isReentrant)
      {
        if (id == _upgradeLockOwnerId)
        {
          //Check for AU->AU 
          throw new SLLockRecursionException("Recursive Upgrade Not Allowed");
        }
        if (id == _writeLockOwnerId)
        {
          //Check for AU->AW
          throw new SLLockRecursionException("Upgrade After Write Not Allowed");
        }

        EnterMyLock();
        lrwc = GetThreadRWCount(id, true);
        //Can't acquire upgrade lock with reader lock held.
        if (lrwc != null && lrwc.ReaderCount > 0)
        {
          ExitMyLock();
          throw new SLLockRecursionException("Upgrade After Read Not Allowed");
        }
      }
      else
      {
        EnterMyLock();
        lrwc = GetThreadRWCount(id, false);

        if (id == _upgradeLockOwnerId)
        {
          lrwc.Rc.UpgradeCount++;
          ExitMyLock();
          return true;
        }

        if (id == _writeLockOwnerId)
        {
          //Write lock is already held, Just update the global state 
          //to show presence of upgrader.
          Debug.Assert((_owners & WriterHeld) > 0);
          _owners++;
          _upgradeLockOwnerId = id;
          lrwc.Rc.UpgradeCount++;
          if (lrwc.ReaderCount > 0)
            _upgradeThreadHoldingRead = true;
          ExitMyLock();
          return true;
        }

        if (lrwc.ReaderCount > 0)
        {
          //Upgrade locks may not be acquired if only read locks have been
          //acquired. 
          ExitMyLock();
          throw new SLLockRecursionException("Upgrade After Read Not Allowed");
        }
      }

      var spincount = 0;

      for (; ; )
      {
        //Once an upgrade lock is taken, it's like having a reader lock held
        //until upgrade or downgrade operations are performed. 

        if ((_upgradeLockOwnerId == -1) && (_owners < MaxReader))
        {
          _owners++;
          _upgradeLockOwnerId = id;
          break;
        }

        if (spincount < MaxSpinCount)
        {
          ExitMyLock();
          if (millisecondsTimeout == 0)
            return false;
          spincount++;
          SpinWait(spincount);
          EnterMyLock();
          continue;
        }

        // Drat, we need to wait.  Mark that we have waiters and wait. 
        if (_upgradeEvent == null)   // Create the needed event 
        {
          LazyCreateEvent(ref _upgradeEvent, true);
          continue;   // since we left the lock, start over.
        }

        //Only one thread with the upgrade lock held can proceed. 
        var retVal = WaitOnEvent(_upgradeEvent, ref _numUpgradeWaiters, millisecondsTimeout);
        if (!retVal)
          return false;
      }

      if (_isReentrant)
      {
        //The lock may have been dropped getting here, so make a quick check to see whether some other
        //thread did not grab the entry. 
        if (IsRwHashEntryChanged(lrwc, id))
          lrwc = GetThreadRWCount(id, false);
        lrwc.Rc.UpgradeCount++;
      }

      ExitMyLock();

      return true;
    }

    public void ExitReadLock()
    {
      var id = Thread.CurrentThread.ManagedThreadId;

      EnterMyLock();

      var lrwc = GetThreadRWCount(id, true);

      if (!_isReentrant)
      {
        if (lrwc == null)
        {
          //You have to be holding the read lock to make this call.
          ExitMyLock();
          throw new SLSynchronizationLockException("Mismatched Read");
        }
      }
      else
      {
        if (lrwc == null || lrwc.ReaderCount < 1)
        {
          ExitMyLock();
          throw new SLSynchronizationLockException("Mismatched Read");
        }

        if (lrwc.ReaderCount > 1)
        {
          lrwc.ReaderCount--;
          ExitMyLock();
          return;
        }

        if (id == _upgradeLockOwnerId)
        {
          _upgradeThreadHoldingRead = false;
        }
      }

      Debug.Assert(_owners > 0, "ReleasingReaderLock: releasing lock and no read lock taken");

      --_owners;

      Debug.Assert(lrwc.ReaderCount == 1);
      lrwc.ReaderCount--;

      ExitAndWakeUpAppropriateWaiters();
    }

    public void ExitWriteLock()
    {
      var id = Thread.CurrentThread.ManagedThreadId;

      if (!_isReentrant)
      {
        if (id != _writeLockOwnerId)
        {
          //You have to be holding the write lock to make this call.
          throw new SLSynchronizationLockException("Mismatched Write");
        }
        EnterMyLock();
      }
      else
      {
        EnterMyLock();
        var lrwc = GetThreadRWCount(id, false);

        if (lrwc == null)
        {
          ExitMyLock();
          throw new SLSynchronizationLockException("Mismatched Write");
        }

        var rc = lrwc.Rc;

        if (rc.WriterCount < 1)
        {
          ExitMyLock();
          throw new SLSynchronizationLockException("Mismatched Write");
        }

        rc.WriterCount--;
        if (rc.WriterCount > 0)
        {
          ExitMyLock();
          return;
        }
      }

      Debug.Assert((_owners & WriterHeld) > 0, "Calling ReleaseWriterLock when no write lock is held");

      ClearWriterAcquired();

      _writeLockOwnerId = -1;

      ExitAndWakeUpAppropriateWaiters();
    }

    public void ExitUpgradeableReadLock()
    {
      var id = Thread.CurrentThread.ManagedThreadId;

      if (!_isReentrant)
      {
        if (id != _upgradeLockOwnerId)
        {
          //You have to be holding the upgrade lock to make this call.
          throw new SLSynchronizationLockException("Mismatched Upgrade");
        }
        EnterMyLock();
      }
      else
      {
        EnterMyLock();
        var lrwc = GetThreadRWCount(id, true);

        if (lrwc == null)
        {
          ExitMyLock();
          throw new SLSynchronizationLockException("Mismatched Upgrade");
        }

        var rc = lrwc.Rc;

        if (rc.UpgradeCount < 1)
        {
          ExitMyLock();
          throw new SLSynchronizationLockException("Mismatched Upgrade");
        }

        rc.UpgradeCount--;

        if (rc.UpgradeCount > 0)
        {
          ExitMyLock();
          return;
        }

        _upgradeThreadHoldingRead = false;
      }

      _owners--;
      _upgradeLockOwnerId = -1;

      ExitAndWakeUpAppropriateWaiters();
    }

    /// <summary>
    /// A routine for lazily creating a event outside the lock (so if errors 
    /// happen they are outside the lock and that we don't do much work
    /// while holding a spin lock).  If all goes well, reenter the lock and
    /// set 'waitEvent'
    /// </summary> 
    private void LazyCreateEvent(ref EventWaitHandle waitEvent, bool makeAutoResetEvent)
    {
#if DEBUG
      Debug.Assert(MyLockHeld);
      Debug.Assert(waitEvent == null);
#endif
      ExitMyLock();
      EventWaitHandle newEvent;
      if (makeAutoResetEvent)
        newEvent = new AutoResetEvent(false);
      else
        newEvent = new ManualResetEvent(false);
      EnterMyLock();
      if (waitEvent == null)          // maybe someone snuck in. 
        waitEvent = newEvent;
      else
        newEvent.Close();
    }

    /// <summary> 
    /// Waits on 'waitEvent' with a timeout of 'millisceondsTimeout. 
    /// Before the wait 'numWaiters' is incremented and is restored before leaving this routine.
    /// </summary> 
    private bool WaitOnEvent(EventWaitHandle waitEvent, ref uint numWaiters, int millisecondsTimeout)
    {
#if DEBUG
      Debug.Assert(MyLockHeld);
#endif
      waitEvent.Reset();
      numWaiters++;
      _noWaiters = false;

      //Setting these bits will prevent new readers from getting in.
      if (_numWriteWaiters == 1)
        SetWritersWaiting();
      if (_numWriteUpgradeWaiters == 1)
        SetUpgraderWaiting();

      var waitSuccessful = false;
      ExitMyLock();      // Do the wait outside of any lock

      try
      {
        waitSuccessful = waitEvent.WaitOne(millisecondsTimeout);
      }
      finally
      {
        EnterMyLock();
        --numWaiters;

        if (_numWriteWaiters == 0 && _numWriteUpgradeWaiters == 0 && _numUpgradeWaiters == 0 && _numReadWaiters == 0)
          _noWaiters = true;

        if (_numWriteWaiters == 0)
          ClearWritersWaiting();
        if (_numWriteUpgradeWaiters == 0)
          ClearUpgraderWaiting();

        if (!waitSuccessful)        // We may also be aboutto throw for some reason.  Exit myLock. 
          ExitMyLock();
      }
      return waitSuccessful;
    }

    /// <summary> 
    /// Determines the appropriate events to set, leaves the locks, and sets the events. 
    /// </summary>
    private void ExitAndWakeUpAppropriateWaiters()
    {
#if DEBUG
      Debug.Assert(MyLockHeld);
#endif
      if (_noWaiters)
      {
        ExitMyLock();
        return;
      }

      ExitAndWakeUpAppropriateWaitersPreferringWriters();
    }

    private void ExitAndWakeUpAppropriateWaitersPreferringWriters()
    {
      var setUpgradeEvent = false;
      var setReadEvent = false;
      var readercount = GetNumReaders();

      //We need this case for EU->ER->EW case, as the read count will be 2 in
      //that scenario.
      if (_isReentrant)
      {
        if (_numWriteUpgradeWaiters > 0 && _upgradeThreadHoldingRead && readercount == 2)
        {
          ExitMyLock();          // Exit before signaling to improve efficiency (wakee will need the lock)
          _waitUpgradeEvent.Set();     // release all upgraders (however there can be at most one). 
          return;
        }
      }

      if (readercount == 1 && _numWriteUpgradeWaiters > 0)
      {
        //We have to be careful now, as we are droppping the lock. 
        //No new writes should be allowed to sneak in if an upgrade
        //was pending. 

        ExitMyLock();          // Exit before signaling to improve efficiency (wakee will need the lock)
        _waitUpgradeEvent.Set();     // release all upgraders (however there can be at most one).
      }
      else if (readercount == 0 && _numWriteWaiters > 0)
      {
        ExitMyLock();      // Exit before signaling to improve efficiency (wakee will need the lock) 
        _writeEvent.Set();   // release one writer.
      }
      else if (readercount >= 0)
      {
        if (_numReadWaiters != 0 || _numUpgradeWaiters != 0)
        {
          if (_numReadWaiters != 0)
            setReadEvent = true;

          if (_numUpgradeWaiters != 0 && _upgradeLockOwnerId == -1)
          {
            setUpgradeEvent = true;
          }

          ExitMyLock();    // Exit before signaling to improve efficiency (wakee will need the lock) 

          if (setReadEvent)
            _readEvent.Set();  // release all readers. 

          if (setUpgradeEvent)
            _upgradeEvent.Set(); //release one upgrader.
        }
        else
          ExitMyLock();
      }
      else
        ExitMyLock();
    }

    private bool IsWriterAcquired()
    {
      return (_owners & ~WaitingWriters) == 0;
    }

    private void SetWriterAcquired()
    {
      _owners |= WriterHeld;    // indicate we have a writer.
    }

    private void ClearWriterAcquired()
    {
      _owners &= ~WriterHeld;
    }

    private void SetWritersWaiting()
    {
      _owners |= WaitingWriters;
    }

    private void ClearWritersWaiting()
    {
      _owners &= ~WaitingWriters;
    }

    private void SetUpgraderWaiting()
    {
      _owners |= WaitingUpgrader;
    }

    private void ClearUpgraderWaiting()
    {
      _owners &= ~WaitingUpgrader;
    }

    private uint GetNumReaders()
    {
      return _owners & ReaderMask;
    }

    private void EnterMyLock()
    {
      if (Interlocked.CompareExchange(ref _myLock, 1, 0) != 0)
        EnterMyLockSpin();
    }

    private void EnterMyLockSpin()
    {
      var pc = Environment.ProcessorCount;
      for (var i = 0; ; i++)
      {
        if (i < LockSpinCount && pc > 1)
        {
          Thread.SpinWait(LockSpinCycles * (i + 1));    // Wait a few dozen instructions to let another processor release lock. 
        }
        else if (i < (LockSpinCount + LockSleep0Count))
        {
          Thread.Sleep(0);        // Give up my quantum. 
        }
        else
        {
          Thread.Sleep(1);        // Give up my quantum.
        }

        if (_myLock == 0 && Interlocked.CompareExchange(ref _myLock, 1, 0) == 0)
          return;
      }
    }

    private void ExitMyLock()
    {
      Debug.Assert(_myLock != 0, "Exiting spin lock that is not held");
      _myLock = 0;
    }

#if DEBUG
    private bool MyLockHeld { get { return _myLock != 0; } }
#endif

    private static void SpinWait(int spinCount)
    {
      //Exponential backoff
      if ((spinCount < 5) && (Environment.ProcessorCount > 1))
      {
        Thread.SpinWait(LockSpinCycles * spinCount);
      }
      else if (spinCount < MaxSpinCount - 3)
      {
        Thread.Sleep(0);
      }
      else
      {
        Thread.Sleep(1);
      }
    }

    public void Dispose()
    {
      Dispose(true);
    }

    private void Dispose(bool disposing)
    {
      if (disposing)
      {
        if (_disposed)
          throw new ObjectDisposedException(null);

        if (WaitingReadCount > 0 || WaitingUpgradeCount > 0 || WaitingWriteCount > 0)
          throw new SLSynchronizationLockException("Incorrect Dispose");

        if (IsReadLockHeld || IsUpgradeableReadLockHeld || IsWriteLockHeld)
          throw new SLSynchronizationLockException("Incorrect Dispose");

        if (_writeEvent != null)
        {
          _writeEvent.Close();
          _writeEvent = null;
        }

        if (_readEvent != null)
        {
          _readEvent.Close();
          _readEvent = null;
        }

        if (_upgradeEvent != null)
        {
          _upgradeEvent.Close();
          _upgradeEvent = null;
        }

        if (_waitUpgradeEvent != null)
        {
          _waitUpgradeEvent.Close();
          _waitUpgradeEvent = null;
        }

        _disposed = true;
      }
    }

    public bool IsReadLockHeld
    {
      get
      {
        return RecursiveReadCount > 0;
      }
    }

    public bool IsUpgradeableReadLockHeld
    {
      get
      {
        return RecursiveUpgradeCount > 0;
      }
    }

    public bool IsWriteLockHeld
    {
      get
      {
        return RecursiveWriteCount > 0;
      }
    }

    public LockRecursionPolicy RecursionPolicy
    {
      get
      {
        return _isReentrant ? LockRecursionPolicy.SupportsRecursion : LockRecursionPolicy.NoRecursion;
      }
    }

    public int CurrentReadCount
    {
      get
      {
        var numreaders = (int)GetNumReaders();

        if (_upgradeLockOwnerId != -1)
          return numreaders - 1;

        return numreaders;
      }
    }


    public int RecursiveReadCount
    {
      get
      {
        var id = Thread.CurrentThread.ManagedThreadId;
        var count = 0;

        EnterMyLock();
        var lrwc = GetThreadRWCount(id, true);
        if (lrwc != null)
          count = lrwc.ReaderCount;
        ExitMyLock();

        return count;
      }
    }

    public int RecursiveUpgradeCount
    {
      get
      {
        var id = Thread.CurrentThread.ManagedThreadId;

        if (_isReentrant)
        {
          var count = 0;

          EnterMyLock();
          var lrwc = GetThreadRWCount(id, true);
          if (lrwc != null)
            count = lrwc.Rc.UpgradeCount;
          ExitMyLock();

          return count;
        }

        return id == _upgradeLockOwnerId ? 1 : 0;
      }
    }

    public int RecursiveWriteCount
    {
      get
      {
        var id = Thread.CurrentThread.ManagedThreadId;
        var count = 0;

        if (_isReentrant)
        {
          EnterMyLock();
          var lrwc = GetThreadRWCount(id, true);
          if (lrwc != null)
            count = lrwc.Rc.WriterCount;
          ExitMyLock();

          return count;
        }

        return id == _writeLockOwnerId ? 1 : 0;
      }
    }

    public int WaitingReadCount { get { return (int)_numReadWaiters; } }

    public int WaitingUpgradeCount { get { return (int)_numUpgradeWaiters; } }

    public int WaitingWriteCount { get { return (int)_numWriteWaiters; } }
  }
}

#elif PORTABLE

using System;

namespace Lex.Db
{
  /// <summary>
  /// Portable stub
  /// </summary>
  class ReaderWriterLockSlim : IDisposable
  {

    #region IDisposable Members

    public void Dispose()
    {
    }

    #endregion

    public void EnterReadLock()
    {
    }

    public void ExitReadLock()
    {
    }

    public void EnterWriteLock()
    {
    }

    public void ExitWriteLock()
    {
    }
  }
}
#endif