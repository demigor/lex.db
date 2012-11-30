#if NETFX_CORE
using System;
using System.Threading;
using Windows.Foundation;

namespace Lex.Db
{
  public struct Awaiter : IDisposable
  {
    ManualResetEventSlim _event;

    public Awaiter(bool ctor)
    {
      _event = new ManualResetEventSlim();
    }

    public T Await<T, K>(IAsyncOperationWithProgress<T, K> op)
    {
      for (; ; )
        switch (op.Status)
        {
          case AsyncStatus.Error:
            throw op.ErrorCode;

          case AsyncStatus.Canceled:
            throw new OperationCanceledException();

          case AsyncStatus.Completed:
            return op.GetResults();

          default:
            _event.Reset();
            op.Completed = OperationCompleted2<T, K>;
            _event.Wait();
            break;
        }
    }

    public T Await<T>(IAsyncOperation<T> op)
    {
      for (; ; )
        switch (op.Status)
        {
          case AsyncStatus.Error:
            throw op.ErrorCode;

          case AsyncStatus.Canceled:
            throw new OperationCanceledException();

          case AsyncStatus.Completed:
            return op.GetResults();

          default:
            _event.Reset();
            op.Completed = OperationCompleted<T>;
            _event.Wait();
            break;
        }
    }

    public void Await(IAsyncAction op)
    {
      for (; ; )
        switch (op.Status)
        {
          case AsyncStatus.Error:
            throw op.ErrorCode;

          case AsyncStatus.Canceled:
            throw new OperationCanceledException();

          case AsyncStatus.Completed:
            return;

          default:
            _event.Reset();
            op.Completed = ActionCompleted;
            _event.Wait();
            break;
        }
    }

    void ActionCompleted(IAsyncAction asyncInfo, AsyncStatus asyncStatus)
    {
      _event.Set();
    }

    void OperationCompleted<T>(IAsyncOperation<T> asyncInfo, AsyncStatus asyncStatus)
    {
      _event.Set();
    }

    void OperationCompleted2<T, K>(IAsyncOperationWithProgress<T, K> asyncInfo, AsyncStatus asyncStatus)
    {
      _event.Set();
    }

    public void Dispose()
    {
      if (_event != null)
        _event.Dispose();
    }
  }
}
#endif