using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace RogerWaters.RealTimeDb.SqlObjects.Caching
{
    internal sealed class CacheWorker:IDisposable
    {
        public interface ISignal
        {
            void Set();
        }

        private class Handle:ISignal
        {
            private readonly ConcurrentQueue<Handle> _handles;
            private readonly AutoResetEvent _evt;
            private readonly Action _toDo;
            internal volatile bool Valid = true;
            private readonly object _lock = new object();

            public Handle(ConcurrentQueue<Handle> handles,AutoResetEvent evt, Action toDo)
            {
                _handles = handles;
                _evt = evt;
                _toDo = toDo;
            }

            public void Set()
            {
                if (Valid && _handles.Contains(this) == false)
                {
                    _handles.Enqueue(this);
                    _evt.Set();
                }
            }

            public void Execute()
            {
                if (Valid)
                {
                    lock (_lock)
                    {
                        _toDo();
                    }
                }
            }
        }

        private readonly Thread _worker;
        private readonly AutoResetEvent _refreshEvent = new AutoResetEvent(false);

        private readonly object _lock = new object();
        private readonly CancellationTokenSource _source = new CancellationTokenSource();
        private readonly WaitHandle _tokenWaitHandle;

        private readonly ConcurrentQueue<Handle> _handles = new ConcurrentQueue<Handle>();

        public CacheWorker()
        {
            _tokenWaitHandle = _source.Token.WaitHandle;

            _worker = new Thread(DoWork){IsBackground = true};
            _worker.Start();
        }

        public ISignal AddWorkerHandle(bool state,Action onSignal)
        {
            var handle = new Handle(_handles, _refreshEvent, onSignal);
            _refreshEvent.Set();
            return handle;
        }

        public void RemoveWorkerHandle(ISignal handle)
        {
            lock (_lock)
            {
                if (handle is Handle h)
                {
                    h.Valid = false;
                }
            }
        }

        private void DoWork()
        {
            WaitHandle[] handles;
            lock (_lock)
            {
                handles = new []{_refreshEvent , _tokenWaitHandle};
            }

            while (true)
            {
                int i = WaitHandle.WaitAny(handles);
                if (_source.IsCancellationRequested)
                {
                    return;
                }
                if (handles[i] == _refreshEvent)
                {
                    List<Handle> toExecute = new List<Handle>(_handles.Count);
                    while (_handles.TryDequeue(out Handle h))
                    {
                        if (toExecute.Contains(h) == false)
                        {
                            toExecute.Add(h);
                        }
                    }
                    toExecute.AsParallel().ForAll(h => h.Execute());
                }
                else if(handles[i] == _tokenWaitHandle)
                {
                    return;
                }
            }
        }

        public void Dispose()
        {
            if (_source.IsCancellationRequested == false)
            {
                _source.Cancel();
                _worker.Join();
            }
        }
    }
}
