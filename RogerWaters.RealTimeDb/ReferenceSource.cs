using System;
using System.Threading;

namespace RogerWaters.RealTimeDb
{
    public sealed partial class ReferenceSource<T>:IDisposable where T : class, IDisposable
    {
        private int _refCounter;

        private readonly object _lock = new object();

        public readonly T Value;

        private readonly Action<ReferenceSource<T>> _disposed;

        private volatile bool _isDisposed = false;

        public ReferenceSource(T value, Action<ReferenceSource<T>> disposed)
        {
            Value = value;
            _disposed = disposed;
            _refCounter = 0;
        }

        private void PropablyDispose()
        {
            lock (_lock)
            {
                if (Interlocked.Decrement(ref _refCounter) == -1)
                {
                    Dispose();
                }
                else
                {
                    Interlocked.Increment(ref _refCounter);
                }
            }
        }

        public bool TryCreateReference(out IReference<T> reference)
        {
            reference = null;
            if (_isDisposed == false)
            {
                lock (_lock)
                {
                    if (_isDisposed == false)
                    {
                        reference = new Reference(this);
                        return true;
                    }
                }
            }

            return false;
        }

        public void Dispose()
        {
            if (_isDisposed)
            {
                return;
            }

            lock (_lock)
            {
                if (_isDisposed)
                {
                    return;
                }
                _isDisposed = true;
                Value.Dispose();
                _disposed(this);
            }

        }
    }
}
