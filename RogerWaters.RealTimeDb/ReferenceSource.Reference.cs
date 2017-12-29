using System;
using System.Threading;

namespace RogerWaters.RealTimeDb
{
    public sealed partial class ReferenceSource<T> where T : class, IDisposable
    {
        private sealed class Reference : IReference<T>
        {
            private readonly ReferenceSource<T> _source;

            private readonly object _lock = new object();

            private volatile bool _isDisposed = false;

            public T Value => _source.Value;

            public Reference(ReferenceSource<T> source)
            {
                _source = source;
                Interlocked.Increment(ref _source._refCounter);
            }

            public void Dispose()
            {
                if (_isDisposed)
                {
                    return;
                }
                lock (_lock)
                {
                    if (_isDisposed == false)
                    {
                        if (Interlocked.Decrement(ref _source._refCounter) == 0)
                        {
                            _source.PropablyDispose();
                        }
                    }
                }
            }
        }
    }
}
