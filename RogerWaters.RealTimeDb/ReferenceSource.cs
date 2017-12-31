using System;
using System.Threading;

namespace RogerWaters.RealTimeDb
{
    public sealed partial class ReferenceSource<T>:IDisposable where T : class, IDisposable
    {
        private int _refCounter;

        public readonly T Value;
        
        private readonly LockedDisposeHelper _disposeHelper = new LockedDisposeHelper();

        public ReferenceSource(T value, Action<ReferenceSource<T>> disposed)
        {
            Value = value;
            _disposeHelper.Attach(Value);
            _disposeHelper.Attach(() => disposed(this));
            _refCounter = 0;
        }

        private void PropablyDispose()
        {
            _disposeHelper.Do(() =>
            {
                if (Interlocked.Decrement(ref _refCounter) == -1)
                {
                    Dispose();
                }
                else
                {
                    Interlocked.Increment(ref _refCounter);
                }
            });
        }

        public bool TryCreateReference(out IReference<T> reference)
        {
            IReference<T> tmp = null;
            var result = _disposeHelper.Do(() => { tmp = new Reference(this); });
            reference = tmp;
            return result;
        }

        public void Dispose()
        {
            _disposeHelper.Dispose();
        }
    }
}
