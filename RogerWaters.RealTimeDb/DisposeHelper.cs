using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RogerWaters.RealTimeDb
{
    public sealed class LockedDisposeHelper : IDisposable
    {
        private static readonly Exception _defaultDisposeException = new ObjectDisposedException(nameof(LockedDisposeHelper), "Object is disposed and can be used anymore");
        private volatile bool _isDisposed = false;

        private readonly object _lock = new object();
        
        private readonly List<Action> _disposeActions = new List<Action>();

        public void Attach(IDisposable disposable)
        {
            if (Do(() => _disposeActions.Add(disposable.Dispose)) == false)
            {
                disposable.Dispose();
            }
        }

        public void Attach(Action disposeAction)
        {
            if (Do(() => _disposeActions.Add(disposeAction)) == false)
            {
                disposeAction();
            }
        }

        public bool Do(Action action)
        {
            if (_isDisposed)
            {
                return false;
            }

            lock (_lock)
            {
                if (_isDisposed)
                {
                    return false;
                }

                action();
                return true;
            }
        }

        public void DoOrThrow(Action action, Exception exception)
        {
            if (Do(action) == false)
            {
                throw exception;
            }
        }

        public void DoOrThrow(Action action)
        {
            DoOrThrow(action,_defaultDisposeException);
        }

        public T DoOrThrow<T>(Func<T> action)
        {
            T result = default(T);
            DoOrThrow(() => result = action(), _defaultDisposeException);
            return result;
        }

        public void Dispose()
        {
            Do(() =>
            {
                _isDisposed = true;
                _disposeActions.ForEach(a => a());
            });
        }
    }
}
