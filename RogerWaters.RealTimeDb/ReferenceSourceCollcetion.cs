using System;
using System.Collections.Generic;
using System.Linq;

namespace RogerWaters.RealTimeDb
{
    public sealed class ReferenceSourceCollcetion<TKey, TValue> : IDisposable where TValue : class, IDisposable
    {
        private readonly object _lock = new object();

        private readonly Dictionary<TKey,ReferenceSource<TValue>> _sources = new Dictionary<TKey, ReferenceSource<TValue>>();

        public IReference<TValue> GetOrCreate(TKey key, Func<TKey, TValue> factory)
        {
            lock (_lock)
            {
                if (_sources.TryGetValue(key, out var source) == false)
                {
                    source = new ReferenceSource<TValue>(factory(key),s =>
                    {
                        lock (_lock)
                        {
                            _sources.Remove(key);
                        }
                    });
                    _sources.Add(key,source);
                }

                if (!source.TryCreateReference(out var reference))
                {
                    //source is disposed so we can remove it
                    _sources.Remove(key);
                    return GetOrCreate(key, factory);
                }

                return reference;
            }
        }

        public bool TryCreateReference(TKey key, out IReference<TValue> reference)
        {
            lock (_lock)
            {
                reference = null;
                return _sources.TryGetValue(key, out var source) && source.TryCreateReference(out reference);
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                foreach (var source in _sources.Values.ToList())
                {
                    source.Dispose();
                }
                _sources.Clear();
            }
        }
    }
}