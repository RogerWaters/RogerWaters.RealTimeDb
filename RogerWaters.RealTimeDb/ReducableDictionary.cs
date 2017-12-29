using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;

namespace RogerWaters.RealTimeDb
{
    [DebuggerDisplay("Count = {Count}, Capacity = {Capacity}")]
    [ComVisible(false)]
    [Serializable]
    [KnownType(typeof(Dictionary<,>))]
    internal sealed class ReducableDictionary<TKey, TValue> : IDictionary<TKey,TValue>, IDictionary, IReadOnlyDictionary<TKey, TValue>, ISerializable, IDeserializationCallback
    {
        private Dictionary<TKey, TValue> _store;

        private static readonly Lazy<Func<Dictionary<TKey, TValue>,int>> _getCapacityFunc = new Lazy<Func<Dictionary<TKey, TValue>,int>>(BuildCapacity);

        public int Capacity => _getCapacityFunc.Value(_store);

        private static Func<Dictionary<TKey, TValue>,int> BuildCapacity()
        {
            var fieldFreeCount = typeof(Dictionary<TKey, TValue>).GetField("freeCount",BindingFlags.NonPublic | BindingFlags.Instance);
            var fieldCount = typeof(Dictionary<TKey, TValue>).GetField("count",BindingFlags.NonPublic | BindingFlags.Instance);
            var parameter = Expression.Parameter(typeof(Dictionary<TKey,TValue>));

            var add = Expression.Add(Expression.Field(parameter,fieldCount), Expression.Field(parameter,fieldFreeCount));
            return Expression.Lambda<Func<Dictionary<TKey, TValue>, int>>(add, parameter).Compile();
        }

        /// <summary>Initializes a new instance of the <see cref="T:System.Collections.Generic.Dictionary`2" /> class that is empty, has the default initial capacity, and uses the default equality comparer for the key type.</summary>
        public ReducableDictionary(): this(0)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="T:System.Collections.Generic.Dictionary`2" /> class that is empty, has the default initial capacity, and uses the specified <see cref="T:System.Collections.Generic.IEqualityComparer`1" />.</summary>
        /// <param name="comparer">The <see cref="T:System.Collections.Generic.IEqualityComparer`1" /> implementation to use when comparing keys, or <see langword="null" /> to use the default <see cref="T:System.Collections.Generic.EqualityComparer`1" /> for the type of the key.</param>
        public ReducableDictionary(IEqualityComparer<TKey> comparer)
          : this(0, comparer)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="T:System.Collections.Generic.Dictionary`2" /> class that is empty, has the specified initial capacity, and uses the specified <see cref="T:System.Collections.Generic.IEqualityComparer`1" />.</summary>
        /// <param name="capacity">The initial number of elements that the <see cref="T:System.Collections.Generic.Dictionary`2" /> can contain.</param>
        /// <param name="comparer">The <see cref="T:System.Collections.Generic.IEqualityComparer`1" /> implementation to use when comparing keys, or <see langword="null" /> to use the default <see cref="T:System.Collections.Generic.EqualityComparer`1" /> for the type of the key.</param>
        /// <exception cref="T:System.ArgumentOutOfRangeException">
        /// <paramref name="capacity" /> is less than 0.</exception>
        public ReducableDictionary(int capacity, IEqualityComparer<TKey> comparer = null)
        {
            _store = new Dictionary<TKey, TValue>(capacity,comparer);
        }

        /// <summary>Initializes a new instance of the <see cref="T:System.Collections.Generic.Dictionary`2" /> class that contains elements copied from the specified <see cref="T:System.Collections.Generic.IDictionary`2" /> and uses the specified <see cref="T:System.Collections.Generic.IEqualityComparer`1" />.</summary>
        /// <param name="dictionary">The <see cref="T:System.Collections.Generic.IDictionary`2" /> whose elements are copied to the new <see cref="T:System.Collections.Generic.Dictionary`2" />.</param>
        /// <param name="comparer">The <see cref="T:System.Collections.Generic.IEqualityComparer`1" /> implementation to use when comparing keys, or <see langword="null" /> to use the default <see cref="T:System.Collections.Generic.EqualityComparer`1" /> for the type of the key.</param>
        /// <exception cref="T:System.ArgumentNullException">
        /// <paramref name="dictionary" /> is <see langword="null" />.</exception>
        /// <exception cref="T:System.ArgumentException">
        /// <paramref name="dictionary" /> contains one or more duplicate keys.</exception>
        public ReducableDictionary(IDictionary<TKey, TValue> dictionary, IEqualityComparer<TKey> comparer = null)
        {
          _store = new Dictionary<TKey, TValue>(dictionary, comparer);
        }

        /// <summary>Initializes a new instance of the <see cref="T:System.Collections.Generic.Dictionary`2" /> class with serialized data.</summary>
        /// <param name="info">A <see cref="T:System.Runtime.Serialization.SerializationInfo" /> object containing the information required to serialize the <see cref="T:System.Collections.Generic.Dictionary`2" />.</param>
        /// <param name="context">A <see cref="T:System.Runtime.Serialization.StreamingContext" /> structure containing the source and destination of the serialized stream associated with the <see cref="T:System.Collections.Generic.Dictionary`2" />.</param>
        private ReducableDictionary(SerializationInfo info, StreamingContext context)
        {
            _store = (Dictionary<TKey, TValue>) info.GetValue(nameof(_store), typeof(Dictionary<TKey, TValue>));
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => _store.GetEnumerator();

        public void Remove(object key)
        {
            ((IDictionary) _store).Remove(key);
            ReduceStore();
        }

        object IDictionary.this[object key]
        {
            get => ((IDictionary) _store)[key];
            set => ((IDictionary) _store)[key] = value;
        }

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable) _store).GetEnumerator();

        public void Add(KeyValuePair<TKey, TValue> item) => ((IDictionary<TKey,TValue>)_store).Add(item);

        public bool Contains(object key) => ((IDictionary) _store).Contains(key);

        void IDictionary.Add(object key, object value) => ((IDictionary) _store).Add(key, value);

        public void Clear()
        {
            _store = new Dictionary<TKey, TValue>(0);
        }

        IDictionaryEnumerator IDictionary.GetEnumerator() => ((IDictionary) _store).GetEnumerator();

        public bool Contains(KeyValuePair<TKey, TValue> item) => _store.Contains(item);

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex) => ((IDictionary<TKey,TValue>)_store).CopyTo(array, arrayIndex);

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            var result = ((IDictionary<TKey, TValue>) _store).Remove(item);
            ReduceStore();
            return result;
        }

        public void CopyTo(Array array, int index) => ((ICollection) _store).CopyTo(array, index);

        public int Count => _store.Count;

        public object SyncRoot => ((ICollection) _store).SyncRoot;

        public bool IsSynchronized => ((ICollection) _store).IsSynchronized;

        IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys => _store.Keys;

        IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values => _store.Values;

        ICollection IDictionary.Values => ((IDictionary) _store).Values;

        public bool IsReadOnly => ((IDictionary<TKey,TValue>)_store).IsReadOnly;

        public bool IsFixedSize => ((IDictionary) _store).IsFixedSize;

        public bool ContainsKey(TKey key) => _store.ContainsKey(key);

        public void Add(TKey key, TValue value) => _store.Add(key, value);

        public bool Remove(TKey key)
        {
            var remove = _store.Remove(key);
            ReduceStore();
            return remove;
        }

        public void RemoveAll(IEnumerable<TKey> keys)
        {
            foreach (var key in keys)
            {
                _store.Remove(key);
            }
            ReduceStore();
        }

        public bool TryGetValue(TKey key, out TValue value) => _store.TryGetValue(key, out value);

        public TValue this[TKey key]
        {
            get => _store[key];
            set => _store[key] = value;
        }

        public ICollection<TKey> Keys => _store.Keys;

        ICollection IDictionary.Keys => ((IDictionary) _store).Keys;

        public ICollection<TValue> Values => _store.Values;

        void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) => info.AddValue(nameof(_store), _store);

        public void OnDeserialization(object sender) { }

        private void ReduceStore()
        {
            if (Count < Capacity / 2)
            {
                _store = new Dictionary<TKey, TValue>(_store,_store.Comparer);
            }
        }
    }
}
