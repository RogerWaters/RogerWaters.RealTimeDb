using System;

namespace RogerWaters.RealTimeDb
{
    public interface IReference<out T> : IDisposable  where T : class, IDisposable
    {
        T Value { get; }
    }
}