using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace RogerWaters.RealTimeDb.Linq2Sql
{
    /// <summary>
    /// Helper to extract members that are accessed from keys
    /// </summary>
    internal static class KeyMemberExtractor
    {
        /// <summary>
        /// Find all Members that are accessed in key function
        /// </summary>
        /// <typeparam name="T">Type of row object that contains the key</typeparam>
        /// <typeparam name="TKey">Type of the key that is created</typeparam>
        /// <param name="keyExtractExpression">Expression to function that explains the key from <typeparamref name="T"/></param>
        /// <returns>The collection of membernames that are used in Keys</returns>
        public static IReadOnlyCollection<string> GetMembers<T, TKey>(this Expression<Func<T, TKey>> keyExtractExpression)
        {
            var visitor = new KeyVisitor(keyExtractExpression.Parameters.First());
            visitor.Visit(keyExtractExpression.Body);
            return visitor.Members;
        }

        /// <inheritdoc />
        private sealed class KeyVisitor : ExpressionVisitor
        {
            /// <summary>
            /// Collection of membernames that are extracted
            /// </summary>
            internal readonly List<string> Members = new List<string>();

            /// <summary>
            /// The parameter that contains members
            /// </summary>
            private readonly ParameterExpression _parameter;

            /// <summary>
            /// Creates a new instance of <see cref="KeyVisitor"/>
            /// </summary>
            /// <param name="parameter">The parameter members are extracted</param>
            public KeyVisitor(ParameterExpression parameter)
            {
                _parameter = parameter;
            }

            /// <inheritdoc />
            protected override Expression VisitMember(MemberExpression node)
            {
                if (node.Expression == _parameter)
                {
                    if (Members.Contains(node.Member.Name) == false)
                    {
                        Members.Add(node.Member.Name);
                    }
                    return node;
                }

                return base.VisitMember(node);
            }
        }
    }
}
