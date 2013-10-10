/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Kafka.Client.Utils
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text;

    internal static class Extensions
    {
        public static string ToMultiString<T>(this IEnumerable<T> items, string separator)
        {
            if (items.Count() == 0)
            {
                return "NULL";
            }

            return String.Join(separator, items);
        }

        public static string ToMultiString<T>(this IEnumerable<T> items, Expression<Func<T, object>> selector, string separator)
        {
            if (items.Count() == 0)
            {
                return "NULL";
            }

            Func<T, object> compiled = selector.Compile();
            return String.Join(separator, items.Select(compiled));
        }

        public static IEnumerable Shuffle(this IEnumerable enumerable)
        {
            List<object> tmpList = new List<object>();
            var r = new Random();
            int j = 0;
            object tmp;

            foreach (object o in enumerable)
            {
                tmpList.Add(o);
            }

            for (int i = tmpList.Count(); i > 1; i--)
            {
                // Pick random element to swap.
                j = r.Next(i); // 0 <= j <= i-1
                // Swap.
                tmp = tmpList[j];
                tmpList[j] = tmpList[i - 1];
                tmpList[i - 1] = tmp;
            }

            return (IEnumerable)tmpList;

      
        }
    }
}
