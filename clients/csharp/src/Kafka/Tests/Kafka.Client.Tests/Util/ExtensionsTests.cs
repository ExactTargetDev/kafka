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

namespace Kafka.Client.Tests.Util
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using Kafka.Client.Utils;
    using NUnit.Framework;
   

    /// <summary>
    /// Tests for <see cref="Extensions"/> utility class.
    /// </summary>
    [TestFixture]
    public class ExtensionsTests
    {
        private static readonly int length = 1000;
        private List<int> list = new List<int>(length);
        private List<int> shuffledList = new List<int>();

        /// <summary>
        /// Ensures collections are shuffled
        /// </summary>
        [Test]
        public void TestShuffle()
        {
            for (int i = 0; i < length; i++)
            {
                list.Add(i);
            }

            IEnumerable tmpShuffledlist = list.Shuffle();

            foreach (object o in tmpShuffledlist)
            {
                shuffledList.Add((int)o); 
            }
        }

        /// <summary>
        /// Verify collections are the same length
        /// </summary>
        [Test]
        public void TestShuffleLength()
        {
            Assert.AreEqual(list.Count<int>(), shuffledList.Count<int>());  
        }

        /// <summary>
        /// Verify all elements are present
        /// </summary>
        [Test]
        public void TestShuffleMemberShip()
        {
            Assert.AreEqual(list.Except(shuffledList).ToList().Count<int>(), 0);
        }

        /// <summary>
        /// Verify elements have been shuffled
        /// </summary>
        [Test]
        [Ignore("Ignore. Slight chance of false negative")]
        public void TestShuffleOrder()
        {
            var lastList = (IEnumerable)list;
            IEnumerable currentList;

            for (int i = 0; i < length; i++)
            {
                currentList = lastList.Shuffle();
                CollectionAssert.AreNotEqual(currentList, lastList);
                lastList = currentList;
            }
        }
    }
}
