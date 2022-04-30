using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace hadoop
{
    class Program
    {
        static void Main(string[] args)
        {
            var ch = runProcess(6, @"C:\FileForCounting.txt"); // test
            Console.WriteLine(ch);
        }//

        static string runProcess(int threads, string filePath)
        {
            string res = string.Empty;
            char[] Alpha = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };
            using (StreamReader stream = new StreamReader(filePath))
            {
                #region clean

                var text = stream.ReadToEnd().ToLower();
                text = Regex.Replace(text, @"[A-Za-z0-9_\-\+]+@[A-Za-z0-9\-]+\.([A-Za-z]{2,3})(?:\.[a-z]{2})?", "");      // emails
                text = Regex.Replace(text, @"http[^\s]+", "");      // urls
                text = Regex.Replace(text, "(?:[^a-z0-9 ]|(?<=['\"])s)", " "); // special characters
                text = Regex.Replace(text, @"[\d-]", ""); //  numerics
                text = text.Trim();

                #endregion

                #region Spilt
                var words = text.StripPunctuation().Split();
                Array.Sort(words);

                words = words.Where(o => o.Length >= 1).ToArray();

                List<string[]> spiltString = new List<string[]>();

                int count = (Alpha.Length - 1) / threads;
                int rem = (Alpha.Length - 1) - (count * threads);
                int startIndex = count + rem;

                char pAlpha = ' ';
                for (int i = 0; i < threads; ++i)
                {
                    var templist = new List<string>();
                    foreach (var word in words)
                    {
                        if (word[0] > pAlpha && word[0] <= Alpha[startIndex])
                            templist.Add(word);
                    }
                    pAlpha = Alpha[startIndex];
                    spiltString.Add(templist.ToArray());
                    startIndex += count;
                }

                #endregion

                var reducers = new List<List<KeyValuePair<string, List<int>>>>();

                int verify = 0;
                foreach (var spilt in spiltString)
                {
                    Thread thread = new Thread(() =>
                    {
                        var tempMap = Mapper(spilt);
                        tempMap = tempMap.OrderBy(o => o.Key).ToList<KeyValuePair<string, int>>();  // sort by key
                        var reduce = Reducer(tempMap);
                        reducers.Add(reduce);
                        ++verify;
                        if (verify == spiltString.Count)
                        {
                            var tempCombine = new List<KeyValuePair<string, List<int>>>();

                            for (int i = 0; i < reducers.Count; ++i)
                                tempCombine.AddRange(reducers[i]);

                            tempCombine = tempCombine.OrderBy(o => o.Key).ToList<KeyValuePair<string, List<int>>>();

                            List<List<KeyValuePair<string, List<int>>>> spiltCombiner = new List<List<KeyValuePair<string, List<int>>>>();

                            count = tempCombine.Count / threads;
                            rem = tempCombine.Count - (count * threads);
                            startIndex = count + rem;
                            spiltCombiner.Add(tempCombine.Take(startIndex).ToList<KeyValuePair<string, List<int>>>());

                            var combinerL = new List<KeyValuePair<string, int>>();

                            for (int i = 1; i < threads; ++i)
                            {
                                spiltCombiner.Add(tempCombine.Skip(startIndex).Take(count).ToList<KeyValuePair<string, List<int>>>());
                                startIndex += count;
                            }

                            verify = 0;
                            foreach (var result in spiltCombiner)
                            {
                                Thread cThread = new Thread(() =>
                                {
                                    Combiner(result, combinerL);
                                    ++verify;
                                    if (verify == spiltCombiner.Count)
                                    {
                                        combinerL = combinerL.OrderBy(o => o.Key).ToList<KeyValuePair<string, int>>();

                                        foreach (var c in combinerL)
                                            res += $"{c.Key}: { c.Value}\n";
                                    }
                                });
                                cThread.Start();
                                cThread.Join();
                            }
                        }
                    });
                    thread.Start();
                    thread.Join();
                }
            }
            return res;
        }//

        /// <summary>
        /// Group all same words in the list and aggreate their count
        /// </summary>
        static List<KeyValuePair<string, List<int>>> Reducer(List<KeyValuePair<string, int>> map)
        {
            var combiner = new List<KeyValuePair<string, List<int>>>();
            var tempvals = new List<int> { 1 };

            for (int i = 0; i < map.Count; ++i)
            {
                if (i < map.Count - 1)
                {
                    if (map[i].Key == map[i + 1].Key)
                    {
                        tempvals.Add(map[i].Value);

                        if (i + 1 == map.Count - 1)
                        {
                            combiner.Add(new KeyValuePair<string, List<int>>(map[i].Key, new List<int>(tempvals)));
                            return combiner;
                        }
                    }

                    else
                    {
                        combiner.Add(new KeyValuePair<string, List<int>>(map[i].Key, new List<int>(tempvals)));
                        tempvals = new List<int> { 1 };
                    }
                }
                else
                    combiner.Add(new KeyValuePair<string, List<int>>(map[i].Key, new List<int> { map[i].Value }));
            }//

            return combiner;
        }

        /// <summary>
        /// Combines all same keyvaluepairs and add their count together 
        /// </summary>
        static void Combiner(List<KeyValuePair<string, List<int>>> slice, List<KeyValuePair<string, int>> combiner)
        {
            var keyPairList = new List<KeyValuePair<string, int>>();
            foreach (var s in slice)
                combiner.Add(new KeyValuePair<string, int>(s.Key, s.Value.Count));

        }//

        /// <summary>
        /// mapper function mapper; assign each word with a value of 1
        /// </summary>
        static List<KeyValuePair<string, int>> Mapper(string[] words)
        {
            var keyPairList = new List<KeyValuePair<string, int>>();

            foreach (var word in words)
            {
                if (!string.IsNullOrEmpty(word))
                    keyPairList.Add(new KeyValuePair<string, int>(word, 1));
            }

            return keyPairList;
        }//
    }

    /// <summary>
    /// String extension method to remove punctuations from data file
    /// </summary>
    public static class StringExtension
    {
        public static string StripPunctuation(this string s)
        {
            var sb = new StringBuilder();
            foreach (char c in s)
            {
                if (!char.IsPunctuation(c))
                    sb.Append(c);
            }
            return sb.ToString();
        }
    }

}
