using log4net;
using Resin.Sys;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Resin.IO
{
    public class TrieBuilder
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(TrieBuilder));
        
        private readonly IDictionary<string, BlockingCollection<WordInfo>> _queues;
        private readonly IDictionary<string, LcrsTrie> _tries;
        private readonly IList<Task> _consumers;
        private readonly Stopwatch _timer = new Stopwatch();

        public TrieBuilder()
        {
            _queues = new Dictionary<string, BlockingCollection<WordInfo>>();
            _consumers = new List<Task>();
            _tries = new Dictionary<string, LcrsTrie>();
        }

        public void Add(WordInfo word)
        {
            _timer.Start();

            BlockingCollection<WordInfo> queue;

            if (!_queues.TryGetValue(word.Field, out queue))
            {
                queue = new BlockingCollection<WordInfo>();

                _queues.Add(word.Field, queue);

                var key = word.Field.ToHash().ToString();

                InitTrie(key);

                _consumers.Add(Task.Run(() =>
                {
                    try
                    {
                        Log.InfoFormat("building in-memory tree for field {0}", word.Field);

                        var trie = _tries[key];

                        while (true)
                        {
                            var w = queue.Take();

                            trie.Add(w.Token, w.Posting);
                        }
                    }
                    catch (InvalidOperationException)
                    {
                        // Done
                    }
                }));
            }

            queue.Add(word);
        }

        private void InitTrie(string key)
        {
            if (!_tries.ContainsKey(key))
            {
                _tries[key] = new LcrsTrie();
            }
        }

        private void CompleteAdding()
        {
            foreach (var queue in _queues.Values)
            {
                queue.CompleteAdding();
            }
        }

        public IDictionary<string, LcrsTrie> GetTries()
        {
            CompleteAdding();

            Task.WaitAll(_consumers.ToArray());

            foreach (var queue in _queues.Values)
            {
                queue.Dispose();
            }

            Log.InfoFormat("Built in-memory trees in {0}", _timer.Elapsed);

            return _tries;
        }
    }
}
