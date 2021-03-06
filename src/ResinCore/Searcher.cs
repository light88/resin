using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using log4net;
using Resin.Analysis;
using Resin.Querying;
using Resin.Sys;
using StreamIndex;
using DocumentTable;

namespace Resin
{
    /// <summary>
    /// Query a index.
    /// </summary>
    public class Searcher : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(Searcher));
        private readonly string _directory;
        private readonly QueryParser _parser;
        private readonly IScoringSchemeFactory _scorerFactory;
        private readonly long _version;
        private readonly int _blockSize;
        private readonly IReadSessionFactory _sessionFactory;
        private readonly IReadSession _readSession;

        public Searcher(string directory)
            :this(directory, new QueryParser(new Analyzer()), new TfIdfFactory(), new ReadSessionFactory(directory))
        {
        }

        public Searcher(string directory, long version)
            : this(directory, version, new QueryParser(new Analyzer()), new TfIdfFactory(), new ReadSessionFactory(directory))
        {
        }

        public Searcher(string directory, QueryParser parser, IScoringSchemeFactory scorerFactory, IReadSessionFactory sessionFactory = null)
        {
            _directory = directory;
            _parser = parser;
            _scorerFactory = scorerFactory;
            _version = long.Parse(Path.GetFileNameWithoutExtension(
                Util.GetIndexFileNamesInChronologicalOrder(directory).First()));
            _blockSize = BlockSerializer.SizeOfBlock();
            _sessionFactory = sessionFactory ?? new ReadSessionFactory(directory);
            _readSession = _sessionFactory.OpenReadSession(_version);
        }

        public Searcher(string directory, long version, QueryParser parser, IScoringSchemeFactory scorerFactory, IReadSessionFactory sessionFactory = null)
        {
            _directory = directory;
            _parser = parser;
            _scorerFactory = scorerFactory;
            _version = version;
            _blockSize = BlockSerializer.SizeOfBlock();
            _sessionFactory = sessionFactory ?? new ReadSessionFactory(directory);
            _readSession = _sessionFactory.OpenReadSession(_version);
        }

        public ScoredResult Search(QueryContext query, int page = 0, int size = 10000)
        {
            if (query == null)
            {
                return new ScoredResult { Docs = new List<ScoredDocument>() };
            }

            var searchTime = Stopwatch.StartNew();
            var skip = page * size;
            var scores = Collect(query, _version);
            var paged = scores.Skip(skip).Take(size).ToList();
            var docs = new List<ScoredDocument>(size);
            var docTime = new Stopwatch();
            docTime.Start();

            GetDocs(paged, _version, docs);

            Log.DebugFormat("fetched {0} docs for query {1} in {2}", docs.Count, query, docTime.Elapsed);

            var result = new ScoredResult
            {
                Total = scores.Count,
                Docs = docs.OrderByDescending(d => d.Score).ToList()
            };

            Log.DebugFormat("searched {0} in {1}", query, searchTime.Elapsed);

            return result;
        }

        public ScoredResult Search(string query, int page = 0, int size = 10000)
        {
            var queryContext = _parser.Parse(query);

            return Search(queryContext, page, size);
        }

        private IList<DocumentScore> Collect(QueryContext query, long version)
        {
            using (var collector = new Collector(_directory, _readSession, _scorerFactory))
            {
                return collector.Collect(query);
            }
        }

        private void GetDocs(IList<DocumentScore> scores, long version, IList<ScoredDocument> result)
        {
            var documentIds = scores.Select(s => s.DocumentId).ToList();

            var docFileName = Path.Combine(_directory, version + ".dtbl");

            var dic = scores.ToDictionary(x => x.DocumentId, y => y.Score);

            foreach (var doc in _readSession.ReadDocuments(documentIds))
            {
                var score = dic[doc.Id];

                result.Add(new ScoredDocument(doc, score));
            }
        }

        public void Dispose()
        {
            _readSession.Dispose();
        }
    }
}