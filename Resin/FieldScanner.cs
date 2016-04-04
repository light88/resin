﻿using System.Collections.Generic;
using System.IO;
using System.Linq;
using log4net;
using Resin.IO;

namespace Resin
{
    public class FieldScanner
    {
        private readonly string _directory;

        // field/files
        private readonly IDictionary<string, IList<string>> _fieldIndex;

        // field/reader
        private readonly IDictionary<string, FieldReader> _readerCache;

        private static readonly object Sync = new object();
        private static readonly ILog Log = LogManager.GetLogger(typeof(FieldScanner));

        public FieldScanner(string directory, IDictionary<string, IList<string>> fieldIndex)
        {
            _readerCache = new Dictionary<string, FieldReader>();
            _directory = directory;
            _fieldIndex = fieldIndex;
        }

        public static FieldScanner MergeLoad(string directory)
        {
            var ixIds = Directory.GetFiles(directory, "*.ix")
                .Where(f => Path.GetExtension(f) != ".tmp")
                .Select(f => int.Parse(Path.GetFileNameWithoutExtension(f)))
                .OrderBy(i => i).ToList();

            var fieldIndex = new Dictionary<string, IList<string>>();
            foreach (var ixFileName in ixIds.Select(id => Path.Combine(directory, id + ".ix")))
            {
                var ix = IxFile.Load(ixFileName);
                var fix = FixFile.Load(ix.FixFileName);
                foreach (var field in fix.FieldIndex)
                {
                    IList<string> files;
                    if (fieldIndex.TryGetValue(field.Key, out files))
                    {
                        files.Add(field.Value);
                    }
                    else
                    {
                        fieldIndex.Add(field.Key, new List<string> { field.Value });
                    }
                }
            }
            return new FieldScanner(directory, fieldIndex);
        }

        public IEnumerable<DocumentScore> GetDocIds(Term term)
        {
            IList<string> fieldFileIds;
            if (_fieldIndex.TryGetValue(term.Field, out fieldFileIds))
            {
                var reader = GetReader(term.Field);
                if (reader != null)
                {
                    return ExactMatch(term, reader);
                }
            }
            return Enumerable.Empty<DocumentScore>();
        }

        public IList<Term> Expand(Term term)
        {
            var reader = GetReader(term.Field);
            if (term.Fuzzy)
            {
                var expanded = reader.GetSimilar(term.Token, term.Edits).Select(token => new Term { Field = term.Field, Token = token }).ToList();
                Log.DebugFormat("query-rewrite from {0} to{1}", term, string.Join(string.Empty, expanded.Select(t => t.ToString())));
                return expanded;
            }
            else if (term.Prefix)
            {
                var expanded = reader.GetTokens(term.Token).Select(token => new Term { Field = term.Field, Token = token }).ToList();
                Log.DebugFormat("query-rewrite from {0} to{1}", term, string.Join(string.Empty, expanded.Select(t => t.ToString())));
                return expanded;
            }
            return new List<Term>{term};
        }

        private IEnumerable<DocumentScore> ExactMatch(Term term, FieldReader reader)
        {
            var postings = reader.GetPostings(term.Token);
            if (postings != null)
            {
                foreach (var doc in postings)
                {
                    yield return new DocumentScore(doc.Key, doc.Value);
                }
            }
        }

        private FieldReader GetReader(string field)
        {
            FieldReader reader;
            if (!_readerCache.TryGetValue(field, out reader))
            {
                lock (Sync)
                {
                    if (!_readerCache.TryGetValue(field, out reader))
                    {
                        IList<string> files;
                        if (_fieldIndex.TryGetValue(field, out files))
                        {
                            foreach (var file in files)
                            {
                                var r = FieldReader.Load(Path.Combine(_directory, file + ".f"));
                                if (reader == null)
                                {
                                    reader = r;
                                }
                                else
                                {
                                    reader.Merge(r);
                                }
                            }
                            _readerCache.Add(field, reader);
                            return reader;
                        }
                        return null;
                    }
                }
                
            }
            return reader;
        }

        public IEnumerable<string> GetAllTokensFromTrie(string field)
        {
            var reader = GetReader(field);
            return reader == null ? Enumerable.Empty<string>() : reader.GetAllTokensFromTrie();
        }

        public IEnumerable<TokenInfo> GetAllTokens(string field)
        {
            var reader = GetReader(field);
            return reader == null ? Enumerable.Empty<TokenInfo>() : reader.GetAllTokens();
        }

        public int DocsInCorpus(string field)
        {
            var reader = GetReader(field);
            if (reader != null)
            {
                return reader.DocCount;
            }
            return 0;
        }
    }
}