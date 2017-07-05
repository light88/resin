using Resin.Analysis;
using Resin.IO;

namespace Resin
{
    public class DocumentUpsertOperation
    {
        public void Write(
            Document document,
            IDocumentStoreWriter storeWriter,
            IAnalyzer analyzer,
            TrieBuilder trieBuilder)
        {
            var analyzed = analyzer.AnalyzeDocument(document);

            foreach (var term in analyzed.Words)
            {
                var field = term.Term.Field;
                var token = term.Term.Word.Value;
                var posting = term.Posting;

                trieBuilder.Add(new WordInfo(field, token, posting));
            }

            storeWriter.Write(document);
        }
    }
}