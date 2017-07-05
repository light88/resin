﻿using System.Diagnostics;
using System.IO;
using log4net;

namespace DocumentTable
{
    [DebuggerDisplay("{VersionId}")]
    public class BatchInfo
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof (BatchInfo));

        public long VersionId { get; set; }

        public int DocumentCount { get; set; }

        public Compression Compression { get; set; }

        public string PrimaryKeyFieldName { get; set; }

        public static BatchInfo Load(string fileName)
        {
            var time = new Stopwatch();
            time.Start();

            BatchInfo ix;

            using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                ix = TableSerializer.DeserializeIxInfo(fs);
            }

            Log.DebugFormat("loaded ix in {0}", time.Elapsed);

            return ix;
        }
    }
}