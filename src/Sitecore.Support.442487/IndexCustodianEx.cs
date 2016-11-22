
namespace Sitecore.Support.ContentSearch.Maintenance
{
    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.Maintenance;
    using Sitecore.Diagnostics;
    using Sitecore.Jobs;
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public static class IndexCustodianEx
    {
        private const string JobNamePrefix = "Index_Update";

        private static readonly Func<ISearchIndex, string, object[], bool, JobOptions> dGetJobOptions;

        static IndexCustodianEx()
        {
            var mi = typeof(IndexCustodian).GetMethod("GetJobOptions", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);
            Assert.IsNotNull(mi, "SUPPORT Can't find the 'GetJobOptions' method in IndexCustodian type...");

            dGetJobOptions = mi.CreateDelegate(typeof(Func<ISearchIndex, string, object[], bool, JobOptions>), null) as Func<ISearchIndex, string, object[], bool, JobOptions>;
        }

        public static string GetJobName(string key)
        {
            return string.IsNullOrEmpty(key) ? JobNamePrefix : string.Format("{0}_IndexName={1}", JobNamePrefix, key);
        }

        public static Job IncrementalUpdate(ISearchIndex index, IEnumerable<IndexableInfo> indexableInfo, bool start)
        {
            Assert.ArgumentNotNull(index, "index");
            Assert.ArgumentNotNull(indexableInfo, "indexableInfo");

            if (start)
            {
                return IndexCustodian.IncrementalUpdate(index, indexableInfo);
            }

            return CreateUpdateJob(index, new object[] { indexableInfo }, indexableInfo.Count<IndexableInfo>());
        }

        private static Job CreateUpdateJob(ISearchIndex index, object[] parameters, int itemsCount)
        {
            JobOptions options = dGetJobOptions(index, "Update", parameters, false);
            options.CustomData = string.Format("Count={0}", itemsCount);
            return new Job(options);
        }
    }
}