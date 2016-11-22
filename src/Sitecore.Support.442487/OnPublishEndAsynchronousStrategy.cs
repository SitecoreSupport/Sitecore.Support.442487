
namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
    using Sitecore.ContentSearch.Maintenance.Strategies;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using Sitecore.ContentSearch;
    using Sitecore.Diagnostics;
    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.ContentSearch.Utilities;
    using Sitecore.ContentSearch.Abstractions;
    using Sitecore.ContentSearch.Maintenance;
    using Sitecore.Eventing;
    using Sitecore.Jobs;
    using System.Threading;
    using System.Reflection;

    [DataContract]
    public class OnPublishEndAsynchronousStrategy : Sitecore.ContentSearch.Maintenance.Strategies.OnPublishEndAsynchronousStrategy, IIndexUpdateStrategy, ISearchIndexInitializable
    {
        private IContentSearchConfigurationSettings contentSearchSettings;

        private ISearchIndex index;

        private ISettings settings;

        // Controls if an additional job should be launch after an index job has been finished  
        private volatile int jobShouldBeQueued;

        // Controls execution state of the strategy   
        private volatile int strategyActiveState;

        public OnPublishEndAsynchronousStrategy(string database) : base(database)
        {
        }

        void ISearchIndexInitializable.Initialize(ISearchIndex searchIndex)
        {
            Assert.IsNotNull(searchIndex, "searchIndex");
            CrawlingLog.Log.Info(string.Format("[Index={0}] SUPPORT Initializing OnPublishEndAsynchronousStrategy.", searchIndex.Name));

            // Initialization of local and private fields of the base type: 
            var baseStrategyType = typeof(Sitecore.ContentSearch.Maintenance.Strategies.OnPublishEndAsynchronousStrategy);

            this.index = searchIndex;
            this.SetPrivateField(baseStrategyType, this, "index", this.index);

            this.settings = this.index.Locator.GetInstance<ISettings>();
            this.SetPrivateField(baseStrategyType, this, "settings", this.settings);

            this.contentSearchSettings = this.index.Locator.GetInstance<IContentSearchConfigurationSettings>();
            this.SetPrivateField(baseStrategyType, this, "contentSearchSettings", this.contentSearchSettings);


            if (!this.settings.EnableEventQueues())
            {
                CrawlingLog.Log.Fatal(string.Format("[Index={0}] Initialization of OnPublishEndAsynchronousStrategy failed because event queue is not enabled.", searchIndex.Name));
                return;
            }

            this.jobShouldBeQueued = 0;
            this.strategyActiveState = 0;

            EventHub.PublishEnd += (sender, args) => this.HandleFixed();
        }

        protected void HandleFixed()
        {
            OperationMonitor.Register(new Action(this.RunFixed));
            OperationMonitor.Trigger();
        }

        public virtual void RunFixed()
        {
            CrawlingLog.Log.Debug(string.Format("[Index={0}] OnPublishEndAsynchronousStrategy triggered.", this.index.Name), null);

            if (this.Database == null)
            {
                CrawlingLog.Log.Fatal(string.Format("[Index={0}] OperationMonitor has invalid parameters. Index Update cancelled.", this.index.Name), null);
                return;
            }

            EventQueue eventQueue = this.Database.RemoteEvents.Queue;
            if (eventQueue == null)
            {
                CrawlingLog.Log.Fatal(string.Format("[Index={0}] Event Queue is empty. Returning.", this.index.Name), null);
                return;
            }

            var isStrategyAlreadyActive = Interlocked.CompareExchange(ref this.strategyActiveState, value: 1, comparand: 0) == 1;

            try
            {
                if (isStrategyAlreadyActive)
                {
                    // The strategy is already executing: this execution is skipped and additional processing is queued:
                    this.jobShouldBeQueued = 1;
                    return;
                }

                EventManager.RaiseQueuedEvents();

                // Reset the flag since for now the current update job can still process changes of later-queued jobs:
                this.jobShouldBeQueued = 0;
                List<QueuedEvent> queue = this.ReadQueue(eventQueue);

                if (queue.Count <= 0)
                {
                    CrawlingLog.Log.Debug(string.Format("[Index={0}] Event Queue is empty. Incremental update returns", this.index.Name), null);
                    
                    // Release the active state:
                    this.strategyActiveState = 0;

                    return;
                }

                if (this.CheckForThreshold && queue.Count > this.contentSearchSettings.FullRebuildItemCountThreshold())
                {
                    CrawlingLog.Log.Warn(string.Format("[Index={0}] The number of changes exceeded maximum threshold of '{1}'.", this.index.Name, this.contentSearchSettings.FullRebuildItemCountThreshold()), null);

                    // Reset the flag since for now the current full rebuild job can still process changes of later-queued jobs:
                    this.jobShouldBeQueued = 0;
                    long? timestamp = this.GetLastTimeStamp(queue);

                    if (timestamp == null)
                    {
                        Log.Warn("SUPPORT Can't retrieve timestamp...", this);
                    }

                    var rebuildJob = IndexCustodian.FullRebuild(this.index, false);

                    this.AppendHandlerAndRun(rebuildJob, (sender, args) => this.IndexJobFinishedHandler(timestamp));
                    return;
                }

                CrawlingLog.Log.Debug(string.Format("[Index={0}] Updating '{1}' items from Event Queue.", this.index.Name, queue.Count), null);
                var updateJob = IndexCustodianEx.IncrementalUpdate(this.index, this.ExtractIndexableInfoFromQueue(queue), false);
                this.AppendHandlerAndRun(updateJob, (sender, args) => this.IndexJobFinishedHandler(null));
            }
            catch
            {
                if (!isStrategyAlreadyActive)
                {
                    this.strategyActiveState = 0;
                }

                throw;
            }
        }

        protected virtual void AppendHandlerAndRun(Job indexJob, EventHandler<JobFinishedEventArgs> handler)
        {
            indexJob.Finished += handler;
            JobManager.Start(indexJob);
        }    

        protected virtual void IndexJobFinishedHandler(long? lastProcessedTimeStamp)
        {
            try
            {
                if (lastProcessedTimeStamp != null)
                {
                    // lastProcessedTimeStamp is not null only for rebuild by threshold. 
                    // Taking into account that the line is executed by one thread since only a single job can be executed per an index: 
                    this.index.Summary.LastUpdatedTimestamp = lastProcessedTimeStamp;
                }
            }
            finally
            {
                // The jobs is complete so release the state:
                this.strategyActiveState = 0;
            }

            if (Interlocked.CompareExchange(ref this.jobShouldBeQueued,  value: 0, comparand: 1) == 1)
            {
                this.HandleFixed();
            }
        }

        protected virtual long? GetLastTimeStamp(List<QueuedEvent> queue)
        {
            return queue.Count == 0 ? null : new long?(queue.Max(ev => ev.Timestamp));
        }

        protected void SetPrivateField(Type type, object instance, string fieldName, object value)
        {
            var fi = type.GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.IsNotNull(fi, fieldName);
            fi.SetValue(instance, value);
        }
    }
}