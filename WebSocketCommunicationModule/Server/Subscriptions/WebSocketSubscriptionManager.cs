using System;
using System.Collections.Concurrent;
using Jtext103.CFET2.Core;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Exception;
using Jtext103.CFET2.Core.Log;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions
{
    internal sealed partial class WebSocketSubscriptionManager : IDisposable
    {
        private readonly Hub hub;

        private readonly ConcurrentDictionary<ServerSubscriptionKey, AggregatedServerSubscription> aggregatedSubscriptions = new();
        private readonly ConcurrentDictionary<RemoteSubscriptionKey, ServerSubscriptionKey[]> remoteIndex = new();
        private readonly object syncRoot = new();
        private readonly int maxFanOutConcurrency;
        private readonly int maxConcurrentFanOutEvents;
        private int activeFanOutEventCount;
        private bool disposed;

        private sealed class Preparation
        {
            internal required ServerSubscriptionKey Key;
            internal required AggregatedServerSubscription Group;
            internal readonly HashSet<Request> Waiters = new();
            internal LinkedListNode<Preparation>? QueueNode;
            internal bool Running, Ready, Removed;
        }
        private sealed class Request
        {
            internal required RemoteSubscriptionKey Key;
            internal required RemoteSubscriber Subscriber;
            internal readonly List<Preparation> Needs = new();
            internal readonly TaskCompletionSource<ServerSubscribeResult> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            internal bool Pending;
        }
        private readonly Dictionary<ServerSubscriptionKey, Preparation> preparations = new();
        private readonly Dictionary<RemoteSubscriptionKey, Request> requests = new();
        private readonly Dictionary<Guid, HashSet<RemoteSubscriptionKey>> sessionRequests = new();
        private readonly HashSet<AggregatedServerSubscription> changedGroups = new();
        private readonly LinkedList<Preparation> queue = new();
        private readonly int maxPreparations, maxPending;
        private int runningPreparations, pendingRequests;

        public WebSocketSubscriptionManager(Hub hub, int maxFanOutConcurrency,
            int? maxConcurrentFanOutEvents = null, int maxConcurrentPreparations = 4, int maxPendingPreparations = 256, int maxPendingFanOutEvents = 256)
        {
            this.hub = hub ?? throw new ArgumentNullException(nameof(hub));
            this.maxFanOutConcurrency = Math.Max(1, maxFanOutConcurrency);
            this.maxConcurrentFanOutEvents = Math.Max(1, maxConcurrentFanOutEvents ?? Math.Max(1, Environment.ProcessorCount));
            if (maxConcurrentPreparations <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrentPreparations));
            if (maxPendingPreparations < 0) throw new ArgumentOutOfRangeException(nameof(maxPendingPreparations));
            maxPreparations = maxConcurrentPreparations;
            maxPending = maxPendingPreparations;
            InitializeFanOut(maxPendingFanOutEvents);
        }

        // Reservation is synchronous so receive order is preserved. Device reads only run in workers.
        public Task<ServerSubscribeResult> SubscribeAsync(WebSocketSession session, Guid remoteTokenId,
            int revision, EventFilter remoteFilter)
        {
            var task = ReserveSubscription(session, remoteTokenId, revision, remoteFilter, out var cleanup);
            cleanup.Dispose();
            return task;
        }

        internal Task<ServerSubscribeResult> ReserveSubscription(WebSocketSession session, Guid remoteTokenId,
            int revision, EventFilter remoteFilter, out IDisposable detached)
        {
            ArgumentNullException.ThrowIfNull(session);
            ArgumentNullException.ThrowIfNull(remoteFilter);
            var keys = remoteFilter.SourcesAndTypes.Select(r => new ServerSubscriptionKey(r.Source, r.EventType, remoteFilter.IsResourceSubscription)).Distinct().ToArray();
            var signature = CreateFilterSignature(keys);
            var keySet = keys.ToHashSet();
            var key = new RemoteSubscriptionKey(session.Id, remoteTokenId);
            var cleanup = new List<AggregatedServerSubscription>();
            detached = new DetachedGroups(this, cleanup);
            var filters = new Dictionary<ServerSubscriptionKey, EventFilter>();
            lock (syncRoot)
            {
                ThrowIfDisposed();
                var existing = ExistingRevisionResult(key, revision, signature, remoteFilter.PerformanceLevel);
                if (existing != null) return existing;
                foreach (var groupKey in keys)
                    if (preparations.TryGetValue(groupKey, out var prep)) filters[groupKey] = prep.Group.LocalFilter;
            }
            Exception? validationError = null;
            try
            {
                if (keys.Length == 0) throw new ArgumentException("Filter must contain at least one source/type.");
                if (remoteFilter.PerformanceLevel is not 0 and not 1) throw new ArgumentException("Unsupported PerformanceLevel: " + remoteFilter.PerformanceLevel);
                foreach (var groupKey in keys)
                    if (!filters.ContainsKey(groupKey))
                        filters[groupKey] = new EventFilter(groupKey.Source, groupKey.EventType, 0, string.Empty) { IsResourceSubscription = groupKey.IsResourceSubscription };
            }
            catch (Exception ex) { validationError = ex; }
            List<Preparation> starts;
            Task<ServerSubscribeResult> task;
            lock (syncRoot)
            {
                ThrowIfDisposed();
                // State may have changed while missing filters were constructed.
                var existing = ExistingRevisionResult(key, revision, signature, remoteFilter.PerformanceLevel);
                if (existing != null) return existing;
                if (requests.TryGetValue(key, out var previous))
                {
                    // Keep shared reservations until the replacement has acquired them. In particular,
                    // a performance-only update must not restart an in-flight initial read.
                    foreach (var prep in previous.Needs.Where(p => !keySet.Contains(p.Key)).ToArray())
                    {
                        previous.Needs.Remove(prep);
                        prep.Waiters.Remove(previous);
                        RemoveUnused(prep, cleanup);
                    }
                }
                var request = new Request { Key = key, Subscriber = new RemoteSubscriber(session.Id, remoteTokenId,
                    session, signature, remoteFilter.PerformanceLevel, revision) };
                requests[key] = request;
                if (!sessionRequests.TryGetValue(session.Id, out var sessionKeys))
                    sessionRequests.Add(session.Id, sessionKeys = new());
                sessionKeys.Add(key);
                task = request.Completion.Task;
                try
                {
                    // Preserve revision/idempotency results before reporting lock-free validation.
                    if (validationError != null) throw validationError;
                    var missing = keys.Count(k => !preparations.ContainsKey(k));
                    var waits = keys.Any(k => !preparations.TryGetValue(k, out var p) || !p.Ready);
                    if (waits && (pendingRequests - (previous?.Pending == true ? 1 : 0) >= (long)maxPending + maxPreparations ||
                        missing > (long)maxPending + Math.Max(0, maxPreparations - runningPreparations) - queue.Count))
                        throw new InvalidOperationException("subscriptionPreparationCapacityExceeded");
                    request.Pending = true;
                    pendingRequests++;
                    foreach (var groupKey in keys)
                    {
                        if (!preparations.TryGetValue(groupKey, out var prep))
                        {
                            prep = new Preparation { Key = groupKey, Group = new AggregatedServerSubscription { LocalFilter = filters[groupKey] } };
                            preparations.Add(groupKey, prep);
                            prep.QueueNode = queue.AddLast(prep);
                        }
                        request.Needs.Add(prep);
                        prep.Waiters.Add(request);
                    }
                    TryCommit(request, cleanup);
                }
                catch (Exception ex) { Fail(request, ex.Message, cleanup); }
                if (previous != null) CancelRequest(previous, cleanup);
                RefreshSnapshots();
                starts = ReserveQueued();
            }
            StartPreparations(starts);
            return task;
        }

        // Called under syncRoot both before validation and before committing reservations.
        private Task<ServerSubscribeResult>? ExistingRevisionResult(RemoteSubscriptionKey key, int revision,
            string signature, int performanceLevel)
        {
            if (!requests.TryGetValue(key, out var previous) || revision > previous.Subscriber.Revision) return null;
            if (revision < previous.Subscriber.Revision)
                return Task.FromResult(new ServerSubscribeResult(revision, false, "staleRevision"));
            return previous.Subscriber.FilterSignature == signature && previous.Subscriber.PerformanceLevel == performanceLevel
                ? previous.Completion.Task : Task.FromResult(new ServerSubscribeResult(revision, false, "revisionConflict"));
        }

        public ServerSubscribeResult Subscribe(WebSocketSession session, Guid token, int revision, EventFilter filter) =>
            SubscribeAsync(session, token, revision, filter).GetAwaiter().GetResult();
        public ServerSubscribeResult Subscribe(WebSocketSession session, Guid token, EventFilter filter) => Subscribe(session, token, 0, filter);

        private List<Preparation> ReserveQueued()
        {
            var starts = new List<Preparation>();
            while (!disposed && runningPreparations < maxPreparations && queue.First != null)
            {
                var prep = queue.First.Value;
                queue.RemoveFirst();
                prep.QueueNode = null;
                prep.Running = true;
                runningPreparations++;
                starts.Add(prep);
            }
            return starts;
        }
        private void StartPreparations(List<Preparation> starts)
        {
            foreach (var prep in starts)
            {
                try
                {
                    // Reserve capacity under syncRoot; start blocking workers outside it.
                    _ = Task.Factory.StartNew(() => Prepare(prep), CancellationToken.None,
                        TaskCreationOptions.LongRunning, TaskScheduler.Default);
                }
                catch (Exception ex) { CompletePreparation(prep, ex); }
            }
        }
        private void Prepare(Preparation prep)
        {
            Exception? error = null;
            lock (syncRoot)
            {
                if (prep.Removed || disposed) error = new OperationCanceledException();
            }
            try { if (error == null) SubscribeLocal(prep.Key, prep.Group); }
            catch (Exception ex) { error = ex; }
            CompletePreparation(prep, error);
        }
        private void CompletePreparation(Preparation prep, Exception? error)
        {
            var cleanup = new List<AggregatedServerSubscription>();
            lock (syncRoot)
            {
                prep.Running = false;
                if (prep.Removed || disposed) cleanup.Add(prep.Group);
                else if (error != null)
                {
                    foreach (var request in prep.Waiters.ToArray()) Fail(request, error.Message, cleanup);
                    RemoveUnused(prep, cleanup);
                }
                else
                {
                    prep.Ready = true;
                    foreach (var request in prep.Waiters.ToArray()) TryCommit(request, cleanup);
                    RemoveUnused(prep, cleanup);
                }
                RefreshSnapshots();
            }
            try { DisposeGroups(cleanup); }
            finally
            {
                List<Preparation> starts;
                lock (syncRoot) { runningPreparations--; starts = ReserveQueued(); }
                StartPreparations(starts);
            }
        }
        private void TryCommit(Request request, List<AggregatedServerSubscription> cleanup)
        {
            if (!request.Pending || request.Needs.Any(p => !p.Ready)) return;
            var oldKeys = remoteIndex.TryGetValue(request.Key, out var old) ? old : Array.Empty<ServerSubscriptionKey>();
            var skipped = request.Needs.SelectMany(p => p.Group.SkippedResources).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
            var next = request.Needs.Where(p => !p.Group.IsSkipped).Select(p => p.Key).ToArray();
            foreach (var prep in request.Needs.Where(p => !p.Group.IsSkipped))
            {
                prep.Group.Subscribers[request.Key] = request.Subscriber;
                changedGroups.Add(prep.Group);
                aggregatedSubscriptions[prep.Key] = prep.Group;
            }
            if (next.Length != 0) remoteIndex[request.Key] = next;
            else remoteIndex.TryRemove(request.Key, out _);
            foreach (var oldKey in oldKeys.Except(next)) RemoveSubscriber(oldKey, request.Key, cleanup);
            ReleaseNeeds(request, cleanup);
            request.Completion.TrySetResult(new ServerSubscribeResult(request.Subscriber.Revision, true, null, skipped, next.Length == 0));
        }
        private void ReleaseNeeds(Request request, List<AggregatedServerSubscription> cleanup)
        {
            if (request.Pending) { request.Pending = false; pendingRequests--; }
            foreach (var prep in request.Needs) { prep.Waiters.Remove(request); RemoveUnused(prep, cleanup); }
            request.Needs.Clear();
        }
        private void Fail(Request request, string reason, List<AggregatedServerSubscription> cleanup)
        {
            ReleaseNeeds(request, cleanup);
            request.Completion.TrySetResult(new ServerSubscribeResult(request.Subscriber.Revision, false, reason));
        }
        private void CancelRequest(Request request, List<AggregatedServerSubscription> cleanup)
        {
            ReleaseNeeds(request, cleanup);
            request.Completion.TrySetCanceled();
        }
        private void RemoveUnused(Preparation prep, List<AggregatedServerSubscription> cleanup)
        {
            if (prep.Removed || prep.Waiters.Count != 0 || !prep.Group.Subscribers.IsEmpty) return;
            prep.Removed = true;
            prep.Group.IsActive = false;
            preparations.Remove(prep.Key);
            aggregatedSubscriptions.TryRemove(prep.Key, out _);
            if (prep.QueueNode != null) { queue.Remove(prep.QueueNode); prep.QueueNode = null; }
            if (!prep.Running) cleanup.Add(prep.Group);
        }
        private void RemoveSubscriber(ServerSubscriptionKey key, RemoteSubscriptionKey remote, List<AggregatedServerSubscription> cleanup)
        {
            if (!preparations.TryGetValue(key, out var prep)) return;
            if (prep.Group.Subscribers.TryRemove(remote, out _)) changedGroups.Add(prep.Group);
            RemoveUnused(prep, cleanup);
        }
        private void RefreshSnapshots()
        {
            foreach (var group in changedGroups) group.RefreshSubscriberSnapshot();
            changedGroups.Clear();
        }
        private void UnsubscribeCore(RemoteSubscriptionKey key, List<AggregatedServerSubscription> cleanup)
        {
            if (requests.Remove(key, out var request)) CancelRequest(request, cleanup);
            if (sessionRequests.TryGetValue(key.SessionId, out var sessionKeys))
            {
                sessionKeys.Remove(key);
                if (sessionKeys.Count == 0) sessionRequests.Remove(key.SessionId);
            }
            if (remoteIndex.TryRemove(key, out var keys))
                foreach (var groupKey in keys) RemoveSubscriber(groupKey, key, cleanup);
        }
        public void Unsubscribe(Guid sessionId, Guid remoteTokenId)
        {
            using var cleanup = DetachSubscription(sessionId, remoteTokenId);
        }
        internal IDisposable DetachSubscription(Guid sessionId, Guid remoteTokenId)
        {
            var cleanup = new List<AggregatedServerSubscription>();
            List<Preparation> starts;
            lock (syncRoot)
            {
                UnsubscribeCore(new RemoteSubscriptionKey(sessionId, remoteTokenId), cleanup);
                RefreshSnapshots();
                starts = ReserveQueued();
            }
            StartPreparations(starts);
            return new DetachedGroups(this, cleanup);
        }
        public void UnsubscribeSession(Guid sessionId)
        {
            using var cleanup = DetachSession(sessionId);
        }
        internal IDisposable DetachSession(Guid sessionId)
        {
            var cleanup = new List<AggregatedServerSubscription>();
            List<Preparation> starts;
            lock (syncRoot)
            {
                if (sessionRequests.TryGetValue(sessionId, out var sessionKeys))
                    foreach (var key in sessionKeys.ToArray()) UnsubscribeCore(key, cleanup);
                RefreshSnapshots();
                starts = ReserveQueued();
            }
            StartPreparations(starts);
            return new DetachedGroups(this, cleanup);
        }
        private sealed class DetachedGroups(WebSocketSubscriptionManager owner, List<AggregatedServerSubscription> groups) : IDisposable
        {
            private List<AggregatedServerSubscription>? pending = groups;
            public void Dispose()
            {
                var cleanup = Interlocked.Exchange(ref pending, null);
                if (cleanup != null) owner.DisposeGroups(cleanup);
            }
        }
        public void Dispose()
        {
            var cleanup = new List<AggregatedServerSubscription>();
            lock (syncRoot)
            {
                if (disposed) return;
                disposed = true;
                CloseFanOutQueue();
                foreach (var key in requests.Keys.ToArray()) UnsubscribeCore(key, cleanup);
                RefreshSnapshots();
            }
            StopFanOut();
            DisposeGroups(cleanup);
        }
        internal bool IsCurrent(Guid sessionId, Guid tokenId, Task<ServerSubscribeResult> task)
        {
            lock (syncRoot) return !disposed && requests.TryGetValue(new RemoteSubscriptionKey(sessionId, tokenId), out var request)
                && ReferenceEquals(request.Completion.Task, task);
        }
        internal int RunningPreparationCount { get { lock (syncRoot) return runningPreparations; } }
        internal int QueuedPreparationCount { get { lock (syncRoot) return queue.Count; } }
        internal int PendingRequestCount { get { lock (syncRoot) return pendingRequests; } }
        private static string CreateFilterSignature(IEnumerable<ServerSubscriptionKey> keys) => string.Concat(
            keys.Select(k => Connections.SubscriptionExpression.Encode(k.Source) + Connections.SubscriptionExpression.Encode(k.EventType) + (k.IsResourceSubscription ? "R" : "E"))
                .OrderBy(s => s, StringComparer.Ordinal).Select(s => s.Length + ":" + s));
        private void DisposeGroups(IEnumerable<AggregatedServerSubscription> groups)
        { foreach (var group in groups.Distinct()) UnsubscribeLocal(group); }

        internal int RemoteSubscriptionCount
        {
            get
            {
                lock (syncRoot)
                {
                    return remoteIndex.Count;
                }
            }
        }

        internal int AggregatedSubscriptionCount
        {
            get
            {
                lock (syncRoot)
                {
                    return aggregatedSubscriptions.Count;
                }
            }
        }

        internal int ActiveFanOutEventCount => Math.Max(0, Volatile.Read(ref activeFanOutEventCount));

        private void SubscribeLocal(ServerSubscriptionKey key, AggregatedServerSubscription aggregated)
        {
            if (!key.IsResourceSubscription && (!IsExactLocalResourcePath(key.Source) || hub.FindLocalResourceWithPath(key.Source).NoEvent))
            {
                var ordinary = hub.SubscribeEventsWithObservation(aggregated.LocalFilter,
                    eventArg => TryEnqueueFanOut(aggregated, eventArg));
                aggregated.LocalToken = ordinary.Token;
                aggregated.LocalSubscription = ordinary;
                return;
            }
            if (IsExactLocalResourcePath(key.Source))
            {
                var resourceSubscription = hub.SubscribeResource(
                    key.Source,
                    eventArg => TryEnqueueFanOut(aggregated, eventArg),
                    key.EventType);

                aggregated.IsSkipped = resourceSubscription.Status == ResourceSubscriptionStatus.Skipped;
                if (aggregated.IsSkipped) aggregated.SkippedResources = new[] { key.Source };
                aggregated.LocalToken = aggregated.IsSkipped ? null : resourceSubscription.Token;
                aggregated.LocalSubscription = resourceSubscription;
                return;
            }

            var subscription = hub.SubscribeResources(
                aggregated.LocalFilter,
                eventArg => TryEnqueueFanOut(aggregated, eventArg));
            aggregated.IsSkipped = subscription.Status == ResourceSubscriptionStatus.Skipped;
            aggregated.SkippedResources = subscription.ObservationResults
                .Where(r => r.ReasonCode == ResourceObservationReason.NoEvent).Select(r => r.ResourcePath).ToArray();
            aggregated.LocalToken = aggregated.IsSkipped ? null : subscription.Token;
            aggregated.LocalSubscription = subscription;
        }

        private bool IsExactLocalResourcePath(string source)
        {
            try
            {
                var resource = hub.FindLocalResourceWithPath(source);
                return resource != null && string.Equals(
                    resource.Path,
                    source,
                    StringComparison.OrdinalIgnoreCase) && !resource.Info.HasGetterParameters;
            }
            catch (ResourceDoesNotExistException)
            {
                return false;
            }
        }

        private void UnsubscribeLocal(AggregatedServerSubscription aggregated)
        {
            if (aggregated.LocalSubscription == null)
            {
                return;
            }

            try
            {
                aggregated.LocalSubscription.Dispose();
            }
            catch (Exception ex)
            {
                try { WebSocketLog.Logger.Error($"Local subscription cleanup failed: {ex}"); }
                catch { /* A failing log provider must not prevent the remaining groups from releasing. */ }
            }
        }

        private void ThrowIfDisposed()
        {
            if (disposed)
            {
                throw new ObjectDisposedException(nameof(WebSocketSubscriptionManager));
            }
        }
    }
}
