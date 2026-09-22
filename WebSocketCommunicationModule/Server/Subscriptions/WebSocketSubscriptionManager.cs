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
        private int overloadWarningActive;
        private long overloadDropCount;
        private long lastOverloadLogTimestamp;
        private bool disposed;

        public WebSocketSubscriptionManager(
            Hub hub,
            int maxFanOutConcurrency,

            int? maxConcurrentFanOutEvents = null)
        {
            this.hub = hub ?? throw new ArgumentNullException(nameof(hub));
            this.maxFanOutConcurrency = Math.Max(1, maxFanOutConcurrency);

            this.maxConcurrentFanOutEvents = Math.Max(
                1,
                maxConcurrentFanOutEvents ?? Math.Max(1, Environment.ProcessorCount));
        }

        public ServerSubscribeResult Subscribe(
            WebSocketSession session,
            Guid remoteTokenId,
            int revision,
            EventFilter remoteFilter)
        {
            if (session == null)
            {
                throw new ArgumentNullException(nameof(session));
            }

            if (remoteFilter == null)
            {
                throw new ArgumentNullException(nameof(remoteFilter));
            }

            ThrowIfDisposed();

            if (remoteFilter.SourcesAndTypes.Count == 0)
            {
                throw new ArgumentException("Filter must contain at least one source/type.", nameof(remoteFilter));
            }

            if (remoteFilter.PerformanceLevel is not 0 and not 1)
            {
                return new ServerSubscribeResult(
                    revision,
                    false,
                    $"Unsupported PerformanceLevel: {remoteFilter.PerformanceLevel}");
            }

            var distinctSourceAndTypes = remoteFilter.SourcesAndTypes
                .Select(item => new ServerSubscriptionKey(item.Source, item.EventType))
                .Distinct()
                .ToArray();
            var filterSignature = CreateFilterSignature(distinctSourceAndTypes);

            var remoteKey = new RemoteSubscriptionKey(session.Id, remoteTokenId);

            lock (syncRoot)
            {
                ThrowIfDisposed();
                if (TryGetSubscriber(remoteKey, out var existingSubscriber))
                {
                    if (revision < existingSubscriber.Revision)
                    {
                        return new ServerSubscribeResult(revision, false, "staleRevision");
                    }

                    if (revision == existingSubscriber.Revision)
                    {
                        if (existingSubscriber.FilterSignature == filterSignature &&
                            existingSubscriber.PerformanceLevel == remoteFilter.PerformanceLevel)
                        {
                            return new ServerSubscribeResult(
                                revision,
                                existingSubscriber.LastAccepted,
                                existingSubscriber.LastReason);
                        }

                        return new ServerSubscribeResult(revision, false, "revisionConflict");
                    }

                    if (existingSubscriber.FilterSignature == filterSignature)
                    {
                        existingSubscriber.PerformanceLevel = remoteFilter.PerformanceLevel;
                        existingSubscriber.Revision = revision;
                        existingSubscriber.LastAccepted = true;
                        existingSubscriber.LastReason = null;
                        return new ServerSubscribeResult(revision, true, null);
                    }
                }

                var subscriber = new RemoteSubscriber(
                    session.Id,
                    remoteTokenId,
                    session,
                    filterSignature,
                    remoteFilter.PerformanceLevel,
                    revision);
                var preparedSubscriptions = new List<(ServerSubscriptionKey Key, AggregatedServerSubscription Aggregated)>();
                var createdSubscriptions = new List<(ServerSubscriptionKey Key, AggregatedServerSubscription Aggregated)>();

                try
                {
                    foreach (var key in distinctSourceAndTypes)
                    {
                        if (!aggregatedSubscriptions.TryGetValue(key, out var aggregated))
                        {
                            var localFilter = new EventFilter(
                                key.Source,
                                key.EventType,
                                0,
                                string.Empty);

                            aggregated = new AggregatedServerSubscription
                            {
                                LocalFilter = localFilter,
                            };

                            SubscribeLocal(key, aggregated);
                            WebSocketLog.Logger.Info("new : add sub to eventhub");
                            aggregatedSubscriptions[key] = aggregated;
                            createdSubscriptions.Add((key, aggregated));
                        }

                        preparedSubscriptions.Add((key, aggregated));
                    }
                }
                catch
                {
                    foreach (var created in createdSubscriptions)
                    {
                        if (created.Aggregated.Subscribers.IsEmpty)
                        {
                            aggregatedSubscriptions.TryRemove(created.Key, out _);
                            UnsubscribeLocal(created.Aggregated);
                        }
                    }

                    throw;
                }

                var previousKeys = remoteIndex.TryGetValue(remoteKey, out var existingKeys)
                    ? existingKeys
                    : Array.Empty<ServerSubscriptionKey>();
                var nextKeys = preparedSubscriptions
                    .Select(item => item.Key)
                    .Distinct()
                    .ToArray();
                var nextKeySet = nextKeys.ToHashSet();

                foreach (var prepared in preparedSubscriptions)
                {
                    prepared.Aggregated.Subscribers[remoteKey] = subscriber;
                }

                remoteIndex[remoteKey] = nextKeys;
                RemoveSubscriberFromKeys(remoteKey, previousKeys.Where(key => !nextKeySet.Contains(key)));
                return new ServerSubscribeResult(revision, true, null);
            }
        }

        public ServerSubscribeResult Subscribe(
            WebSocketSession session,
            Guid remoteTokenId,
            EventFilter remoteFilter)
        {
            return Subscribe(session, remoteTokenId, 0, remoteFilter);
        }

        public void Unsubscribe(Guid sessionId, Guid remoteTokenId)
        {
            var remoteKey = new RemoteSubscriptionKey(sessionId, remoteTokenId);

            lock (syncRoot)
            {
                UnsubscribeCore(remoteKey);
            }
        }

        public void UnsubscribeSession(Guid sessionId)
        {
            lock (syncRoot)
            {
                var remoteKeys = remoteIndex.Keys
                    .Where(key => key.SessionId == sessionId)
                    .ToArray();

                foreach (var remoteKey in remoteKeys)
                {
                    UnsubscribeCore(remoteKey);
                }
            }
        }

        public void Dispose()
        {
            lock (syncRoot)
            {
                if (disposed)
                {
                    return;
                }

                disposed = true;

                foreach (var aggregated in aggregatedSubscriptions.Values)
                {
                    UnsubscribeLocal(aggregated);
                }

                aggregatedSubscriptions.Clear();
                remoteIndex.Clear();
            }
        }

        private void UnsubscribeCore(RemoteSubscriptionKey remoteKey)
        {
            if (!remoteIndex.TryRemove(remoteKey, out var keys))
            {
                return;
            }

            RemoveSubscriberFromKeys(remoteKey, keys);
        }

        private bool TryGetSubscriber(RemoteSubscriptionKey remoteKey, out RemoteSubscriber subscriber)
        {
            subscriber = null!;
            if (!remoteIndex.TryGetValue(remoteKey, out var keys))
            {
                return false;
            }

            foreach (var key in keys)
            {
                if (aggregatedSubscriptions.TryGetValue(key, out var aggregated) &&
                    aggregated.Subscribers.TryGetValue(remoteKey, out var existingSubscriber))
                {
                    subscriber = existingSubscriber;
                    return true;
                }
            }

            return false;
        }

        private static string CreateFilterSignature(IEnumerable<ServerSubscriptionKey> keys)
        {
            return string.Join(
                "\n",
                keys.Select(key => $"{key.Source}\0{key.EventType}".ToUpperInvariant())
                    .OrderBy(value => value, StringComparer.OrdinalIgnoreCase));
        }

        private void RemoveSubscriberFromKeys(RemoteSubscriptionKey remoteKey, IEnumerable<ServerSubscriptionKey> keys)
        {
            foreach (var key in keys.Distinct())
            {
                if (!aggregatedSubscriptions.TryGetValue(key, out var aggregated))
                {
                    continue;
                }

                aggregated.Subscribers.TryRemove(remoteKey, out _);

                if (!aggregated.Subscribers.IsEmpty)
                {
                    continue;
                }

                aggregatedSubscriptions.TryRemove(key, out _);
                UnsubscribeLocal(aggregated);
            }
        }

        private async Task FanOutAsync(ServerSubscriptionKey key, EventArg eventArg)
        {
            if (!aggregatedSubscriptions.TryGetValue(key, out var aggregated))
            {
                return;
            }

            var subscribers = aggregated.Subscribers.Values.ToArray();
            await Parallel.ForEachAsync(
                subscribers,
                new ParallelOptions { MaxDegreeOfParallelism = maxFanOutConcurrency },
                async (subscriber, _) =>
                {
                    try
                    {
                        await subscriber.Session
                            .SendRemoteEventAsync(
                                subscriber.RemoteTokenId,
                                eventArg,
                                subscriber.PerformanceLevel)
                            .ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        WebSocketLog.Logger.Error(
                            $"WebSocket fan-out failed for subscriber " +
                            $"{subscriber.SessionId}/{subscriber.RemoteTokenId}: {ex}");
                    }
                }).ConfigureAwait(false);
        }

        private async Task ObserveFanOutAsync(ServerSubscriptionKey key, EventArg eventArg)
        {
            var activeCount = Interlocked.Increment(ref activeFanOutEventCount);
            if (activeCount > maxConcurrentFanOutEvents)
            {
                Interlocked.Decrement(ref activeFanOutEventCount);
                var dropped = Interlocked.Increment(ref overloadDropCount);
                if (ShouldLogOverload(dropped))
                {
                    WebSocketLog.Logger.Error(
                        $"WebSocket fan-out overload: event dropped, " +
                        $"active={activeCount - 1}, limit={maxConcurrentFanOutEvents}, " +
                        $"droppedTotal={dropped}.");
                }

                return;
            }

            try
            {
                await FanOutAsync(key, eventArg).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                WebSocketLog.Logger.Error(
                    $"WebSocket fan-out failed for {key}: {ex}");
            }
            finally
            {
                activeCount = Interlocked.Decrement(ref activeFanOutEventCount);
                if (activeCount < maxConcurrentFanOutEvents)
                {
                    Interlocked.Exchange(ref overloadWarningActive, 0);
                }
            }
        }

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

        private bool ShouldLogOverload(long dropped)
        {
            if (Interlocked.CompareExchange(ref overloadWarningActive, 1, 0) == 0)
            {
                Interlocked.Exchange(ref lastOverloadLogTimestamp, System.Diagnostics.Stopwatch.GetTimestamp());
                return true;
            }

            var now = System.Diagnostics.Stopwatch.GetTimestamp();
            var last = Interlocked.Read(ref lastOverloadLogTimestamp);
            var elapsedMs = (now - last) * 1000.0 / System.Diagnostics.Stopwatch.Frequency;
            if (elapsedMs < 1000)
            {
                return false;
            }

            return Interlocked.CompareExchange(ref lastOverloadLogTimestamp, now, last) == last;
        }

        private void SubscribeLocal(ServerSubscriptionKey key, AggregatedServerSubscription aggregated)
        {
            if (IsExactLocalResourcePath(key.Source))
            {
                var resourceSubscription = hub.SubscribeResource(
                    key.Source,
                    eventArg => _ = ObserveFanOutAsync(key, eventArg),
                    key.EventType);

                aggregated.LocalToken = resourceSubscription.Token;
                aggregated.LocalSubscription = resourceSubscription;
                return;
            }

            var token = hub.Subscribe(
                aggregated.LocalFilter,
                eventArg => _ = ObserveFanOutAsync(key, eventArg));
            aggregated.LocalToken = token;
            aggregated.LocalSubscription = token;
        }

        private bool IsExactLocalResourcePath(string source)
        {
            try
            {
                var resource = hub.FindLocalResourceWithPath(source);
                return resource != null && string.Equals(
                    resource.Path,
                    source,
                    StringComparison.OrdinalIgnoreCase);
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
            catch
            {
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
