using System.Diagnostics;
using System.Threading.Channels;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.Core.Log;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Server.Subscriptions;

internal sealed partial class WebSocketSubscriptionManager
{
    private sealed record FanOutWorkItem(AggregatedServerSubscription Group, EventArg Event,
        RemoteSubscriber[] Subscribers);

    private Channel<FanOutWorkItem> fanOutQueue = null!;
    private readonly CancellationTokenSource fanOutStop = new();
    private Task[] fanOutWorkers = [];
    private int maxPendingFanOutEvents, pendingFanOutEvents, peakPendingFanOutEvents;
    private long queueFullDrops, shutdownDiscards, reportedDrops, lastOverloadLog;
    private bool overloadActive, hasLoggedOverload;

    private void InitializeFanOut(int capacity)
    {
        if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity));
        maxPendingFanOutEvents = capacity;
        fanOutQueue = Channel.CreateBounded<FanOutWorkItem>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = maxConcurrentFanOutEvents == 1,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });
        fanOutWorkers = Enumerable.Range(0, maxConcurrentFanOutEvents)
            .Select(_ => Task.Run(ConsumeFanOutAsync)).ToArray();
    }

    // syncRoot also serializes subscription commits, shutdown and queue accounting.
    private void TryEnqueueFanOut(AggregatedServerSubscription group, EventArg eventArg)
    {
        string? log = null;
        lock (syncRoot)
        {
            if (disposed || !group.IsActive || group.Subscribers.IsEmpty) return;
            if (pendingFanOutEvents == maxPendingFanOutEvents)
            {
                queueFullDrops++;
                overloadActive = true;
                var now = Stopwatch.GetTimestamp();
                if (!hasLoggedOverload || Stopwatch.GetElapsedTime(lastOverloadLog, now) >= TimeSpan.FromSeconds(1))
                {
                    hasLoggedOverload = true;
                    lastOverloadLog = now;
                    log = ReportDrops("overload: event dropped");
                }
            }
            else
            {
                var item = new FanOutWorkItem(group, eventArg, group.Subscribers.Values.ToArray());
                if (!fanOutQueue.Writer.TryWrite(item))
                    throw new InvalidOperationException("Open fan-out queue rejected an available slot.");
                pendingFanOutEvents++;
                peakPendingFanOutEvents = Math.Max(peakPendingFanOutEvents, pendingFanOutEvents);
            }
        }
        LogFanOut(log);
    }

    private async Task ConsumeFanOutAsync()
    {
        var cancellation = fanOutStop.Token;
        try
        {
            while (await fanOutQueue.Reader.WaitToReadAsync(cancellation).ConfigureAwait(false))
            {
                FanOutWorkItem? item;
                string? recovery = null;
                lock (syncRoot)
                {
                    if (disposed) return;
                    if (!fanOutQueue.Reader.TryRead(out item)) continue;
                    pendingFanOutEvents--;
                    Interlocked.Increment(ref activeFanOutEventCount);
                    if (overloadActive && pendingFanOutEvents <= maxPendingFanOutEvents / 2)
                    {
                        overloadActive = false;
                        recovery = ReportDrops("recovered");
                    }
                }
                LogFanOut(recovery);
                try { await FanOutAsync(item, cancellation).ConfigureAwait(false); }
                catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
                catch (Exception ex) { LogFanOut($"WebSocket fan-out failed: {ex}"); }
                finally
                {
                    item = null;
                    Interlocked.Decrement(ref activeFanOutEventCount);
                }
            }
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
    }

    private async Task FanOutAsync(FanOutWorkItem item, CancellationToken cancellation)
    {
        if (!item.Group.IsActive) return;
        await Parallel.ForEachAsync(item.Subscribers,
            new ParallelOptions { MaxDegreeOfParallelism = maxFanOutConcurrency, CancellationToken = cancellation },
            async (subscriber, token) =>
            {
                bool IsValid() => !cancellation.IsCancellationRequested && item.Group.IsActive &&
                    item.Group.Subscribers.TryGetValue(new RemoteSubscriptionKey(subscriber.SessionId, subscriber.RemoteTokenId), out var current) &&
                    ReferenceEquals(current, subscriber);
                try
                {
                    if (!IsValid()) return;
                    await subscriber.Session.SendRemoteEventAsync(subscriber.RemoteTokenId, item.Event,
                        subscriber.PerformanceLevel, token, IsValid).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
                catch (Exception ex)
                {
                    LogFanOut($"WebSocket fan-out failed for subscriber {subscriber.SessionId}/{subscriber.RemoteTokenId}: {ex}");
                }
            }).ConfigureAwait(false);
    }

    private string ReportDrops(string reason)
    {
        var delta = queueFullDrops - reportedDrops;
        reportedDrops = queueFullDrops;
        return $"WebSocket fan-out {reason}: pending={pendingFanOutEvents}, active={activeFanOutEventCount}, " +
            $"capacity={maxPendingFanOutEvents}, limit={maxConcurrentFanOutEvents}, droppedTotal={queueFullDrops}, droppedSinceLastReport={delta}.";
    }

    // Called while holding syncRoot after disposed is set. No other reader can take an item.
    private void CloseFanOutQueue()
    {
        fanOutQueue.Writer.TryComplete();
        while (fanOutQueue.Reader.TryRead(out _))
        {
            pendingFanOutEvents--;
            shutdownDiscards++;
        }
    }

    private void StopFanOut()
    {
        string? summary;
        lock (syncRoot)
            summary = queueFullDrops > reportedDrops ? ReportDrops("closed") : null;
        LogFanOut(summary);
        fanOutStop.Cancel();
        var completion = Task.WhenAll(fanOutWorkers);
        // Do not block while holding subscription locks. Cleanup still runs after a timed-out wait.
        try
        {
            if (!completion.Wait(TimeSpan.FromSeconds(5)))
                LogFanOut("WebSocket fan-out shutdown timed out after 5 seconds; cleanup deferred until consumers exit.");
        }
        catch (AggregateException) { /* Observed and reported by the continuation below. */ }
        _ = completion.ContinueWith(task =>
        {
            if (task.Exception != null) LogFanOut($"WebSocket fan-out consumers failed: {task.Exception.Flatten()}");
            fanOutStop.Dispose();
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private static void LogFanOut(string? message)
    {
        if (message == null) return;
        try { WebSocketLog.Logger.Error(message); }
        catch { /* Logging must never kill a fixed consumer or interrupt cleanup. */ }
    }

    internal int PendingFanOutEventCount { get { lock (syncRoot) return pendingFanOutEvents; } }
    internal int PeakPendingFanOutEventCount { get { lock (syncRoot) return peakPendingFanOutEvents; } }
    internal long FanOutQueueFullDropCount { get { lock (syncRoot) return queueFullDrops; } }
    internal long FanOutShutdownDiscardCount { get { lock (syncRoot) return shutdownDiscards; } }
    internal int MaxPendingFanOutEvents => maxPendingFanOutEvents;
    internal int MaxConcurrentFanOutEvents => maxConcurrentFanOutEvents;
    internal Task FanOutCompletion => Task.WhenAll(fanOutWorkers);
}
