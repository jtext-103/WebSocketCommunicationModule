using Jtext103.CFET2.Core.Communication;
using Jtext103.CFET2.Core.Event;
using Jtext103.CFET2.WebSocketCommunicationModule.Connections;

namespace Jtext103.CFET2.WebSocketCommunicationModule.Client
{
    internal sealed partial class ClientSubscriptionAggregator
    {
        private readonly Dictionary<ClientSubscriptionGroupKey, ClientSubscriptionGroup> clientSubscriptionGroups = new();
        private readonly Dictionary<Guid, LocalSubscriptionBinding> localSubscriptionIndex = new();
        private readonly object syncRoot = new();

        internal sealed record PreparedSubscription(RemoteEndpointKey Endpoint,
            ClientSubscriptionGroupKey[] Keys, HashSet<ClientSubscriptionGroupKey> KeySet,
            Dictionary<ClientSubscriptionGroupKey, EventFilter[]> Filters, int PerformanceLevel);

        public ClientSubscriptionPlan Subscribe(Token token, EventFilter filter)
        {
            ArgumentNullException.ThrowIfNull(token);
            return Subscribe(token, Prepare(filter));
        }

        internal PreparedSubscription Prepare(EventFilter filter)
        {
            if (filter == null)
            {
                throw new ArgumentNullException(nameof(filter));
            }

            if (string.IsNullOrWhiteSpace(filter.Host))
            {
                throw new ArgumentException("Remote event filter host is required.", nameof(filter));
            }

            var endpoint = RemoteEndpointKey.Parse(filter.Host);
            var groupKeys = CreateDistinctGroupKeys(endpoint, filter).ToArray();
            if (groupKeys.Length == 0)
            {
                throw new ArgumentException("Remote event filter must include at least one source and event type.", nameof(filter));
            }

            if (filter.PerformanceLevel is not 0 and not 1)
                throw new ArgumentOutOfRangeException(nameof(filter), $"Unsupported PerformanceLevel: {filter.PerformanceLevel}");
            var filters = new Dictionary<ClientSubscriptionGroupKey, EventFilter[]>();
            lock (syncRoot)
            {
                foreach (var key in groupKeys)
                    if (clientSubscriptionGroups.TryGetValue(key, out var group)) filters[key] = group.Filters;
            }
            // Regex construction runs outside both aggregation and module lifecycle locks.
            foreach (var key in groupKeys)
                if (!filters.ContainsKey(key)) filters[key] = [
                    CreateSingleRuleRemoteFilter(filter.Host, key, 0),
                    CreateSingleRuleRemoteFilter(filter.Host, key, 1)];
            return new(endpoint, groupKeys, groupKeys.ToHashSet(), filters, filter.PerformanceLevel);
        }

        internal ClientSubscriptionPlan Subscribe(Token token, PreparedSubscription prepared)
        {
            var endpoint = prepared.Endpoint;
            var groupKeys = prepared.Keys;
            lock (syncRoot)
            {
                var actions = new List<ClientRemoteAction>();
                var newKeySet = prepared.KeySet;
                localSubscriptionIndex.TryGetValue(token.Id, out var oldBinding);

                if (oldBinding != null)
                {
                    RemoveLocalTokenFromGroups(
                        token.Id,
                        oldBinding.GroupKeys.Where(key => !newKeySet.Contains(key)),
                        actions);
                }

                var masterTokenIds = new List<Guid>(groupKeys.Length);
                var reusedSubscriptions = new List<(RemoteEndpointKey Endpoint, Guid MasterTokenId, int Revision)>();
                foreach (var groupKey in groupKeys)
                {
                    var actionStart = actions.Count;
                    var group = GetOrCreateClientSubscriptionGroup(groupKey, prepared, actions);
                    AddOrUpdateLocalSubscription(group, token.Id, prepared.PerformanceLevel, actions);
                    masterTokenIds.Add(group.MasterTokenId);
                    if (actions.Count == actionStart)
                    {
                        reusedSubscriptions.Add((group.Endpoint, group.MasterTokenId, group.Revision));
                    }
                }

                localSubscriptionIndex[token.Id] = new LocalSubscriptionBinding
                {
                    LocalTokenId = token.Id,
                    Endpoint = endpoint,
                    GroupKeys = groupKeys,
                    MasterTokenIds = masterTokenIds.ToArray(),
                };

                return new ClientSubscriptionPlan
                {
                    Actions = actions,
                    ReusedSubscriptions = reusedSubscriptions,
                };
            }
        }

        public ClientSubscriptionPlan Unsubscribe(Token token)
        {
            if (token == null)
            {
                return new ClientSubscriptionPlan();
            }

            lock (syncRoot)
            {
                if (!localSubscriptionIndex.Remove(token.Id, out var binding))
                {
                    return new ClientSubscriptionPlan();
                }

                var actions = new List<ClientRemoteAction>();
                RemoveLocalTokenFromGroups(token.Id, binding.GroupKeys, actions);
                return new ClientSubscriptionPlan
                {
                    Actions = actions,
                };
            }
        }

        public bool HasLocalSubscriptions(RemoteEndpointKey endpoint)
        {
            lock (syncRoot)
            {
                return clientSubscriptionGroups.Values.Any(group => group.Endpoint.Equals(endpoint) && !group.LocalTokenIds.IsEmpty);
            }
        }

        internal int GroupCount
        {
            get
            {
                lock (syncRoot)
                {
                    return clientSubscriptionGroups.Count;
                }
            }
        }

        internal int LocalSubscriptionCount
        {
            get
            {
                lock (syncRoot)
                {
                    return localSubscriptionIndex.Count;
                }
            }
        }

        internal bool HasSubscriptionKind(Guid localTokenId, RemoteEndpointKey endpoint, bool resourceSubscription)
        {
            lock (syncRoot)
                return localSubscriptionIndex.TryGetValue(localTokenId, out var binding) &&
                    binding.Endpoint.Equals(endpoint) && binding.GroupKeys.Any(key => key.IsResourceSubscription == resourceSubscription);
        }

        public void Clear()
        {
            lock (syncRoot)
            {
                clientSubscriptionGroups.Clear();
                localSubscriptionIndex.Clear();
            }
        }

        private ClientSubscriptionGroup GetOrCreateClientSubscriptionGroup(
            ClientSubscriptionGroupKey groupKey,
            PreparedSubscription prepared,
            ICollection<ClientRemoteAction> actions)
        {
            if (clientSubscriptionGroups.TryGetValue(groupKey, out var existingGroup))
            {
                return existingGroup;
            }

            var masterTokenId = Guid.NewGuid();
            var filters = prepared.Filters[groupKey];
            var remoteFilter = filters[prepared.PerformanceLevel];
            var group = new ClientSubscriptionGroup
            {
                MasterTokenId = masterTokenId,
                Key = groupKey,
                RemoteFilter = remoteFilter,
                Filters = filters,
                EffectivePerformanceLevel = prepared.PerformanceLevel,
                CreatedAtUtc = DateTime.UtcNow,
            };

            clientSubscriptionGroups[groupKey] = group;
            actions.Add(new ClientRemoteAction
            {
                Kind = RemoteEndpointActionKind.Subscribe,
                Endpoint = groupKey.Endpoint,
                MasterTokenId = masterTokenId,
                Revision = group.Revision,
                RemoteFilter = remoteFilter,
            });

            return group;
        }

        private void RemoveLocalTokenFromGroups(
            Guid localTokenId,
            IEnumerable<ClientSubscriptionGroupKey> groupKeys,
            ICollection<ClientRemoteAction> actions)
        {
            foreach (var groupKey in groupKeys.ToArray())
            {
                if (!clientSubscriptionGroups.TryGetValue(groupKey, out var group))
                {
                    continue;
                }

                var oldEffectivePerformanceLevel = group.EffectivePerformanceLevel;
                if (group.LocalTokenIds.TryRemove(localTokenId, out var oldLevel) && oldLevel == 0)
                    group.FullPerformanceTokenCount--;
                if (!group.LocalTokenIds.IsEmpty)
                {
                    UpdateEffectivePerformanceLevelIfNeeded(group, oldEffectivePerformanceLevel, actions);
                    continue;
                }

                clientSubscriptionGroups.Remove(groupKey);
                actions.Add(new ClientRemoteAction
                {
                    Kind = RemoteEndpointActionKind.Unsubscribe,
                    Endpoint = group.Endpoint,
                    MasterTokenId = group.MasterTokenId,
                    Revision = group.Revision,
                });
            }
        }

        private static void AddOrUpdateLocalSubscription(
            ClientSubscriptionGroup group,
            Guid localTokenId,
            int performanceLevel,
            ICollection<ClientRemoteAction> actions)
        {
            var oldEffectivePerformanceLevel = group.EffectivePerformanceLevel;
            if (group.LocalTokenIds.TryGetValue(localTokenId, out var oldLevel) && oldLevel == 0)
                group.FullPerformanceTokenCount--;
            group.LocalTokenIds[localTokenId] = performanceLevel;
            if (performanceLevel == 0) group.FullPerformanceTokenCount++;
            UpdateEffectivePerformanceLevelIfNeeded(group, oldEffectivePerformanceLevel, actions);
        }

        private static void UpdateEffectivePerformanceLevelIfNeeded(
            ClientSubscriptionGroup group,
            int oldEffectivePerformanceLevel,
            ICollection<ClientRemoteAction> actions)
        {
            if (group.LocalTokenIds.IsEmpty)
            {
                return;
            }

            var newEffectivePerformanceLevel = group.FullPerformanceTokenCount > 0 ? 0 : 1;
            if (newEffectivePerformanceLevel == oldEffectivePerformanceLevel)
            {
                return;
            }

            group.EffectivePerformanceLevel = newEffectivePerformanceLevel;
            group.Revision++;
            group.RemoteFilter = group.Filters[newEffectivePerformanceLevel];
            actions.Add(new ClientRemoteAction
            {
                Kind = RemoteEndpointActionKind.Subscribe,
                Endpoint = group.Endpoint,
                MasterTokenId = group.MasterTokenId,
                Revision = group.Revision,
                RemoteFilter = group.RemoteFilter,
            });
        }

        private static IEnumerable<ClientSubscriptionGroupKey> CreateDistinctGroupKeys(
            RemoteEndpointKey endpoint,
            EventFilter filter)
        {
            return filter.SourcesAndTypes
                .Where(rule => !string.IsNullOrWhiteSpace(rule.Source) && !string.IsNullOrWhiteSpace(rule.EventType))
                .Select(rule => new ClientSubscriptionGroupKey(endpoint, rule.Source, rule.EventType, filter.IsResourceSubscription))
                .Distinct();
        }

        private static EventFilter CreateSingleRuleRemoteFilter(
            string host,
            ClientSubscriptionGroupKey groupKey,
            int performanceLevel)
        {
            var remoteFilter = new EventFilter
            {
                Host = host,
                IsResourceSubscription = groupKey.IsResourceSubscription,
                PerformanceLevel = performanceLevel,
            };
            remoteFilter.AddSourceAndType(groupKey.Source, groupKey.EventType);
            return remoteFilter;
        }
    }
}
